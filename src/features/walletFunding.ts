// ============================================================
// BUYSUB — Fund the wallet with Paystack (migration 10)
// ============================================================
// POST /v2/me/wallet/fund           { amount_ngn, callback_url } → Paystack URL
// GET  /v2/me/wallet/fund/verify    ?reference=  (the return page calls this)
// and the webhook hands top-up charges to settleTopupTx.
//
// The transaction's metadata carries kind 'wallet_topup' and the topup id.
// settle_wallet_topup credits the wallet once, under a row lock, only when
// Paystack took at least the requested amount in NGN.

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, ok, err, requireAuth, logEvent } from '../http';
import { cfg, getFlags, naira, notifyUser } from './core';
import { paystackInitialize, paystackVerifyTx, safeCallbackUrl } from './paystack';
import { serviceBlocked } from './status';

export const TOPUP_KIND = 'wallet_topup';

async function walletFor(db: SupabaseClient, userId: string) {
  const { data } = await db.from('wallets').select('id, is_active, balance_ngn').eq('user_id', userId).maybeSingle();
  if (data) return data;
  const { data: created } = await db.from('wallets').insert({ user_id: userId, balance_ngn: 0 }).select('id, is_active, balance_ngn').single();
  return created;
}

export async function handleFundWallet(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const blocked = await serviceBlocked(db, 'wallet_funding', request, env);
  if (blocked) return blocked;

  const flags = await getFlags(db);
  const min = Number(cfg(flags, 'wallet_funding', 'min_ngn', 1000));
  const max = Number(cfg(flags, 'wallet_funding', 'max_ngn', 500000));
  const body = await request.json().catch(() => ({})) as any;
  const amount = Math.round(Number(body?.amount_ngn));
  if (!Number.isFinite(amount) || amount < min || amount > max) {
    return err(`Enter an amount between ${naira(min)} and ${naira(max)}`, 400, request, env);
  }
  if (!auth.email) return err('Your account has no email address', 400, request, env);

  const wallet = await walletFor(db, auth.userId);
  if (!wallet) return err('Could not open your wallet', 500, request, env);
  if (wallet.is_active === false) return err('Your wallet is frozen. Contact support.', 403, request, env);

  const { data: topup, error } = await db.from('wallet_topups')
    .insert({ user_id: auth.userId, wallet_id: wallet.id, amount_ngn: amount })
    .select('id').single();
  if (error || !topup) return err('Wallet top-ups aren’t available yet', 503, request, env);

  const reference = `WT-${topup.id.slice(0, 8)}-${Date.now()}`;
  const init = await paystackInitialize(env, {
    email: auth.email,
    amountNGN: amount,
    reference,
    callbackUrl: safeCallbackUrl(body?.callback_url, env, '/account/wallet'),
    metadata: { kind: TOPUP_KIND, topup_id: topup.id, user_id: auth.userId },
  });
  if (!init.ok) {
    await db.from('wallet_topups').update({ status: 'failed' }).eq('id', topup.id);
    return err('Payment initialization failed: ' + init.error, 502, request, env);
  }
  await db.from('wallet_topups').update({ paystack_ref: reference }).eq('id', topup.id);
  return ok({ authorization_url: init.authorization_url, reference }, request, env);
}

/** Credits a successful top-up transaction. Safe to call more than once. */
export async function settleTopupTx(db: SupabaseClient, tx: any): Promise<'ok' | 'already' | 'mismatch' | 'missing' | 'ignored'> {
  if (tx?.status !== 'success') return 'ignored';
  if (tx.currency !== 'NGN') return 'mismatch';
  let topupId: string | undefined = tx?.metadata?.topup_id;
  if (!topupId) {
    const { data } = await db.from('wallet_topups').select('id').eq('paystack_ref', tx.reference).maybeSingle();
    topupId = data?.id;
  }
  if (!topupId) return 'missing';

  const { data: result, error } = await db.rpc('settle_wallet_topup', { p_topup_id: topupId, p_paid_kobo: Number(tx.amount) });
  if (error) throw new Error('settle_wallet_topup: ' + error.message);

  const { data: t } = await db.from('wallet_topups').select('user_id, amount_ngn').eq('id', topupId).maybeSingle();
  if (result === 'ok' && t) {
    await logEvent(db, 'wallet', topupId, 'topup_paid', t.user_id, { reference: tx.reference, amount_ngn: t.amount_ngn });
    await notifyUser(db, t.user_id, {
      kind: 'wallet', title: `${naira(t.amount_ngn)} added to your wallet`,
      body: 'Your top-up was successful.', href: '/account/wallet', dedupe: `topup:${topupId}`,
    });
  }
  if (result === 'mismatch' && t) {
    await logEvent(db, 'wallet', topupId, 'topup_amount_mismatch', t.user_id, { reference: tx.reference, paid_kobo: tx.amount });
  }
  return result as any;
}

export async function handleVerifyWalletFunding(db: SupabaseClient, url: URL, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const reference = url.searchParams.get('reference') || '';
  if (!reference) return err('Reference is required', 400, request, env);

  const tx = await paystackVerifyTx(reference, env);
  if (!tx || tx.status !== 'success') return err('Payment not completed', 400, request, env);
  if (tx?.metadata?.kind !== TOPUP_KIND || tx?.metadata?.user_id !== auth.userId) {
    return err('This payment isn’t a top-up on your account', 404, request, env);
  }
  let result: string;
  try { result = await settleTopupTx(db, tx); }
  catch (e: any) { return err('Payment received but your wallet couldn’t be updated yet. It will be credited shortly.', 500, request, env); }
  if (result === 'mismatch') return err('The amount paid doesn’t match the top-up. Contact support with your reference.', 409, request, env);
  if (result === 'missing') return err('Top-up not found. Contact support with your reference.', 404, request, env);

  const { data: wallet } = await db.from('wallets').select('balance_ngn').eq('user_id', auth.userId).maybeSingle();
  return ok({ credited: true, amount_ngn: Number(tx.amount) / 100, balance_ngn: wallet?.balance_ngn ?? null }, request, env);
}

