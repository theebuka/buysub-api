// ============================================================
// BUYSUB — Partner payout requests and tiers (migration 15)
// ============================================================
// GET  /v2/partners/me/payouts       balance (available / on hold), open request, history
// POST /v2/partners/me/payouts       request everything available (request_partner_payout)
// GET  /v2/admin/payouts             ?status=&page=
// POST /v2/admin/payouts/:id/settle  { action: 'paid' | 'rejected', note?, reference? }
//
// Tiers: feature_flags.partner_tiers lists tiers by lifetime referred sales
// (paid orders, after discount). When on, a new commission uses the higher of
// the partner's own rate and their tier's rate (commissionRate).

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, ok, err, requireAuth, requireAdmin, logEvent } from '../http';
import { cfg, emailHtml, frontend, getFlags, isOn, naira, notifyUser, sendEmail, type Flags } from './core';

type Tier = { name: string; min_sales_ngn: number; rate: number };

async function affiliateFor(db: SupabaseClient, userId: string) {
  const { data } = await db.from('affiliates')
    .select('id, user_id, status, commission_rate, bank_name, account_name, account_number')
    .eq('user_id', userId).maybeSingle();
  return data;
}

async function lifetimeSales(db: SupabaseClient, affiliateId: string, excludeOrderId?: string): Promise<number> {
  let q = db.from('orders').select('subtotal_ngn, discount_ngn').eq('affiliate_id', affiliateId).eq('status', 'paid').limit(20000);
  if (excludeOrderId) q = q.neq('id', excludeOrderId);
  const { data } = await q;
  return (data || []).reduce((s, o: any) => s + Math.max(0, Number(o.subtotal_ngn) - Number(o.discount_ngn || 0)), 0);
}

function tiersOf(flags: Flags): Tier[] {
  const t = cfg<any[]>(flags, 'partner_tiers', 'tiers', []);
  return (Array.isArray(t) ? t : []).map(x => ({ name: String(x.name), min_sales_ngn: Number(x.min_sales_ngn) || 0, rate: Number(x.rate) || 0 }))
    .sort((a, b) => a.min_sales_ngn - b.min_sales_ngn);
}

export async function tierInfo(db: SupabaseClient, affiliateId: string, baseRate: number) {
  const flags = await getFlags(db);
  if (!isOn(flags, 'partner_tiers', false)) return null;
  const tiers = tiersOf(flags);
  if (!tiers.length) return null;
  const sales = await lifetimeSales(db, affiliateId);
  let i = -1;
  tiers.forEach((t, k) => { if (sales >= t.min_sales_ngn) i = k; });
  const current = i >= 0 ? tiers[i] : null;
  const next = tiers[i + 1] ?? null;
  return {
    sales_ngn: sales,
    current: current ? { ...current } : null,
    next: next ? { ...next, remaining_ngn: Math.max(0, next.min_sales_ngn - sales) } : null,
    effective_rate: Math.max(baseRate, current?.rate ?? 0),
    tiers,
  };
}

/** Rate (percent) for a new commission on `orderId`. Sales before this order decide the tier. */
export async function commissionRate(db: SupabaseClient, affiliateId: string, baseRate: number, orderId: string): Promise<number> {
  try {
    const flags = await getFlags(db);
    if (!isOn(flags, 'partner_tiers', false)) return baseRate;
    const tiers = tiersOf(flags);
    if (!tiers.length) return baseRate;
    const sales = await lifetimeSales(db, affiliateId, orderId);
    const reached = tiers.filter(t => sales >= t.min_sales_ngn).pop();
    return Math.max(baseRate, reached?.rate ?? 0);
  } catch {
    return baseRate;
  }
}

function payoutTerms(flags: Flags) {
  return {
    enabled: isOn(flags, 'partner_payouts', false),
    min_ngn: Number(cfg(flags, 'partner_payouts', 'min_ngn', 5000)),
    hold_days: Number(cfg(flags, 'partner_payouts', 'hold_days', 7)),
  };
}

export async function handleMyPayouts(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const aff = await affiliateFor(db, auth.userId);
  if (!aff) return err('No partner account', 404, request, env);
  const terms = payoutTerms(await getFlags(db));

  const cutoff = Date.now() - terms.hold_days * 86_400_000;
  const [comms, history] = await Promise.all([
    db.from('affiliate_commissions')
      .select('amount_ngn, created_at, status, payout_id, orders!inner(status)')
      .eq('affiliate_id', aff.id).is('payout_id', null).in('status', ['pending', 'approved'])
      .eq('orders.status', 'paid').limit(10000),
    db.from('payout_requests')
      .select('id, amount_ngn, status, created_at, processed_at, admin_note, reference')
      .eq('affiliate_id', aff.id).order('created_at', { ascending: false }).limit(50),
  ]);
  if (history.error) return ok({ ...terms, enabled: false, available_ngn: 0, on_hold_ngn: 0, open: null, history: [] }, request, env);

  let available = 0, held = 0;
  for (const c of comms.data || []) {
    const amt = Number((c as any).amount_ngn) || 0;
    if (new Date((c as any).created_at).getTime() < cutoff) available += amt; else held += amt;
  }
  const rows = history.data || [];
  return ok({
    ...terms,
    available_ngn: Math.round(available * 100) / 100,
    on_hold_ngn: Math.round(held * 100) / 100,
    open: rows.find(r => r.status === 'pending') ?? null,
    history: rows,
  }, request, env);
}

const RPC_ERRORS: Record<string, string> = {
  below_minimum: 'You don’t have enough available to request a payout yet.',
  already_open: 'You already have a payout request in progress.',
  not_approved: 'Your partner account isn’t active.',
  not_found: 'No partner account',
};

export async function handleRequestPayout(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const aff = await affiliateFor(db, auth.userId);
  if (!aff) return err('No partner account', 404, request, env);
  const terms = payoutTerms(await getFlags(db));
  if (!terms.enabled) return err('Payout requests are switched off right now', 503, request, env);

  // Bank details live on the partner application (editable in /partner/profile).
  const { data: app } = await db.from('partner_applications')
    .select('payout_method, bank_name, account_name, account_number, crypto_token, crypto_chain, wallet_address')
    .eq('user_id', auth.userId).maybeSingle();
  const details = {
    payout_method: app?.payout_method || 'Bank Transfer',
    bank_name: app?.bank_name || aff.bank_name, account_name: app?.account_name || aff.account_name,
    account_number: app?.account_number || aff.account_number,
    crypto_token: app?.crypto_token || null, crypto_chain: app?.crypto_chain || null, wallet_address: app?.wallet_address || null,
  };
  const hasBank = details.bank_name && details.account_number;
  const hasCrypto = details.payout_method === 'Crypto' && details.wallet_address;
  if (!hasBank && !hasCrypto) return err('Add your payout details in your profile first', 400, request, env);

  const { data, error } = await db.rpc('request_partner_payout', {
    p_affiliate_id: aff.id, p_hold_days: terms.hold_days, p_min_ngn: terms.min_ngn,
  });
  if (error) {
    const key = Object.keys(RPC_ERRORS).find(k => error.message.includes(k));
    return err(key ? RPC_ERRORS[key] : error.message, key ? 409 : 500, request, env);
  }
  const row: any = Array.isArray(data) ? data[0] : data;
  await db.from('payout_requests').update({ payout_details: details }).eq('id', row.id);
  await logEvent(db, 'payout', row.id, 'requested', auth.userId, { amount_ngn: row.amount_ngn });
  return ok({ ...row, payout_details: details }, request, env);
}

export async function handleAdminPayouts(db: SupabaseClient, url: URL, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1') || 1);
  const limit = Math.min(100, Math.max(1, parseInt(url.searchParams.get('limit') || '25') || 25));
  const status = url.searchParams.get('status');
  let q = db.from('payout_requests')
    .select('id, amount_ngn, status, payout_details, admin_note, reference, created_at, processed_at, affiliate_id, affiliates(store_name, business_name, referral_code)', { count: 'exact' })
    .order('created_at', { ascending: false })
    .range((page - 1) * limit, page * limit - 1);
  if (status && ['pending', 'paid', 'rejected'].includes(status)) q = q.eq('status', status);
  const { data, error, count } = await q;
  if (error) return err(error.message, 500, request, env);
  return ok(data || [], request, env, { pagination: { page, limit, total: count ?? 0, pages: Math.ceil((count || 0) / limit) } });
}

export async function handleAdminSettlePayout(db: SupabaseClient, id: string, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
  const body = await request.json().catch(() => ({})) as any;
  const action = body?.action;
  if (action !== 'paid' && action !== 'rejected') return err('action must be paid or rejected', 400, request, env);
  const note = String(body?.note || '').trim().slice(0, 500) || null;
  const reference = String(body?.reference || '').trim().slice(0, 120) || null;
  if (action === 'rejected' && !note) return err('Give a reason so the partner knows what to fix', 400, request, env);

  const { data: result, error } = await db.rpc('settle_partner_payout', {
    p_payout_id: id, p_action: action, p_admin: auth.userId, p_note: note, p_reference: reference,
  });
  if (error) return err(error.message, 500, request, env);
  if (result === 'missing') return err('Payout not found', 404, request, env);
  if (result === 'already') return err('This payout was already processed', 409, request, env);

  const { data: p } = await db.from('payout_requests').select('amount_ngn, affiliates(user_id)').eq('id', id).maybeSingle();
  const userId = (p as any)?.affiliates?.user_id ?? null;
  await logEvent(db, 'payout', id, `payout_${action}`, auth.userId, { note, reference });
  if (p) {
    const paid = action === 'paid';
    await notifyUser(db, userId, {
      kind: 'payout',
      title: paid ? `Payout of ${naira(p.amount_ngn)} sent` : 'Payout request declined',
      body: paid ? (reference ? `Reference ${reference}.` : 'It should reach your account shortly.') : note,
      href: '/partner/payouts', dedupe: `payout:${id}`,
    });
    if (userId) {
      const { data: prof } = await db.from('profiles').select('email').eq('id', userId).maybeSingle();
      if (prof?.email) {
        await sendEmail(env, prof.email, paid ? `Your BuySub payout of ${naira(p.amount_ngn)} is on its way` : 'Your BuySub payout request', emailHtml({
          heading: paid ? `${naira(p.amount_ngn)} is on its way` : 'We couldn’t process your payout',
          paragraphs: paid
            ? [`We’ve sent your partner payout of ${naira(p.amount_ngn)}.`, ...(reference ? [`Transfer reference: ${reference}`] : [])]
            : [`Your payout request of ${naira(p.amount_ngn)} was declined.`, `Reason: ${note}`, 'The commissions are available to request again once this is sorted.'],
          cta: { label: 'View payouts', href: `${frontend(env)}/partner/payouts` },
        }));
      }
    }
  }
  return ok({ id, status: action }, request, env);
}
