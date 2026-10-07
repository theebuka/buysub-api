// ============================================================
// BUYSUB — Customer refer-and-earn (migration 12)
// ============================================================
// GET /v2/me/referrals   the caller's code, link, reward terms and history
//
// The link is the partner link format (/shop?ref=CODE). At checkout
// prepareOrder tries the affiliate code first, then resolveCustomerCode, and
// stores the referrer on orders.referrer_user_id. When the order is paid,
// rewardReferral credits the referrer (and the friend, if configured) for a
// new customer's first paid order that meets the minimum. Off until enabled in
// admin Settings (feature_flags.customer_referrals).

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, ok, err, requireAuth, escapeLike, logEvent } from '../http';
import { cfg, frontend, getFlags, isOn, naira, notifyUser, userIdForOrder } from './core';

const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function newCode(): string {
  const b = crypto.getRandomValues(new Uint8Array(6));
  return 'BS' + Array.from(b, x => ALPHABET[x % ALPHABET.length]).join('');
}

async function codeFor(db: SupabaseClient, userId: string): Promise<string | null> {
  const { data } = await db.from('customer_referral_codes').select('code').eq('user_id', userId).maybeSingle();
  if (data?.code) return data.code;
  for (let i = 0; i < 6; i++) {
    const code = newCode();
    const { data: aff } = await db.from('affiliates').select('id').eq('referral_code', code).limit(1);
    if (aff?.length) continue;
    const { error } = await db.from('customer_referral_codes').insert({ user_id: userId, code });
    if (!error) return code;
    if (error.code === '23505') {
      // Another request created this user's code first, or the code is taken.
      const { data: again } = await db.from('customer_referral_codes').select('code').eq('user_id', userId).maybeSingle();
      if (again?.code) return again.code;
      continue;
    }
    console.error('referral code insert:', error.message);
    return null;
  }
  return null;
}

function maskEmail(e: string): string {
  const [u, d] = String(e).split('@');
  if (!d) return 'a friend';
  return `${u.slice(0, 1)}${'•'.repeat(Math.max(2, Math.min(5, u.length - 1)))}@${d}`;
}

export async function handleMyReferrals(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const flags = await getFlags(db);
  const terms = {
    reward_ngn: Number(cfg(flags, 'customer_referrals', 'reward_ngn', 0)),
    friend_reward_ngn: Number(cfg(flags, 'customer_referrals', 'friend_reward_ngn', 0)),
    min_order_ngn: Number(cfg(flags, 'customer_referrals', 'min_order_ngn', 0)),
  };
  if (!isOn(flags, 'customer_referrals', false)) return ok({ enabled: false, ...terms }, request, env);

  const code = await codeFor(db, auth.userId);
  if (!code) return err('Couldn’t create your referral code. Try again shortly.', 500, request, env);

  const { data: rewards } = await db.from('referral_rewards')
    .select('id, referred_email, amount_ngn, created_at')
    .eq('referrer_user_id', auth.userId)
    .order('created_at', { ascending: false })
    .limit(50);
  const { count: pending } = await db.from('orders')
    .select('id', { count: 'exact', head: true })
    .eq('referrer_user_id', auth.userId)
    .in('status', ['pending', 'pending_manual']);

  return ok({
    enabled: true,
    ...terms,
    code,
    link: `${frontend(env)}/shop?ref=${encodeURIComponent(code)}`,
    earned_ngn: (rewards || []).reduce((s, r: any) => s + (Number(r.amount_ngn) || 0), 0),
    referred: (rewards || []).length,
    pending_orders: pending ?? 0,
    rewards: (rewards || []).map((r: any) => ({ id: r.id, friend: maskEmail(r.referred_email), amount_ngn: Number(r.amount_ngn), created_at: r.created_at })),
  }, request, env);
}

/** The referrer behind a customer code, when the programme is on. */
export async function resolveCustomerCode(db: SupabaseClient, code: string): Promise<{ userId: string; firstName: string } | null> {
  if (!code) return null;
  const flags = await getFlags(db);
  if (!isOn(flags, 'customer_referrals', false)) return null;
  const { data } = await db.from('customer_referral_codes').select('user_id').eq('code', code.toUpperCase()).maybeSingle();
  if (!data) return null;
  const { data: p } = await db.from('profiles').select('full_name').eq('id', data.user_id).maybeSingle();
  return { userId: data.user_id, firstName: String(p?.full_name || '').trim().split(/\s+/)[0] || '' };
}

/** Called once an order is paid. Never throws. */
export async function rewardReferral(db: SupabaseClient, order: any): Promise<void> {
  try {
    if (!order?.referrer_user_id || !order.customer_email) return;
    const flags = await getFlags(db);
    if (!isOn(flags, 'customer_referrals', false)) return;
    const reward = Number(cfg(flags, 'customer_referrals', 'reward_ngn', 0));
    const friendReward = Number(cfg(flags, 'customer_referrals', 'friend_reward_ngn', 0));
    const min = Number(cfg(flags, 'customer_referrals', 'min_order_ngn', 0));
    const base = Math.max(0, Number(order.subtotal_ngn) - Number(order.discount_ngn || 0));
    if (base < min || (reward <= 0 && friendReward <= 0)) return;

    // Self-referral: the referrer bought with their own code.
    const friendUserId = await userIdForOrder(db, order);
    if (friendUserId === order.referrer_user_id) return;
    const { data: ref } = await db.from('profiles').select('email').eq('id', order.referrer_user_id).maybeSingle();
    if (ref?.email && String(ref.email).toLowerCase() === String(order.customer_email).toLowerCase()) return;

    // First paid order for this email only.
    const { count } = await db.from('orders')
      .select('id', { count: 'exact', head: true })
      .ilike('customer_email', escapeLike(order.customer_email))
      .eq('status', 'paid')
      .neq('id', order.id);
    if ((count ?? 0) > 0) return;

    const { data: granted, error } = await db.rpc('grant_referral_reward', {
      p_referrer: order.referrer_user_id,
      p_email: String(order.customer_email).toLowerCase(),
      p_order_id: order.id,
      p_amount: reward,
      p_friend: friendReward > 0 ? friendUserId : null,
      p_friend_amount: friendReward,
      p_order_ref: order.order_ref,
    });
    if (error) { console.error('grant_referral_reward:', error.message); return; }
    if (!granted) return;

    await logEvent(db, 'order', order.id, 'referral_rewarded', null, { referrer: order.referrer_user_id, reward_ngn: reward, friend_reward_ngn: friendReward });
    if (reward > 0) {
      await notifyUser(db, order.referrer_user_id, {
        kind: 'referral', title: `You earned ${naira(reward)}`,
        body: 'A friend you invited made their first purchase. The reward is in your wallet.',
        href: '/account/referrals', dedupe: `referral:${order.id}`,
      });
    }
    if (friendReward > 0 && friendUserId) {
      await notifyUser(db, friendUserId, {
        kind: 'referral', title: `${naira(friendReward)} welcome reward`,
        body: 'Thanks for your first purchase. The reward is in your wallet.',
        href: '/account/wallet', dedupe: `referral-friend:${order.id}`,
      });
    }
  } catch (e: any) {
    console.error('rewardReferral failed:', e?.message);
  }
}
