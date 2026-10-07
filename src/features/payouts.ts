// ============================================================
// BUYSUB — Scheduled partner payouts and tiers (migration 15)
// ============================================================
// Partners are paid on the schedule they chose (partner_applications.
// payout_frequency). The daily cron calls runPartnerPayouts, which creates one
// payout per partner per period through create_partner_payout. Commissions
// younger than hold_days at the period end roll into the next period.
//
// GET  /v2/partners/me/payouts             next payout date and estimate, open payouts, history
// GET  /v2/admin/payouts                   ?status=&page=
// POST /v2/admin/payouts/:id/settle        { action: 'paid' | 'rejected', note?, reference? }
// POST /v2/admin/jobs/partner-payouts      run the scheduler now (idempotent)
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
    hold_days: Number(cfg(flags, 'partner_payouts', 'hold_days', 14)),
  };
}

// ── Payout periods ──────────────────────────────────────────
// A period ends on the 1st of a month (Africa/Lagos, UTC+1, no DST) in which
// the partner's frequency starts a new period. Dates are 'YYYY-MM-DD'.

const PERIOD_MONTHS: Record<string, number[]> = {
  monthly: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
  quarterly: [0, 3, 6, 9],
  biannual: [0, 6],
  annual: [0],
};
export const FREQUENCIES = ['Monthly', 'Quarterly', 'Biannual', 'Annual'] as const;

export function normalFrequency(f: string | null | undefined): string {
  const k = String(f || '').trim().toLowerCase();
  return FREQUENCIES.find(x => x.toLowerCase() === k) ?? 'Monthly';
}

const LAGOS_OFFSET_MS = 3_600_000;
const lagosToday = (at: Date) => {
  const d = new Date(at.getTime() + LAGOS_OFFSET_MS);
  return { y: d.getUTCFullYear(), m: d.getUTCMonth(), day: d.getUTCDate() };
};
const ymd = (y: number, m: number) => {
  const d = new Date(Date.UTC(y, m, 1));
  return d.toISOString().slice(0, 10);
};

/** The last period end on or before `at`, the one before it, and the next one. */
export function payoutPeriods(frequency: string, at = new Date()) {
  const months = PERIOD_MONTHS[normalFrequency(frequency).toLowerCase()];
  const { y, m } = lagosToday(at);
  // Walk back from this month to the latest boundary month.
  const back = (fromY: number, fromM: number) => {
    let yy = fromY, mm = fromM;
    for (let i = 0; i < 13; i++) {
      if (months.includes(mm)) return { y: yy, m: mm };
      mm -= 1; if (mm < 0) { mm = 11; yy -= 1; }
    }
    return { y: fromY, m: fromM };
  };
  const last = back(y, m);
  const prev = back(last.m === 0 ? last.y - 1 : last.y, last.m === 0 ? 11 : last.m - 1);
  let ny = last.y, nm = last.m;
  for (let i = 0; i < 13; i++) { nm += 1; if (nm > 11) { nm = 0; ny += 1; } if (months.includes(nm)) break; }
  return { last_end: ymd(last.y, last.m), last_start: ymd(prev.y, prev.m), next_end: ymd(ny, nm) };
}

/** Start of a period end date in Lagos, minus the hold: commissions created before this count. */
function cutoffFor(periodEnd: string, holdDays: number): Date {
  return new Date(Date.parse(periodEnd + 'T00:00:00Z') - LAGOS_OFFSET_MS - Math.max(0, holdDays) * 86_400_000);
}

// A period that ended more than this many days ago is not created late (a
// partner who joins mid-period waits for the next end; a missed cron run
// catches up within this window).
const CATCH_UP_DAYS = 10;

async function frequencyByUser(db: SupabaseClient, userIds: string[]): Promise<Map<string, string>> {
  const out = new Map<string, string>();
  if (!userIds.length) return out;
  const { data } = await db.from('partner_applications').select('user_id, payout_frequency, created_at')
    .in('user_id', userIds).order('created_at', { ascending: true });
  for (const r of data || []) if (r.user_id) out.set(r.user_id, normalFrequency(r.payout_frequency));
  return out;
}

export async function runPartnerPayouts(db: SupabaseClient, env: Env, at = new Date()) {
  const result = { created: 0, below_minimum: 0, exists: 0, not_due: 0, failed: 0 };
  const flags = await getFlags(db, true);
  const terms = payoutTerms(flags);
  if (!terms.enabled) return result;

  const { data: affs, error } = await db.from('affiliates').select('id, user_id').eq('status', 'approved').limit(5000);
  if (error) { console.error('partner payouts:', error.message); return result; }
  const freq = await frequencyByUser(db, (affs || []).map(a => a.user_id).filter(Boolean));

  for (const a of affs || []) {
    const frequency = freq.get(a.user_id) ?? 'Monthly';
    const p = payoutPeriods(frequency, at);
    if (at.getTime() - Date.parse(p.last_end + 'T00:00:00Z') + LAGOS_OFFSET_MS > CATCH_UP_DAYS * 86_400_000) { result.not_due++; continue; }
    const { data, error: rpcErr } = await db.rpc('create_partner_payout', {
      p_affiliate_id: a.id, p_period_start: p.last_start, p_period_end: p.last_end, p_frequency: frequency,
      p_hold_days: terms.hold_days, p_min_ngn: terms.min_ngn,
    });
    if (rpcErr) { console.error('create_partner_payout:', rpcErr.message); result.failed++; continue; }
    const r: any = data || {};
    if (r.result === 'created') {
      result.created++;
      await logEvent(db, 'payout', r.id, 'created', null, { amount_ngn: r.amount_ngn, period_end: p.last_end });
      await notifyUser(db, a.user_id, {
        kind: 'payout', title: `Payout of ${naira(r.amount_ngn)} is being processed`,
        body: `Your ${frequency.toLowerCase()} payout is with our team. We’ll let you know when it’s sent.`,
        href: '/partner/payouts', dedupe: `payout-created:${r.id}`,
      });
    } else if (r.result === 'below_minimum') result.below_minimum++;
    else if (r.result === 'exists') result.exists++;
  }
  return result;
}

export async function handleRunPartnerPayouts(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
  return ok(await runPartnerPayouts(db, env), request, env);
}

export async function handleMyPayouts(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const aff = await affiliateFor(db, auth.userId);
  if (!aff) return err('No partner account', 404, request, env);
  const terms = payoutTerms(await getFlags(db));

  const [comms, history, app] = await Promise.all([
    db.from('affiliate_commissions')
      .select('amount_ngn, created_at, status, payout_id, orders!inner(status)')
      .eq('affiliate_id', aff.id).is('payout_id', null).in('status', ['pending', 'approved'])
      .eq('orders.status', 'paid').limit(10000),
    db.from('payout_requests')
      .select('id, amount_ngn, status, period_start, period_end, frequency, created_at, processed_at, admin_note, reference')
      .eq('affiliate_id', aff.id).order('created_at', { ascending: false }).limit(50),
    db.from('partner_applications')
      .select('payout_frequency, payout_method, bank_name, account_number, wallet_address')
      .eq('user_id', auth.userId).order('created_at', { ascending: false }).limit(1).maybeSingle(),
  ]);

  const frequency = normalFrequency(app.data?.payout_frequency);
  const periods = payoutPeriods(frequency);
  const cutoff = cutoffFor(periods.next_end, terms.hold_days).getTime();
  let next = 0, later = 0;
  for (const c of comms.data || []) {
    const amt = Number((c as any).amount_ngn) || 0;
    if (new Date((c as any).created_at).getTime() < cutoff) next += amt; else later += amt;
  }
  const d = app.data;
  const hasDetails = !!((d?.bank_name || aff.bank_name) && (d?.account_number || aff.account_number)) || !!d?.wallet_address;
  const rows = history.error ? [] : history.data || [];
  const round = (n: number) => Math.round(n * 100) / 100;
  return ok({
    ...terms,
    enabled: terms.enabled && !history.error,
    frequency,
    next_payout_date: periods.next_end,
    cutoff_at: new Date(cutoff).toISOString(),
    next_ngn: round(next),
    later_ngn: round(later),
    has_details: hasDetails,
    open: rows.filter(r => r.status === 'pending'),
    history: rows,
  }, request, env);
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
      title: paid ? `Payout of ${naira(p.amount_ngn)} sent` : 'Payout declined',
      body: paid ? (reference ? `Reference ${reference}.` : 'It should reach your account shortly.') : note,
      href: '/partner/payouts', dedupe: `payout:${id}`,
    });
    if (userId) {
      const { data: prof } = await db.from('profiles').select('email').eq('id', userId).maybeSingle();
      if (prof?.email) {
        await sendEmail(env, prof.email, paid ? `Your BuySub payout of ${naira(p.amount_ngn)} is on its way` : 'Your BuySub payout', emailHtml({
          heading: paid ? `${naira(p.amount_ngn)} is on its way` : 'We couldn’t process your payout',
          paragraphs: paid
            ? [`We’ve sent your partner payout of ${naira(p.amount_ngn)}.`, ...(reference ? [`Transfer reference: ${reference}`] : [])]
            : [`Your payout of ${naira(p.amount_ngn)} was declined.`, `Reason: ${note}`, 'These commissions will be included in your next payout once this is sorted.'],
          cta: { label: 'View payouts', href: `${frontend(env)}/partner/payouts` },
        }));
      }
    }
  }
  return ok({ id, status: action }, request, env);
}
