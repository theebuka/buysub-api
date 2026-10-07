// ============================================================
// BUYSUB — Service switches and the maintenance page
// ============================================================
// GET /v2/status is public: the web reads it to show the maintenance page and
// to hide what is switched off. The API enforces the same switches
// (serviceBlocked), so hiding a button is never the only guard.
// Admins edit them at /v2/admin/flags (migration 14).

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, ok, err, requireAdmin } from '../http';
import { getFlags, clearFlagCache, isOn, cfg, type Flags } from './core';

export function publicStatus(flags: Flags) {
  return {
    maintenance: {
      enabled: isOn(flags, 'maintenance_mode', false),
      message: String(cfg(flags, 'maintenance_mode', 'message', '') || ''),
    },
    services: {
      paystack: isOn(flags, 'paystack_checkout'),
      whatsapp: isOn(flags, 'whatsapp_checkout'),
      wallet_pay: isOn(flags, 'wallet_enabled'),
      wallet_funding: isOn(flags, 'wallet_funding', false),
      partner_applications: isOn(flags, 'partner_applications'),
      reviews: isOn(flags, 'reviews', false),
      referrals: isOn(flags, 'customer_referrals', false),
      payouts: isOn(flags, 'partner_payouts', false),
    },
    wallet_funding: {
      min_ngn: Number(cfg(flags, 'wallet_funding', 'min_ngn', 1000)),
      max_ngn: Number(cfg(flags, 'wallet_funding', 'max_ngn', 500000)),
    },
  };
}

export async function handleStatus(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  return ok(publicStatus(await getFlags(db)), request, env);
}

const OFF_MESSAGE: Record<string, string> = {
  maintenance_mode: 'BuySub is down for maintenance. Please try again shortly.',
  paystack_checkout: 'Card and bank payments are paused right now. Please order on WhatsApp or try again later.',
  whatsapp_checkout: 'WhatsApp ordering is paused right now. Please pay online or try again later.',
  wallet_enabled: 'Paying from your wallet is paused right now.',
  wallet_funding: 'Wallet top-ups are paused right now.',
  partner_applications: 'Partner applications are closed right now.',
};

/**
 * A 503 response when `key` is off or the site is in maintenance, else null.
 * Missing switches count as on, so this never blocks before migration 14.
 */
export async function serviceBlocked(db: SupabaseClient, key: string | null, request: Request, env: Env): Promise<Response | null> {
  const flags = await getFlags(db);
  if (isOn(flags, 'maintenance_mode', false)) return err(OFF_MESSAGE.maintenance_mode, 503, request, env);
  if (key && !isOn(flags, key, key === 'wallet_funding' ? false : true)) return err(OFF_MESSAGE[key] || 'This is switched off right now.', 503, request, env);
  return null;
}

// ── Admin ───────────────────────────────────────────────────────
// Only these keys are editable here, each with the config fields it takes.
// paystack_live, multi_currency, tax_enabled etc. stay as they are.
type FieldSpec = { type: 'number'; min: number; max: number } | { type: 'string'; max: number } | { type: 'tiers' };
const EDITABLE: Record<string, Record<string, FieldSpec>> = {
  maintenance_mode: { message: { type: 'string', max: 300 } },
  paystack_checkout: {},
  whatsapp_checkout: {},
  wallet_enabled: {},
  wallet_funding: { min_ngn: { type: 'number', min: 100, max: 10_000_000 }, max_ngn: { type: 'number', min: 100, max: 10_000_000 } },
  partner_applications: {},
  reviews: { show_sold_from: { type: 'number', min: 0, max: 100_000 } },
  renewal_reminders: { days_before: { type: 'number', min: 1, max: 30 } },
  customer_referrals: {
    reward_ngn: { type: 'number', min: 0, max: 1_000_000 },
    friend_reward_ngn: { type: 'number', min: 0, max: 1_000_000 },
    min_order_ngn: { type: 'number', min: 0, max: 10_000_000 },
  },
  partner_payouts: { min_ngn: { type: 'number', min: 0, max: 10_000_000 }, hold_days: { type: 'number', min: 0, max: 90 } },
  partner_tiers: { tiers: { type: 'tiers' } },
};

function validTiers(v: any): { name: string; min_sales_ngn: number; rate: number }[] | null {
  if (!Array.isArray(v) || v.length < 1 || v.length > 8) return null;
  const out = [];
  for (const t of v) {
    const name = String(t?.name || '').trim().slice(0, 40);
    const min = Number(t?.min_sales_ngn), rate = Number(t?.rate);
    if (!name || !Number.isFinite(min) || min < 0 || !Number.isFinite(rate) || rate < 0 || rate > 50) return null;
    out.push({ name, min_sales_ngn: Math.round(min), rate: Math.round(rate * 100) / 100 });
  }
  return out.sort((a, b) => a.min_sales_ngn - b.min_sales_ngn);
}

export async function handleAdminGetFlags(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
  const flags = await getFlags(db, true);
  // exists: false means the row's migration hasn't been applied yet.
  const out: Record<string, { enabled: boolean; config: Record<string, any>; exists: boolean }> = {};
  for (const key of Object.keys(EDITABLE)) {
    out[key] = { enabled: !!flags[key]?.enabled, config: flags[key]?.config ?? {}, exists: !!flags[key] };
  }
  return ok(out, request, env);
}

export async function handleAdminUpdateFlag(db: SupabaseClient, key: string, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
  const { data: me } = await db.from('profiles').select('role').eq('id', auth.userId).maybeSingle();
  if (!['admin', 'super_admin'].includes(me?.role)) return err('Only admins can change these settings', 403, request, env);

  const spec = EDITABLE[key];
  if (!spec) return err('This setting can’t be changed here', 400, request, env);
  const body = await request.json().catch(() => ({})) as any;

  const { data: row } = await db.from('feature_flags').select('enabled, config').eq('key', key).maybeSingle();
  if (!row) return err('This setting needs its database migration first', 409, request, env);

  const config: Record<string, any> = { ...(row.config as any || {}) };
  for (const [field, f] of Object.entries(spec)) {
    if (body?.config?.[field] === undefined) continue;
    const v = body.config[field];
    if (f.type === 'number') {
      const n = Number(v);
      if (!Number.isFinite(n) || n < f.min || n > f.max) return err(`${field} must be between ${f.min} and ${f.max}`, 400, request, env);
      config[field] = n;
    } else if (f.type === 'string') {
      config[field] = String(v ?? '').slice(0, f.max);
    } else {
      const t = validTiers(v);
      if (!t) return err('Each tier needs a name, a minimum sales figure and a rate between 0 and 50', 400, request, env);
      config[field] = t;
    }
  }
  if (key === 'wallet_funding' && Number(config.min_ngn) > Number(config.max_ngn)) {
    return err('The minimum top-up can’t be more than the maximum', 400, request, env);
  }

  const enabled = typeof body.enabled === 'boolean' ? body.enabled : row.enabled;
  const { error } = await db.from('feature_flags')
    .update({ enabled, config, updated_at: new Date().toISOString() })
    .eq('key', key);
  if (error) return err(error.message, 500, request, env);
  clearFlagCache();
  return ok({ key, enabled, config }, request, env);
}
