// ============================================================
// BUYSUB — Shared pieces for the feature modules
// ============================================================
// Feature flags (feature_flags table), the per-user inbox
// (user_notifications, migration 09), plain transactional email, and
// resolving the account behind an order.

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, escapeLike } from '../http';

// ── Feature flags ───────────────────────────────────────────────
export type Flag = { enabled: boolean; config: Record<string, any> };
export type Flags = Record<string, Flag>;

// Cached per isolate for 30s: /v2/status is read on every page load. An
// admin change clears this isolate's copy; others catch up within 30s.
let flagCache: { at: number; flags: Flags } | null = null;
const FLAG_TTL = 30_000;

export async function getFlags(db: SupabaseClient, fresh = false): Promise<Flags> {
  if (!fresh && flagCache && Date.now() - flagCache.at < FLAG_TTL) return flagCache.flags;
  const { data, error } = await db.from('feature_flags').select('key, enabled, config');
  if (error) return flagCache?.flags ?? {};
  const flags: Flags = {};
  for (const r of data || []) flags[r.key] = { enabled: !!r.enabled, config: (r.config as any) || {} };
  flagCache = { at: Date.now(), flags };
  return flags;
}

export function clearFlagCache() { flagCache = null; }

/** A switch that is missing from the table counts as `dflt`. */
export function isOn(flags: Flags, key: string, dflt = true): boolean {
  return flags[key] ? flags[key].enabled : dflt;
}

export function cfg<T = any>(flags: Flags, key: string, field: string, dflt: T): T {
  const v = flags[key]?.config?.[field];
  return v === undefined || v === null ? dflt : (v as T);
}

// ── Accounts ────────────────────────────────────────────────────
/** The auth user behind an order: its customer row's user, else a profile with that email. */
export async function userIdForOrder(db: SupabaseClient, order: { customer_id?: string | null; customer_email?: string | null }): Promise<string | null> {
  if (order.customer_id) {
    const { data } = await db.from('customers').select('user_id').eq('id', order.customer_id).maybeSingle();
    if (data?.user_id) return data.user_id;
  }
  if (order.customer_email) return userIdForEmail(db, order.customer_email);
  return null;
}

export async function userIdForEmail(db: SupabaseClient, email: string): Promise<string | null> {
  const { data } = await db.from('profiles').select('id').ilike('email', escapeLike(email.trim())).limit(1);
  return data?.[0]?.id ?? null;
}

// ── Inbox ───────────────────────────────────────────────────────
export type Notice = {
  kind: string;
  title: string;
  body?: string | null;
  href?: string | null;
  /** Same key for the same user is stored once (webhook retries, cron re-runs). */
  dedupe?: string;
};

export async function notifyUser(db: SupabaseClient, userId: string | null | undefined, n: Notice): Promise<void> {
  if (!userId) return;
  const { error } = await db.from('user_notifications').insert({
    user_id: userId, kind: n.kind, title: n.title, body: n.body ?? null, href: n.href ?? null,
    dedupe_key: n.dedupe ?? null,
  });
  // 23505: already sent. Anything else is logged; a notice is never worth failing the caller.
  if (error && error.code !== '23505') console.error('notifyUser failed:', n.kind, error.message);
}

// ── Email ───────────────────────────────────────────────────────
export function esc(s: unknown): string {
  return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]!));
}

export const naira = (n: number | string) => `₦${Math.round(Number(n) || 0).toLocaleString('en-NG')}`;

/** One plain layout for the notification emails. Paragraphs are escaped. */
export function emailHtml(o: { heading: string; paragraphs: string[]; cta?: { label: string; href: string }; footnote?: string }): string {
  const p = o.paragraphs.map(t => `<p style="margin:0 0 14px;font-size:15px;line-height:1.6;color:#334155">${esc(t)}</p>`).join('');
  const cta = o.cta
    ? `<p style="margin:22px 0 6px"><a href="${esc(o.cta.href)}" style="display:inline-block;background:#6d28d9;color:#ffffff;text-decoration:none;font-weight:600;font-size:15px;padding:12px 20px;border-radius:8px">${esc(o.cta.label)}</a></p>`
    : '';
  const foot = o.footnote ? `<p style="margin:18px 0 0;font-size:13px;color:#64748b">${esc(o.footnote)}</p>` : '';
  return `<!doctype html><html><body style="margin:0;background:#f8fafc;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f8fafc;padding:24px 12px"><tr><td align="center">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:520px;background:#ffffff;border:1px solid #e2e8f0;border-radius:12px">
<tr><td style="padding:24px 28px 8px;font-weight:700;font-size:18px;color:#0f172a">BuySub</td></tr>
<tr><td style="padding:8px 28px 28px">
<h1 style="margin:0 0 14px;font-size:20px;line-height:1.3;color:#0f172a">${esc(o.heading)}</h1>
${p}${cta}${foot}
</td></tr></table>
<p style="font-size:12px;color:#94a3b8;margin:16px 0 0">BuySub · app.buysub.ng</p>
</td></tr></table></body></html>`;
}

export async function sendEmail(env: Env, to: string, subject: string, html: string): Promise<boolean> {
  if (!env.RESEND_API_KEY) { console.error('sendEmail: RESEND_API_KEY not set'); return false; }
  try {
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { Authorization: `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from: 'BuySub <noreply@buysub.ng>', to: [to], subject, html }),
    });
    if (!res.ok) console.error('resend error:', res.status, await res.text().catch(() => ''));
    return res.ok;
  } catch (e: any) {
    console.error('sendEmail failed:', e?.message);
    return false;
  }
}

/** Calendar months, clamped like the web's addMonths (31 Jan + 1 → 28/29 Feb). */
export function addMonths(d: Date, months: number): Date {
  const r = new Date(d);
  const day = r.getUTCDate();
  r.setUTCMonth(r.getUTCMonth() + months);
  if (r.getUTCDate() < day) r.setUTCDate(0);
  return r;
}

export function frontend(env: Env): string {
  return (env.FRONTEND_URL || 'https://app.buysub.ng').replace(/\/+$/, '');
}
