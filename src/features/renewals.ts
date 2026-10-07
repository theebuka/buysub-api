// ============================================================
// BUYSUB — Subscription expiry and renewal reminders (migration 08)
// ============================================================
// setOrderExpiry: when an order is paid, each subscription line gets
// starts_at = paid_at and expires_at = starts_at + duration_months.
//
// runRenewalReminders: the daily cron (wrangler.toml [triggers]). Every paid
// line ending within days_before that hasn't been reminded gets one email and
// one inbox notice, unless the customer has already bought the same product
// again with a later end date. A line is claimed (reminder_sent_at) before
// sending, so overlapping runs can't double-send.

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, ok, requireAdmin, escapeLike } from '../http';
import { addMonths, cfg, emailHtml, frontend, getFlags, isOn, notifyUser, sendEmail, userIdForOrder } from './core';

export async function setOrderExpiry(db: SupabaseClient, order: any): Promise<void> {
  const start = new Date(order.paid_at || Date.now());
  for (const it of order.order_items || []) {
    const months = Number(it.duration_months);
    if (!(months > 0) || it.billing_type === 'one_time' || it.expires_at) continue;
    const { error } = await db.from('order_items').update({
      starts_at: start.toISOString(),
      expires_at: addMonths(start, months).toISOString(),
    }).eq('id', it.id);
    // Before migration 08 the columns don't exist; the web falls back to its own maths.
    if (error) { console.error('setOrderExpiry:', error.message); return; }
  }
}

const BATCH = 100;

export async function runRenewalReminders(db: SupabaseClient, env: Env): Promise<{ sent: number; skipped: number }> {
  const flags = await getFlags(db, true);
  if (!isOn(flags, 'renewal_reminders', false)) return { sent: 0, skipped: 0 };
  const days = Number(cfg(flags, 'renewal_reminders', 'days_before', 7)) || 7;
  const now = new Date();
  const until = new Date(now.getTime() + days * 86_400_000);

  const { data: items, error } = await db.from('order_items')
    .select('id, product_id, product_name, billing_period, expires_at, starts_at, orders!inner(id, order_ref, status, customer_id, customer_email, customer_name)')
    .is('reminder_sent_at', null)
    .gt('expires_at', now.toISOString())
    .lte('expires_at', until.toISOString())
    .eq('orders.status', 'paid')
    .order('expires_at', { ascending: true })
    .limit(BATCH);
  if (error) { console.error('renewal reminders:', error.message); return { sent: 0, skipped: 0 }; }

  let sent = 0, skipped = 0;
  const site = frontend(env);
  for (const it of items || []) {
    const order: any = (it as any).orders;
    // Claim first.
    const { data: claimed } = await db.from('order_items')
      .update({ reminder_sent_at: new Date().toISOString() })
      .eq('id', it.id).is('reminder_sent_at', null).select('id');
    if (!claimed?.length) continue;

    // Already renewed: a later paid line for the same product and email that ends later.
    if (it.product_id && order?.customer_email) {
      const { data: later } = await db.from('order_items')
        .select('id, orders!inner(status, customer_email)')
        .eq('product_id', it.product_id)
        .gt('expires_at', it.expires_at)
        .eq('orders.status', 'paid')
        .ilike('orders.customer_email', escapeLike(order.customer_email))
        .limit(1);
      if (later?.length) { skipped++; continue; }
    }

    const ends = new Date(it.expires_at as string);
    const endsText = ends.toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric', timeZone: 'Africa/Lagos' });
    const renewHref = `${site}/account/subscriptions?renew=${encodeURIComponent(it.id)}`;
    const name = order?.customer_name ? String(order.customer_name).split(' ')[0] : 'there';

    if (order?.customer_email) {
      await sendEmail(env, order.customer_email, `Your ${it.product_name} subscription ends on ${endsText}`, emailHtml({
        heading: `${it.product_name} ends on ${endsText}`,
        paragraphs: [
          `Hi ${name},`,
          `Your ${it.product_name} subscription (order ${order.order_ref}) ends on ${endsText}. Renew now to keep it running without a gap.`,
        ],
        cta: { label: 'Renew now', href: renewHref },
        footnote: 'You’re getting this because you bought this subscription on BuySub. Each subscription gets one reminder.',
      }));
    }
    await notifyUser(db, await userIdForOrder(db, order || {}), {
      kind: 'renewal',
      title: `${it.product_name} ends on ${endsText}`,
      body: 'Renew now to keep it running without a gap.',
      href: `/account/subscriptions?renew=${encodeURIComponent(it.id)}`,
      dedupe: `renewal:${it.id}`,
    });
    sent++;
  }
  return { sent, skipped };
}

// POST /v2/admin/jobs/renewal-reminders — run the daily job now.
export async function handleRunRenewalReminders(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
  return ok(await runRenewalReminders(db, env), request, env);
}
