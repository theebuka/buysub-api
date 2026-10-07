// ============================================================
// BUYSUB — Back-in-stock alerts (migration 13)
// ============================================================
// POST /v2/stock-alerts  { product_id, email }  public; signed-in callers are linked
// sendBackInStock        called when admin moves a product back to in_stock

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, ok, err, EMAIL_RE } from '../http';
import { emailHtml, frontend, notifyUser, sendEmail } from './core';

export async function handleCreateStockAlert(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const body = await request.json().catch(() => ({})) as any;
  const email = String(body?.email || '').trim().toLowerCase();
  const productId = String(body?.product_id || '');
  if (!EMAIL_RE.test(email) || email.length > 200) return err('Enter a valid email address', 400, request, env);

  const { data: product } = await db.from('products').select('id, name, stock_status, status').eq('id', productId).is('deleted_at', null).maybeSingle();
  if (!product || product.status !== 'active') return err('Product not found', 404, request, env);
  if (product.stock_status === 'in_stock') return err(`${product.name} is in stock now`, 409, request, env);

  // Optional: link a signed-in caller so the alert also lands in their inbox.
  let userId: string | null = null;
  const token = request.headers.get('Authorization')?.replace('Bearer ', '').trim();
  if (token) {
    const { data } = await db.auth.getUser(token);
    userId = data?.user?.id ?? null;
  }

  const { error } = await db.from('stock_alerts').upsert(
    { product_id: product.id, email, user_id: userId, notified_at: null, created_at: new Date().toISOString() },
    { onConflict: 'product_id,email' },
  );
  if (error) return err('Alerts aren’t available yet', 503, request, env);
  return ok({ subscribed: true }, request, env);
}

export async function sendBackInStock(db: SupabaseClient, env: Env, product: { id: string; name: string; slug: string }): Promise<number> {
  const { data: alerts, error } = await db.from('stock_alerts')
    .select('id, email, user_id').eq('product_id', product.id).is('notified_at', null).limit(500);
  if (error || !alerts?.length) return 0;
  const href = `${frontend(env)}/shop/${encodeURIComponent(product.slug)}`;
  let n = 0;
  for (const a of alerts) {
    const { data: claimed } = await db.from('stock_alerts')
      .update({ notified_at: new Date().toISOString() }).eq('id', a.id).is('notified_at', null).select('id');
    if (!claimed?.length) continue;
    await sendEmail(env, a.email, `${product.name} is back in stock`, emailHtml({
      heading: `${product.name} is back in stock`,
      paragraphs: ['You asked us to tell you when this was available again. It is now.'],
      cta: { label: `Buy ${product.name}`, href },
      footnote: 'You’re getting this once because you asked for a back-in-stock alert on BuySub.',
    }));
    await notifyUser(db, a.user_id, {
      kind: 'stock', title: `${product.name} is back in stock`, href: `/shop/${encodeURIComponent(product.slug)}`,
      dedupe: `stock:${a.id}:${Date.now()}`,
    });
    n++;
  }
  return n;
}
