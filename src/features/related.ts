// ============================================================
// BUYSUB — Frequently bought together
// ============================================================
// GET /v2/products/:slug/related → [{ product_id, count }], most often bought
// with this product on the same paid order, from the latest 500 such orders.
// No personal data leaves: only product ids and counts. The web fills the
// gaps from the same category.

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, ok, err } from '../http';

export async function handleRelatedProducts(db: SupabaseClient, slug: string, request: Request, env: Env): Promise<Response> {
  const { data: product } = await db.from('products').select('id').eq('slug', slug).eq('status', 'active').is('deleted_at', null).maybeSingle();
  if (!product) return err('Product not found', 404, request, env);

  const { data: lines } = await db.from('order_items')
    .select('order_id, orders!inner(status, created_at)')
    .eq('product_id', product.id)
    .eq('orders.status', 'paid')
    .order('created_at', { ascending: false, referencedTable: 'orders' })
    .limit(500);
  const orderIds = [...new Set((lines || []).map((l: any) => l.order_id))];
  if (!orderIds.length) return ok([], request, env);

  const counts = new Map<string, number>();
  for (let i = 0; i < orderIds.length; i += 100) {
    const { data } = await db.from('order_items').select('order_id, product_id').in('order_id', orderIds.slice(i, i + 100)).neq('product_id', product.id);
    // Count each other product once per order.
    const seen = new Set<string>();
    for (const r of data || []) {
      if (!r.product_id) continue;
      const k = `${r.order_id}:${r.product_id}`;
      if (seen.has(k)) continue;
      seen.add(k);
      counts.set(r.product_id, (counts.get(r.product_id) || 0) + 1);
    }
  }
  const top = [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 6).map(([product_id, count]) => ({ product_id, count }));
  return ok(top, request, env);
}
