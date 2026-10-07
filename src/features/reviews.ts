// ============================================================
// BUYSUB — Reviews, ratings and sold counts (migration 11)
// ============================================================
// attachProductStats    adds sold_count / rating_avg / rating_count to public products
// GET  /v2/products/:slug/reviews        published reviews + rating summary
// GET  /v2/me/reviews/:productId         can I review it, and my review if any
// POST /v2/me/reviews                    { product_id, rating, body } (create or edit)
// GET  /v2/admin/reviews                 ?status=&q=&page=
// PATCH /v2/admin/reviews/:id            { status: 'published' | 'hidden' }
//
// Only a customer with a paid order containing the product may review it.
// Sold counts below show_sold_from are sent as null, so a new product never
// advertises "2 sold".

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, ok, err, requireAuth, requireAdmin, escapeLike, orSafe, logEvent } from '../http';
import { cfg, getFlags, isOn } from './core';

type Stats = { sold_count: number | null; rating_avg: number | null; rating_count: number };

export async function attachProductStats<T extends { id: string }>(db: SupabaseClient, products: T[]): Promise<(T & Partial<Stats>)[]> {
  if (!products?.length) return products;
  const flags = await getFlags(db);
  if (!isOn(flags, 'reviews', false)) return products;
  const { data, error } = await db.rpc('product_public_stats');
  if (error || !Array.isArray(data)) return products;
  const soldFrom = Number(cfg(flags, 'reviews', 'show_sold_from', 10));
  const by = new Map<string, any>(data.map((r: any) => [r.product_id, r]));
  return products.map(p => {
    const s = by.get(p.id);
    if (!s) return { ...p, sold_count: null, rating_avg: null, rating_count: 0 };
    const sold = Number(s.sold_count) || 0;
    return {
      ...p,
      sold_count: sold >= soldFrom ? sold : null,
      rating_avg: s.rating_avg === null ? null : Number(s.rating_avg),
      rating_count: Number(s.rating_count) || 0,
    };
  });
}

export async function handleGetProductReviews(db: SupabaseClient, slug: string, url: URL, request: Request, env: Env): Promise<Response> {
  const flags = await getFlags(db);
  if (!isOn(flags, 'reviews', false)) return ok({ enabled: false, items: [], summary: null }, request, env);
  const { data: product } = await db.from('products').select('id').eq('slug', slug).eq('status', 'active').is('deleted_at', null).maybeSingle();
  if (!product) return err('Product not found', 404, request, env);

  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1') || 1);
  const limit = Math.min(50, Math.max(1, parseInt(url.searchParams.get('limit') || '10') || 10));
  const [list, all] = await Promise.all([
    db.from('product_reviews')
      .select('id, rating, body, display_name, created_at, updated_at', { count: 'exact' })
      .eq('product_id', product.id).eq('status', 'published')
      .order('created_at', { ascending: false })
      .range((page - 1) * limit, page * limit - 1),
    db.from('product_reviews').select('rating').eq('product_id', product.id).eq('status', 'published').limit(10000),
  ]);
  if (list.error) return ok({ enabled: true, items: [], summary: { average: null, count: 0, distribution: [0, 0, 0, 0, 0] } }, request, env);

  const dist = [0, 0, 0, 0, 0];
  let sum = 0;
  for (const r of all.data || []) { dist[r.rating - 1]++; sum += r.rating; }
  const count = (all.data || []).length;
  return ok({
    enabled: true,
    items: list.data || [],
    summary: { average: count ? Math.round((sum / count) * 10) / 10 : null, count, distribution: dist },
  }, request, env, { pagination: { page, limit, total: list.count ?? 0, pages: Math.ceil((list.count || 0) / limit) } });
}

async function purchaseOf(db: SupabaseClient, email: string, productId: string): Promise<string | null> {
  const { data } = await db.from('order_items')
    .select('order_id, orders!inner(status, customer_email, paid_at)')
    .eq('product_id', productId)
    .eq('orders.status', 'paid')
    .ilike('orders.customer_email', escapeLike(email))
    .limit(1);
  return data?.[0]?.order_id ?? null;
}

async function emailOf(db: SupabaseClient, auth: { userId: string; email: string }) {
  const { data } = await db.from('profiles').select('email, full_name').eq('id', auth.userId).maybeSingle();
  return { email: String(data?.email || auth.email || '').toLowerCase(), fullName: String(data?.full_name || '') };
}

/** "Ada Okonkwo" → "Ada O." */
function shortName(full: string): string {
  const parts = full.trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return 'BuySub customer';
  return parts.length > 1 ? `${parts[0]} ${parts[parts.length - 1][0].toUpperCase()}.` : parts[0];
}

export async function handleMyReviewFor(db: SupabaseClient, productId: string, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const flags = await getFlags(db);
  if (!isOn(flags, 'reviews', false)) return ok({ enabled: false, can_review: false, review: null }, request, env);
  const me = await emailOf(db, auth);
  const [orderId, mine] = await Promise.all([
    purchaseOf(db, me.email, productId),
    db.from('product_reviews').select('id, rating, body, status, created_at, updated_at').eq('product_id', productId).eq('user_id', auth.userId).maybeSingle(),
  ]);
  return ok({ enabled: true, can_review: !!orderId, review: mine.data ?? null }, request, env);
}

export async function handleSubmitReview(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const flags = await getFlags(db);
  if (!isOn(flags, 'reviews', false)) return err('Reviews are switched off', 503, request, env);

  const body = await request.json().catch(() => ({})) as any;
  const productId = String(body?.product_id || '');
  const rating = Number(body?.rating);
  const text = typeof body?.body === 'string' ? body.body.trim().slice(0, 2000) : '';
  if (!productId) return err('product_id is required', 400, request, env);
  if (!Number.isInteger(rating) || rating < 1 || rating > 5) return err('Choose a rating from 1 to 5 stars', 400, request, env);

  const me = await emailOf(db, auth);
  const orderId = await purchaseOf(db, me.email, productId);
  if (!orderId) return err('You can review a product once you’ve bought it', 403, request, env);

  const now = new Date().toISOString();
  const { data: existing } = await db.from('product_reviews').select('id, status').eq('product_id', productId).eq('user_id', auth.userId).maybeSingle();
  const row = { rating, body: text || null, display_name: shortName(me.fullName), order_id: orderId, updated_at: now };
  const res = existing
    // Editing keeps a hidden review hidden.
    ? await db.from('product_reviews').update(row).eq('id', existing.id).select('id, rating, body, status, created_at, updated_at').single()
    : await db.from('product_reviews').insert({ ...row, product_id: productId, user_id: auth.userId }).select('id, rating, body, status, created_at, updated_at').single();
  if (res.error) return err(res.error.message, 500, request, env);
  return ok(res.data, request, env);
}

export async function handleAdminReviews(db: SupabaseClient, url: URL, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1') || 1);
  const limit = Math.min(100, Math.max(1, parseInt(url.searchParams.get('limit') || '25') || 25));
  const status = url.searchParams.get('status');
  const rating = parseInt(url.searchParams.get('rating') || '');
  const q = url.searchParams.get('q')?.trim();

  let query = db.from('product_reviews')
    .select('id, rating, body, display_name, status, created_at, updated_at, product_id, products(name, slug), orders(order_ref)', { count: 'exact' })
    .order('created_at', { ascending: false })
    .range((page - 1) * limit, page * limit - 1);
  if (status === 'published' || status === 'hidden') query = query.eq('status', status);
  if (rating >= 1 && rating <= 5) query = query.eq('rating', rating);
  if (q) query = query.or(`body.ilike.%${orSafe(q)}%,display_name.ilike.%${orSafe(q)}%`);

  const { data, error, count } = await query;
  if (error) return err(error.message, 500, request, env);
  return ok(data || [], request, env, { pagination: { page, limit, total: count ?? 0, pages: Math.ceil((count || 0) / limit) } });
}

export async function handleAdminUpdateReview(db: SupabaseClient, id: string, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
  const body = await request.json().catch(() => ({})) as any;
  if (!['published', 'hidden'].includes(body?.status)) return err('status must be published or hidden', 400, request, env);
  const { data, error } = await db.from('product_reviews').update({ status: body.status }).eq('id', id).select('id, status').maybeSingle();
  if (error) return err(error.message, 500, request, env);
  if (!data) return err('Review not found', 404, request, env);
  await logEvent(db, 'review', id, `review_${body.status}`, auth.userId, {});
  return ok(data, request, env);
}
