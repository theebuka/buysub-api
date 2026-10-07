// ============================================================
// BUYSUB — Saved products on the account (migration 16)
// ============================================================
// GET    /v2/me/saved               product ids, newest first
// PUT    /v2/me/saved/:productId    save (idempotent)
// DELETE /v2/me/saved/:productId    unsave
// POST   /v2/me/saved/merge         { product_ids } from the browser list, on sign-in; returns the merged list
//
// Signed-out shoppers keep the list in the browser (web lib/saved.ts). Before
// migration 16 these return 503 and the web stays browser-only.

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, ok, err, requireAuth } from '../http';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const MAX_SAVED = 500;

async function list(db: SupabaseClient, userId: string) {
  return db.from('saved_products').select('product_id, created_at')
    .eq('user_id', userId).order('created_at', { ascending: false }).limit(MAX_SAVED);
}

function unavailable(request: Request, env: Env, message: string) {
  console.error('saved_products:', message);
  return err('Saved items aren’t available right now', 503, request, env);
}

export async function handleMySaved(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const { data, error } = await list(db, auth.userId);
  if (error) return unavailable(request, env, error.message);
  return ok((data || []).map(r => r.product_id), request, env);
}

export async function handleSaveProduct(db: SupabaseClient, productId: string, save: boolean, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  if (!UUID_RE.test(productId)) return err('Unknown product', 400, request, env);
  if (save) {
    const { data: p } = await db.from('products').select('id').eq('id', productId).maybeSingle();
    if (!p) return err('Unknown product', 404, request, env);
    const { error } = await db.from('saved_products')
      .upsert({ user_id: auth.userId, product_id: productId }, { onConflict: 'user_id,product_id', ignoreDuplicates: true });
    if (error) return unavailable(request, env, error.message);
  } else {
    const { error } = await db.from('saved_products').delete().eq('user_id', auth.userId).eq('product_id', productId);
    if (error) return unavailable(request, env, error.message);
  }
  return ok({ product_id: productId, saved: save }, request, env);
}

export async function handleMergeSaved(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const body = await request.json().catch(() => ({})) as any;
  const ids = [...new Set<string>((Array.isArray(body?.product_ids) ? body.product_ids : []).map(String))]
    .filter(id => UUID_RE.test(id)).slice(0, 200);
  if (ids.length) {
    // Only products that still exist; the browser list can hold deleted ones.
    const { data: known } = await db.from('products').select('id').in('id', ids);
    const rows = (known || []).map(p => ({ user_id: auth.userId, product_id: p.id }));
    if (rows.length) {
      const { error } = await db.from('saved_products').upsert(rows, { onConflict: 'user_id,product_id', ignoreDuplicates: true });
      if (error) return unavailable(request, env, error.message);
    }
  }
  const { data, error } = await list(db, auth.userId);
  if (error) return unavailable(request, env, error.message);
  return ok((data || []).map(r => r.product_id), request, env);
}
