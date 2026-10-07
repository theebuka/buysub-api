// ============================================================
// BUYSUB — Cart on the account (migration 17)
// ============================================================
// GET /v2/me/cart   { items: [{ product_id, period, qty }], updated_at }
// PUT /v2/me/cart   { items } replaces the whole cart; returns the same shape
//
// Lines only, no prices: the web app re-prices from the live product list and
// checkout prices on the server. Signed-out carts stay in the browser (web
// lib/cartSync.ts merges them in on sign-in). Last write wins between devices.
// Before migration 17 these return 503 and the web stays browser-only.

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, ok, err, requireAuth } from '../http';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const PERIOD_RE = /^[A-Za-z_-]{1,20}$/;
const MAX_LINES = 50;
const MAX_QTY = 20;

type Line = { product_id: string; period: string; qty: number };

function clean(raw: unknown): Line[] | null {
  if (!Array.isArray(raw) || raw.length > MAX_LINES) return null;
  const seen = new Set<string>();
  const out: Line[] = [];
  for (const r of raw as any[]) {
    const product_id = String(r?.product_id || '');
    const period = String(r?.period || '');
    const qty = Number(r?.qty);
    if (!UUID_RE.test(product_id) || !PERIOD_RE.test(period) || !Number.isInteger(qty) || qty < 1) return null;
    const key = `${product_id}:${period}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ product_id, period, qty: Math.min(qty, MAX_QTY) });
  }
  return out;
}

function unavailable(request: Request, env: Env, message: string) {
  console.error('carts:', message);
  return err('Cart sync isn’t available right now', 503, request, env);
}

export async function handleMyCart(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const { data, error } = await db.from('carts').select('items, updated_at').eq('user_id', auth.userId).maybeSingle();
  if (error) return unavailable(request, env, error.message);
  return ok({ items: clean(data?.items) ?? [], updated_at: data?.updated_at ?? null }, request, env);
}

export async function handlePutCart(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const body = await request.json().catch(() => ({})) as any;
  const items = clean(body?.items);
  if (!items) return err(`items must be up to ${MAX_LINES} lines of { product_id, period, qty }`, 400, request, env);
  const { data, error } = await db.from('carts')
    .upsert({ user_id: auth.userId, items, updated_at: new Date().toISOString() }, { onConflict: 'user_id' })
    .select('items, updated_at').single();
  if (error) return unavailable(request, env, error.message);
  return ok({ items: data.items, updated_at: data.updated_at }, request, env);
}
