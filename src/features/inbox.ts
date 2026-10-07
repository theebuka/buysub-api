// ============================================================
// BUYSUB — Per-user notifications inbox (migration 09)
// ============================================================
// GET  /v2/me/notifications?limit=&before=   newest first, plus the unread count
// POST /v2/me/notifications/read             { ids?: string[] } — no ids marks all read
//
// Written by the API when something happens to the user's orders, wallet,
// referrals, payouts or stock alerts (notifyUser in ./core).

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, ok, err, requireAuth } from '../http';

export async function handleGetMyNotifications(db: SupabaseClient, url: URL, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const limit = Math.min(50, Math.max(1, parseInt(url.searchParams.get('limit') || '20') || 20));
  const before = url.searchParams.get('before');

  let q = db.from('user_notifications')
    .select('id, kind, title, body, href, read_at, created_at')
    .eq('user_id', auth.userId)
    .order('created_at', { ascending: false })
    .limit(limit + 1);
  if (before) q = q.lt('created_at', before);

  const [list, unread] = await Promise.all([
    q,
    db.from('user_notifications').select('id', { count: 'exact', head: true }).eq('user_id', auth.userId).is('read_at', null),
  ]);
  // Before migration 09 the table doesn't exist: an empty inbox, not an error.
  if (list.error) return ok({ items: [], unread: 0, has_more: false }, request, env);
  const rows = list.data || [];
  return ok({ items: rows.slice(0, limit), unread: unread.count ?? 0, has_more: rows.length > limit }, request, env);
}

export async function handleReadMyNotifications(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const body = await request.json().catch(() => ({})) as any;
  const ids: string[] = Array.isArray(body?.ids) ? body.ids.filter((x: any) => typeof x === 'string').slice(0, 100) : [];

  let q = db.from('user_notifications')
    .update({ read_at: new Date().toISOString() })
    .eq('user_id', auth.userId)
    .is('read_at', null);
  if (ids.length) q = q.in('id', ids);
  const { error } = await q;
  if (error) return err(error.message, 500, request, env);
  return ok({ read: true }, request, env);
}
