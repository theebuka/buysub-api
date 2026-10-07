// ============================================================
// BUYSUB — Support conversations (migration 18)
// ============================================================
// Customers and partners open a thread with BuySub and both sides reply in
// it. Plain text only; the web app polls while a thread is open.
//
// Signed-in user:
// GET    /v2/me/support?audience=customer|partner   threads, newest activity first
// POST   /v2/me/support                              { subject, body, audience?, order_ref? } → thread
// GET    /v2/me/support/:id                          { thread, messages }; marks staff replies read
// POST   /v2/me/support/:id/messages                 { body } → message
// POST   /v2/me/support/:id/close                    marks it resolved
//
// Staff:
// GET    /v2/admin/support?status=open|closed|all&q=  threads with the user's name and email
// GET    /v2/admin/support/:id                        { thread, messages, user }; marks user messages read
// POST   /v2/admin/support/:id/messages               { body } → message; notifies the user
// PATCH  /v2/admin/support/:id                        { status: 'open'|'closed' }
//
// Before migration 18 every route returns 503 and the web shows a notice.

import type { SupabaseClient } from '@supabase/supabase-js';
import { type Env, ok, err, requireAuth, requireAdmin, orSafe, logEvent } from '../http';
import { notifyUser } from './core';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const THREAD_COLS = 'id, audience, subject, order_ref, status, user_unread, admin_unread, last_sender, last_message_preview, last_message_at, created_at, closed_at';
const MAX_BODY = 4000;
const MAX_OPEN = 20;

function unavailable(request: Request, env: Env, message: string) {
  console.error('support:', message);
  return err('Support chat isn’t available right now', 503, request, env);
}

function cleanBody(v: unknown): string {
  return String(v ?? '').replace(/\r\n/g, '\n').trim().slice(0, MAX_BODY);
}

async function isPartner(db: SupabaseClient, userId: string): Promise<boolean> {
  const { data } = await db.from('affiliates').select('id').eq('user_id', userId).limit(1);
  return !!data?.length;
}

// ── User ────────────────────────────────────────────────────────

export async function handleMySupportThreads(db: SupabaseClient, url: URL, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  let q = db.from('support_threads').select(THREAD_COLS).eq('user_id', auth.userId)
    .order('last_message_at', { ascending: false }).limit(100);
  const audience = url.searchParams.get('audience');
  if (audience === 'customer' || audience === 'partner') q = q.eq('audience', audience);
  const { data, error } = await q;
  if (error) return unavailable(request, env, error.message);
  return ok(data || [], request, env);
}

export async function handleCreateSupportThread(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  const body = await request.json().catch(() => ({})) as any;
  const subject = String(body?.subject ?? '').replace(/\s+/g, ' ').trim().slice(0, 140);
  const text = cleanBody(body?.body);
  if (!subject) return err('Add a subject', 400, request, env);
  if (!text) return err('Write a message', 400, request, env);
  const orderRef = String(body?.order_ref ?? '').trim().toUpperCase().slice(0, 40) || null;
  // Partner threads only for accounts that are partners; anything else is a customer thread.
  const audience = body?.audience === 'partner' && await isPartner(db, auth.userId) ? 'partner' : 'customer';

  const { count, error: countErr } = await db.from('support_threads').select('id', { count: 'exact', head: true })
    .eq('user_id', auth.userId).eq('status', 'open');
  if (countErr) return unavailable(request, env, countErr.message);
  if ((count ?? 0) >= MAX_OPEN) return err('You have a lot of open conversations. Reply in one of those, or close some first.', 429, request, env);

  const { data: thread, error } = await db.from('support_threads')
    .insert({ user_id: auth.userId, audience, subject, order_ref: orderRef }).select('id').single();
  if (error || !thread) return unavailable(request, env, error?.message || 'insert failed');
  const { error: msgErr } = await db.rpc('post_support_message', { p_thread_id: thread.id, p_sender: 'user', p_sender_id: auth.userId, p_body: text });
  if (msgErr) {
    await db.from('support_threads').delete().eq('id', thread.id);
    return unavailable(request, env, msgErr.message);
  }
  await logEvent(db, 'support_thread', thread.id, 'opened', auth.userId, { audience });
  const { data } = await db.from('support_threads').select(THREAD_COLS).eq('id', thread.id).single();
  return ok(data, request, env);
}

async function threadFor(db: SupabaseClient, id: string, userId: string | null) {
  let q = db.from('support_threads').select(`${THREAD_COLS}, user_id`).eq('id', id);
  if (userId) q = q.eq('user_id', userId);
  return q.maybeSingle();
}

async function messagesOf(db: SupabaseClient, threadId: string) {
  return db.from('support_messages').select('id, sender, body, created_at')
    .eq('thread_id', threadId).order('created_at', { ascending: true }).limit(500);
}

export async function handleMySupportThread(db: SupabaseClient, id: string, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  if (!UUID_RE.test(id)) return err('Conversation not found', 404, request, env);
  const { data: thread, error } = await threadFor(db, id, auth.userId);
  if (error) return unavailable(request, env, error.message);
  if (!thread) return err('Conversation not found', 404, request, env);
  const { data: messages, error: mErr } = await messagesOf(db, id);
  if (mErr) return unavailable(request, env, mErr.message);
  if (thread.user_unread) await db.from('support_threads').update({ user_unread: 0 }).eq('id', id);
  const { user_id: _u, ...t } = thread as any;
  return ok({ thread: { ...t, user_unread: 0 }, messages: messages || [] }, request, env);
}

export async function handleMySupportReply(db: SupabaseClient, id: string, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  if (!UUID_RE.test(id)) return err('Conversation not found', 404, request, env);
  const body = await request.json().catch(() => ({})) as any;
  const text = cleanBody(body?.body);
  if (!text) return err('Write a message', 400, request, env);
  const { data: thread, error } = await threadFor(db, id, auth.userId);
  if (error) return unavailable(request, env, error.message);
  if (!thread) return err('Conversation not found', 404, request, env);
  const { data: msg, error: msgErr } = await db.rpc('post_support_message', { p_thread_id: id, p_sender: 'user', p_sender_id: auth.userId, p_body: text });
  if (msgErr) return unavailable(request, env, msgErr.message);
  return ok({ id: msg.id, sender: msg.sender, body: msg.body, created_at: msg.created_at }, request, env);
}

export async function handleMySupportClose(db: SupabaseClient, id: string, request: Request, env: Env): Promise<Response> {
  const auth = await requireAuth(db, request, env);
  if (!auth.ok) return auth.response;
  if (!UUID_RE.test(id)) return err('Conversation not found', 404, request, env);
  const { data, error } = await db.from('support_threads')
    .update({ status: 'closed', closed_at: new Date().toISOString() })
    .eq('id', id).eq('user_id', auth.userId).select(THREAD_COLS).maybeSingle();
  if (error) return unavailable(request, env, error.message);
  if (!data) return err('Conversation not found', 404, request, env);
  return ok(data, request, env);
}

// ── Staff ───────────────────────────────────────────────────────

async function usersById(db: SupabaseClient, ids: string[]) {
  const map = new Map<string, { full_name: string | null; email: string | null }>();
  if (!ids.length) return map;
  const { data } = await db.from('profiles').select('id, full_name, email').in('id', [...new Set(ids)]);
  for (const p of data || []) map.set(p.id, { full_name: p.full_name ?? null, email: p.email ?? null });
  return map;
}

export async function handleAdminSupportThreads(db: SupabaseClient, url: URL, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
  const status = url.searchParams.get('status') || 'open';
  const q = orSafe(url.searchParams.get('q') || '');
  let query = db.from('support_threads').select(`${THREAD_COLS}, user_id`, { count: 'exact' })
    .order('last_message_at', { ascending: false }).limit(200);
  if (status === 'open' || status === 'closed') query = query.eq('status', status);
  if (q) {
    const { data: people } = await db.from('profiles').select('id').or(`email.ilike.%${q}%,full_name.ilike.%${q}%`).limit(50);
    const ids = (people || []).map(p => p.id);
    query = ids.length
      ? query.or(`subject.ilike.%${q}%,order_ref.ilike.%${q}%,user_id.in.(${ids.join(',')})`)
      : query.or(`subject.ilike.%${q}%,order_ref.ilike.%${q}%`);
  }
  const { data, error, count } = await query;
  if (error) return unavailable(request, env, error.message);
  const people = await usersById(db, (data || []).map(t => t.user_id));
  const rows = (data || []).map(t => ({ ...t, user: people.get(t.user_id) || null }));
  return ok(rows, request, env, { total: count ?? rows.length });
}

export async function handleAdminSupportThread(db: SupabaseClient, id: string, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
  if (!UUID_RE.test(id)) return err('Conversation not found', 404, request, env);
  const { data: thread, error } = await threadFor(db, id, null);
  if (error) return unavailable(request, env, error.message);
  if (!thread) return err('Conversation not found', 404, request, env);
  const { data: messages, error: mErr } = await messagesOf(db, id);
  if (mErr) return unavailable(request, env, mErr.message);
  if (thread.admin_unread) await db.from('support_threads').update({ admin_unread: 0 }).eq('id', id);
  const people = await usersById(db, [thread.user_id]);
  return ok({ thread: { ...thread, admin_unread: 0, user: people.get(thread.user_id) || null }, messages: messages || [] }, request, env);
}

export async function handleAdminSupportReply(db: SupabaseClient, id: string, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
  if (!UUID_RE.test(id)) return err('Conversation not found', 404, request, env);
  const body = await request.json().catch(() => ({})) as any;
  const text = cleanBody(body?.body);
  if (!text) return err('Write a message', 400, request, env);
  const { data: thread, error } = await threadFor(db, id, null);
  if (error) return unavailable(request, env, error.message);
  if (!thread) return err('Conversation not found', 404, request, env);
  const { data: msg, error: msgErr } = await db.rpc('post_support_message', { p_thread_id: id, p_sender: 'admin', p_sender_id: auth.userId, p_body: text });
  if (msgErr) return unavailable(request, env, msgErr.message);
  await notifyUser(db, thread.user_id, {
    kind: 'support_reply',
    title: `BuySub replied: ${thread.subject}`,
    body: text.length > 140 ? `${text.slice(0, 137)}…` : text,
    href: `${thread.audience === 'partner' ? '/partner/support' : '/account/support'}?t=${id}`,
  });
  return ok({ id: msg.id, sender: msg.sender, body: msg.body, created_at: msg.created_at }, request, env);
}

export async function handleAdminSupportUpdate(db: SupabaseClient, id: string, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
  if (!UUID_RE.test(id)) return err('Conversation not found', 404, request, env);
  const body = await request.json().catch(() => ({})) as any;
  const status = body?.status;
  if (status !== 'open' && status !== 'closed') return err('Status must be open or closed', 400, request, env);
  const { data, error } = await db.from('support_threads')
    .update({ status, closed_at: status === 'closed' ? new Date().toISOString() : null })
    .eq('id', id).select(THREAD_COLS).maybeSingle();
  if (error) return unavailable(request, env, error.message);
  if (!data) return err('Conversation not found', 404, request, env);
  await logEvent(db, 'support_thread', id, status === 'closed' ? 'closed' : 'reopened', auth.userId, {});
  return ok(data, request, env);
}

/** For /v2/admin/stats: open threads waiting on staff. 0 before migration 18. */
export async function supportWaitingCount(db: SupabaseClient): Promise<number> {
  const { count, error } = await db.from('support_threads').select('id', { count: 'exact', head: true })
    .eq('status', 'open').eq('last_sender', 'user');
  return error ? 0 : count ?? 0;
}
