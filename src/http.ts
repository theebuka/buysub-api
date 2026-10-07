// ============================================================
// BUYSUB — HTTP helpers shared by every handler module
// ============================================================
// Moved out of index.ts unchanged, so handler modules under src/features/
// can use the same envelope, CORS, auth checks and event log.

import type { SupabaseClient } from '@supabase/supabase-js';
import type { ApiResponse } from './shared/types';

export interface Env {
  SUPABASE_URL: string;
  SUPABASE_SERVICE_ROLE_KEY: string;
  PAYSTACK_SECRET_KEY: string;
  PAYSTACK_PUBLIC_KEY: string;
  RESEND_API_KEY: string;
  WHATSAPP_NUMBER: string;
  FRONTEND_URL: string;        // https://app.buysub.ng
  WEBHOOK_SECRET: string;       // for verifying internal webhooks
  ALLOWED_ORIGINS: string;      // comma-separated
}

export function corsHeaders(request: Request, env: Env): Record<string, string> {
  const origin = request.headers.get('Origin') || '';
  const allowed = env.ALLOWED_ORIGINS?.split(',').map(s => s.trim()) || [];
  const isAllowed = allowed.includes(origin) || allowed.includes('*');
  return {
    'Access-Control-Allow-Origin': isAllowed ? origin : allowed[0] || '',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Api-Key',
    'Access-Control-Max-Age': '86400',
  };
}

export function jsonResponse<T>(data: ApiResponse<T>, status: number, request: Request, env: Env): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...corsHeaders(request, env) },
  });
}

export function ok<T>(data: T, request: Request, env: Env, meta?: Record<string, any>): Response {
  return jsonResponse({ ok: true, data, meta }, 200, request, env);
}

export function err(message: string, status: number, request: Request, env: Env): Response {
  return jsonResponse({ ok: false, error: message }, status, request, env);
}

export const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

// Escape LIKE wildcards so a value can be used for an exact, case-insensitive ilike.
export function escapeLike(s: string): string {
  return s.replace(/[\\%_]/g, c => '\\' + c);
}

// Search text going into a PostgREST .or() filter: strip the characters that
// delimit filters (commas, parentheses, quotes) and the * wildcard, so input
// can't add conditions of its own.
export function orSafe(q: string): string {
  return escapeLike(q.replace(/[,()"*]/g, ' ').trim()).slice(0, 100);
}

export async function logEvent(
  db: SupabaseClient,
  entity: string,
  entityId: string,
  action: string,
  actorId: string | null,
  metadata: Record<string, any>,
): Promise<void> {
  try {
    await db.from('event_logs').insert({
      entity,
      entity_id: entityId,
      action,
      actor_id: actorId,
      metadata,
    });
  } catch {} // non-critical
}

export const STAFF_ROLES = ['admin', 'super_admin', 'support_agent'];

export async function requireAdmin(
  db: SupabaseClient, request: Request, env: Env
): Promise<{ ok: true; userId: string } | { ok: false; response: Response }> {
  const auth = request.headers.get('Authorization')?.replace('Bearer ', '');
  if (!auth) return { ok: false, response: err('Unauthorized', 401, request, env) };

  const { data: { user }, error: authErr } = await db.auth.getUser(auth);
  if (authErr || !user) return { ok: false, response: err('Invalid token', 401, request, env) };

  const { data: profile } = await db
    .from('profiles')
    .select('role')
    .eq('id', user.id)
    .single();

  if (!profile || !STAFF_ROLES.includes(profile.role)) {
    return { ok: false, response: err('Forbidden — admin access required', 403, request, env) };
  }

  return { ok: true, userId: user.id };
}

// User id from the Bearer token.
export async function requireAuth(
  db: any, request: Request, env: any
): Promise<{ ok: true; userId: string; email: string } | { ok: false; response: Response }> {
  const authHeader = request.headers.get('Authorization') || ''
  const token = authHeader.replace('Bearer ', '').trim()
  if (!token) return { ok: false, response: err('Unauthorized', 401, request, env) }
 
  // Verify JWT with Supabase
  const { data, error } = await db.auth.getUser(token)
  if (error || !data?.user) return { ok: false, response: err('Unauthorized', 401, request, env) }
  return { ok: true, userId: data.user.id, email: data.user.email || '' }
}
