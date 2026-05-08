// ============================================================
// BUYSUB — CLOUDFLARE WORKERS API (v2)
// ============================================================
// All sensitive logic validated server-side. Never trust frontend.
// Uses Supabase service_role key for DB access (bypasses RLS).
// ============================================================

import { createClient, SupabaseClient } from '@supabase/supabase-js';
import type {
  ApiResponse, CartItemPayload, CreateOrderRequest, DiscountCode,
  PaystackInitRequest, AdminApproveRequest, Order,
} from './shared/types';
import {
  validateAndCalcDiscount, getEligibleSubtotalNGN, isItemEligibleForDiscount,
  calcDiscountNGN, buildDiscountDisplay, splitList, norm,
} from './shared/discount';

// ── Environment bindings ──
interface Env {
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

// ── Supabase client factory ──
function getSupabase(env: Env): SupabaseClient {
  return createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
}

// ── CORS headers ──
function corsHeaders(request: Request, env: Env): Record<string, string> {
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

function jsonResponse<T>(data: ApiResponse<T>, status: number, request: Request, env: Env): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...corsHeaders(request, env) },
  });
}

function ok<T>(data: T, request: Request, env: Env, meta?: Record<string, any>): Response {
  return jsonResponse({ ok: true, data, meta }, 200, request, env);
}

function err(message: string, status: number, request: Request, env: Env): Response {
  return jsonResponse({ ok: false, error: message }, status, request, env);
}

// ── Router ──
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    
    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(request, env) });
    }

    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, ''); // strip trailing slash
    const method = request.method;
    const db = getSupabase(env);

    try {
      // ── Products ──
      if (path === '/v2/products' && method === 'GET') {
        return handleGetProducts(db, url, request, env);
      }
      if (path.startsWith('/v2/products/') && method === 'GET') {
        const slug = path.split('/v2/products/')[1];
        return handleGetProductBySlug(db, slug, request, env);
      }

      // ── Discounts ──
      if (path === '/v2/discount/validate' && method === 'POST') {
        return handleValidateDiscount(db, request, env);
      }
      if (path === '/v2/discount/auto-apply' && method === 'GET') {
        return handleAutoApplyDiscounts(db, request, env);
      }

      // ── Orders ──
      if (path === '/v2/orders' && method === 'POST') {
        return handleCreateOrder(db, request, env);
      }
      if (path === '/v2/orders/whatsapp' && method === 'POST') {
        return handleWhatsAppOrder(db, request, env);
      }

      // ── Payments ──
      if (path === '/v2/pay/init' && method === 'POST') {
        return handlePaystackInit(db, request, env);
      }
      if (path === '/v2/pay/webhook' && method === 'POST') {
        return handlePaystackWebhook(db, request, env, ctx);
      }
      if (path === '/v2/pay/verify' && method === 'GET') {
        const ref = url.searchParams.get('reference');
        return handlePaystackVerify(db, ref, request, env);
      }

      // ── Admin ──
      if (path === '/v2/admin/orders' && method === 'GET') {
        return handleAdminGetOrders(db, url, request, env);
      }
      // if (path === '/v2/admin/orders/approve' && method === 'POST') {
      //   return handleAdminApproveOrder(db, request, env);
      // }
      if (path.startsWith('/v2/admin/orders/') && method === 'GET') {
        const ref = path.split('/v2/admin/orders/')[1];
        return handleAdminGetOrder(db, ref, request, env);
      }
      // ── Admin Manual Order Creation ──
      if (path === '/v2/admin/orders' && method === 'POST') {
        return handleAdminCreateOrder(db, request, env);
      }

      // ── Customers ──
      if (path === '/v2/customers/search' && method === 'GET') {
        return handleSearchCustomers(db, url, request, env);
      }

      // ── Health ──
      if (path === '/v2/health') {
        return ok({ status: 'ok', timestamp: new Date().toISOString() }, request, env);
      }

      // ════════════════════════════════════════════════════════
      // PHASE 3 ROUTES — Admin Dashboard, Partners, Receipts
      // ════════════════════════════════════════════════════════

      // ── Admin Stats (Dashboard overview) ──
      if (path === '/v2/admin/stats' && method === 'GET') {
        return handleAdminStats(db, request, env);
      }

      // ── Admin Customers ──
      if (path === '/v2/admin/customers' && method === 'GET') {
        return handleAdminCustomers(db, url, request, env);
      }
      if (path === '/v2/admin/customers/search' && method === 'GET') {
        return handleAdminCustomerSearch(db, url, request, env);
      }
      // GET /v2/admin/customers/:id/wallet  (for reading current balance in the panel)
      if (path.match(/^\/v2\/admin\/customers\/[^/]+\/wallet$/) && method === 'GET') {
        return handleAdminGetCustomerWallet(db, request, env)
      }

      // ── Admin Products ──
      if (path === '/v2/admin/products' && method === 'GET') {
        return handleAdminProducts(db, url, request, env);
      }
      if (path === '/v2/admin/products' && method === 'POST') {
        return handleAdminCreateProduct(db, request, env);
      }
      if (path.startsWith('/v2/admin/products/') && method === 'PATCH') {
        const productId = path.split('/v2/admin/products/')[1];
        return handleAdminUpdateProduct(db, productId, request, env);
      }

      // ── Admin Discounts (CRUD) ──
      if (path === '/v2/admin/discounts' && method === 'GET') {
        return handleAdminGetDiscounts(db, url, request, env);
      }
      if (path === '/v2/admin/discounts' && method === 'POST') {
        return handleAdminCreateDiscount(db, request, env);
      }
      if (path.match(/^\/v2\/admin\/discounts\/[^/]+$/) && method === 'PATCH') {
        const discountId = path.split('/v2/admin/discounts/')[1];
        return handleAdminUpdateDiscount(db, discountId, request, env);
      }
      if (path.match(/^\/v2\/admin\/discounts\/[^/]+$/) && method === 'DELETE') {
        const discountId = path.split('/v2/admin/discounts/')[1];
        return handleAdminDeleteDiscount(db, discountId, request, env);
      }

      // ── Admin Order actions (approve/reject with ref in URL) ──
      if (path.match(/^\/v2\/admin\/orders\/[^/]+\/approve$/) && method === 'POST') {
        const ref = path.split('/')[4];
        return handleAdminApproveOrderV2(db, ref, request, env);
      }
      if (path.match(/^\/v2\/admin\/orders\/[^/]+\/reject$/) && method === 'POST') {
        const ref = path.split('/')[4];
        return handleAdminRejectOrder(db, ref, request, env);
      }
      if (path.match(/^\/v2\/admin\/orders\/[^/]+\/undo-reject$/) && method === 'POST') {
        const ref = path.split('/')[4];
        return handleAdminUndoReject(db, ref, request, env);
      }

      // ── Partners (public submission) ──
      if (path === '/v2/partners' && method === 'POST') {
        return handleSubmitPartnerApplication(db, request, env);
      }

      // ── Admin Partners ──
      if (path === '/v2/admin/partners' && method === 'GET') {
        return handleAdminPartners(db, url, request, env);
      }
      if (path.match(/^\/v2\/admin\/partners\/[^/]+\/approve$/) && method === 'POST') {
        const id = path.split('/')[4];
        return handleAdminApprovePartner(db, id, request, env);
      }
      if (path.match(/^\/v2\/admin\/partners\/[^/]+\/reject$/) && method === 'POST') {
        const id = path.split('/')[4];
        return handleAdminRejectPartner(db, id, request, env);
      }

      // ── Admin Wallets ──
      if (path === '/v2/admin/wallets' && method === 'GET') {
        return handleAdminWallets(db, url, request, env);
      }

      // ── Discount Validation (for receipt generator) ──
      if (path === '/v2/discounts/validate' && method === 'GET') {
        return handleValidateDiscountV2(db, url, request, env);
      }

      // ── Notifications ──
      if (path === '/v2/admin/notifications' && method === 'POST') {
        const auth = await requireAdmin(db, request, env);
        if (!auth.ok) return auth.response;

        const body = await request.json().catch(() => null) as any;

        if (!body?.type || (!body?.steps?.length && !body?.message)) {
          return err('Message or steps required', 400, request, env);
        }

        const { data, error } = await db
          .from('notifications')
          .insert([{
            title: body.title || null,
            message: body.steps?.length ? null : body.message,
            type: body.type,
            active: true,
            audience: body.audience || 'all',
            image_url: body.image_url || null,
            image_position: body.image_position || 'top',
            steps: body.steps?.length ? body.steps : null,
            scheduled_for: body.scheduled_for || null,
            expires_at: body.expires_at || null,
          }])
          .select()
          .single();

        if (error) return err(error.message, 500, request, env);

        return ok(data, request, env);
      }

      if (path === '/v2/notifications' && method === 'GET') {
        const now = new Date().toISOString()
      
        // extract user (if logged in)
        const token = request.headers.get('Authorization')?.replace('Bearer ', '')
        let role = 'public'
      
        if (token) {
          const { data: userData } = await db.auth.getUser(token)
          const userId = userData?.user?.id
      
          if (userId) {
            const { data: profile } = await db
              .from('profiles')
              .select('role')
              .eq('id', userId)
              .single()
      
            role = profile?.role === 'admin' ? 'admins' : 'users'
          }
        }
      
        const { data, error } = await db
          .from('notifications')
          .select('*')
          .eq('active', true)
          .in('audience', ['all', 'users', 'admins'])

        if (error) return err(error.message, 500, request, env)

        // ✅ filter in JS (this is the fix)
        const filtered = (data || []).filter((n: any) => {
          const scheduledOk =
            !n.scheduled_for || n.scheduled_for <= now

          const expiryOk =
            !n.expires_at || n.expires_at > now

          return scheduledOk && expiryOk
        })

        return ok(
          filtered
            .sort((a: any, b: any) =>
              new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
            )
            .slice(0, 5),
          request,
          env
        )
      }

      if (path === '/v2/admin/notifications' && method === 'GET') {
        const auth = await requireAdmin(db, request, env)
        if (!auth.ok) return auth.response
      
        const { data, error } = await db
          .from('notifications')
          .select('*')
          .order('created_at', { ascending: false })
          .limit(50)
      
        if (error) return err(error.message, 500, request, env)
      
        return ok(data, request, env)
      }

      if (path.startsWith('/v2/admin/notifications/') && method === 'PUT') {
        const auth = await requireAdmin(db, request, env)
        if (!auth.ok) return auth.response
      
        const id = path.split('/').pop()
        const body = await request.json().catch(() => null) as any
      
        const { data, error } = await db
          .from('notifications')
          .update({ active: body.active })
          .eq('id', id)
          .select()
          .single()
      
        if (error) return err(error.message, 500, request, env)
      
        return ok(data, request, env)
      }

// ════════════════════════════════════════════════════════
      // PHASE 4 ROUTES — Affiliates, Short Links, Ads
      // ════════════════════════════════════════════════════════
 
      // ── Affiliates (public — track click) ──
      if (path === '/v2/affiliates/click' && method === 'POST') {
        return handleAffiliateClick(db, request, env);
      }
 
      // ── Affiliates (authenticated — own dashboard) ──
      if (path === '/v2/affiliates/me' && method === 'GET') {
        return handleAffiliateMe(db, request, env);
      }
      if (path === '/v2/affiliates/me/stats' && method === 'GET') {
        return handleAffiliateMyStats(db, request, env);
      }
      if (path === '/v2/affiliates/me/commissions' && method === 'GET') {
        return handleAffiliateMyCommissions(db, url, request, env);
      }
      if (path === '/v2/affiliates/resolve' && method === 'GET') {
        return handleAffiliateResolve(db, url, request, env);
      }
 
      // ── Admin Affiliates ──
      if (path === '/v2/admin/affiliates' && method === 'GET') {
        return handleAdminAffiliates(db, url, request, env);
      }
      if (path.match(/^\/v2\/admin\/affiliates\/[^/]+\/approve$/) && method === 'POST') {
        const id = path.split('/')[4];
        return handleAdminApproveAffiliate(db, id, request, env);
      }
      if (path.match(/^\/v2\/admin\/affiliates\/[^/]+\/suspend$/) && method === 'POST') {
        const id = path.split('/')[4];
        return handleAdminSuspendAffiliate(db, id, request, env);
      }
 
      // ── Short Links (admin) ──
      if (path === '/v2/admin/links' && method === 'GET')   return handleAdminGetLinks(db, url, request, env);
      if (path === '/v2/admin/links' && method === 'POST')  return handleAdminCreateLink(db, request, env);
      const linkMatch = path.match(/^\/v2\/admin\/links\/([^/]+)$/);
      if (linkMatch && method === 'GET')    return handleAdminGetLink(db, linkMatch[1], request, env);
      if (linkMatch && method === 'PATCH')  return handleAdminUpdateLink(db, linkMatch[1], request, env);
      if (linkMatch && method === 'DELETE') return handleAdminDeleteLink(db, linkMatch[1], request, env);
      const statsMatch = path.match(/^\/v2\/admin\/links\/([^/]+)\/stats$/);
      if (statsMatch && method === 'GET')   return handleAdminLinkStats(db, statsMatch[1], request, env);
      const rulesMatch = path.match(/^\/v2\/admin\/links\/([^/]+)\/rules$/);
      if (rulesMatch && method === 'GET')   return handleAdminGetLinkRules(db, rulesMatch[1], request, env);
      if (rulesMatch && method === 'POST')  return handleAdminCreateLinkRule(db, rulesMatch[1], request, env);
      const ruleMatch = path.match(/^\/v2\/admin\/links\/[^/]+\/rules\/([^/]+)$/);
      if (ruleMatch && method === 'PATCH')  return handleAdminUpdateLinkRule(db, ruleMatch[1], request, env);
      if (ruleMatch && method === 'DELETE') return handleAdminDeleteLinkRule(db, ruleMatch[1], request, env);

 
      // ── Ads (public — get ads for a placement) ──
      if (path === '/v2/ads' && method === 'GET') {
        return handleGetAds(db, url, request, env);
      }
      if (path === '/v2/ads/click' && method === 'POST') {
        return handleAdClick(db, request, env);
      }
      if (path === '/v2/ads/impression' && method === 'POST') {
        return handleAdImpression(db, request, env);
      }
 
      // ── Admin Ads ──
      if (path === '/v2/admin/ads' && method === 'GET') {
        return handleAdminGetAds(db, url, request, env);
      }
      if (path === '/v2/admin/ads' && method === 'POST') {
        return handleAdminCreateAd(db, request, env);
      }
      if (path.match(/^\/v2\/admin\/ads\/[^/]+$/) && method === 'PATCH') {
        const id = path.split('/')[4];
        return handleAdminUpdateAd(db, id, request, env);
      }
      if (path.match(/^\/v2\/admin\/ads\/[^/]+$/) && method === 'DELETE') {
        const id = path.split('/')[4];
        return handleAdminDeleteAd(db, id, request, env);
      }

      // Partner dashboard (self-service)
      if (path === '/v2/partners/me' && method === 'GET') {
        return handlePartnerMe(db, request, env);
      }
      if (path === '/v2/partners/me' && method === 'PATCH') {
        return handlePartnerUpdateMe(db, request, env);
      }
      if (path === '/v2/partners/me/stats' && method === 'GET') {
        return handlePartnerMyStats(db, request, env);
      }

      // ── Admin Settings ──
      if (path === '/v2/admin/settings' && method === 'GET') {
        return handleGetSettings(db, request, env)
      }
      if (path === '/v2/admin/settings' && method === 'PATCH') {
        return handleUpdateSettings(db, request, env)
      }

      // Customer auth
      if (path === '/v2/auth/signup'           && method === 'POST')  return handleCustomerSignup(db, request, env)
    
      // Authenticated customer endpoints
      if (path === '/v2/me'                    && method === 'GET')   return handleGetMe(db, request, env)
      if (path === '/v2/me'                    && method === 'PATCH') return handleUpdateMe(db, request, env)
      if (path === '/v2/me/orders'             && method === 'GET')   return handleGetMyOrders(db, request, env)
      if (path === '/v2/me/wallet'             && method === 'GET')   return handleGetMyWallet(db, request, env)
      if (path === '/v2/me/wallet/transactions'&& method === 'GET')   return handleGetMyWalletTxns(db, request, env)
      if (path === '/v2/me/messages'           && method === 'GET')   return handleGetMyMessages(db, request, env)
      if (path.match(/^\/v2\/me\/messages\/[^/]+\/read$/) && method === 'PATCH') return handleMarkMessageRead(db, request, env)
    
      // Admin: send message to customer
      if (path.match(/^\/v2\/admin\/customers\/[^/]+\/messages$/) && method === 'POST') return handleAdminSendMessage(db, request, env)
    
      // Admin: top up wallet
      if (path.match(/^\/v2\/admin\/customers\/[^/]+\/wallet\/topup$/) && method === 'POST') return handleAdminWalletTopup(db, request, env)

      // Admin: debit wallet (body: { customer_id, amount, reference })
      if (path === '/v2/admin/wallet/debit' && method === 'POST') return handleAdminWalletDebit(db, request, env)

      if (path.match(/^\/v2\/admin\/customers\/[^/]+\/wallet\/toggle$/) && method === 'POST') {
        return handleAdminToggleWallet(db, request, env)
      }
    
      // Admin: force reset password
      if (path.match(/^\/v2\/admin\/customers\/[^/]+\/reset-password$/) && method === 'POST') return handleAdminForceReset(db, request, env)

      return err('Not found', 404, request, env);
    } catch (e: any) {
      console.error('Unhandled error:', e);
      return err('Internal server error', 500, request, env);
    }
  },
};


// ============================================================
// HANDLER: GET /v2/products
// ============================================================
async function handleGetProducts(
  db: SupabaseClient, url: URL, request: Request, env: Env,
): Promise<Response> {
  const category = url.searchParams.get('category');
  const status = url.searchParams.get('status') || 'active';
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '500'), 1000);
  const offset = parseInt(url.searchParams.get('offset') || '0');

  let query = db.from('products')
    .select('*', { count: 'exact' })
    .is('deleted_at', null)
    .eq('status', status)
    .order('sort_order', { ascending: true })
    .order('name', { ascending: true })
    .limit(limit);

  if (offset > 0) {
    query = query.range(offset, offset + limit - 1);
  }

  if (category && category !== 'all') {
    query = query.ilike('category', `%${category}%`);
  }

  const { data, error, count } = await query;
  if (error) return err(error.message, 500, request, env);
  return ok(data, request, env, { count: count ?? data?.length, offset, limit });
}


// ============================================================
// HANDLER: GET /v2/products/:slug
// ============================================================
async function handleGetProductBySlug(
  db: SupabaseClient, slug: string, request: Request, env: Env,
): Promise<Response> {
  const { data, error } = await db.from('products')
    .select('*')
    .eq('slug', slug)
    .is('deleted_at', null)
    .single();

  if (error || !data) return err('Product not found', 404, request, env);
  return ok(data, request, env);
}


// ============================================================
// HANDLER: POST /v2/discount/validate
// ============================================================
async function handleValidateDiscount(
  db: SupabaseClient, request: Request, env: Env,
): Promise<Response> {
  const body = await request.json() as {
    code: string;
    items: CartItemPayload[];
    is_manual: boolean;
  };

  if (!body.code || !body.items?.length) {
    return err('Code and items are required', 400, request, env);
  }

  const code = body.code.trim().toUpperCase();
  const { data: discount, error } = await db.from('discount_codes')
    .select('*')
    .eq('code', code)
    .single();

  if (error || !discount) {
    return err('Code not found or inactive.', 404, request, env);
  }

  const result = validateAndCalcDiscount(
    discount as DiscountCode,
    body.items,
    body.is_manual !== false, // default to manual
  );

  if (!result.valid) {
    return jsonResponse({ ok: false, error: result.error }, 400, request, env);
  }

  return ok({
    valid: true,
    code: discount.code,
    type: discount.type,
    value: discount.value,
    display: result.display,
    discount_ngn: result.discount_ngn,
    eligible_subtotal_ngn: result.eligible_subtotal_ngn,
    is_auto_apply: discount.auto_apply,
    is_exclusive: discount.exclusive,
  }, request, env);
}


// ============================================================
// HANDLER: GET /v2/discount/auto-apply
// ============================================================
async function handleAutoApplyDiscounts(
  db: SupabaseClient, request: Request, env: Env,
): Promise<Response> {
  const { data, error } = await db.from('discount_codes')
    .select('*')
    .eq('active', true)
    .eq('auto_apply', true);

  if (error) return err(error.message, 500, request, env);

  const discounts = (data || []).map((d: any) => ({
    code: d.code,
    type: d.type,
    value: d.value,
    display: buildDiscountDisplay(d as DiscountCode),
    max_discount_ngn: d.max_discount_ngn,
    min_order_ngn: d.min_order_ngn,
    included_products: d.included_products,
    excluded_products: d.excluded_products,
    included_categories: d.included_categories,
    excluded_categories: d.excluded_categories,
    scope: d.scope,
    exclusive: d.exclusive,
  }));

  return ok({ discounts }, request, env);
}


// ============================================================
// HANDLER: POST /v2/orders  (Paystack checkout)
// ============================================================
async function handleCreateOrder(
  db: SupabaseClient, request: Request, env: Env,
): Promise<Response> {
  try {
    const body = await request.json() as CreateOrderRequest;

  // Validate required fields
  if (!body.customer_email || !body.items?.length) {
    return err('Email and items are required', 400, request, env);
  }

  // ── Server-side price validation ──
  const productIds = body.items.map(i => i.product_id);
  const { data: products, error: pErr } = await db.from('products')
    .select('*')
    .in('id', productIds);

  if (pErr || !products?.length) {
    return err('Could not validate product prices', 400, request, env);
  }

  const productMap = new Map(products.map((p: any) => [p.id, p]));
  let serverSubtotal = 0;

  for (const item of body.items) {
    const product = productMap.get(item.product_id);
    if (!product) return err(`Product ${item.product_name} not found`, 400, request, env);
    if (product.stock_status !== 'in_stock') {
      return err(`${item.product_name} is out of stock`, 400, request, env);
    }
    // Validate unit price against DB
    const priceField = getPriceField(item.billing_period);
    const dbPrice = product[priceField];
    if (dbPrice == null) {
      return err(`${item.product_name} is not available for ${item.billing_period}`, 400, request, env);
    }
    if (Math.abs(item.unit_price_ngn - dbPrice) > 1) {
      return err(`Price mismatch for ${item.product_name}. Expected ₦${dbPrice}, got ₦${item.unit_price_ngn}`, 400, request, env);
    }
    item.unit_price_ngn = dbPrice; // use DB price
    serverSubtotal += dbPrice * item.quantity;
  }

  // ── Discount validation (server-side) ──
  let discountNGN = 0;
  let discountCode: string | null = null;
  if (body.discount_code) {
    const { data: disc } = await db.from('discount_codes')
      .select('*')
      .eq('code', body.discount_code.toUpperCase())
      .single();

    if (disc) {
      const result = validateAndCalcDiscount(disc as DiscountCode, body.items, !disc.auto_apply);
      if (result.valid) {
        discountNGN = result.discount_ngn;
        discountCode = disc.code;
      }
      // If invalid, we silently ignore (don't block the order)
    }
  }

  // ── Wallet deduction ──
  let walletNGN = 0;
  // Wallet is handled during paystack init, not here

  // ── Affiliate lookup ──
  let affiliateId: string | null = null;
  if (body.affiliate_code) {
    const { data: aff } = await db.from('affiliates')
      .select('id, user_id')
      .eq('referral_code', body.affiliate_code)
      .eq('status', 'approved')
      .single();

    if (aff) {
      // Self-referral check: match affiliate's user_id to customer email
      // (customer may not have account, so we check email match via customers table)
      affiliateId = aff.id;
    }
  }

  const totalNGN = Math.max(0, serverSubtotal - discountNGN);

  // ── Find or create customer ──
  const customerId = await findOrCreateCustomer(db, {
    email: body.customer_email,
    name: body.customer_name,
    phone: body.customer_phone,
    source: body.payment_method,
  });

  // ── Generate order ref ──
  const { data: refData } = await db.rpc('generate_order_ref');
  const orderRef = refData as string;

  // ── Create order ──
  const { data: order, error: oErr } = await db.from('orders').insert({
    order_ref: orderRef,
    customer_id: customerId,
    customer_email: body.customer_email,
    customer_name: body.customer_name || null,
    customer_phone: body.customer_phone || null,
    status: 'pending',
    payment_method: body.payment_method,
    subtotal_ngn: serverSubtotal,
    discount_ngn: discountNGN,
    wallet_ngn: walletNGN,
    tax_ngn: 0,
    total_ngn: totalNGN,
    currency: body.currency || 'NGN',
    fx_rate: body.fx_rate || 1,
    display_total: totalNGN * (body.fx_rate || 1),
    discount_code: discountCode,
    affiliate_id: affiliateId,
  }).select().single();

  if (oErr || !order) {
    return err('Failed to create order: ' + (oErr?.message || 'unknown'), 500, request, env);
  }

  // ── Insert order items ──
  const orderItems = body.items.map(item => ({
    order_id: order.id,
    product_id: item.product_id,
    product_name: item.product_name,
    category: item.category,
    duration_months: item.duration_months,
    billing_period: item.billing_period,
    billing_type: item.billing_type,
    unit_price_ngn: item.unit_price_ngn,
    quantity: item.quantity,
    total_price_ngn: item.unit_price_ngn * item.quantity,
  }));

  await db.from('order_items').insert(orderItems);

  // ── Log event ──
  await logEvent(db, 'order', order.id, 'created', null, {
    order_ref: orderRef,
    payment_method: body.payment_method,
    total_ngn: totalNGN,
  });

  return ok({
    order_id: order.id,
    order_ref: orderRef,
    total_ngn: totalNGN,
    discount_ngn: discountNGN,
    status: 'pending',
  }, request, env);
  } catch (e: any) {
    console.error('Create order error:', e);
    return err('Failed to process order: ' + (e?.message || 'unknown error'), 500, request, env);
  }
}


// ============================================================
// HANDLER: POST /v2/orders/whatsapp
// ============================================================
async function handleWhatsAppOrder(
  db: SupabaseClient, request: Request, env: Env,
): Promise<Response> {
  try {
    const body = await request.json() as CreateOrderRequest;

    if (!body.customer_email || !body.items?.length) {
      return err('Email and items are required', 400, request, env);
    }

    // Server-side price validation
    const productIds = body.items.map(i => i.product_id);
    const { data: products } = await db.from('products')
      .select('*')
      .in('id', productIds);

    if (!products?.length) {
      return err('Could not validate products', 400, request, env);
    }

    const productMap = new Map(products.map((p: any) => [p.id, p]));
    let serverSubtotal = 0;

    for (const item of body.items) {
      const product = productMap.get(item.product_id);
      if (!product) return err(`Product ${item.product_name} not found`, 400, request, env);
      const priceField = getPriceField(item.billing_period);
      const dbPrice = product[priceField];
      if (dbPrice != null) item.unit_price_ngn = dbPrice;
      serverSubtotal += item.unit_price_ngn * item.quantity;
    }

    // Discount validation
    let discountNGN = 0;
    let discountCode: string | null = null;
    if (body.discount_code) {
      const { data: disc } = await db.from('discount_codes')
        .select('*').eq('code', body.discount_code.toUpperCase()).single();
      if (disc) {
        const result = validateAndCalcDiscount(disc as DiscountCode, body.items, !disc.auto_apply);
        if (result.valid) {
          discountNGN = result.discount_ngn;
          discountCode = disc.code;
        }
      }
    }

    // Affiliate
    let affiliateId: string | null = null;
    if (body.affiliate_code) {
      const { data: aff } = await db.from('affiliates')
        .select('id').eq('referral_code', body.affiliate_code).eq('status', 'approved').single();
      if (aff) affiliateId = aff.id;
    }

    const totalNGN = Math.max(0, serverSubtotal - discountNGN);

    // Find or create customer
    let customerId: string | null = null;
    try {
      customerId = await findOrCreateCustomer(db, {
        email: body.customer_email,
        name: body.customer_name,
        phone: body.customer_phone,
        source: 'whatsapp',
      });
    } catch (e: any) {
      console.error('Customer creation failed:', e);
    }

    // Generate order ref
    const { data: refData } = await db.rpc('generate_order_ref');
    const orderRef = refData as string;

    // Create order as pending_manual
    const { data: order, error: oErr } = await db.from('orders').insert({
      order_ref: orderRef,
      customer_id: customerId,
      customer_email: body.customer_email,
      customer_name: body.customer_name || null,
      customer_phone: body.customer_phone || null,
      status: 'pending_manual',
      payment_method: 'whatsapp',
      subtotal_ngn: serverSubtotal,
      discount_ngn: discountNGN,
      tax_ngn: 0,
      total_ngn: totalNGN,
      currency: body.currency || 'NGN',
      fx_rate: body.fx_rate || 1,
      display_total: totalNGN * (body.fx_rate || 1),
      discount_code: discountCode,
      affiliate_id: affiliateId,
    }).select().single();

    if (oErr || !order) {
      return err('Failed to create order: ' + (oErr?.message || 'unknown'), 500, request, env);
    }

    // Insert order items
    const orderItems = body.items.map(item => ({
      order_id: order.id,
      product_id: item.product_id,
      product_name: item.product_name,
      category: item.category,
      duration_months: item.duration_months,
      billing_period: item.billing_period,
      billing_type: item.billing_type,
      unit_price_ngn: item.unit_price_ngn,
      quantity: item.quantity,
      total_price_ngn: item.unit_price_ngn * item.quantity,
    }));

    await db.from('order_items').insert(orderItems);

    // Build WhatsApp message for admin
    // Fetch product WA/social links for items
    const waProductIds = body.items.map(i => i.product_id);
    const { data: waProducts } = await db
      .from('products')
      .select('id, whatsapp_group_url, social_links')
      .in('id', waProductIds);
    const waProductMap = new Map((waProducts || []).map((p: any) => [p.id, p]));
    
    const fxRate = body.fx_rate || 1;
    const currency = body.currency || 'NGN';
    const fmtAmt = (v: number) => {
      if (currency === 'NGN') return `₦${Math.ceil(v).toLocaleString()}`;
      return `${currency} ${(v * fxRate).toFixed(2)}`;
    };
    
    const whatsappNumber = env.WHATSAPP_NUMBER || '2348107872916';
    const frontendUrl = env.FRONTEND_URL || 'https://app.buysub.ng';
    
    const lines: string[] = [
      `🛒 *New WhatsApp Order*`, ``,
      `Order Ref: *${orderRef}*`,
      `Customer: ${body.customer_email}`,
      body.customer_name ? `Name: ${body.customer_name}` : '',
      body.customer_phone ? `Phone: ${body.customer_phone}` : '',
      `Currency: ${currency}`, ``,
      `*Items:*`,
    ];
    
    for (const item of body.items) {
      const lineTotal = item.unit_price_ngn * item.quantity;
      lines.push(`• ${item.product_name} ×${item.quantity} (${item.billing_period}) — ${fmtAmt(lineTotal)}`);
      const p: any = waProductMap.get(item.product_id);
      if (p?.whatsapp_group_url) {
        lines.push(`   └ Group: ${p.whatsapp_group_url}`);
      }
    }
    lines.push(``);
    
    if (discountNGN > 0 && discountCode) {
      lines.push(`Subtotal: ${fmtAmt(serverSubtotal)}`);
      lines.push(`Promo (${discountCode}): -${fmtAmt(discountNGN)}`);
    }
    lines.push(`*Total: ${fmtAmt(totalNGN)}*`, ``);
    lines.push(`⚠️ Status: Pending Manual Approval`);
    lines.push(`Approve at: ${frontendUrl}/admin/orders/${orderRef}`);
    
    const message = lines.filter(Boolean).join('\n');
    const whatsappUrl = `https://wa.me/${whatsappNumber}?text=${encodeURIComponent(message)}`;

    await logEvent(db, 'order', order.id, 'created', null, {
      order_ref: orderRef,
      payment_method: 'whatsapp',
      total_ngn: totalNGN,
    });

    return ok({
      order_id: order.id,
      order_ref: orderRef,
      total_ngn: totalNGN,
      whatsapp_url: whatsappUrl,
      message,
      status: 'pending_manual',
    }, request, env);
  } catch (e: any) {
    console.error('WhatsApp order error:', e);
    return err('Failed to process order: ' + (e?.message || 'unknown error'), 500, request, env);
  }
}


// ============================================================
// HANDLER: POST /v2/pay/init  (Paystack)
// ============================================================
async function handlePaystackInit(
  db: SupabaseClient, request: Request, env: Env,
): Promise<Response> {
  const body = await request.json() as PaystackInitRequest & { use_wallet?: boolean };

  if (!body.order_id) return err('order_id is required', 400, request, env);

  // Fetch order
  const { data: order, error: oErr } = await db.from('orders')
    .select('*')
    .eq('id', body.order_id)
    .eq('status', 'pending')
    .single();

  if (oErr || !order) return err('Order not found or already processed', 404, request, env);

  let amountToCharge = order.total_ngn;
  let walletDeducted = 0;

  // ── Wallet deduction (explicit opt-in) ──
  if (body.use_wallet && order.customer_id) {
    const { data: customer } = await db.from('customers')
      .select('user_id').eq('id', order.customer_id).single();

    if (customer?.user_id) {
      const { data: wallet } = await db.from('wallets')
        .select('*').eq('user_id', customer.user_id).single();

      if (wallet && wallet.balance_ngn > 0) {
        walletDeducted = Math.min(wallet.balance_ngn, amountToCharge);
        // Debit wallet
        await db.rpc('debit_wallet', {
          p_wallet_id: wallet.id,
          p_amount: walletDeducted,
          p_reference: order.order_ref,
        });
        amountToCharge -= walletDeducted;

        // Update order
        await db.from('orders').update({
          wallet_ngn: walletDeducted,
          total_ngn: amountToCharge,
        }).eq('id', order.id);
      }
    }
  }

  // If fully paid by wallet
  if (amountToCharge <= 0) {
    await fulfillOrder(db, order.id, 'wallet', env);
    return ok({
      fully_paid_by_wallet: true,
      order_ref: order.order_ref,
    }, request, env);
  }

  // ── Init Paystack transaction ──
  const paystackRef = `BS-${order.order_ref}-${Date.now()}`;

  const paystackRes = await fetch('https://api.paystack.co/transaction/initialize', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${env.PAYSTACK_SECRET_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      email: order.customer_email,
      amount: Math.round(amountToCharge * 100), // Paystack uses kobo
      reference: paystackRef,
      callback_url: body.callback_url || `${env.FRONTEND_URL}/order/verify`,
      metadata: {
        order_id: order.id,
        order_ref: order.order_ref,
        custom_fields: [
          { display_name: 'Order Ref', variable_name: 'order_ref', value: order.order_ref },
        ],
      },
    }),
  });

  const paystackData = await paystackRes.json() as any;

  if (!paystackData.status) {
    return err('Payment initialization failed: ' + (paystackData.message || 'Unknown error'), 500, request, env);
  }

  // Save paystack ref on order
  await db.from('orders').update({ paystack_ref: paystackRef }).eq('id', order.id);

  return ok({
    authorization_url: paystackData.data.authorization_url,
    access_code: paystackData.data.access_code,
    reference: paystackRef,
  }, request, env);
}


// ============================================================
// HANDLER: POST /v2/pay/webhook (Paystack Webhook)
// ============================================================
async function handlePaystackWebhook(
  db: SupabaseClient, request: Request, env: Env, ctx: ExecutionContext,
): Promise<Response> {
  // Verify Paystack signature
  const body = await request.text();
  const signature = request.headers.get('x-paystack-signature') || '';

  const isValid = await verifyPaystackSignature(body, signature, env.PAYSTACK_SECRET_KEY);
  if (!isValid) {
    return new Response('Invalid signature', { status: 401 });
  }

  const event = JSON.parse(body);

  // Only handle charge.success
  if (event.event !== 'charge.success') {
    return new Response('OK', { status: 200 });
  }

  const reference = event.data.reference;

  // ── Idempotency check ──
  const { data: existing } = await db.from('payment_events')
    .select('id')
    .eq('payment_reference', reference)
    .single();

  if (existing) {
    return new Response('Already processed', { status: 200 });
  }

  // ── Record payment event ──
  await db.from('payment_events').insert({
    payment_reference: reference,
    status: event.data.status,
    amount_ngn: event.data.amount / 100, // kobo → NGN
    provider: 'paystack',
    raw_payload: event.data,
  });

  // ── Find order by paystack reference ──
  const { data: order } = await db.from('orders')
    .select('*')
    .eq('paystack_ref', reference)
    .single();

  if (!order) {
    console.error(`Webhook: No order found for reference ${reference}`);
    return new Response('OK', { status: 200 });
  }

  // ── Fulfill order (async, non-blocking) ──
  ctx.waitUntil(fulfillOrder(db, order.id, 'paystack', env));

  return new Response('OK', { status: 200 });
}


// ============================================================
// HANDLER: GET /v2/pay/verify?reference=xxx
// ============================================================
async function handlePaystackVerify(
  db: SupabaseClient, reference: string | null, request: Request, env: Env,
): Promise<Response> {
  if (!reference) return err('Reference is required', 400, request, env);

  const paystackRes = await fetch(`https://api.paystack.co/transaction/verify/${encodeURIComponent(reference)}`, {
    headers: { Authorization: `Bearer ${env.PAYSTACK_SECRET_KEY}` },
  });

  const data = await paystackRes.json() as any;

  if (!data.status || data.data?.status !== 'success') {
    return err('Payment not verified', 400, request, env);
  }

  // Find order
  const { data: order } = await db.from('orders')
    .select('id, order_ref, status, total_ngn')
    .eq('paystack_ref', reference)
    .single();

  return ok({
    verified: true,
    order_ref: order?.order_ref,
    status: order?.status,
    amount_ngn: data.data.amount / 100,
  }, request, env);
}


// ============================================================
// HANDLER: POST /v2/admin/orders/approve  (Manual WhatsApp approval)
// ============================================================
// async function handleAdminApproveOrder(
//   db: SupabaseClient, request: Request, env: Env,
// ): Promise<Response> {
//   // TODO: Add admin auth check here (JWT from Supabase)
//   const authHeader = request.headers.get('Authorization');
//   if (!authHeader) return err('Unauthorized', 401, request, env);

//   const body = await request.json() as AdminApproveRequest;
//   if (!body.order_ref) return err('order_ref is required', 400, request, env);

//   // Find order
//   const { data: order, error: oErr } = await db.from('orders')
//     .select('*')
//     .eq('order_ref', body.order_ref)
//     .eq('status', 'pending_manual')
//     .single();

//   if (oErr || !order) {
//     return err('Order not found or not in pending_manual status', 404, request, env);
//   }

//   // Update payment method if provided
//   if (body.payment_method) {
//     await db.from('orders').update({
//       payment_method: body.payment_method,
//       notes: body.notes || null,
//     }).eq('id', order.id);
//   }

//   // Record payment event for idempotency
//   const manualRef = `MANUAL-${order.order_ref}-${Date.now()}`;
//   await db.from('payment_events').insert({
//     payment_reference: manualRef,
//     order_id: order.id,
//     status: 'success',
//     amount_ngn: order.total_ngn,
//     provider: 'manual',
//     raw_payload: { approved_by: 'admin', method: body.payment_method, notes: body.notes },
//   });

//   // Fulfill (same pipeline as Paystack)
//   await fulfillOrder(db, order.id, body.payment_method || 'cash', env);

//   await logEvent(db, 'order', order.id, 'approved_manual', null, {
//     order_ref: order.order_ref,
//     payment_method: body.payment_method,
//     notes: body.notes,
//   });

//   return ok({ approved: true, order_ref: order.order_ref }, request, env);
// }


// ============================================================
// HANDLER: GET /v2/admin/orders
// ============================================================
async function handleAdminGetOrders(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const status = url.searchParams.get('status');
  const q = url.searchParams.get('q')?.trim();
  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1'));
  const limit = Math.min(50, Math.max(1, parseInt(url.searchParams.get('limit') || '20')));
  const offset = (page - 1) * limit;

  let query = db
    .from('orders')
    .select('*', { count: 'exact' })
    .order('created_at', { ascending: false })
    .range(offset, offset + limit - 1);

  if (status) query = query.eq('status', status);
  if (q) query = query.or(`order_ref.ilike.%${q}%,customer_email.ilike.%${q}%,customer_name.ilike.%${q}%`);

  const { data, error: dbErr, count } = await query;
  if (dbErr) return err(dbErr.message, 500, request, env);

  return ok(data, request, env, {
    pagination: { page, limit, total: count, pages: Math.ceil((count || 0) / limit) }
  });
}


// ============================================================
// HANDLER: GET /v2/admin/orders/:ref
// ============================================================
async function handleAdminGetOrder(
  db: SupabaseClient, ref: string, request: Request, env: Env,
): Promise<Response> {
  const { data, error } = await db.from('orders')
    .select('*, order_items(*)')
    .eq('order_ref', ref)
    .single();

  if (error || !data) return err('Order not found', 404, request, env);
  return ok(data, request, env);
}

// ============================================================
// HANDLER: POST /v2/admin/orders  (manual order creation)
// ============================================================
async function handleAdminCreateOrder(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json().catch(() => null) as any;
  if (!body) return err('Invalid request body', 400, request, env);

  const { customer_name, customer_email, customer_phone, items, payment_method, notes, status, currency } = body;

  if (!customer_email) return err('Customer email is required', 400, request, env);
  if (!items?.length)   return err('At least one item is required', 400, request, env);

  // Validate products and resolve prices
  const productIds = items.map((i: any) => i.product_id).filter(Boolean);
  const { data: products } = await db.from('products').select('*').in('id', productIds);
  const productMap = new Map((products || []).map((p: any) => [p.id, p]));

  let subtotalNGN = 0;
  const resolvedItems: any[] = [];

  for (const item of items) {
    const product = productMap.get(item.product_id);
    if (!product) return err(`Product not found: ${item.product_id}`, 400, request, env);

    // Admin can override price — if override provided, use it; else resolve from DB
    const priceField = getPriceField(item.billing_period || 'Quarterly');
    const unitPrice = item.unit_price_ngn != null
      ? Number(item.unit_price_ngn)
      : (product[priceField] ?? 0);

    const qty = Math.max(1, parseInt(item.quantity) || 1);
    const lineTotal = unitPrice * qty;
    subtotalNGN += lineTotal;

    resolvedItems.push({
      product_id: product.id,
      product_name: product.name,
      category: product.category,
      billing_period: item.billing_period || 'Quarterly',
      billing_type: product.billing_type || 'subscription',
      duration_months: item.duration_months || 3,
      unit_price_ngn: unitPrice,
      quantity: qty,
      total_price_ngn: lineTotal,
    });
  }

  const discountNGN  = Number(body.discount_ngn)  || 0;
  const taxNGN       = Number(body.tax_ngn)        || 0;
  const totalNGN     = Math.max(0, subtotalNGN - discountNGN + taxNGN);
  const orderStatus  = status || 'pending_manual';

  // Find or create customer
  const customerId = await findOrCreateCustomer(db, {
    email: customer_email,
    name: customer_name,
    phone: customer_phone,
    source: 'admin_manual',
  });

  // Generate order ref
  const { data: refData } = await db.rpc('generate_order_ref');
  const orderRef = refData as string;

  // Insert order
  const { data: order, error: oErr } = await db.from('orders').insert({
    order_ref:      orderRef,
    customer_id:    customerId,
    customer_name:  customer_name  || null,
    customer_email: customer_email,
    customer_phone: customer_phone || null,
    status:         orderStatus,
    payment_method: payment_method || 'manual',
    subtotal_ngn:   subtotalNGN,
    discount_ngn:   discountNGN,
    discount_code:  body.discount_code || null,
    tax_ngn:        taxNGN,
    total_ngn:      totalNGN,
    currency:       currency || 'NGN',
    fx_rate:        1,
    display_total:  totalNGN,
    notes:          notes || null,
  }).select().single();

  if (oErr || !order) return err('Failed to create order: ' + (oErr?.message || 'unknown'), 500, request, env);

  // Insert order items
  const orderItemRows = resolvedItems.map(i => ({ ...i, order_id: order.id }));
  await db.from('order_items').insert(orderItemRows);

  await logEvent(db, 'order', order.id, 'created_manual', auth.userId, {
    order_ref: orderRef,
    total_ngn: totalNGN,
    item_count: resolvedItems.length,
  });

  return ok({ order_id: order.id, order_ref: orderRef, total_ngn: totalNGN, status: orderStatus }, request, env);
}


// ============================================================
// HANDLER: GET /v2/customers/search
// ============================================================
async function handleSearchCustomers(
  db: SupabaseClient, url: URL, request: Request, env: Env,
): Promise<Response> {
  const q = url.searchParams.get('q') || '';
  if (q.length < 2) return ok([], request, env);

  const { data, error } = await db.from('customers')
    .select('*')
    .or(`name.ilike.%${q}%,email.ilike.%${q}%,phone.ilike.%${q}%`)
    .limit(10);

  if (error) return err(error.message, 500, request, env);
  return ok(data, request, env);
}


// ============================================================
// FULFILLMENT PIPELINE
// ============================================================
async function fulfillOrder(
  db: SupabaseClient,
  orderId: string,
  paymentMethod: string,
  env: Env,
): Promise<void> {
  // 1. Update order status to paid
  await db.from('orders').update({
    status: 'paid',
    payment_method: paymentMethod as any,
    paid_at: new Date().toISOString(),
  }).eq('id', orderId);

  // 2. Fetch full order with items
  const { data: order } = await db.from('orders')
    .select('*, order_items(*)')
    .eq('id', orderId)
    .single();

  if (!order) return;

  console.log('Fulfillment started:', orderId);
  
  // 3. Increment discount usage
  if (order.discount_code) {
    const { data: disc } = await db.from('discount_codes')
      .select('id').eq('code', order.discount_code).single();
    if (disc) {
      await db.rpc('increment_discount_usage', { p_discount_id: disc.id });
      if (order.customer_id) {
        try {
          await db.from('discount_usages').insert({
            discount_id: disc.id,
            order_id: order.id,
            customer_id: order.customer_id,
          });
        } catch {} // ignore if already exists (unique constraint)
      }
    }
  }

  // 4. Affiliate commission
  if (order.affiliate_id) {
    const { data: aff } = await db.from('affiliates')
      .select('commission_rate, user_id')
      .eq('id', order.affiliate_id)
      .single();

    if (aff) {
      // Self-referral check: affiliate's user_id ≠ customer's user_id
      let isSelfReferral = false;
      if (order.customer_id && aff.user_id) {
        const { data: cust } = await db.from('customers')
          .select('user_id').eq('id', order.customer_id).single();
        if (cust?.user_id === aff.user_id) isSelfReferral = true;
      }

      if (!isSelfReferral) {
        const commissionAmount = order.total_ngn * (aff.commission_rate / 100);
        await db.from('affiliate_commissions').insert({
          affiliate_id: order.affiliate_id,
          order_id: order.id,
          amount_ngn: Math.round(commissionAmount * 100) / 100,
          status: 'pending',
        });
      }
    }
  }

  // 5. Send confirmation email (via Resend)
  try {
    await sendConfirmationEmail(order, env);
  } catch (e) {
    console.error('Email send failed:', e);
  }

  // 6. Log fulfillment
  await logEvent(db, 'order', order.id, 'fulfilled', null, {
    order_ref: order.order_ref,
    payment_method: paymentMethod,
    total_ngn: order.total_ngn,
  });
}


// ============================================================
// EMAIL (Resend)
// ============================================================
async function sendConfirmationEmail(order: any, env: Env): Promise<void> {
  const items = order.order_items || [];
 
  // Collect distinct product ids and fetch their WA/social links
  const productIds = [...new Set(items.map((i: any) => i.product_id).filter(Boolean))];
  let productLinks: Record<string, { whatsapp_group_url?: string; social_links?: any; name?: string }> = {};
  if (productIds.length) {
    try {
      const db = createClient(env.SUPABASE_URL!, env.SUPABASE_SERVICE_ROLE_KEY!, {
        auth: { persistSession: false, autoRefreshToken: false },
      });
      const { data } = await db
        .from('products')
        .select('id, name, whatsapp_group_url, social_links')
        .in('id', productIds);
      for (const p of data || []) productLinks[p.id] = p;
    } catch (e) { console.error('product-links fetch failed:', e); }
  }
 
  // Build PDF receipt and base64-encode it for Resend attachment
  let pdfBase64: string | null = null;
  try {
    const pdfBytes = await buildReceiptPdf(order);
    pdfBase64 = bytesToBase64(pdfBytes);
  } catch (e) {
    console.error('PDF build failed, sending without attachment:', e);
  }
 
  const html = buildOrderEmailHtml(order, items, productLinks);
 
  const payload: any = {
    from: 'BuySub <noreply@buysub.ng>',
    to: [order.customer_email],
    subject: `Your BuySub receipt — ${order.order_ref}`,
    html,
  };
  if (pdfBase64) {
    payload.attachments = [{
      filename: `BuySub-Receipt-${order.order_ref}.pdf`,
      content: pdfBase64,
    }];
  }
 
  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${env.RESEND_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    console.error('resend error:', res.status, text);
  }
}

function buildOrderEmailHtml(
  order: any,
  items: any[],
  productLinks: Record<string, { whatsapp_group_url?: string; social_links?: any; name?: string }>
): string {
  const fmt = (n: number) => `₦${Number(n || 0).toLocaleString('en-NG')}`;
  const customerName = order.customer_name || 'there';
 
  // Per-product CTA section. Only products that have a WA group or
  // any social link show a card.
  const productCards = items.map((it: any) => {
    const p = productLinks[it.product_id];
    if (!p) return '';
    const sl = p.social_links || {};
    const links: { label: string; url: string; kind: string }[] = [];
    if (p.whatsapp_group_url) links.push({ label: 'Join WhatsApp Group', url: p.whatsapp_group_url, kind: 'whatsapp' });
    if (sl.telegram)           links.push({ label: 'Telegram',            url: sl.telegram,           kind: 'telegram'  });
    if (sl.instagram)          links.push({ label: 'Instagram',           url: sl.instagram,          kind: 'instagram' });
    if (sl.twitter)            links.push({ label: 'Twitter / X',         url: sl.twitter,            kind: 'twitter'   });
    if (sl.discord)            links.push({ label: 'Discord',             url: sl.discord,            kind: 'discord'   });
    if (sl.website)            links.push({ label: 'Website',             url: sl.website,            kind: 'web'       });
    if (!links.length) return '';
 
    const linkButtons = links.map(l => `
      <a href="${escHtml(l.url)}" target="_blank" rel="noopener"
         style="display:inline-block;margin:4px 4px 0 0;padding:8px 14px;border-radius:8px;background:#1a1a20;border:1px solid #2a2a32;color:#e8e8ec;font-size:13px;font-weight:500;text-decoration:none;">
        ${escHtml(l.label)}
      </a>`).join('');
 
    return `
      <tr><td style="padding:16px 0 0;">
        <div style="background:#0f0f14;border:1px solid #1c1c22;border-radius:12px;padding:16px 18px;">
          <div style="font-size:12px;color:#9b82ff;text-transform:uppercase;letter-spacing:0.06em;font-weight:600;">Next steps for ${escHtml(p.name || it.product_name)}</div>
          <div style="margin-top:10px;">${linkButtons}</div>
        </div>
      </td></tr>`;
  }).join('');
 
  const itemRows = items.map((it: any) => `
    <tr>
      <td style="padding:12px 0;border-bottom:1px solid #1c1c22;">
        <div style="color:#e8e8ec;font-size:14px;font-weight:500;">${escHtml(it.product_name)}</div>
        <div style="color:#a0a0b0;font-size:12px;margin-top:2px;">${escHtml(it.billing_period || 'One-time')} · ×${it.quantity}</div>
      </td>
      <td style="padding:12px 0;border-bottom:1px solid #1c1c22;text-align:right;color:#e8e8ec;font-size:14px;font-weight:600;white-space:nowrap;">
        ${fmt(it.total_price_ngn)}
      </td>
    </tr>`).join('');
 
  const discountRow = order.discount_ngn > 0 ? `
    <tr>
      <td style="padding:6px 0;color:#a0a0b0;font-size:13px;">Discount${order.discount_code ? ` (${escHtml(order.discount_code)})` : ''}</td>
      <td style="padding:6px 0;color:#22c55e;font-size:13px;text-align:right;">-${fmt(order.discount_ngn)}</td>
    </tr>` : '';
 
  const subtotalRow = order.discount_ngn > 0 ? `
    <tr>
      <td style="padding:6px 0;color:#a0a0b0;font-size:13px;">Subtotal</td>
      <td style="padding:6px 0;color:#a0a0b0;font-size:13px;text-align:right;">${fmt(order.subtotal_ngn)}</td>
    </tr>` : '';
 
  return `<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>BuySub receipt</title>
  </head>
  <body style="margin:0;padding:0;background:#0a0a0c;font-family:'Inter',-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a0c;">
      <tr><td align="center" style="padding:32px 16px;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:560px;background:#111114;border:1px solid #1c1c22;border-radius:20px;overflow:hidden;">
 
          <!-- Header -->
          <tr><td style="padding:32px 32px 24px;background:linear-gradient(135deg,#1a1432 0%,#0e0a1f 100%);">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td>
                  <div style="display:inline-block;width:42px;height:42px;border-radius:12px;background:#7C5CFF;color:#fff;font-weight:700;font-size:22px;line-height:42px;text-align:center;box-shadow:0 6px 22px rgba(124,92,255,0.35);">B</div>
                </td>
                <td style="text-align:right;">
                  <span style="display:inline-block;padding:6px 12px;border-radius:999px;background:rgba(34,197,94,0.12);border:1px solid rgba(34,197,94,0.3);color:#22c55e;font-size:11px;font-weight:600;letter-spacing:0.04em;">✓ CONFIRMED</span>
                </td>
              </tr>
            </table>
            <div style="margin-top:24px;color:#e8e8ec;font-size:24px;font-weight:700;letter-spacing:-0.02em;">Payment received</div>
            <div style="margin-top:6px;color:#a0a0b0;font-size:14px;">Order <span style="color:#e8e8ec;font-family:'SF Mono',Menlo,monospace;">${escHtml(order.order_ref)}</span></div>
          </td></tr>
 
          <!-- Body -->
          <tr><td style="padding:28px 32px 8px;">
            <p style="margin:0 0 16px;color:#e8e8ec;font-size:15px;line-height:1.6;">Hi ${escHtml(customerName)},</p>
            <p style="margin:0 0 24px;color:#a0a0b0;font-size:14px;line-height:1.7;">
              Thanks for your purchase. Your receipt is attached as a PDF and your full order summary is below.
              We'll reach out shortly with subscription details.
            </p>
 
            <!-- Items -->
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-top:1px solid #1c1c22;">
              ${itemRows}
            </table>
 
            <!-- Totals -->
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-top:10px;">
              ${subtotalRow}
              ${discountRow}
              <tr>
                <td style="padding:14px 0 0;color:#e8e8ec;font-size:16px;font-weight:700;">Total paid</td>
                <td style="padding:14px 0 0;color:#7C5CFF;font-size:20px;font-weight:700;text-align:right;">${fmt(order.total_ngn)}</td>
              </tr>
            </table>
 

            <!-- Product-specific CTAs (WhatsApp groups + social links) -->
            ${productCards ? `<table role="presentation" width="100%" cellpadding="0" cellspacing="0">${productCards}</table>` : ''}
 
          </td></tr>
 
          <!-- Footer -->
          <tr><td style="padding:24px 32px 32px;border-top:1px solid #1c1c22;margin-top:24px;">
            <p style="margin:0;color:#6b6b7e;font-size:12px;line-height:1.7;">
              Need help? Reply to this email or message us on
              <a href="https://wa.me/2348107872916" style="color:#7C5CFF;text-decoration:none;">WhatsApp</a>.
            </p>
            <p style="margin:10px 0 0;color:#6b6b7e;font-size:11px;">
              BuySub · <a href="https://buysub.ng" style="color:#7C5CFF;text-decoration:none;">buysub.ng</a>
            </p>
          </td></tr>
 
        </table>
      </td></tr>
    </table>
  </body>
</html>`;
}
 
 
/* ══════════════════════════════════════════════════════════════════
   PART 5c — Partner signup welcome email (F4)
   ══════════════════════════════════════════════════════════════════ */
 
async function sendPartnerSignupEmail(
  args: { to: string; ownerName: string; storeName: string },
  env: Env
): Promise<void> {
  const html = `<!DOCTYPE html>
<html>
  <head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Welcome to BuySub Partners</title></head>
  <body style="margin:0;padding:0;background:#0a0a0c;font-family:'Inter',-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a0c;">
      <tr><td align="center" style="padding:32px 16px;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:560px;background:#111114;border:1px solid #1c1c22;border-radius:20px;overflow:hidden;">
          <tr><td style="padding:40px 32px 28px;background:linear-gradient(135deg,#1a1432 0%,#0e0a1f 100%);text-align:center;">
            <div style="display:inline-block;width:56px;height:56px;border-radius:16px;background:#7C5CFF;color:#fff;font-weight:700;font-size:28px;line-height:56px;text-align:center;box-shadow:0 6px 22px rgba(124,92,255,0.4);">B</div>
            <div style="margin-top:20px;color:#e8e8ec;font-size:24px;font-weight:700;letter-spacing:-0.02em;">Welcome aboard</div>
            <div style="margin-top:6px;color:#a0a0b0;font-size:14px;">We've received your application</div>
          </td></tr>
          <tr><td style="padding:28px 32px;">
            <p style="margin:0 0 16px;color:#e8e8ec;font-size:15px;line-height:1.6;">Hi ${escHtml(args.ownerName)},</p>
            <p style="margin:0 0 16px;color:#a0a0b0;font-size:14px;line-height:1.7;">
              Thanks for applying to the BuySub Partner Program for <strong style="color:#e8e8ec;">${escHtml(args.storeName)}</strong>.
              Our team will review your application within <strong style="color:#e8e8ec;">3–5 business days</strong> and get back to you via your preferred contact method.
            </p>
            <p style="margin:0 0 24px;color:#a0a0b0;font-size:14px;line-height:1.7;">
              Once approved, you can log in to your partner dashboard to view affiliate stats, track earnings, and manage your profile.
            </p>
            <div style="text-align:center;margin:24px 0 8px;">
              <a href="https://app.buysub.ng/login" style="display:inline-block;padding:14px 28px;border-radius:10px;background:#7C5CFF;color:#fff;font-size:14px;font-weight:600;text-decoration:none;box-shadow:0 6px 20px rgba(124,92,255,0.35);">Go to dashboard</a>
            </div>
          </td></tr>
          <tr><td style="padding:20px 32px 32px;border-top:1px solid #1c1c22;">
            <p style="margin:0;color:#6b6b7e;font-size:12px;line-height:1.7;">
              Questions? Message us on <a href="https://wa.me/2348107872916" style="color:#7C5CFF;text-decoration:none;">WhatsApp</a> or reply to this email.
            </p>
            <p style="margin:10px 0 0;color:#6b6b7e;font-size:11px;">BuySub · <a href="https://buysub.ng" style="color:#7C5CFF;text-decoration:none;">buysub.ng</a></p>
          </td></tr>
        </table>
      </td></tr>
    </table>
  </body>
</html>`;
 
  await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${env.RESEND_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      from: 'BuySub Partners <partners@buysub.ng>',
      to: [args.to],
      subject: `Welcome to BuySub Partners — ${args.storeName}`,
      html,
    }),
  });
}


// ============================================================
// HELPERS
// ============================================================

function getPriceField(billingPeriod: string): string {
  const map: Record<string, string> = {
    'Quarterly': 'price_3m',
    'Biannual': 'price_6m',
    'Annual': 'price_1y',
    'One-time': 'price_1m', // one-time products store same price in all fields
    'quarterly': 'price_3m',
    'biannual': 'price_6m',
    'annual': 'price_1y',
    'one_time': 'price_1m',
  };
  return map[billingPeriod] || 'price_3m';
}

async function findOrCreateCustomer(
  db: SupabaseClient,
  info: { email: string; name?: string; phone?: string; source?: string },
): Promise<string | null> {
  // Try to find existing customer by email
  const { data: existingRows } = await db.from('customers')
    .select('id')
    .eq('email', info.email)
    .limit(1);

  if (existingRows && existingRows.length > 0) return existingRows[0].id;

  // Create new customer
  const { data: created, error } = await db.from('customers').insert({
    name: info.name || info.email.split('@')[0],
    email: info.email,
    phone: info.phone || null,
    source: info.source || 'website',
  }).select('id').single();

  if (error || !created) {
    console.error('Failed to create customer:', error?.message);
    return null;
  }

  return created.id;
}

async function verifyPaystackSignature(
  body: string,
  signature: string,
  secret: string,
): Promise<boolean> {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-512' },
    false,
    ['sign'],
  );
  const sig = await crypto.subtle.sign('HMAC', key, encoder.encode(body));
  const hex = Array.from(new Uint8Array(sig))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
  return hex === signature;
}

async function logEvent(
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

// ════════════════════════════════════════════════════════════════
// PHASE 3 HANDLER FUNCTIONS
// ════════════════════════════════════════════════════════════════

async function requireAdmin(
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

  if (!profile || !['admin', 'super_admin', 'support_agent'].includes(profile.role)) {
    return { ok: false, response: err('Forbidden — admin access required', 403, request, env) };
  }

  return { ok: true, userId: user.id };
}

async function handleAdminStats(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const { data, error: rpcErr } = await db.rpc('admin_dashboard_stats');
  if (rpcErr) return err(rpcErr.message, 500, request, env);

  return ok(data, request, env);
}

async function handleAdminCustomers(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const q = url.searchParams.get('q')?.trim();
  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1'));
  const limit = Math.min(50, Math.max(1, parseInt(url.searchParams.get('limit') || '20')));
  const offset = (page - 1) * limit;

  let query = db
    .from('customers')
    .select('id, name, email, phone, category, source, is_active, created_at', { count: 'exact' })
    .order('created_at', { ascending: false })
    .range(offset, offset + limit - 1);

  if (q) query = query.or(`name.ilike.%${q}%,email.ilike.%${q}%,phone.ilike.%${q}%`);

  const { data, error: dbErr, count } = await query;
  if (dbErr) return err(dbErr.message, 500, request, env);

  return ok(data, request, env, {
    pagination: { page, limit, total: count, pages: Math.ceil((count || 0) / limit) }
  });
}

async function handleAdminCustomerSearch(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const q = url.searchParams.get('q')?.trim();
  if (!q || q.length < 2) return ok([], request, env);

  const { data, error: dbErr } = await db
    .from('customers')
    .select('id, name, email, phone, category')
    .or(`name.ilike.%${q}%,email.ilike.%${q}%,phone.ilike.%${q}%`)
    .limit(10);

  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok(data, request, env);
}

async function handleAdminGetCustomerWallet(db: SupabaseClient, request: Request, env: Env): Promise<Response> {
  const auth = await requireAdmin(db, request, env)
  if (!auth.ok) return auth.response
  const customerId = new URL(request.url).pathname.split('/').at(-2)!
  const { data: customer } = await db.from('customers').select('user_id').eq('id', customerId).limit(1)
  if (!customer?.length || !customer[0].user_id) return ok({ balance_ngn: 0 }, request, env)
  const { data: wallet } = await db.from('wallets').select('*').eq('user_id', customer[0].user_id).limit(1)
  return ok(wallet?.[0] || { balance_ngn: 0 }, request, env)
}

async function handleAdminProducts(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const q = url.searchParams.get('q')?.trim();
  const status = url.searchParams.get('status');
  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1'));
  const limit = Math.min(100, Math.max(1, parseInt(url.searchParams.get('limit') || '50')));
  const offset = (page - 1) * limit;

  let query = db
    .from('products')
    .select('*', { count: 'exact' })
    .order('name', { ascending: true })
    .range(offset, offset + limit - 1);

  if (status) query = query.eq('status', status);
  if (q) query = query.or(`name.ilike.%${q}%,category.ilike.%${q}%,tags.ilike.%${q}%`);

  const { data, error: dbErr, count } = await query;
  if (dbErr) return err(dbErr.message, 500, request, env);

  return ok(data, request, env, {
    pagination: { page, limit, total: count, pages: Math.ceil((count || 0) / limit) }
  });
}

async function handleAdminUpdateProduct(
  db: SupabaseClient, productId: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json() as any;

  const allowed = [
    'name', 'slug', 'status', 'stock_status', 'price_1m', 'price_3m', 'price_6m', 'price_1y',
    'category', 'tags', 'short_description', 'description', 'category_tagline',
    'domain', 'billing_type', 'billing_period', 'featured', 'sort_order', 'image_url', 'whatsapp_group_url', 'social_links'
  ];
  const updates: Record<string, any> = {};
  for (const key of allowed) {
    if (body[key] !== undefined) updates[key] = body[key];
  }
  updates.updated_at = new Date().toISOString();

  const { data, error: dbErr } = await db
    .from('products')
    .update(updates)
    .eq('id', productId)
    .select()
    .single();

  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok(data, request, env);
}

async function handleAdminApproveOrderV2(
  db: SupabaseClient, ref: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json().catch(() => ({})) as any;
  const paymentMethod = 'whatsapp';

  const { data: order, error: findErr } = await db
    .from('orders')
    .select('id, status, order_ref, total_ngn, discount_code')
    .eq('order_ref', ref)
    .single();

  if (findErr || !order) return err('Order not found', 404, request, env);
  if (order.status !== 'pending_manual') return err(`Cannot approve — status is "${order.status}"`, 400, request, env);

  // Use fulfillOrder pipeline
  // 🔴 force update BEFORE fulfill (guarantees persistence)
  await db.from('orders').update({
    status: 'paid',
    payment_method: paymentMethod,
    paid_at: new Date().toISOString(),
  }).eq('id', order.id);

  // then run rest of pipeline
  // 🔍 STEP A — force update + log result
const { data: updated, error: updateErr } = await db
.from('orders')
.update({
  status: 'paid',
  payment_method: paymentMethod,
  paid_at: new Date().toISOString(),
})
.eq('id', order.id)
.select()
.single();

console.log('UPDATE RESULT:', updated, updateErr);

// 🔴 STOP if update failed
if (updateErr || !updated) {
return err('Failed to update order status', 500, request, env);
}

// 🔍 STEP B — re-fetch immediately
const { data: check } = await db
.from('orders')
.select('status')
.eq('id', order.id)
.single();

console.log('AFTER UPDATE STATUS:', check?.status);

// continue pipeline
await fulfillOrder(db, order.id, paymentMethod, env);

  await logEvent(db, 'order', order.id, 'approved_manual_v2', auth.userId, {
    order_ref: order.order_ref,
    payment_method: paymentMethod,
  });

  console.log('Approving order:', ref);

  return ok({ approved: true, order_ref: ref }, request, env);
}

async function handleAdminRejectOrder(
  db: SupabaseClient, ref: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json().catch(() => ({})) as any;
  const reason = body.reason || '';
  const confirmReject = body.confirm === true; // second-stage confirmation

  const { data: order, error: findErr } = await db
    .from('orders')
    .select('id, status, order_ref, notes')
    .eq('order_ref', ref)
    .single();

  if (findErr || !order) return err('Order not found', 404, request, env);

  if (confirmReject && order.status === 'rejected_pending') {
    // Final rejection — move to cancelled
    await db.from('orders').update({
      status: 'cancelled',
      notes: reason || order.notes,
      updated_at: new Date().toISOString(),
    }).eq('id', order.id);

    await logEvent(db, 'order', order.id, 'rejected_confirmed', auth.userId, {
      order_ref: order.order_ref, reason,
    });

    return ok({ rejected: true, confirmed: true, order_ref: ref }, request, env);
  }

  if (['pending', 'pending_manual'].includes(order.status)) {
    // First-stage rejection — move to rejected_pending
    await db.from('orders').update({
      status: 'rejected_pending',
      notes: reason,
      updated_at: new Date().toISOString(),
    }).eq('id', order.id);

    await logEvent(db, 'order', order.id, 'rejected_pending', auth.userId, {
      order_ref: order.order_ref, reason,
    });

    return ok({ rejected: true, confirmed: false, status: 'rejected_pending', order_ref: ref }, request, env);
  }

  return err(`Cannot reject — status is "${order.status}"`, 400, request, env);
}

async function handleSubmitPartnerApplication(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const body = await request.json().catch(() => null) as any;
  if (!body) return err('Invalid request body', 400, request, env);
 
  const required = [
    'legal_name', 'store_name', 'address', 'lga', 'state',
    'business_phone', 'business_email', 'owner_name', 'owner_email',
    'owner_phone', 'payout_frequency', 'payout_method',
    'password',
  ];
  for (const field of required) {
    if (!body[field]) return err(`Missing required field: ${field}`, 400, request, env);
  }
 
  if (typeof body.password !== 'string' || body.password.length < 8) {
    return err('Password must be at least 8 characters', 400, request, env);
  }
 
  if (body.payout_method === 'Bank Transfer') {
    if (!body.bank_name || !body.account_name || !body.account_number)
      return err('Bank details required for Bank Transfer', 400, request, env);
  }
  if (body.payout_method === 'Crypto') {
    if (!body.crypto_token || !body.crypto_chain || !body.wallet_address)
      return err('Crypto details required for Crypto payout', 400, request, env);
  }
  if (!body.aml_accepted || !body.privacy_accepted || !body.terms_accepted) {
    return err('All compliance checkboxes must be accepted', 400, request, env);
  }
 
  // Create Supabase auth user (email_confirm false so we don't need SMTP from here).
  // If the email is already registered, return a helpful error.
  const { data: signUp, error: signUpErr } = await db.auth.admin.createUser({
    email: body.owner_email,
    password: body.password,
    email_confirm: true,
    user_metadata: {
      full_name: body.owner_name,
      role: 'partner_applicant',
    },
  });
 
  if (signUpErr || !signUp?.user) {
    const msg = signUpErr?.message || 'Could not create account';
    if (/already|exists|registered/i.test(msg)) {
      return err('An account already exists for this email. Please log in.', 409, request, env);
    }
    return err(msg, 500, request, env);
  }
 
  const userId = signUp.user.id;
 
  const { data, error: dbErr } = await db
    .from('partner_applications')
    .insert({
      user_id: userId,
      legal_name: body.legal_name,
      store_name: body.store_name,
      address: body.address,
      lga: body.lga,
      state: body.state,
      business_phone: body.business_phone,
      alternate_phone: body.alternate_phone || null,
      business_email: body.business_email,
      cac_number: body.cac_number || null,
      registration_year: body.registration_year || null,
      social_media: body.social_media || null,
      owner_name: body.owner_name,
      owner_email: body.owner_email,
      owner_phone: body.owner_phone,
      gender: body.gender || null,
      owner_location: body.owner_location || null,
      contact_method: body.contact_method || null,
      payout_frequency: body.payout_frequency,
      payout_method: body.payout_method,
      bank_name: body.bank_name || null,
      account_name: body.account_name || null,
      account_number: body.account_number || null,
      crypto_token: body.crypto_token || null,
      crypto_chain: body.crypto_chain || null,
      wallet_address: body.wallet_address || null,
      aml_accepted: body.aml_accepted,
      privacy_accepted: body.privacy_accepted,
      terms_accepted: body.terms_accepted,
      status: 'pending_review',
    })
    .select('id, status')
    .single();
 
  if (dbErr) {
    // Roll back the auth user we just created
    try { await db.auth.admin.deleteUser(userId); } catch { /* ignore */ }
    return err(dbErr.message, 500, request, env);
  }
 
  await logEvent(db, 'partner_application', data.id, 'submitted', userId, {
    legal_name: body.legal_name,
    business_email: body.business_email,
  });
 
  // Fire-and-forget welcome email (F4)
  try {
    await sendPartnerSignupEmail({
      to: body.owner_email,
      ownerName: body.owner_name,
      storeName: body.store_name,
    }, env);
  } catch (e) {
    console.error('partner welcome email failed:', e);
  }
 
  return jsonResponse(
    { ok: true, data: { id: data.id, status: data.status, user_id: userId } },
    201, request, env
  );
}

async function handleAdminPartners(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const status = url.searchParams.get('status');
  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1'));
  const limit = Math.min(50, Math.max(1, parseInt(url.searchParams.get('limit') || '20')));
  const offset = (page - 1) * limit;

  let query = db
    .from('partner_applications')
    .select('*', { count: 'exact' })
    .order('created_at', { ascending: false })
    .range(offset, offset + limit - 1);

  if (status) query = query.eq('status', status);

  const { data, error: dbErr, count } = await query;
  if (dbErr) return err(dbErr.message, 500, request, env);

  return ok(data, request, env, {
    pagination: { page, limit, total: count, pages: Math.ceil((count || 0) / limit) }
  });
}

async function handleAdminApprovePartner(
  db: SupabaseClient, id: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
 
  const body = await request.json().catch(() => ({})) as any;
 
  const { data: app, error: appErr } = await db
    .from('partner_applications')
    .update({
      status: 'approved',
      reviewer_notes: body.notes || null,
      reviewed_by: auth.userId,
      reviewed_at: new Date().toISOString(),
    })
    .eq('id', id)
    .eq('status', 'pending_review')
    .select()
    .single();
 
  if (appErr || !app) return err('Application not found or already reviewed', 404, request, env);
 
  // Create affiliate record if one doesn't already exist for this user
  if (app.user_id) {
    const { data: existing } = await db
      .from('affiliates')
      .select('id')
      .eq('user_id', app.user_id)
      .maybeSingle();
 
    if (!existing) {
      // Generate a unique short referral code from the store name
      const base = String(app.store_name || app.owner_name || 'partner')
        .toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 16);
      const suffix = Math.random().toString(36).slice(2, 6);
      const referralCode = `${base}-${suffix}`;
 
      await db.from('affiliates').insert({
        user_id: app.user_id,
        partner_application_id: app.id,
        referral_code: referralCode,
        status: 'approved',
        display_name: app.store_name || app.owner_name,
        email: app.owner_email,
      });
    }
 
    // Promote the auth user's role to 'partner'
    try {
      await db.auth.admin.updateUserById(app.user_id, {
        user_metadata: { role: 'partner' },
      });
    } catch { /* non-fatal */ }
  }
 
  await logEvent(db, 'partner_application', id, 'approved', auth.userId, {});
  return ok(app, request, env);
}

async function handleAdminRejectPartner(
  db: SupabaseClient, id: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json().catch(() => ({})) as any;

  const { data, error: dbErr } = await db
    .from('partner_applications')
    .update({
      status: 'rejected',
      reviewer_notes: body.notes || body.reason || null,
      reviewed_by: auth.userId,
      reviewed_at: new Date().toISOString(),
    })
    .eq('id', id)
    .eq('status', 'pending_review')
    .select()
    .single();

  if (dbErr || !data) return err('Application not found or already reviewed', 404, request, env);

  await logEvent(db, 'partner_application', id, 'rejected', auth.userId, {
    reason: body.notes || body.reason,
  });
  return ok(data, request, env);
}

async function handlePartnerMe(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const token = request.headers.get('Authorization')?.replace('Bearer ', '');
  if (!token) return err('Unauthorized', 401, request, env);
 
  const { data: { user } } = await db.auth.getUser(token);
  if (!user) return err('Invalid token', 401, request, env);
 
  const { data: app } = await db
    .from('partner_applications')
    .select('*')
    .eq('user_id', user.id)
    .single();
 
  if (!app) return err('No partner profile found', 404, request, env);
 
  // Also fetch affiliate record (for referral code, etc.) if approved
  let affiliate: any = null;
  if (app.status === 'approved') {
    const { data: aff } = await db
      .from('affiliates')
      .select('id, referral_code, status, display_name')
      .eq('user_id', user.id)
      .maybeSingle();
    affiliate = aff;
  }
 
  return ok({ profile: app, affiliate }, request, env);
}
 
async function handlePartnerUpdateMe(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const token = request.headers.get('Authorization')?.replace('Bearer ', '');
  if (!token) return err('Unauthorized', 401, request, env);
 
  const { data: { user } } = await db.auth.getUser(token);
  if (!user) return err('Invalid token', 401, request, env);
 
  const body = await request.json().catch(() => ({})) as any;
 
  // Whitelist fields partners are allowed to update themselves
  const allowed = [
    'business_phone', 'alternate_phone', 'business_email',
    'address', 'lga', 'state', 'social_media',
    'owner_phone', 'contact_method', 'owner_location',
    'payout_frequency', 'payout_method',
    'bank_name', 'account_name', 'account_number',
    'crypto_token', 'crypto_chain', 'wallet_address',
  ];
  const updates: Record<string, any> = {};
  for (const k of allowed) {
    if (body[k] !== undefined) updates[k] = body[k];
  }
  if (Object.keys(updates).length === 0) {
    return err('No updatable fields provided', 400, request, env);
  }
  updates.updated_at = new Date().toISOString();
 
  const { data, error: dbErr } = await db
    .from('partner_applications')
    .update(updates)
    .eq('user_id', user.id)
    .select()
    .single();
 
  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok(data, request, env);
}
 
async function handlePartnerMyStats(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const token = request.headers.get('Authorization')?.replace('Bearer ', '');
  if (!token) return err('Unauthorized', 401, request, env);
 
  const { data: { user } } = await db.auth.getUser(token);
  if (!user) return err('Invalid token', 401, request, env);
 
  const { data, error: rpcErr } = await db.rpc('partner_dashboard_stats', {
    p_user_id: user.id,
  });
  if (rpcErr) return err(rpcErr.message, 500, request, env);
 
  return ok(data, request, env);
}

async function handleAdminWallets(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1'));
  const limit = Math.min(50, Math.max(1, parseInt(url.searchParams.get('limit') || '20')));
  const offset = (page - 1) * limit;

  // Try with join first, fallback to plain select
  try {
    const { data, error: dbErr, count } = await db
      .from('wallet_transactions')
      .select('*', { count: 'exact' })
      .order('created_at', { ascending: false })
      .range(offset, offset + limit - 1);

    if (dbErr) return err(dbErr.message, 500, request, env);

    return ok(data || [], request, env, {
      pagination: { page, limit, total: count || 0, pages: Math.ceil((count || 0) / limit) }
    });
  } catch (e: any) {
    // Table might not exist yet
    return ok([], request, env, {
      pagination: { page: 1, limit: 20, total: 0, pages: 0 }
    });
  }
}


async function handleValidateDiscountV2(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const code = url.searchParams.get('code')?.trim().toUpperCase();
  const subtotalNGN = parseFloat(url.searchParams.get('subtotal') || '0');

  if (!code) return err('Missing code parameter', 400, request, env);

  const { data: discount, error: dbErr } = await db
    .from('discount_codes')
    .select('*')
    .eq('code', code)
    .eq('active', true)
    .single();

  if (dbErr || !discount) return ok({ ok: false, error: 'Code not found or inactive.' }, request, env);

  if (discount.active_from && new Date(discount.active_from) > new Date())
    return ok({ ok: false, error: 'Code is not active yet.' }, request, env);
  if (discount.expires_at && new Date(discount.expires_at) < new Date())
    return ok({ ok: false, error: 'Code has expired.' }, request, env);
  if (discount.max_uses != null && (discount.times_used || 0) >= discount.max_uses)
    return ok({ ok: false, error: 'Usage limit reached.' }, request, env);
  if (discount.min_order_ngn && subtotalNGN < discount.min_order_ngn)
    return ok({ ok: false, error: `Minimum order of ₦${Number(discount.min_order_ngn).toLocaleString()} required.` }, request, env);

  let amountNGN = discount.type === 'percentage'
    ? subtotalNGN * (discount.value / 100)
    : discount.value;
  if (discount.max_discount_ngn) amountNGN = Math.min(amountNGN, discount.max_discount_ngn);

  const display = discount.type === 'percentage'
    ? `${discount.value}% off${discount.max_discount_ngn ? ` (max ₦${Number(discount.max_discount_ngn).toLocaleString()})` : ''}`
    : `₦${Number(discount.value).toLocaleString()} off`;

  return ok({
    ok: true,
    result: {
      code: discount.code,
      type: discount.type,
      value: discount.value,
      display,
      amountNGN: Math.round(amountNGN * 100) / 100,
    },
  }, request, env);
}

// ══════════════════════════════════════════════════════════════
// PART 2: HANDLER FUNCTIONS
// Paste these at the very bottom of index.ts,
// after the Phase 3 handler functions.
// ══════════════════════════════════════════════════════════════
 
 
// ── POST /v2/affiliates/click (public — track referral click) ──
async function handleAffiliateClick(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const body = await request.json().catch(() => ({})) as any;
  const code = body.referral_code?.trim().toUpperCase();
  if (!code) return err('Missing referral_code', 400, request, env);
 
  const { data: affiliate } = await db
    .from('affiliates')
    .select('id, status')
    .eq('referral_code', code)
    .single();
 
  if (!affiliate || affiliate.status !== 'approved') {
    return err('Invalid or inactive referral code', 404, request, env);
  }
 
  // Record click
  await db.from('affiliate_clicks').insert({
    affiliate_id: affiliate.id,
    ip: request.headers.get('CF-Connecting-IP') || null,
    user_agent: request.headers.get('User-Agent') || null,
    referrer: request.headers.get('Referer') || body.referrer || null,
    landing_url: body.landing_url || null,
  });
 
  return ok({ tracked: true, affiliate_id: affiliate.id }, request, env);
}
 
 
// ── GET /v2/affiliates/resolve?code=PARTNER123 (public — lookup code) ──
async function handleAffiliateResolve(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const code = url.searchParams.get('code')?.trim().toUpperCase();
  if (!code) return err('Missing code parameter', 400, request, env);
 
  const { data: affiliate } = await db
    .from('affiliates')
    .select('id, referral_code, business_name, store_name, status')
    .eq('referral_code', code)
    .eq('status', 'approved')
    .single();
 
  if (!affiliate) return ok({ valid: false }, request, env);
 
  return ok({
    valid: true,
    affiliate_id: affiliate.id,
    referral_code: affiliate.referral_code,
    store_name: affiliate.store_name || affiliate.business_name,
  }, request, env);
}
 
 
// ── GET /v2/affiliates/me (authenticated — own affiliate record) ──
async function handleAffiliateMe(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const auth = request.headers.get('Authorization')?.replace('Bearer ', '');
  if (!auth) return err('Unauthorized', 401, request, env);
 
  const { data: { user } } = await db.auth.getUser(auth);
  if (!user) return err('Invalid token', 401, request, env);
 
  const { data: affiliate } = await db
    .from('affiliates')
    .select('*')
    .eq('user_id', user.id)
    .single();
 
  if (!affiliate) return err('No affiliate account found', 404, request, env);
  return ok(affiliate, request, env);
}
 
 
// ── GET /v2/affiliates/me/stats (authenticated) ──
async function handleAffiliateMyStats(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const auth = request.headers.get('Authorization')?.replace('Bearer ', '');
  if (!auth) return err('Unauthorized', 401, request, env);
 
  const { data: { user } } = await db.auth.getUser(auth);
  if (!user) return err('Invalid token', 401, request, env);
 
  const { data: affiliate } = await db
    .from('affiliates')
    .select('id')
    .eq('user_id', user.id)
    .single();
 
  if (!affiliate) return err('No affiliate account found', 404, request, env);
 
  const { data, error: rpcErr } = await db.rpc('affiliate_dashboard_stats', {
    p_affiliate_id: affiliate.id,
  });
 
  if (rpcErr) return err(rpcErr.message, 500, request, env);
  return ok(data, request, env);
}
 
 
// ── GET /v2/affiliates/me/commissions?page=&limit= (authenticated) ──
async function handleAffiliateMyCommissions(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const auth = request.headers.get('Authorization')?.replace('Bearer ', '');
  if (!auth) return err('Unauthorized', 401, request, env);
 
  const { data: { user } } = await db.auth.getUser(auth);
  if (!user) return err('Invalid token', 401, request, env);
 
  const { data: affiliate } = await db
    .from('affiliates')
    .select('id')
    .eq('user_id', user.id)
    .single();
 
  if (!affiliate) return err('No affiliate account found', 404, request, env);
 
  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1'));
  const limit = Math.min(50, parseInt(url.searchParams.get('limit') || '20'));
  const offset = (page - 1) * limit;
 
  const { data, error: dbErr, count } = await db
    .from('affiliate_commissions')
    .select(`
      id, amount_ngn, status, created_at,
      orders!affiliate_commissions_order_id_fkey ( order_ref, total_ngn, created_at )
    `, { count: 'exact' })
    .eq('affiliate_id', affiliate.id)
    .order('created_at', { ascending: false })
    .range(offset, offset + limit - 1);
 
  if (dbErr) return err(dbErr.message, 500, request, env);
 
  return ok(data, request, env, {
    pagination: { page, limit, total: count, pages: Math.ceil((count || 0) / limit) }
  });
}
 
 
// ── GET /v2/admin/affiliates?status=&page=&limit= ──
async function handleAdminAffiliates(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
 
  const status = url.searchParams.get('status');
  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1'));
  const limit = Math.min(50, parseInt(url.searchParams.get('limit') || '20'));
  const offset = (page - 1) * limit;
 
  let query = db
    .from('affiliates')
    .select(`
      *,
      profiles!affiliates_user_id_fkey ( display_name, email )
    `, { count: 'exact' })
    .order('created_at', { ascending: false })
    .range(offset, offset + limit - 1);
 
  if (status) query = query.eq('status', status);
 
  const { data, error: dbErr, count } = await query;
  if (dbErr) return err(dbErr.message, 500, request, env);
 
  return ok(data, request, env, {
    pagination: { page, limit, total: count, pages: Math.ceil((count || 0) / limit) }
  });
}
 
 
// ── POST /v2/admin/affiliates/:id/approve ──
async function handleAdminApproveAffiliate(
  db: SupabaseClient, id: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
 
  const body = await request.json().catch(() => ({})) as any;
 
  const { data, error: dbErr } = await db
    .from('affiliates')
    .update({
      status: 'approved',
      commission_rate: body.commission_rate || 5.00,
      updated_at: new Date().toISOString(),
    })
    .eq('id', id)
    .select()
    .single();
 
  if (dbErr || !data) return err('Affiliate not found', 404, request, env);
 
  await logEvent(db, 'affiliate', id, 'approved', auth.userId, {
    commission_rate: data.commission_rate,
  });
 
  return ok(data, request, env);
}
 
 
// ── POST /v2/admin/affiliates/:id/suspend ──
async function handleAdminSuspendAffiliate(
  db: SupabaseClient, id: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
 
  const body = await request.json().catch(() => ({})) as any;
 
  const { data, error: dbErr } = await db
    .from('affiliates')
    .update({ status: 'suspended', updated_at: new Date().toISOString() })
    .eq('id', id)
    .select()
    .single();
 
  if (dbErr || !data) return err('Affiliate not found', 404, request, env);
 
  await logEvent(db, 'affiliate', id, 'suspended', auth.userId, {
    reason: body.reason,
  });
 
  return ok(data, request, env);
}
 
 
// ══════════════════════════════════════════════════════════
// SHORT LINKS — Admin management (REPLACES existing handlers)
// ══════════════════════════════════════════════════════════
//
// What changed vs. the previous version:
//   • POST/PATCH accept the new columns (password, cloak, hide_referrer,
//     deep_link_*, ios_app_store_id, android_package, qr_config)
//   • Password is SHA-256 hashed on write — never round-tripped to client.
//     GET/LIST responses redact password_hash and return { has_password: bool }.
//   • New endpoints for targeting-rule CRUD (short_link_rules).
//
// Router wiring (add to the existing router switch):
//   if (path === '/v2/admin/links' && method === 'GET')   return handleAdminGetLinks(db, url, request, env);
//   if (path === '/v2/admin/links' && method === 'POST')  return handleAdminCreateLink(db, request, env);
//   const linkMatch = path.match(/^\/v2\/admin\/links\/([^/]+)$/);
//   if (linkMatch && method === 'GET')    return handleAdminGetLink(db, linkMatch[1], request, env);
//   if (linkMatch && method === 'PATCH')  return handleAdminUpdateLink(db, linkMatch[1], request, env);
//   if (linkMatch && method === 'DELETE') return handleAdminDeleteLink(db, linkMatch[1], request, env);
//   const statsMatch = path.match(/^\/v2\/admin\/links\/([^/]+)\/stats$/);
//   if (statsMatch && method === 'GET')   return handleAdminLinkStats(db, statsMatch[1], request, env);
//   const rulesMatch = path.match(/^\/v2\/admin\/links\/([^/]+)\/rules$/);
//   if (rulesMatch && method === 'GET')   return handleAdminGetLinkRules(db, rulesMatch[1], request, env);
//   if (rulesMatch && method === 'POST')  return handleAdminCreateLinkRule(db, rulesMatch[1], request, env);
//   const ruleMatch = path.match(/^\/v2\/admin\/links\/[^/]+\/rules\/([^/]+)$/);
//   if (ruleMatch && method === 'PATCH')  return handleAdminUpdateLinkRule(db, ruleMatch[1], request, env);
//   if (ruleMatch && method === 'DELETE') return handleAdminDeleteLinkRule(db, ruleMatch[1], request, env);

// ── Columns editable via the admin API ──
const LINK_WRITE_FIELDS = [
  'destination_url', 'slug',
  'expires_at', 'click_limit',
  'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term',
  'tags', 'active',
  'cloak', 'hide_referrer',
  'deep_link_ios', 'deep_link_android', 'ios_app_store_id', 'android_package',
  'qr_config',
];

async function sha256Hex(msg: string): Promise<string> {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(msg));
  return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, '0')).join('');
}

function redactLink(row: any) {
  if (!row) return row;
  const { password_hash, ...rest } = row;
  return { ...rest, has_password: !!password_hash };
}

// ── GET /v2/admin/links?q=&page=&limit= ──
async function handleAdminGetLinks(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const q = url.searchParams.get('q')?.trim();
  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1'));
  const limit = Math.min(50, parseInt(url.searchParams.get('limit') || '20'));
  const offset = (page - 1) * limit;

  let query = db
    .from('short_links')
    .select('*', { count: 'exact' })
    .order('created_at', { ascending: false })
    .range(offset, offset + limit - 1);

  if (q) query = query.or(`slug.ilike.%${q}%,destination_url.ilike.%${q}%,tags.ilike.%${q}%`);

  const { data, error: dbErr, count } = await query;
  if (dbErr) return err(dbErr.message, 500, request, env);

  const rows = (data || []).map(redactLink);
  return ok(rows, request, env, {
    pagination: { page, limit, total: count, pages: Math.ceil((count || 0) / limit) }
  });
}

// ── GET /v2/admin/links/:id  (single link + rules) ──
async function handleAdminGetLink(
  db: SupabaseClient, id: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const { data, error: dbErr } = await db
    .from('short_links')
    .select('*, rules:short_link_rules(*)')
    .eq('id', id)
    .single();

  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok(redactLink(data), request, env);
}

// ── POST /v2/admin/links ──
async function handleAdminCreateLink(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json().catch(() => null) as any;
  if (!body) return err('Invalid request body', 400, request, env);
  if (!body.destination_url) return err('destination_url is required', 400, request, env);

  const slug = body.slug?.trim().toLowerCase() ||
    Math.random().toString(36).slice(2, 8);

  const { data: existing } = await db
    .from('short_links')
    .select('id')
    .eq('slug', slug)
    .single();

  if (existing) return err(`Slug "${slug}" is already taken`, 409, request, env);

  const insert: Record<string, any> = { slug, active: true };
  for (const f of LINK_WRITE_FIELDS) {
    if (body[f] !== undefined) insert[f] = body[f];
  }
  insert.destination_url = body.destination_url;

  // Hash password if provided
  if (body.password) {
    insert.password_hash = await sha256Hex(String(body.password));
  }

  const { data, error: dbErr } = await db
    .from('short_links')
    .insert(insert)
    .select()
    .single();

  if (dbErr) return err(dbErr.message, 500, request, env);

  return jsonResponse(
    { ok: true, data: { ...redactLink(data), short_url: `https://go.buysub.ng/${data.slug}` } },
    201, request, env
  );
}

// ── PATCH /v2/admin/links/:id ──
async function handleAdminUpdateLink(
  db: SupabaseClient, id: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json().catch(() => ({})) as any;

  const updates: Record<string, any> = {};
  for (const key of LINK_WRITE_FIELDS) {
    if (body[key] !== undefined) updates[key] = body[key];
  }

  // Password handling:
  //   body.password === string   → hash + set
  //   body.password === null     → clear password
  //   body.password === undefined → leave untouched
  if (body.password !== undefined) {
    updates.password_hash = body.password === null || body.password === ''
      ? null
      : await sha256Hex(String(body.password));
  }

  updates.updated_at = new Date().toISOString();

  const { data, error: dbErr } = await db
    .from('short_links')
    .update(updates)
    .eq('id', id)
    .select()
    .single();

  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok(redactLink(data), request, env);
}

// ── DELETE /v2/admin/links/:id ──
async function handleAdminDeleteLink(
  db: SupabaseClient, id: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const { error: dbErr } = await db
    .from('short_links')
    .delete()
    .eq('id', id);

  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok({ deleted: true }, request, env);
}

// ── GET /v2/admin/links/:id/stats ──
async function handleAdminLinkStats(
  db: SupabaseClient, id: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const { data, error: rpcErr } = await db.rpc('short_link_stats', { p_link_id: id });
  if (rpcErr) return err(rpcErr.message, 500, request, env);

  return ok(data, request, env);
}

// ══════════════════════════════════════════════════════════
// TARGETING RULES
// ══════════════════════════════════════════════════════════

const RULE_WRITE_FIELDS = ['priority', 'match_type', 'match_value', 'destination_url'];
const VALID_MATCH_TYPES = ['country', 'region', 'city', 'os'];

// ── GET /v2/admin/links/:linkId/rules ──
async function handleAdminGetLinkRules(
  db: SupabaseClient, linkId: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const { data, error: dbErr } = await db
    .from('short_link_rules')
    .select('*')
    .eq('link_id', linkId)
    .order('priority', { ascending: true });

  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok(data || [], request, env);
}

// ── POST /v2/admin/links/:linkId/rules ──
async function handleAdminCreateLinkRule(
  db: SupabaseClient, linkId: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json().catch(() => null) as any;
  if (!body) return err('Invalid request body', 400, request, env);
  if (!body.match_type || !VALID_MATCH_TYPES.includes(body.match_type)) {
    return err(`match_type must be one of: ${VALID_MATCH_TYPES.join(', ')}`, 400, request, env);
  }
  if (!body.match_value) return err('match_value is required', 400, request, env);
  if (!body.destination_url) return err('destination_url is required', 400, request, env);

  const insert: Record<string, any> = { link_id: linkId };
  for (const f of RULE_WRITE_FIELDS) {
    if (body[f] !== undefined) insert[f] = body[f];
  }

  const { data, error: dbErr } = await db
    .from('short_link_rules')
    .insert(insert)
    .select()
    .single();

  if (dbErr) return err(dbErr.message, 500, request, env);
  return jsonResponse({ ok: true, data }, 201, request, env);
}

// ── PATCH /v2/admin/links/:linkId/rules/:ruleId ──
async function handleAdminUpdateLinkRule(
  db: SupabaseClient, ruleId: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json().catch(() => ({})) as any;
  const updates: Record<string, any> = {};
  for (const f of RULE_WRITE_FIELDS) {
    if (body[f] !== undefined) updates[f] = body[f];
  }
  if (updates.match_type && !VALID_MATCH_TYPES.includes(updates.match_type)) {
    return err(`match_type must be one of: ${VALID_MATCH_TYPES.join(', ')}`, 400, request, env);
  }

  const { data, error: dbErr } = await db
    .from('short_link_rules')
    .update(updates)
    .eq('id', ruleId)
    .select()
    .single();

  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok(data, request, env);
}

// ── DELETE /v2/admin/links/:linkId/rules/:ruleId ──
async function handleAdminDeleteLinkRule(
  db: SupabaseClient, ruleId: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const { error: dbErr } = await db
    .from('short_link_rules')
    .delete()
    .eq('id', ruleId);

  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok({ deleted: true }, request, env);
} 
 
// ══════════════════════════════════════════════════════════
// ADS
// ══════════════════════════════════════════════════════════
 
// ── GET /v2/ads?placement=shop_banner&limit=3 (public) ──
async function handleGetAds(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const placement = url.searchParams.get('placement');
  const limit = Math.min(10, parseInt(url.searchParams.get('limit') || '3'));
 
  if (!placement) return err('placement parameter is required', 400, request, env);
 
  const { data, error: dbErr } = await db.rpc('get_ads_by_placement', {
    p_placement: placement,
    p_limit: limit,
  });
 
  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok(data, request, env);
}
 
 
// ── POST /v2/ads/click (public — track ad click) ──
async function handleAdClick(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const body = await request.json().catch(() => ({})) as any;
  if (!body.ad_id) return err('ad_id required', 400, request, env);
 
  // await db.from('ads')
  //   .update({ click_count: db.rpc ? undefined : 0 }) // fallback
  //   .eq('id', body.ad_id);
 
  // Increment click_count using raw SQL via rpc or direct update
  const { error: rpcErr } = await db.rpc('increment_ad_click', { p_ad_id: body.ad_id });

  if (rpcErr) {
    // Fallback: manual increment
    const { data: ad } = await db
      .from('ads')
      .select('click_count')
      .eq('id', body.ad_id)
      .single();

    if (ad) {
      await db
        .from('ads')
        .update({ click_count: (ad.click_count || 0) + 1 })
        .eq('id', body.ad_id);
    }
  }
 
  return ok({ tracked: true }, request, env);
}
 
 
// ── POST /v2/ads/impression (public — batch track impressions) ──
async function handleAdImpression(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const body = await request.json().catch(() => ({})) as any;
  const adIds = body.ad_ids as string[];
  if (!adIds || !Array.isArray(adIds) || adIds.length === 0) {
    return err('ad_ids array required', 400, request, env);
  }
 
  // Increment view_count for each ad
  for (const adId of adIds) {
    const { data: ad } = await db.from('ads').select('view_count').eq('id', adId).single();
    if (ad) {
      await db.from('ads').update({ view_count: (ad.view_count || 0) + 1 }).eq('id', adId);
    }
  }
 
  return ok({ tracked: true, count: adIds.length }, request, env);
}
 
 
// ── GET /v2/admin/ads?placement=&page=&limit= ──
async function handleAdminGetAds(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
 
  const placement = url.searchParams.get('placement');
  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1'));
  const limit = Math.min(50, parseInt(url.searchParams.get('limit') || '20'));
  const offset = (page - 1) * limit;
 
  let query = db
    .from('ads')
    .select('*', { count: 'exact' })
    .order('created_at', { ascending: false })
    .range(offset, offset + limit - 1);
 
  if (placement) query = query.eq('placement', placement);
 
  const { data, error: dbErr, count } = await query;
  if (dbErr) return err(dbErr.message, 500, request, env);
 
  return ok(data, request, env, {
    pagination: { page, limit, total: count, pages: Math.ceil((count || 0) / limit) }
  });
}
 
 
// ── POST /v2/admin/ads ──
async function handleAdminCreateAd(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
 
  const body = await request.json().catch(() => null) as any;
  if (!body) return err('Invalid request body', 400, request, env);
  if (!body.title || !body.image_url || !body.link || !body.placement) {
    return err('title, image_url, link, and placement are required', 400, request, env);
  }
 
  const { data, error: dbErr } = await db
    .from('ads')
    .insert({
      title: body.title,
      image_url: body.image_url,
      link: body.link,
      placement: body.placement,
      ad_type: body.ad_type || 'banner',
      weight: body.weight || 100,
      active: body.active !== false,
      starts_at: body.starts_at || null,
      ends_at: body.ends_at || null,
      card_name: body.card_name || null,
      card_category: body.card_category || null,
      card_price: body.card_price || null,
      card_badge: body.card_badge || 'Sponsored',
    })
    .select()
    .single();
 
  if (dbErr) return err(dbErr.message, 500, request, env);
  return jsonResponse({ ok: true, data }, 201, request, env);
}
 
 
// ── PATCH /v2/admin/ads/:id ──
async function handleAdminUpdateAd(
  db: SupabaseClient, id: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
 
  const body = await request.json().catch(() => ({})) as any;
 
  const allowed = ['title', 'image_url', 'link', 'placement', 'ad_type', 'weight',
    'active', 'starts_at', 'ends_at', 'card_name', 'card_category', 'card_price', 'card_badge'];
  const updates: Record<string, any> = {};
  for (const key of allowed) {
    if (body[key] !== undefined) updates[key] = body[key];
  }
 
  const { data, error: dbErr } = await db
    .from('ads')
    .update(updates)
    .eq('id', id)
    .select()
    .single();
 
  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok(data, request, env);
}
 
 
// ── DELETE /v2/admin/ads/:id ──
async function handleAdminDeleteAd(
  db: SupabaseClient, id: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
 
  const { error: dbErr } = await db.from('ads').delete().eq('id', id);
  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok({ deleted: true }, request, env);
}

async function handleAdminUndoReject(
  db: SupabaseClient, ref: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const { data: order, error: findErr } = await db
    .from('orders')
    .select('id, status, order_ref')
    .eq('order_ref', ref)
    .single();

  if (findErr || !order) return err('Order not found', 404, request, env);
  if (order.status !== 'rejected_pending') {
    return err(`Cannot undo — status is "${order.status}"`, 400, request, env);
  }

  await db.from('orders').update({
    status: 'pending_manual',
    notes: null,
    updated_at: new Date().toISOString(),
  }).eq('id', order.id);

  await logEvent(db, 'order', order.id, 'rejection_undone', auth.userId, {
    order_ref: order.order_ref,
  });

  return ok({ undone: true, order_ref: ref }, request, env);
}


// ============================================================
// HANDLER: POST /v2/admin/products (Create product)
// ============================================================
async function handleAdminCreateProduct(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json() as any;

  if (!body.name || !body.slug) {
    return err('Name and slug are required', 400, request, env);
  }

  // Check slug uniqueness
  const { data: existing } = await db
    .from('products')
    .select('id')
    .eq('slug', body.slug)
    .limit(1);

  if (existing && existing.length > 0) {
    return err(`Slug "${body.slug}" already exists`, 400, request, env);
  }

  const allowed = [
    'name', 'slug', 'status', 'stock_status', 'price_1m', 'price_3m', 'price_6m', 'price_1y',
    'category', 'tags', 'short_description', 'description', 'category_tagline',
    'domain', 'billing_type', 'billing_period', 'featured', 'sort_order', 'image_url',
  ];
  const insert: Record<string, any> = {};
  for (const key of allowed) {
    if (body[key] !== undefined) insert[key] = body[key];
  }
  // Defaults
  if (!insert.status) insert.status = 'active';
  if (!insert.stock_status) insert.stock_status = 'in_stock';
  if (!insert.billing_type) insert.billing_type = 'subscription';
  if (insert.sort_order === undefined) insert.sort_order = 100;
  insert.created_at = new Date().toISOString();
  insert.updated_at = new Date().toISOString();

  const { data, error: dbErr } = await db
    .from('products')
    .insert(insert)
    .select()
    .single();

  if (dbErr) return err(dbErr.message, 500, request, env);

  await logEvent(db, 'product', data.id, 'created', auth.userId, { name: data.name });

  return ok(data, request, env);
}


// ============================================================
// HANDLER: GET /v2/admin/discounts (List all discount codes)
// ============================================================
async function handleAdminGetDiscounts(
  db: SupabaseClient, url: URL, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const limit = Math.min(200, Math.max(1, parseInt(url.searchParams.get('limit') || '100')));

  const { data, error: dbErr } = await db
    .from('discount_codes')
    .select('*')
    .order('created_at', { ascending: false })
    .limit(limit);

  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok(data, request, env);
}


// ============================================================
// HANDLER: POST /v2/admin/discounts (Create discount code)
// ============================================================
async function handleAdminCreateDiscount(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json() as any;

  if (!body.code) return err('Code is required', 400, request, env);
  if (!body.type || !['percentage', 'fixed'].includes(body.type)) {
    return err('Type must be "percentage" or "fixed"', 400, request, env);
  }
  if (body.value === undefined || body.value === null || Number(body.value) <= 0) {
    return err('Value must be a positive number', 400, request, env);
  }

  // Check code uniqueness
  const { data: existing } = await db
    .from('discount_codes')
    .select('id')
    .eq('code', body.code.toUpperCase())
    .limit(1);

  if (existing && existing.length > 0) {
    return err(`Discount code "${body.code}" already exists`, 400, request, env);
  }

  const allowed = [
    'code', 'type', 'value', 'active', 'min_order_ngn', 'max_uses',
    'expires_at', 'active_from', 'max_discount_ngn',
    'included_products', 'excluded_products',
    'included_categories', 'excluded_categories',
    'auto_apply', 'scope', 'exclusive',
  ];
  const insert: Record<string, any> = {};
  for (const key of allowed) {
    if (body[key] !== undefined) insert[key] = body[key];
  }
  // Normalize
  insert.code = (insert.code || '').toUpperCase();
  if (insert.active === undefined) insert.active = true;
  if (insert.times_used === undefined) insert.times_used = 0;
  if (insert.min_order_ngn === undefined) insert.min_order_ngn = 0;
  if (insert.auto_apply === undefined) insert.auto_apply = false;
  if (insert.exclusive === undefined) insert.exclusive = false;
  if (insert.scope === undefined) insert.scope = 'site_wide';
  // Convert empty strings to null for date fields
  if (insert.expires_at === '') insert.expires_at = null;
  if (insert.active_from === '') insert.active_from = null;
  // Convert empty strings to null for nullable text fields
  if (insert.included_products === '') insert.included_products = null;
  if (insert.excluded_products === '') insert.excluded_products = null;
  if (insert.included_categories === '') insert.included_categories = null;
  if (insert.excluded_categories === '') insert.excluded_categories = null;
  // Convert 0/empty to null for nullable number fields
  if (!insert.max_uses) insert.max_uses = null;
  if (!insert.max_discount_ngn) insert.max_discount_ngn = null;

  insert.created_at = new Date().toISOString();

  const { data, error: dbErr } = await db
    .from('discount_codes')
    .insert(insert)
    .select()
    .single();

  if (dbErr) return err(dbErr.message, 500, request, env);

  await logEvent(db, 'discount', data.id, 'created', auth.userId, { code: data.code });

  return ok(data, request, env);
}


// ============================================================
// HANDLER: PATCH /v2/admin/discounts/:id (Update discount)
// ============================================================
async function handleAdminUpdateDiscount(
  db: SupabaseClient, discountId: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json() as any;

  const allowed = [
    'code', 'type', 'value', 'active', 'min_order_ngn', 'max_uses',
    'expires_at', 'active_from', 'max_discount_ngn',
    'included_products', 'excluded_products',
    'included_categories', 'excluded_categories',
    'auto_apply', 'scope', 'exclusive',
  ];
  const updates: Record<string, any> = {};
  for (const key of allowed) {
    if (body[key] !== undefined) updates[key] = body[key];
  }

  // Normalize
  if (updates.code) updates.code = updates.code.toUpperCase();
  if (updates.expires_at === '') updates.expires_at = null;
  if (updates.active_from === '') updates.active_from = null;
  if (updates.included_products === '') updates.included_products = null;
  if (updates.excluded_products === '') updates.excluded_products = null;
  if (updates.included_categories === '') updates.included_categories = null;
  if (updates.excluded_categories === '') updates.excluded_categories = null;
  if (updates.max_uses === 0 || updates.max_uses === '') updates.max_uses = null;
  if (updates.max_discount_ngn === 0 || updates.max_discount_ngn === '') updates.max_discount_ngn = null;

  if (Object.keys(updates).length === 0) {
    return err('No valid fields to update', 400, request, env);
  }

  const { data, error: dbErr } = await db
    .from('discount_codes')
    .update(updates)
    .eq('id', discountId)
    .select()
    .single();

  if (dbErr) return err(dbErr.message, 500, request, env);

  await logEvent(db, 'discount', discountId, 'updated', auth.userId, updates);

  return ok(data, request, env);
}


// ============================================================
// HANDLER: DELETE /v2/admin/discounts/:id (Delete discount)
// ============================================================
async function handleAdminDeleteDiscount(
  db: SupabaseClient, discountId: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  // Fetch code for logging before delete
  const { data: existing } = await db
    .from('discount_codes')
    .select('id, code')
    .eq('id', discountId)
    .limit(1);

  if (!existing || existing.length === 0) {
    return err('Discount not found', 404, request, env);
  }

  const { error: dbErr } = await db
    .from('discount_codes')
    .delete()
    .eq('id', discountId);

  if (dbErr) return err(dbErr.message, 500, request, env);

  await logEvent(db, 'discount', discountId, 'deleted', auth.userId, { code: existing[0].code });

  return ok({ deleted: true, id: discountId }, request, env);
}

async function handleGetSettings(db: any, request: Request, env: any) {
  const { data, error } = await db.from('settings').select('*').single()

  if (error) return err(error.message, 500, request, env)

  return ok(data, request, env)
}

async function handleUpdateSettings(db: any, request: Request, env: any) {
  const body: unknown = await request.json()
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return err('Invalid request body', 400, request, env)
  }

  const { data, error } = await db
    .from('settings')
    .update({
      ...(body as Record<string, unknown>),
      updated_at: new Date().toISOString()
    })
    .eq('id', 1)
    .select()
    .single()

  if (error) return err(error.message, 500, request, env)

  return ok(data, request, env)
}

// ── helper: get user id from Bearer token ───────────────────────
async function requireAuth(
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
 
// ── POST /v2/auth/signup ─────────────────────────────────────────
async function handleCustomerSignup(db: any, request: Request, env: any): Promise<Response> {
  const body = await request.json().catch(() => null) as any
  if (!body?.email) return err('Email is required', 400, request, env)
 
  const { user_id, full_name, email, phone, gender } = body
 
  // 1. Upsert profile row (id = Supabase auth UUID)
  await db.from('profiles').upsert({
    id:        user_id,
    role:      'customer',
    full_name: full_name || null,
    email:     email,
    phone:     phone || null,
    gender:    gender || null,
  }, { onConflict: 'id' })
 
  // 2. Create or link customers row
  const emailLower = email.toLowerCase()

  const { data: existing } = await db.from('customers')
    .select('id, user_id')
    .ilike('email', emailLower)
    .limit(1)

  if (!existing?.length) {
    await db.from('customers').insert({
      user_id: user_id,
      name:    full_name || email.split('@')[0],
      email: emailLower,
      phone:   phone || null,
      source:  'customer_signup',
      is_active: true,
    })
  } else {
    // Link existing customer record to this auth user
    await db
    .from('customers')
    .update({ user_id })
    .ilike('email', emailLower)
    .is('user_id', null)
  }
 
  // 3. Create wallet if not exists
  const { data: walletExists } = await db.from('wallets').select('id').eq('user_id', user_id).limit(1)
  if (!walletExists?.length) {
    await db.from('wallets').insert({ user_id, balance_ngn: 0 })
  }
 
  return ok({ success: true }, request, env)
}
 
// ── GET /v2/me ───────────────────────────────────────────────────
async function handleGetMe(db: any, request: Request, env: any): Promise<Response> {
  const auth = await requireAuth(db, request, env)
  if (!auth.ok) return auth.response
 
  const { data: profile } = await db.from('profiles').select('*').eq('id', auth.userId).limit(1)
  if (!profile?.length) return err('Profile not found', 404, request, env)
 
  return ok({
    ...profile[0],
    email: profile[0].email || auth.email,
  }, request, env)
}
 
// ── PATCH /v2/me ─────────────────────────────────────────────────
async function handleUpdateMe(db: any, request: Request, env: any): Promise<Response> {
  const auth = await requireAuth(db, request, env)
  if (!auth.ok) return auth.response
 
  const body = await request.json().catch(() => ({})) as any
  const allowed: Record<string, any> = {}
  if (body.full_name !== undefined) allowed.full_name = body.full_name
  if (body.phone     !== undefined) allowed.phone     = body.phone
  if (body.location  !== undefined) allowed.location  = body.location
  if (body.avatar_url!== undefined) allowed.avatar_url= body.avatar_url
 
  const { data, error } = await db.from('profiles').update(allowed).eq('id', auth.userId).select().single()
  if (error) return err(error.message, 500, request, env)
 
  // Also sync to customers table
  if (allowed.full_name || allowed.phone) {
    const patch: any = {}
    if (allowed.full_name) patch.name  = allowed.full_name
    if (allowed.phone)     patch.phone = allowed.phone
    await db.from('customers').update(patch).eq('user_id', auth.userId)
  }
 
  return ok(data, request, env)
}
 
// ── GET /v2/me/orders ────────────────────────────────────────────
async function handleGetMyOrders(db: any, request: Request, env: any): Promise<Response> {
  const auth = await requireAuth(db, request, env)
  if (!auth.ok) return auth.response
 
  // Get customer's email from profile
  const { data: profile } = await db.from('profiles').select('email').eq('id', auth.userId).limit(1)
  const email = profile?.[0]?.email || auth.email
 
  const { data: orders, error } = await db
    .from('orders')
    .select(`
      id, order_ref, status, total_ngn, subtotal_ngn, discount_ngn,
      payment_method, currency, created_at, updated_at,
      order_items (
        id, product_name, billing_period, quantity, unit_price_ngn, total_price_ngn
      )
    `)
    .eq('customer_email', email)
    .order('created_at', { ascending: false })
    .limit(50)
 
  if (error) return err(error.message, 500, request, env)
  return ok(orders || [], request, env)
}
 
// ── GET /v2/me/wallet ────────────────────────────────────────────
async function handleGetMyWallet(db: any, request: Request, env: any): Promise<Response> {
  const auth = await requireAuth(db, request, env)
  if (!auth.ok) return auth.response
 
  const { data, error } = await db.from('wallets').select('*').eq('user_id', auth.userId).limit(1)
  if (error) return err(error.message, 500, request, env)
 
  // Auto-create wallet if missing
  if (!data?.length) {
    const { data: newWallet } = await db.from('wallets').insert({ user_id: auth.userId, balance_ngn: 0 }).select().single()
    return ok(newWallet || { balance_ngn: 0 }, request, env)
  }
 
  return ok(data[0], request, env)
}
 
// ── GET /v2/me/wallet/transactions ───────────────────────────────
async function handleGetMyWalletTxns(
  db: SupabaseClient,
  request: Request,
  env: Env
): Promise<Response> {

  const auth = await requireAuth(db, request, env)
  if (!auth.ok) return auth.response

  // 1. Get wallet
  const { data: wallet, error: wErr } = await db
    .from('wallets')
    .select('id')
    .eq('user_id', auth.userId)
    .single()

  if (wErr || !wallet) {
    return ok([], request, env)
  }

  // 2. Get transactions using wallet.id
  const { data, error } = await db
    .from('wallet_transactions')
    .select('*')
    .eq('wallet_id', wallet.id)
    .order('created_at', { ascending: false })

  if (error) return err(error.message, 500, request, env)

  return ok(data || [], request, env)
}
 
// ── GET /v2/me/messages ──────────────────────────────────────────
async function handleGetMyMessages(db: any, request: Request, env: any): Promise<Response> {
  const auth = await requireAuth(db, request, env)
  if (!auth.ok) return auth.response
 
  // Find customer row
  const { data: customer } = await db.from('customers').select('id').eq('user_id', auth.userId).limit(1)
  if (!customer?.length) return ok([], request, env)
 
  const { data, error } = await db
    .from('customer_messages')
    .select('*')
    .eq('customer_id', customer[0].id)
    .order('created_at', { ascending: false })
    .limit(50)
 
  if (error) return err(error.message, 500, request, env)
  return ok(data || [], request, env)
}
 
// ── PATCH /v2/me/messages/:id/read ──────────────────────────────
async function handleMarkMessageRead(db: any, request: Request, env: any): Promise<Response> {
  const auth = await requireAuth(db, request, env)
  if (!auth.ok) return auth.response
 
  const url  = new URL(request.url)
  const parts = url.pathname.split('/')
  const msgId = parts[parts.indexOf('messages') + 1]
 
  // Verify ownership via customer_id
  const { data: customer } = await db.from('customers').select('id').eq('user_id', auth.userId).limit(1)
  if (!customer?.length) return err('Not found', 404, request, env)
 
  await db.from('customer_messages')
    .update({ is_read: true })
    .eq('id', msgId)
    .eq('customer_id', customer[0].id)
 
  return ok({ updated: true }, request, env)
}
 
// ── POST /v2/admin/customers/:id/messages ────────────────────────
async function handleAdminSendMessage(db: any, request: Request, env: any): Promise<Response> {
  const auth = await requireAdmin(db, request, env)
  if (!auth.ok) return auth.response
 
  const url        = new URL(request.url)
  const customerId = url.pathname.split('/').at(-2)!
  const body       = await request.json().catch(() => null) as any
 
  if (!body?.subject) return err('subject is required', 400, request, env)
  if (!body?.body)    return err('body is required',    400, request, env)
 
  const { data, error } = await db.from('customer_messages').insert({
    customer_id:    customerId,
    subject:        body.subject,
    product_name:   body.product_name   || null,
    product_domain: body.product_domain || null,
    body:           body.body,
    expires_at:     body.expires_at     || null,
    created_by:     auth.userId,
  }).select().single()
 
  if (error) return err(error.message, 500, request, env)
  return ok(data, request, env)
}
 
// ── POST /v2/admin/customers/:id/wallet/topup ────────────────────
async function handleAdminWalletTopup(db: any, request: Request, env: any): Promise<Response> {
  const auth = await requireAdmin(db, request, env)
  if (!auth.ok) return auth.response
 
  const url        = new URL(request.url)
  const parts      = url.pathname.split('/')
  const customerId = parts[parts.indexOf('customers') + 1]
  const body       = await request.json().catch(() => null) as any
 
  if (!body?.amount_ngn || body.amount_ngn <= 0) return err('amount_ngn must be positive', 400, request, env)
 
  // Get customer's user_id
  const { data: customer } = await db.from('customers').select('user_id').eq('id', customerId).limit(1)
  if (!customer?.length || !customer[0].user_id) return err('Customer not found or not linked to auth account', 404, request, env)
 
  const userId = customer[0].user_id

  // Get or create wallet
  let { data: wallet } = await db.from('wallets').select('*').eq('user_id', userId).limit(1)
  if (!wallet?.length) {
    const { data: newWallet } = await db.from('wallets').insert({ user_id: userId, balance_ngn: 0 }).select().single()
    wallet = [newWallet]
  }
 
  const newBalance = Number(wallet[0].balance_ngn) + Number(body.amount_ngn)

  if (!wallet[0].is_active) {
    return err('Wallet disabled', 403, request, env)
  }

  const sourceMap: Record<string, 'admin' | 'refund'> = {
    admin_topup: 'admin',
    refund: 'refund',
    promotion: 'admin',
    compensation: 'admin',
  }
  
  const { error: rpcError } = await db.rpc('credit_wallet', {
    p_wallet_id: wallet[0].id,
    p_amount: Number(body.amount_ngn),
    p_reference: body.reference || body.source || 'admin topup',
    p_source: sourceMap[body.source] || 'admin',
  })
  
  if (rpcError) return err(rpcError.message, 500, request, env)
 
  await logEvent(db, 'wallet', wallet[0].id, 'topup', auth.userId, {
    amount_ngn: body.amount_ngn,
    customer_id: customerId,
    new_balance: newBalance,
  })
 
  // fetch fresh balance
  const { data: updatedWallet } = await db
  .from('wallets')
  .select('balance_ngn')
  .eq('id', wallet[0].id)
  .single()

  return ok({ balance_ngn: updatedWallet?.balance_ngn }, request, env)
}

async function handleAdminWalletDebit(db: any, request: Request, env: any) {
  const auth = await requireAdmin(db, request, env)
  if (!auth.ok) return auth.response

  const { customer_id, amount, reference } = await request.json().catch(() => null) as any

  if (!customer_id || !amount || amount <= 0) {
    return err('Invalid payload', 400, request, env)
  }

  const { data: customer } = await db
    .from('customers')
    .select('user_id')
    .eq('id', customer_id)
    .single()

  if (!customer?.user_id) return err('Customer not found', 404, request, env)

  const { data: wallet } = await db
    .from('wallets')
    .select('id')
    .eq('user_id', customer.user_id)
    .single()

  if (!wallet) return err('Wallet not found', 404, request, env)

  const { error } = await db.rpc('debit_wallet', {
    p_wallet_id: wallet.id,
    p_amount: Number(amount),
    p_reference: reference || 'admin debit',
  })

  if (error) return err(error.message, 500, request, env)

  return ok({ success: true }, request, env)
}


async function handleAdminToggleWallet(db: any, request: Request, env: any) {
  const auth = await requireAdmin(db, request, env)
  if (!auth.ok) return auth.response

  const customerId = request.url.split('/')[4]

  // 1. get user_id from customer
  const { data: customer } = await db
  .from('customers')
  .select('user_id')
  .eq('id', customerId)
  .single()

  if (!customer?.user_id) return err('Customer not found', 404, request, env)

  // 2. get wallet using user_id
  const { data: wallet } = await db
  .from('wallets')
  .select('id, is_active')
  .eq('user_id', customer.user_id)
  .single()

  if (!wallet) return err('Wallet not found', 404, request, env)

  const { error } = await db
    .from('wallets')
    .update({ is_active: !wallet.is_active })
    .eq('id', wallet.id)

  if (error) return err(error.message, 500, request, env)

  return ok({ is_active: !wallet.is_active }, request, env)
}
 
// ── POST /v2/admin/customers/:id/reset-password ──────────────────
async function handleAdminForceReset(db: any, request: Request, env: any): Promise<Response> {
  const auth = await requireAdmin(db, request, env)
  if (!auth.ok) return auth.response
 
  const url        = new URL(request.url)
  const customerId = url.pathname.split('/').at(-2)!
 
  // Get customer auth user_id
  const { data: customer } = await db.from('customers').select('user_id, email').eq('id', customerId).limit(1)
  if (!customer?.length || !customer[0].user_id) return err('Customer not linked to auth account', 404, request, env)
 
  // Use Supabase Admin API (requires service_role key — call from Worker env)
  const serviceRoleKey = (env as any).SUPABASE_SERVICE_ROLE_KEY || ''
  if (!serviceRoleKey) return err('Service role key not configured', 500, request, env)
 
  // Generate random temporary password
  const tempPassword = Math.random().toString(36).slice(2, 10) + Math.random().toString(36).slice(2, 6).toUpperCase() + '!7'
 
  const res = await fetch(`${(env as any).SUPABASE_URL}/auth/v1/admin/users/${customer[0].user_id}`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      'apikey': serviceRoleKey,
      'Authorization': `Bearer ${serviceRoleKey}`,
    },
    body: JSON.stringify({ password: tempPassword }),
  })
 
  if (!res.ok) {
    const detail = await res.text()
    return err('Failed to reset password: ' + detail, 500, request, env)
  }
 
  await logEvent(db, 'customer', customer[0].user_id, 'force_password_reset', auth.userId, {
    customer_id: customerId,
  })
 
  return ok({ temp_password: tempPassword, email: customer[0].email }, request, env)
}

/* ══════════════════════════════════════════════════════════════════
   PART 8 — PDF generator for receipts
   Hand-rolled minimal PDF writer (no external deps). Suitable for
   Cloudflare Workers. Output is a single-page A4 receipt.
   ══════════════════════════════════════════════════════════════════ */
 
async function buildReceiptPdf(order: any): Promise<Uint8Array> {
  const items: any[] = order.order_items || [];
  // PDF strings must not contain Unicode "₦" in a vanilla Helvetica encoding
  const fmtNGN = (n: number) => `NGN ${Number(n || 0).toLocaleString('en-NG', { minimumFractionDigits: 2 })}`;
  const fmtDate = (iso: string) => {
    try { return new Date(iso).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }); }
    catch { return iso || ''; }
  };

  // A4 in PDF points (1pt = 1/72 inch). PdfWriter uses bottom-left origin.
  const W = 595.28, H = 841.89;
  const ML = 36, MR = 36, R = W - MR;

  const p = new PdfWriter();

  // ── Logo placeholder (purple square + "B") ──────────────────────────
  // PdfWriter has no roundedRect; use a plain filled rect as fallback
  const LOGO_X = ML, LOGO_Y = H - 52, LOGO_SZ = 32;
  p.rect(LOGO_X, LOGO_Y, LOGO_SZ, LOGO_SZ, '#7C5CFF');
  p.text('B', LOGO_X + 9, LOGO_Y + 11, 18, '#ffffff', 'bold');

  // Company name + URL beneath logo
  p.text('BuySub',    ML, LOGO_Y - 10, 9.5, '#16161c', 'bold');
  p.text('buysub.ng', ML, LOGO_Y - 20, 8,   '#787888');

  // "RECEIPT" label + order ref (top-right)
  p.text('RECEIPT',              R, H - 22, 22, '#16161c', 'bold', 'right');
  p.text(`Order: ${order.order_ref || ''}`, R, H - 38, 8.5, '#787888', 'normal', 'right');

  // Divider under header area
  const divY = H - 62;
  p.hline(ML, divY, R, '#b4b4c0');

  // ── Two-column customer + meta block ────────────────────────────────
  const metaLX = ML + 258; // right-hand column starts here
  let custY = divY - 14;

  p.text('Addressed To:', ML, custY, 8, '#787888'); custY -= 11;
  p.text(order.customer_name || '—', ML, custY, 10, '#14141c', 'bold'); custY -= 10;
  if (order.customer_phone) { p.text(order.customer_phone, ML, custY, 8.5, '#555566'); custY -= 9; }
  if (order.customer_email) { p.text(order.customer_email, ML, custY, 8.5, '#555566'); custY -= 9; }

  // Meta rows (right column aligned to same top)
  const metaTopY = divY - 14;
  const metaRow = (label: string, val: string, ry: number) => {
    p.text(label, metaLX, ry, 8, '#787888');
    p.text(val || '—', R, ry, 8.5, '#16161c', 'normal', 'right');
  };
  const orderDate = fmtDate(order.created_at || new Date().toISOString());
  metaRow('Payment Date:', orderDate,                  metaTopY);
  metaRow('Payment:',      order.payment_method || '—', metaTopY - 11);
  metaRow('Generated:',    fmtDate(new Date().toISOString()), metaTopY - 22);

  // Items table starts below the lower of the two columns
  let y = Math.min(custY, metaTopY - 33) - 10;

  // ── Table header ────────────────────────────────────────────────────
  const TBL_H = 17;
  p.rect(ML, y - 3, R - ML, TBL_H, '#2a2a34');
  const tx_name   = ML + 6;
  const tx_period = ML + 204;
  const tx_units  = ML + 282;
  const tx_rate   = ML + 318;
  const tx_amount = R - 6;
  p.text('ITEM',   tx_name,   y + 9,  8, '#ffffff', 'bold');
  p.text('PERIOD', tx_period, y + 9,  8, '#ffffff', 'bold');
  p.text('UNITS',  tx_units,  y + 9,  8, '#ffffff', 'bold');
  p.text('RATE',   tx_rate,   y + 9,  8, '#ffffff', 'bold');
  p.text('AMOUNT', tx_amount, y + 9,  8, '#ffffff', 'bold', 'right');
  y -= TBL_H;

  // ── Item rows ────────────────────────────────────────────────────────
  for (const it of items) {
    const unitNGN = it.unit_price_ngn ?? (it.total_price_ngn / (it.quantity || 1));
    const lineNGN = it.total_price_ngn ?? (unitNGN * (it.quantity || 1));
    const nameLine = truncate(it.product_name || '', 32);

    y -= 4; // top padding
    p.text(nameLine, tx_name, y, 9.5, '#14141c');

    // category sub-line (if present)
    const hasCat = !!it.category;
    if (hasCat) {
      p.text(it.category, tx_name, y - 8, 7.5, '#828294');
    }

    p.text(it.billing_period || 'One-time', tx_period, y, 9, '#555566');
    p.text(String(it.quantity || 1),        tx_units,  y, 9, '#555566');
    p.text(fmtNGN(unitNGN),                 tx_rate,   y, 9, '#555566');
    p.text(fmtNGN(lineNGN),                 tx_amount, y, 9.5, '#14141c', 'normal', 'right');

    const rowH = hasCat ? 22 : 14;
    p.hline(ML, y - (hasCat ? 11 : 4), R, '#d7d7de');
    y -= rowH;
  }

  // ── Totals ────────────────────────────────────────────────────────────
  y -= 8;
  const totLX = R - 168, totRX = R - 6;

  const totRow = (label: string, value: string, bold = false, green = false) => {
    const sz = bold ? 10.5 : 9;
    p.text(label, totLX, y, sz, green ? '#16a34a' : '#6b6b7e', bold ? 'bold' : 'normal');
    p.text(value, totRX, y, sz, green ? '#16a34a' : (bold ? '#14141c' : '#444454'), bold ? 'bold' : 'normal', 'right');
    y -= bold ? 8 : 7;
  };

  const hasDiscount = order.discount_ngn && order.discount_ngn > 0;
  if (hasDiscount) {
    totRow('Subtotal', fmtNGN(order.subtotal_ngn || 0));
    totRow(
      `Discount${order.discount_code ? ' (' + order.discount_code + ')' : ''}`,
      '-' + fmtNGN(order.discount_ngn),
      false, true,
    );
    p.hline(totLX, y + 3, R, '#bebece');
    y -= 6;
  }
  totRow('Total', fmtNGN(order.total_ngn), true);

  // ── Notes ────────────────────────────────────────────────────────────
  y -= 14;
  const note = 'Thank you for your purchase with BuySub! For support, email help@buysub.ng or message us on WhatsApp.';
  p.text('Notes:', ML, y, 8, '#787888'); y -= 8;
  // Wrap note manually at ~90 chars per line
  const noteWords = note.split(' ');
  let line = '';
  for (const word of noteWords) {
    if ((line + word).length > 88) {
      p.text(line.trim(), ML, y, 8.5, '#464658'); y -= 8;
      line = '';
    }
    line += word + ' ';
  }
  if (line.trim()) { p.text(line.trim(), ML, y, 8.5, '#464658'); y -= 8; }

  // ── Footer ───────────────────────────────────────────────────────────
  const FOOTER_H = 36;
  p.rect(0, 0, W, FOOTER_H, '#7C5CFF');
  p.text('buysub.ng  ·  help@buysub.ng', W / 2, FOOTER_H / 2 + 2, 9, '#ffffff', 'bold', 'center');

  return p.build();
}
 
function truncate(s: string, n: number): string {
  return s.length > n ? s.slice(0, n - 1) + '…' : s;
}
 
function escHtml(s: string): string {
  if (s == null) return '';
  return String(s).replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c] as string));
}
 
function bytesToBase64(bytes: Uint8Array): string {
  let s = '';
  const CHUNK = 0x8000;
  for (let i = 0; i < bytes.length; i += CHUNK) {
    s += String.fromCharCode.apply(null, bytes.subarray(i, i + CHUNK) as any);
  }
  return btoa(s);
}
 
/* ── Minimal PDF writer (text, lines, filled rects). Single-page. ── */
class PdfWriter {
  private ops: string[] = [];
 
  private esc(s: string): string {
    return String(s).replace(/[\\()]/g, m => '\\' + m);
  }
  private hexToRgb(hex: string): [number, number, number] {
    const h = hex.replace('#', '');
    const n = parseInt(h.length === 3 ? h.split('').map(c => c + c).join('') : h, 16);
    return [((n >> 16) & 255) / 255, ((n >> 8) & 255) / 255, (n & 255) / 255];
  }
 
  text(str: string, x: number, y: number, size: number, color = '#000000',
       weight: 'normal' | 'bold' = 'normal', align: 'left' | 'right' | 'center' = 'left') {
    // Approximate width for alignment (Helvetica avg char width at 500 units/em)
    const approxW = (str.length * size * (weight === 'bold' ? 0.58 : 0.52));
    let ax = x;
    if (align === 'right')  ax = x - approxW;
    if (align === 'center') ax = x - approxW / 2;
 
    const [r, g, b] = this.hexToRgb(color);
    const font = weight === 'bold' ? '/F2' : '/F1';
    this.ops.push(`q ${r} ${g} ${b} rg BT ${font} ${size} Tf ${ax} ${y} Td (${this.esc(str)}) Tj ET Q`);
  }
 
  rect(x: number, y: number, w: number, h: number, fill: string) {
    const [r, g, b] = this.hexToRgb(fill);
    this.ops.push(`q ${r} ${g} ${b} rg ${x} ${y} ${w} ${h} re f Q`);
  }
 
  hline(x1: number, y: number, x2: number, color: string) {
    const [r, g, b] = this.hexToRgb(color);
    this.ops.push(`q ${r} ${g} ${b} RG 0.5 w ${x1} ${y} m ${x2} ${y} l S Q`);
  }
 
  build(): Uint8Array {
    const content = this.ops.join('\n');
    const objs: string[] = [];
    objs.push('<< /Type /Catalog /Pages 2 0 R >>');
    objs.push('<< /Type /Pages /Kids [3 0 R] /Count 1 >>');
    objs.push(
      '<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595.28 841.89] ' +
      '/Resources << /Font << /F1 5 0 R /F2 6 0 R >> >> /Contents 4 0 R >>'
    );
    objs.push(`<< /Length ${content.length} >>\nstream\n${content}\nendstream`);
    objs.push('<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>');
    objs.push('<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>');
 
    let body = '%PDF-1.4\n%\xFF\xFF\xFF\xFF\n';
    const offsets: number[] = [];
    objs.forEach((o, i) => {
      offsets.push(body.length);
      body += `${i + 1} 0 obj\n${o}\nendobj\n`;
    });
    const xrefStart = body.length;
    body += `xref\n0 ${objs.length + 1}\n0000000000 65535 f \n`;
    for (const off of offsets) body += `${String(off).padStart(10, '0')} 00000 n \n`;
    body += `trailer\n<< /Size ${objs.length + 1} /Root 1 0 R >>\nstartxref\n${xrefStart}\n%%EOF`;
 
    const buf = new Uint8Array(body.length);
    for (let i = 0; i < body.length; i++) buf[i] = body.charCodeAt(i) & 0xff;
    return buf;
  }
}