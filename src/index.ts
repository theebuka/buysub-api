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
      if (path === '/v2/admin/links' && method === 'GET') {
        return handleAdminGetLinks(db, url, request, env);
      }
      if (path === '/v2/admin/links' && method === 'POST') {
        return handleAdminCreateLink(db, request, env);
      }
      if (path.match(/^\/v2\/admin\/links\/[^/]+$/) && method === 'PATCH') {
        const id = path.split('/')[4];
        return handleAdminUpdateLink(db, id, request, env);
      }
      if (path.match(/^\/v2\/admin\/links\/[^/]+$/) && method === 'DELETE') {
        const id = path.split('/')[4];
        return handleAdminDeleteLink(db, id, request, env);
      }
      if (path.match(/^\/v2\/admin\/links\/[^/]+\/stats$/) && method === 'GET') {
        const id = path.split('/')[4];
        return handleAdminLinkStats(db, id, request, env);
      }
 
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
    const fxRate = body.fx_rate || 1;
    const currency = body.currency || 'NGN';
    const fmtAmt = (v: number) => {
      if (currency === 'NGN') return `₦${Math.ceil(v).toLocaleString()}`;
      return `${currency} ${(v * fxRate).toFixed(2)}`;
    };

    const whatsappNumber = env.WHATSAPP_NUMBER || '2348107872916';
    const frontendUrl = env.FRONTEND_URL || 'https://app.buysub.ng';

    const lines: string[] = [
      `🛒 *New WhatsApp Order*`,
      ``,
      `📋 Order Ref: *${orderRef}*`,
      `📧 Customer: ${body.customer_email}`,
      body.customer_name ? `👤 Name: ${body.customer_name}` : '',
      body.customer_phone ? `📱 Phone: ${body.customer_phone}` : '',
      `💱 Currency: ${currency}`,
      ``,
      `*Items:*`,
      ...body.items.map(item => {
        const lineTotal = item.unit_price_ngn * item.quantity;
        return `• ${item.product_name} ×${item.quantity} (${item.billing_period}) — ${fmtAmt(lineTotal)}`;
      }),
      ``,
    ];

    if (discountNGN > 0 && discountCode) {
      lines.push(`Subtotal: ${fmtAmt(serverSubtotal)}`);
      lines.push(`Promo (${discountCode}): -${fmtAmt(discountNGN)}`);
    }
    lines.push(`*Total: ${fmtAmt(totalNGN)}*`);
    lines.push(``);
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
  const itemRows = items.map((item: any) =>
    `<tr>
      <td style="padding:8px 12px;border-bottom:1px solid #eee;">${item.product_name}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #eee;">${item.billing_period || 'One-time'}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;">${item.quantity}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:right;">₦${Number(item.total_price_ngn).toLocaleString()}</td>
    </tr>`
  ).join('');

  const html = `
    <div style="font-family:Inter,system-ui,sans-serif;max-width:600px;margin:0 auto;padding:32px 24px;color:#1a1a1a;">
      <div style="text-align:center;margin-bottom:32px;">
        <h1 style="font-size:22px;font-weight:700;margin:0;">Order Confirmed!</h1>
        <p style="font-size:14px;color:#666;margin:8px 0 0;">Ref: ${order.order_ref}</p>
      </div>
      <p style="font-size:14px;line-height:1.6;">Hi ${order.customer_name || 'there'},</p>
      <p style="font-size:14px;line-height:1.6;">Thank you for your purchase with BuySub. Here's your order summary:</p>
      <table style="width:100%;border-collapse:collapse;margin:20px 0;">
        <thead>
          <tr style="background:#2a2a34;color:#fff;">
            <th style="padding:10px 12px;text-align:left;font-size:13px;">Item</th>
            <th style="padding:10px 12px;text-align:left;font-size:13px;">Period</th>
            <th style="padding:10px 12px;text-align:center;font-size:13px;">Qty</th>
            <th style="padding:10px 12px;text-align:right;font-size:13px;">Amount</th>
          </tr>
        </thead>
        <tbody>${itemRows}</tbody>
      </table>
      ${order.discount_ngn > 0 ? `<p style="font-size:14px;">Subtotal: ₦${Number(order.subtotal_ngn).toLocaleString()}<br/>Discount (${order.discount_code}): -₦${Number(order.discount_ngn).toLocaleString()}</p>` : ''}
      <p style="font-size:18px;font-weight:700;">Total: ₦${Number(order.total_ngn).toLocaleString()}</p>
      <p style="font-size:14px;line-height:1.6;">We'll be in touch with your subscription details shortly.</p>
      <hr style="border:none;border-top:1px solid #eee;margin:24px 0;"/>
      <p style="font-size:12px;color:#999;text-align:center;">
        BuySub · <a href="https://buysub.ng" style="color:#7C5CFF;">buysub.ng</a> · help@buysub.ng
      </p>
    </div>
  `;

  await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${env.RESEND_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      from: 'BuySub <noreply@buysub.ng>',
      to: [order.customer_email],
      subject: `Order Confirmed — ${order.order_ref}`,
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
    'domain', 'billing_type', 'billing_period', 'featured', 'sort_order', 'image_url',
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
  ];
  for (const field of required) {
    if (!body[field]) return err(`Missing required field: ${field}`, 400, request, env);
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

  const { data, error: dbErr } = await db
    .from('partner_applications')
    .insert({
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

  if (dbErr) return err(dbErr.message, 500, request, env);

  await logEvent(db, 'partner_application', data.id, 'submitted', null, {
    legal_name: body.legal_name,
    business_email: body.business_email,
  });

  return jsonResponse({ ok: true, data: { id: data.id, status: data.status } }, 201, request, env);
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

  const { data, error: dbErr } = await db
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

  if (dbErr || !data) return err('Application not found or already reviewed', 404, request, env);

  await logEvent(db, 'partner_application', id, 'approved', auth.userId, {});
  return ok(data, request, env);
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
// SHORT LINKS — Admin management
// ══════════════════════════════════════════════════════════
 
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
 
  return ok(data, request, env, {
    pagination: { page, limit, total: count, pages: Math.ceil((count || 0) / limit) }
  });
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
 
  // Generate slug if not provided
  const slug = body.slug?.trim().toLowerCase() ||
    Math.random().toString(36).slice(2, 8);
 
  // Check slug uniqueness
  const { data: existing } = await db
    .from('short_links')
    .select('id')
    .eq('slug', slug)
    .single();
 
  if (existing) return err(`Slug "${slug}" is already taken`, 409, request, env);
 
  const { data, error: dbErr } = await db
    .from('short_links')
    .insert({
      slug,
      destination_url: body.destination_url,
      expires_at: body.expires_at || null,
      click_limit: body.click_limit || null,
      utm_source: body.utm_source || null,
      utm_medium: body.utm_medium || null,
      utm_campaign: body.utm_campaign || null,
      tags: body.tags || null,
      active: true,
    })
    .select()
    .single();
 
  if (dbErr) return err(dbErr.message, 500, request, env);
 
  return jsonResponse({ ok: true, data: { ...data, short_url: `https://go.buysub.ng/${data.slug}` } }, 201, request, env);
}
 
 
// ── PATCH /v2/admin/links/:id ──
async function handleAdminUpdateLink(
  db: SupabaseClient, id: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;
 
  const body = await request.json().catch(() => ({})) as any;
 
  const allowed = ['destination_url', 'slug', 'expires_at', 'click_limit',
    'utm_source', 'utm_medium', 'utm_campaign', 'tags', 'active'];
  const updates: Record<string, any> = {};
  for (const key of allowed) {
    if (body[key] !== undefined) updates[key] = body[key];
  }
  updates.updated_at = new Date().toISOString();
 
  const { data, error: dbErr } = await db
    .from('short_links')
    .update(updates)
    .eq('id', id)
    .select()
    .single();
 
  if (dbErr) return err(dbErr.message, 500, request, env);
  return ok(data, request, env);
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

