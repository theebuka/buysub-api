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
  normalizeVolumeTiers, volumeDiscountNGN,
  calcDiscountNGN, buildDiscountDisplay, splitList, norm,
} from './shared/discount';
import {
  type Env, corsHeaders, jsonResponse, ok, err, escapeLike, orSafe, logEvent,
  requireAdmin, requireAuth, STAFF_ROLES, EMAIL_RE,
} from './http';
import { paystackVerifyTx, safeCallbackUrl } from './features/paystack';
import { getFlags, isOn, notifyUser, userIdForOrder, naira } from './features/core';
import { handleStatus, serviceBlocked, handleAdminGetFlags, handleAdminUpdateFlag } from './features/status';
import { handleGetMyNotifications, handleReadMyNotifications } from './features/inbox';
import { setOrderExpiry, runRenewalReminders, handleRunRenewalReminders } from './features/renewals';
import { handleFundWallet, handleVerifyWalletFunding, settleTopupTx, TOPUP_KIND } from './features/walletFunding';
import {
  attachProductStats, handleGetProductReviews, handleMyReviewFor, handleSubmitReview,
  handleAdminReviews, handleAdminUpdateReview,
} from './features/reviews';
import { handleMyReferrals, resolveCustomerCode, rewardReferral } from './features/referrals';
import { handleCreateStockAlert, sendBackInStock } from './features/stockAlerts';
import {
  handleMyPayouts, handleAdminPayouts, handleAdminSettlePayout, runPartnerPayouts, handleRunPartnerPayouts,
  commissionRate, tierInfo,
} from './features/payouts';
import { handleMySaved, handleSaveProduct, handleMergeSaved } from './features/saved';
import { handleMyCart, handlePutCart, clearCartForPaidOrder } from './features/cart';
import {
  handleMySupportThreads, handleCreateSupportThread, handleMySupportThread, handleMySupportReply, handleMySupportClose,
  handleAdminSupportThreads, handleAdminSupportThread, handleAdminSupportReply, handleAdminSupportUpdate, supportWaitingCount,
} from './features/support';
import { handleRelatedProducts } from './features/related';

// ── Supabase client factory ──
function getSupabase(env: Env): SupabaseClient {
  return createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
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
      const productSub = path.match(/^\/v2\/products\/([^/]+)\/(reviews|related)$/);
      if (productSub && method === 'GET') {
        const slug = decodeURIComponent(productSub[1]);
        return productSub[2] === 'reviews'
          ? handleGetProductReviews(db, slug, url, request, env)
          : handleRelatedProducts(db, slug, request, env);
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

      // ── Service switches, back-in-stock alerts ──
      if (path === '/v2/status' && method === 'GET') return handleStatus(db, request, env);
      if (path === '/v2/stock-alerts' && method === 'POST') return handleCreateStockAlert(db, request, env);

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
      
        // Audience is decided by who is asking: anonymous visitors get 'all',
        // signed-in users add 'users', staff add 'admins'.
        const token = request.headers.get('Authorization')?.replace('Bearer ', '')
        const audiences = ['all']

        if (token) {
          const { data: userData } = await db.auth.getUser(token)
          const userId = userData?.user?.id

          if (userId) {
            const { data: profile } = await db
              .from('profiles')
              .select('role')
              .eq('id', userId)
              .single()

            audiences.push('users')
            if (STAFF_ROLES.includes(profile?.role)) audiences.push('admins')
          }
        }

        const { data, error } = await db
          .from('notifications')
          .select('*')
          .eq('active', true)
          .in('audience', audiences)

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

      // Edit a notification. The admin composer has always sent PATCH for an
      // edit, but only the PUT toggle below existed, so edits fell through to
      // 404. Same fields and rules as the POST above; `active` is left alone.
      if (path.match(/^\/v2\/admin\/notifications\/[^/]+$/) && method === 'PATCH') {
        const auth = await requireAdmin(db, request, env)
        if (!auth.ok) return auth.response

        const id = path.split('/').pop()
        const body = await request.json().catch(() => null) as any
        if (!body?.type || (!body?.steps?.length && !body?.message)) {
          return err('Message or steps required', 400, request, env)
        }

        const { data, error } = await db
          .from('notifications')
          .update({
            title: body.title || null,
            message: body.steps?.length ? null : body.message,
            type: body.type,
            audience: body.audience || 'all',
            image_url: body.image_url || null,
            image_position: body.image_position || 'top',
            steps: body.steps?.length ? body.steps : null,
            scheduled_for: body.scheduled_for || null,
            expires_at: body.expires_at || null,
          })
          .eq('id', id)
          .select()
          .single()

        if (error) return err(error.message, 500, request, env)
        return ok(data, request, env)
      }

      if (path.startsWith('/v2/admin/notifications/') && method === 'PUT') {
        const auth = await requireAdmin(db, request, env)
        if (!auth.ok) return auth.response
      
        const id = path.split('/').pop()
        const body = await request.json().catch(() => null) as any
        if (typeof body?.active !== 'boolean') return err('active (boolean) is required', 400, request, env)

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
      if (path.match(/^\/v2\/me\/orders\/[^/]+$/) && method === 'GET') return handleGetMyOrder(db, decodeURIComponent(path.split('/').pop() || ''), request, env)
      if (path.match(/^\/v2\/me\/orders\/[^/]+\/confirmation$/) && method === 'GET') return handleMyOrderConfirmation(db, decodeURIComponent(path.split('/')[4] || ''), request, env)
      if (path === '/v2/me/wallet'             && method === 'GET')   return handleGetMyWallet(db, request, env)
      if (path === '/v2/me/wallet/transactions'&& method === 'GET')   return handleGetMyWalletTxns(db, request, env)
      if (path === '/v2/me/messages'           && method === 'GET')   return handleGetMyMessages(db, request, env)
      if (path.match(/^\/v2\/me\/messages\/[^/]+\/read$/) && method === 'PATCH') return handleMarkMessageRead(db, request, env)
      if (path === '/v2/me/notifications'      && method === 'GET')   return handleGetMyNotifications(db, url, request, env)
      if (path === '/v2/me/notifications/read' && method === 'POST')  return handleReadMyNotifications(db, request, env)
      if (path === '/v2/me/wallet/fund'        && method === 'POST')  return handleFundWallet(db, request, env)
      if (path === '/v2/me/wallet/fund/verify' && method === 'GET')   return handleVerifyWalletFunding(db, url, request, env)
      if (path === '/v2/me/reviews'            && method === 'POST')  return handleSubmitReview(db, request, env)
      if (path.match(/^\/v2\/me\/reviews\/[^/]+$/) && method === 'GET') return handleMyReviewFor(db, decodeURIComponent(path.split('/').pop() || ''), request, env)
      if (path === '/v2/me/referrals'          && method === 'GET')   return handleMyReferrals(db, request, env)
      if (path === '/v2/me/cart'               && method === 'GET')   return handleMyCart(db, request, env)
      if (path === '/v2/me/cart'               && method === 'PUT')   return handlePutCart(db, request, env)
      if (path === '/v2/me/saved'              && method === 'GET')   return handleMySaved(db, request, env)
      if (path === '/v2/me/saved/merge'        && method === 'POST')  return handleMergeSaved(db, request, env)
      if (path.match(/^\/v2\/me\/saved\/[^/]+$/) && (method === 'PUT' || method === 'DELETE')) return handleSaveProduct(db, decodeURIComponent(path.split('/').pop() || ''), method === 'PUT', request, env)

      // Support conversations (migration 18)
      if (path === '/v2/me/support'            && method === 'GET')   return handleMySupportThreads(db, url, request, env)
      if (path === '/v2/me/support'            && method === 'POST')  return handleCreateSupportThread(db, request, env)
      if (path.match(/^\/v2\/me\/support\/[^/]+$/) && method === 'GET') return handleMySupportThread(db, path.split('/')[4], request, env)
      if (path.match(/^\/v2\/me\/support\/[^/]+\/messages$/) && method === 'POST') return handleMySupportReply(db, path.split('/')[4], request, env)
      if (path.match(/^\/v2\/me\/support\/[^/]+\/close$/) && method === 'POST') return handleMySupportClose(db, path.split('/')[4], request, env)
      if (path === '/v2/admin/support'         && method === 'GET')   return handleAdminSupportThreads(db, url, request, env)
      if (path.match(/^\/v2\/admin\/support\/[^/]+$/) && method === 'GET') return handleAdminSupportThread(db, path.split('/')[4], request, env)
      if (path.match(/^\/v2\/admin\/support\/[^/]+$/) && method === 'PATCH') return handleAdminSupportUpdate(db, path.split('/')[4], request, env)
      if (path.match(/^\/v2\/admin\/support\/[^/]+\/messages$/) && method === 'POST') return handleAdminSupportReply(db, path.split('/')[4], request, env)

      // Partner payouts
      if (path === '/v2/partners/me/payouts'   && method === 'GET')   return handleMyPayouts(db, request, env)

      // Admin: service switches and programme settings, reviews, payouts, jobs
      if (path === '/v2/admin/flags'           && method === 'GET')   return handleAdminGetFlags(db, request, env)
      if (path.match(/^\/v2\/admin\/flags\/[a-z_]+$/) && method === 'PATCH') return handleAdminUpdateFlag(db, path.split('/').pop() || '', request, env)
      if (path === '/v2/admin/reviews'         && method === 'GET')   return handleAdminReviews(db, url, request, env)
      if (path.match(/^\/v2\/admin\/reviews\/[^/]+$/) && method === 'PATCH') return handleAdminUpdateReview(db, path.split('/').pop() || '', request, env)
      if (path === '/v2/admin/payouts'         && method === 'GET')   return handleAdminPayouts(db, url, request, env)
      if (path.match(/^\/v2\/admin\/payouts\/[^/]+\/settle$/) && method === 'POST') return handleAdminSettlePayout(db, path.split('/')[4], request, env)
      if (path === '/v2/admin/jobs/renewal-reminders' && method === 'POST') return handleRunRenewalReminders(db, request, env)
      if (path === '/v2/admin/jobs/partner-payouts'   && method === 'POST') return handleRunPartnerPayouts(db, request, env)
    
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

  // Daily cron (wrangler.toml [triggers]): renewal reminders and scheduled partner payouts.
  async scheduled(_event: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(runRenewalReminders(getSupabase(env), env).then(r => console.log('renewal reminders', r)));
    ctx.waitUntil(runPartnerPayouts(getSupabase(env), env).then(r => console.log('partner payouts', r)));
  },
};


// ============================================================
// HANDLER: GET /v2/products
// ============================================================

// What anonymous visitors may see of a product. Not '*': whatsapp_group_url
// (the paid customers' group) and social_links are post-purchase details,
// sent only in order confirmations. Matches the anon column GRANT in
// supabase-migrations/07_product_merchandising.sql. A new public product
// column goes in both places.
const PUBLIC_PRODUCT_COLUMNS = [
  'id', 'name', 'slug', 'category', 'description', 'short_description', 'category_tagline',
  'price_1m', 'price_3m', 'price_6m', 'price_1y', 'billing_type', 'billing_period', 'tags',
  'domain', 'stock_status', 'status', 'image_url', 'sort_order', 'featured', 'updated_at',
  'badge', 'delivery_time', 'delivery_method', 'region', 'features', 'how_it_works', 'faqs',
  'seo_title', 'seo_description', 'volume_tiers',
].join(',');

async function handleGetProducts(
  db: SupabaseClient, url: URL, request: Request, env: Env,
): Promise<Response> {
  // Public endpoint: only active products. Admins list hidden ones via /v2/admin/products.
  const category = url.searchParams.get('category');
  const limit = Math.min(Math.max(1, parseInt(url.searchParams.get('limit') || '500') || 500), 1000);
  const offset = Math.max(0, parseInt(url.searchParams.get('offset') || '0') || 0);

  let query = db.from('products')
    .select(PUBLIC_PRODUCT_COLUMNS, { count: 'exact' })
    .is('deleted_at', null)
    .eq('status', 'active')
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
  const withStats = await attachProductStats(db, (data || []) as any[]);
  return ok(withStats, request, env, { count: count ?? data?.length, offset, limit });
}


// ============================================================
// HANDLER: GET /v2/products/:slug
// ============================================================
async function handleGetProductBySlug(
  db: SupabaseClient, slug: string, request: Request, env: Env,
): Promise<Response> {
  const { data, error } = await db.from('products')
    .select(PUBLIC_PRODUCT_COLUMNS)
    .eq('slug', slug)
    .eq('status', 'active')
    .is('deleted_at', null)
    .single();

  if (error || !data) return err('Product not found', 404, request, env);
  const [withStats] = await attachProductStats(db, [data as any]);
  return ok(withStats, request, env);
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
    max_discount_ngn: discount.max_discount_ngn,
    min_order_ngn: discount.min_order_ngn,
    included_products: discount.included_products,
    excluded_products: discount.excluded_products,
    included_categories: discount.included_categories,
    excluded_categories: discount.excluded_categories,
    scope: discount.scope,
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

  // Only codes that would pass the date and usage guards right now. Min order
  // and eligibility depend on the cart, so the storefront checks those.
  const now = new Date();
  const live = (data || []).filter((d: any) =>
    (!d.active_from || new Date(d.active_from) <= now) &&
    (!d.expires_at || new Date(d.expires_at) >= now) &&
    (d.max_uses == null || d.times_used < d.max_uses));

  const discounts = live.map((d: any) => ({
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
    // The storefront reads is_* names, matching /v2/discount/validate.
    is_exclusive: d.exclusive,
    is_auto_apply: true,
  }));

  return ok({ discounts }, request, env);
}


// ============================================================
// SHARED: validate a public order payload against the DB
// ============================================================
// Both public order paths go through this, so neither trusts the client for
// prices, names, categories (discount eligibility reads them), quantities or
// the promo code. A promo code that no longer applies fails the order with a
// reason instead of being dropped silently — otherwise the customer would be
// charged more than the cart showed them.

const MAX_LINE_QTY = 100;
const MAX_LINES = 50;
const SUPPORTED_CURRENCIES = ['NGN', 'USD', 'GBP', 'CAD'];

type PreparedOrder = {
  email: string;
  items: CartItemPayload[];
  subtotalNGN: number;
  /** Everything off the subtotal: volume tiers plus the promo code. */
  discountNGN: number;
  /** The volume-tier part of discountNGN (migration 19). */
  volumeNGN: number;
  discountCode: string | null;
  affiliateId: string | null;
  referralCode: string | null;
  /** A customer's refer-and-earn code was used (features/referrals.ts). */
  referrerUserId: string | null;
  currency: string;
  fxRate: number;
};

async function prepareOrder(
  db: SupabaseClient, body: CreateOrderRequest, request: Request, env: Env,
): Promise<{ ok: true; order: PreparedOrder } | { ok: false; response: Response }> {
  const fail = (msg: string, status = 400) => ({ ok: false as const, response: err(msg, status, request, env) });

  const email = String(body?.customer_email || '').trim().toLowerCase();
  if (!EMAIL_RE.test(email)) return fail('A valid email is required');
  if (!Array.isArray(body.items) || body.items.length === 0) return fail('Email and items are required');
  if (body.items.length > MAX_LINES) return fail(`At most ${MAX_LINES} items per order`);

  const productIds = [...new Set(body.items.map(i => i?.product_id).filter(Boolean))];
  const { data: products, error: pErr } = await db.from('products')
    .select('*')
    .in('id', productIds)
    .is('deleted_at', null);
  if (pErr || !products?.length) return fail('Could not validate product prices');

  const productMap = new Map(products.map((p: any) => [p.id, p]));
  const items: CartItemPayload[] = [];
  let subtotalNGN = 0;
  // Volume tiers (migration 19), applied per line before any promo code.
  let volumeNGN = 0;

  for (const raw of body.items) {
    const product: any = productMap.get(raw?.product_id);
    if (!product || product.status !== 'active') return fail(`${raw?.product_name || 'A product'} is no longer available`);
    if (product.stock_status !== 'in_stock') return fail(`${product.name} is out of stock`);

    const quantity = Number(raw.quantity);
    if (!Number.isInteger(quantity) || quantity < 1 || quantity > MAX_LINE_QTY) {
      return fail(`Invalid quantity for ${product.name}`);
    }

    const period = PERIOD_INFO[raw.billing_period];
    if (!period) return fail(`Unknown billing period for ${product.name}`);
    const dbPrice = product[period.field];
    // Null or zero both mean "not sold for this period" (buysub-web/lib/pricing.ts
    // hides such periods). A 0 used to pass and could be ordered for free.
    if (!(Number(dbPrice) > 0)) return fail(`${product.name} is not available for ${raw.billing_period}`);
    if (Math.abs(Number(raw.unit_price_ngn) - dbPrice) > 1) {
      return fail(`Price for ${product.name} has changed. Refresh the page to see the current price.`);
    }

    const volume = volumeDiscountNGN(dbPrice, quantity, normalizeVolumeTiers(product.volume_tiers));
    items.push({
      product_id: product.id,
      product_name: product.name,
      category: product.category,
      billing_period: period.name,
      billing_type: product.billing_type,
      duration_months: period.months,
      unit_price_ngn: dbPrice,
      quantity,
      volume_discount_ngn: volume,
    });
    subtotalNGN += dbPrice * quantity;
    volumeNGN += volume;
  }

  // ── Discount (server-side, authoritative) ──
  let discountNGN = 0;
  let discountCode: string | null = null;
  const requestedCode = String(body.discount_code || '').trim().toUpperCase();
  if (requestedCode) {
    const { data: disc } = await db.from('discount_codes')
      .select('*')
      .eq('code', requestedCode)
      .maybeSingle();
    if (!disc) return fail(`Promo code ${requestedCode} is no longer valid. Remove it and try again.`);

    const result = validateAndCalcDiscount(disc as DiscountCode, items, !disc.auto_apply);
    if (!result.valid) return fail(`Promo code ${requestedCode}: ${result.error} Remove it and try again.`);

    // One use per customer (discount_usages is unique on discount_id + customer_id).
    const { data: existingCustomer } = await db.from('customers')
      .select('id')
      .ilike('email', escapeLike(email))
      .limit(1);
    if (existingCustomer?.length) {
      const { data: used } = await db.from('discount_usages')
        .select('id')
        .eq('discount_id', disc.id)
        .eq('customer_id', existingCustomer[0].id)
        .limit(1);
      if (used?.length) return fail(`You've already used promo code ${requestedCode}. Remove it and try again.`);
    }

    discountNGN = result.discount_ngn;
    discountCode = disc.code;
  }

  // ── Affiliate (codes are stored upper-case) ──
  let affiliateId: string | null = null;
  let referralCode: string | null = null;
  const refCode = String(body.referral_code || body.affiliate_code || '').trim().toUpperCase();
  if (refCode) {
    const { data: aff } = await db.from('affiliates')
      .select('id')
      .eq('referral_code', refCode)
      .eq('status', 'approved')
      .maybeSingle();
    if (aff) {
      affiliateId = aff.id;
      referralCode = refCode;
    }
  }
  // Not a partner code: maybe a customer's refer-and-earn code.
  let referrerUserId: string | null = null;
  if (refCode && !affiliateId) {
    const ref = await resolveCustomerCode(db, refCode);
    if (ref) { referrerUserId = ref.userId; referralCode = refCode; }
  }

  const currency = SUPPORTED_CURRENCIES.includes(body.currency) ? body.currency : 'NGN';
  const fxRate = Number(body.fx_rate) > 0 && Number.isFinite(Number(body.fx_rate)) ? Number(body.fx_rate) : 1;

  return {
    ok: true,
    order: {
      email, items, subtotalNGN,
      discountNGN: Math.min(subtotalNGN, Math.round((volumeNGN + discountNGN) * 100) / 100),
      volumeNGN, discountCode, affiliateId, referralCode, referrerUserId, currency,
      fxRate: currency === 'NGN' ? 1 : fxRate,
    },
  };
}

async function insertOrderWithItems(
  db: SupabaseClient, prepared: PreparedOrder, body: CreateOrderRequest,
  status: 'pending' | 'pending_manual', paymentMethod: 'paystack' | 'whatsapp',
): Promise<{ order: any; orderRef: string; totalNGN: number } | { error: string }> {
  const totalNGN = Math.max(0, prepared.subtotalNGN - prepared.discountNGN);

  const customerId = await findOrCreateCustomer(db, {
    email: prepared.email,
    name: body.customer_name,
    phone: body.customer_phone,
    source: paymentMethod,
  });

  // generate_order_ref() skips references already in orders, but two orders
  // can still draw the same one at once; the UNIQUE constraint on order_ref
  // rejects the second, and it tries again with a fresh reference.
  let orderRef = '';
  let order: any = null;
  let oErr: any = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    const { data: refData, error: refErr } = await db.rpc('generate_order_ref');
    if (refErr || !refData) return { error: 'Could not generate an order reference' };
    orderRef = refData as string;

    ({ data: order, error: oErr } = await db.from('orders').insert({
      order_ref: orderRef,
      customer_id: customerId,
      customer_email: prepared.email,
      customer_name: body.customer_name || null,
      customer_phone: body.customer_phone || null,
      status,
      payment_method: paymentMethod,
      subtotal_ngn: prepared.subtotalNGN,
      discount_ngn: prepared.discountNGN,
      wallet_ngn: 0,
      tax_ngn: 0,
      total_ngn: totalNGN,
      currency: prepared.currency,
      fx_rate: prepared.fxRate,
      display_total: totalNGN * prepared.fxRate,
      discount_code: prepared.discountCode,
      affiliate_id: prepared.affiliateId,
      referral_code: prepared.referralCode,
      // Only sent when a tier applied: products have no tiers before migration
      // 19, so orders keep saving without the column.
      ...(prepared.volumeNGN > 0 ? { volume_discount_ngn: prepared.volumeNGN } : {}),
      // Only sent when set, so orders still save before migration 12.
      ...(prepared.referrerUserId ? { referrer_user_id: prepared.referrerUserId } : {}),
    }).select().single());

    if (!(oErr?.code === '23505' && String(oErr.message).includes('order_ref'))) break;
  }

  if (oErr || !order) return { error: 'Failed to create order: ' + (oErr?.message || 'unknown') };

  const { error: iErr } = await db.from('order_items').insert(prepared.items.map(item => ({
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
    // Same key on every row of the batch insert, and only when a tier applied.
    ...(prepared.volumeNGN > 0 ? { volume_discount_ngn: item.volume_discount_ngn || 0 } : {}),
  })));

  if (iErr) {
    // An order with no lines can't be fulfilled or receipted; don't leave it behind.
    await db.from('orders').delete().eq('id', order.id);
    return { error: 'Failed to save order items: ' + iErr.message };
  }

  await logEvent(db, 'order', order.id, 'created', null, {
    order_ref: orderRef,
    payment_method: paymentMethod,
    total_ngn: totalNGN,
  });

  return { order, orderRef, totalNGN };
}


// ============================================================
// HANDLER: POST /v2/orders  (Paystack checkout)
// ============================================================
async function handleCreateOrder(
  db: SupabaseClient, request: Request, env: Env,
): Promise<Response> {
  try {
    // Maintenance only: the paystack_checkout switch is enforced at /v2/pay/init,
    // so an order the wallet fully covers can still be placed while it's off.
    const blocked = await serviceBlocked(db, null, request, env);
    if (blocked) return blocked;
    const body = await request.json().catch(() => null) as CreateOrderRequest | null;
    if (!body) return err('Invalid request body', 400, request, env);

    const prepared = await prepareOrder(db, body, request, env);
    if (!prepared.ok) return prepared.response;

    const created = await insertOrderWithItems(db, prepared.order, body, 'pending', 'paystack');
    if ('error' in created) return err(created.error, 500, request, env);

    return ok({
      order_id: created.order.id,
      order_ref: created.orderRef,
      total_ngn: created.totalNGN,
      discount_ngn: prepared.order.discountNGN,
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
    const blocked = await serviceBlocked(db, 'whatsapp_checkout', request, env);
    if (blocked) return blocked;
    const body = await request.json().catch(() => null) as CreateOrderRequest | null;
    if (!body) return err('Invalid request body', 400, request, env);

    const prepared = await prepareOrder(db, body, request, env);
    if (!prepared.ok) return prepared.response;

    const created = await insertOrderWithItems(db, prepared.order, body, 'pending_manual', 'whatsapp');
    if ('error' in created) return err(created.error, 500, request, env);

    const { orderRef, totalNGN } = created;
    const { items, subtotalNGN: serverSubtotal, discountNGN, discountCode, currency, fxRate } = prepared.order;

    // The message is returned to the shopper's browser and pre-filled in THEIR
    // WhatsApp, before any payment. It used to append each product's
    // whatsapp_group_url, which handed the paid customers' group link to anyone
    // who placed an unpaid order. Group links go out only after payment, in the
    // confirmation email (sendConfirmationEmail).

    const fmtAmt = (v: number) => {
      if (currency === 'NGN') return `₦${Math.ceil(v).toLocaleString()}`;
      return `${currency} ${(v * fxRate).toFixed(2)}`;
    };

    const whatsappNumber = env.WHATSAPP_NUMBER || '2348107872916';
    const frontendUrl = env.FRONTEND_URL || 'https://app.buysub.ng';

    const lines: string[] = [
      `🛒 *New WhatsApp Order*`, ``,
      `Order Ref: *${orderRef}*`,
      `Customer: ${prepared.order.email}`,
      body.customer_name ? `Name: ${body.customer_name}` : '',
      body.customer_phone ? `Phone: ${body.customer_phone}` : '',
      `Currency: ${currency}`, ``,
      `*Items:*`,
    ];

    for (const item of items) {
      const lineTotal = item.unit_price_ngn * item.quantity;
      lines.push(`• ${item.product_name} ×${item.quantity} (${item.billing_period}) — ${fmtAmt(lineTotal)}`);
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

    return ok({
      order_id: created.order.id,
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
// PAYSTACK HELPERS
// ============================================================


// Turn a successful Paystack transaction into a paid order. Shared by the
// webhook, /v2/pay/verify and /v2/pay/init (an earlier attempt already paid).
// 'mismatch' means Paystack took less than the order total, or another currency.
async function settlePaystackPayment(
  db: SupabaseClient, order: any, tx: any, env: Env,
): Promise<'ok' | 'already' | 'ignored' | 'mismatch'> {
  if (tx?.status !== 'success') return 'ignored';

  const expectedKobo = Math.round(Number(order.total_ngn) * 100);
  if (tx.currency !== 'NGN' || Number(tx.amount) < expectedKobo) {
    await logEvent(db, 'order', order.id, 'payment_amount_mismatch', null, {
      order_ref: order.order_ref,
      reference: tx.reference,
      expected_kobo: expectedKobo,
      paid_kobo: tx.amount,
      currency: tx.currency,
    });
    return 'mismatch';
  }

  // The order may have been re-initialised since this attempt; record which reference paid.
  if (order.paystack_ref !== tx.reference) {
    await db.from('orders').update({ paystack_ref: tx.reference }).eq('id', order.id);
  }

  const transitioned = await fulfillOrder(db, order.id, 'paystack', env);
  return transitioned ? 'ok' : 'already';
}

async function findOrderForTx(db: SupabaseClient, tx: any): Promise<any | null> {
  const { data: byRef } = await db.from('orders')
    .select('*').eq('paystack_ref', tx.reference).maybeSingle();
  if (byRef) return byRef;
  // Earlier attempts' references are overwritten on re-init; metadata still has the id.
  const orderId = tx?.metadata?.order_id;
  if (!orderId) return null;
  const { data: byId } = await db.from('orders').select('*').eq('id', orderId).maybeSingle();
  return byId ?? null;
}


async function refundWalletForOrder(db: SupabaseClient, order: any, amount: number, reason: string): Promise<void> {
  if (!(amount > 0) || !order.customer_id) return;
  const { data: customer } = await db.from('customers').select('user_id').eq('id', order.customer_id).maybeSingle();
  if (!customer?.user_id) return;
  const { data: wallet } = await db.from('wallets').select('id').eq('user_id', customer.user_id).maybeSingle();
  if (!wallet) return;
  const { error } = await db.rpc('credit_wallet', {
    p_wallet_id: wallet.id,
    p_amount: amount,
    p_reference: `${order.order_ref} ${reason}`,
    p_source: 'refund',
  });
  if (error) console.error('Wallet refund failed:', order.order_ref, error.message);
  else await logEvent(db, 'order', order.id, 'wallet_refunded', null, { amount_ngn: amount, reason });
}


// ============================================================
// HANDLER: POST /v2/pay/init  (Paystack)
// ============================================================
async function handlePaystackInit(
  db: SupabaseClient, request: Request, env: Env,
): Promise<Response> {
  const body = await request.json().catch(() => null) as (PaystackInitRequest & { use_wallet?: boolean }) | null;

  if (!body?.order_id) return err('order_id is required', 400, request, env);

  const flags = await getFlags(db);
  const down = await serviceBlocked(db, null, request, env); // maintenance
  if (down) return down;
  const paystackOn = isOn(flags, 'paystack_checkout');
  if (body.use_wallet && !isOn(flags, 'wallet_enabled')) {
    return err('Paying from your wallet is paused right now.', 503, request, env);
  }
  if (!paystackOn && !body.use_wallet) return (await serviceBlocked(db, 'paystack_checkout', request, env))!;

  // Fetch order
  const { data: order, error: oErr } = await db.from('orders')
    .select('*')
    .eq('id', body.order_id)
    .eq('status', 'pending')
    .single();

  if (oErr || !order) return err('Order not found or already processed', 404, request, env);

  // An earlier attempt (another tab, a retry) may already have been paid.
  // Settle it rather than charging the customer a second time.
  if (order.paystack_ref) {
    const prior = await paystackVerifyTx(order.paystack_ref, env);
    if (prior?.status === 'success') {
      const settled = await settlePaystackPayment(db, order, prior, env);
      if (settled === 'ok' || settled === 'already') {
        return ok({ already_paid: true, order_ref: order.order_ref }, request, env);
      }
    }
  }

  const originalTotal = Number(order.total_ngn);
  let amountToCharge = originalTotal;
  let walletDeducted = 0;

  // ── Wallet deduction (explicit opt-in, at most once per order) ──
  if (body.use_wallet && order.customer_id && !(Number(order.wallet_ngn) > 0)) {
    const { data: customer } = await db.from('customers')
      .select('user_id').eq('id', order.customer_id).single();

    // Only the wallet's owner, signed in, can spend it. This used to need
    // nothing but the order id.
    const auth = await requireAuth(db, request, env);
    if (!auth.ok) {
      return err('Sign in to the account that placed this order to pay from its wallet.', 403, request, env);
    }

    // A checkout customer row the account never claimed (handle_new_user
    // only claims at sign-up, and completeProfile doesn't run for staff):
    // the token proves the email, so link it now. customers_user_id_unique
    // stops this if the account already has another customer row.
    if (customer && !customer.user_id && auth.email
      && auth.email.toLowerCase() === String(order.customer_email || '').toLowerCase()) {
      const { data: linked } = await db.from('customers')
        .update({ user_id: auth.userId })
        .eq('id', order.customer_id).is('user_id', null)
        .select('user_id');
      if (linked?.length) customer.user_id = auth.userId;
    }

    if (!customer?.user_id || auth.userId !== customer.user_id) {
      return err('Sign in to the account that placed this order to pay from its wallet.', 403, request, env);
    }

    const { data: wallet } = await db.from('wallets')
      .select('*').eq('user_id', customer.user_id).single();

    if (wallet && wallet.is_active !== false && Number(wallet.balance_ngn) > 0) {
      const amount = Math.min(Number(wallet.balance_ngn), amountToCharge);

      // Claim the deduction on the order first, so two concurrent inits can't both debit.
      const { data: claimed } = await db.from('orders')
        .update({ wallet_ngn: amount, total_ngn: amountToCharge - amount })
        .eq('id', order.id)
        .eq('status', 'pending')
        .or('wallet_ngn.is.null,wallet_ngn.eq.0')
        .select('id');

      if (claimed?.length) {
        const { error: debitErr } = await db.rpc('debit_wallet', {
          p_wallet_id: wallet.id,
          p_amount: amount,
          p_reference: order.order_ref,
        });
        if (debitErr) {
          await db.from('orders').update({ wallet_ngn: 0, total_ngn: originalTotal }).eq('id', order.id);
          return err('Could not use your wallet balance: ' + debitErr.message, 409, request, env);
        }
        walletDeducted = amount;
        amountToCharge -= amount;
      }
    }
  }

  // Checkout showed the shopper a total with the wallet taken off. If none of
  // it could be taken (empty or disabled wallet, lost claim), stop rather than
  // charge them the full amount on Paystack. A retry of an order whose wallet
  // part is already taken passes: its total_ngn is already the remainder.
  if (body.use_wallet && walletDeducted === 0 && !(Number(order.wallet_ngn) > 0)) {
    return err('Your wallet balance couldn’t be applied to this order, so nothing was charged. Refresh and try again.', 409, request, env);
  }

  // If fully paid by wallet
  if (amountToCharge <= 0) {
    await fulfillOrder(db, order.id, 'wallet', env, ['pending']);
    await clearCartForPaidOrder(db, order);
    return ok({
      fully_paid_by_wallet: true,
      order_ref: order.order_ref,
    }, request, env);
  }

  // Undo the wallet part of this attempt if Paystack can't be started.
  const undoWallet = async () => {
    if (walletDeducted <= 0) return;
    await refundWalletForOrder(db, order, walletDeducted, 'paystack init failed');
    await db.from('orders').update({ wallet_ngn: 0, total_ngn: originalTotal }).eq('id', order.id);
  };

  if (!paystackOn) {
    await undoWallet();
    return err('Card and bank payments are paused right now, and your wallet doesn’t cover this order.', 503, request, env);
  }

  // ── Init Paystack transaction ──
  // order_ref already starts with BS-. The timestamp makes each attempt a new
  // Paystack reference, since Paystack rejects a reused one.
  const paystackRef = `${order.order_ref}-${Date.now()}`;

  let paystackData: any = null;
  try {
    const paystackRes = await fetch('https://api.paystack.co/transaction/initialize', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${env.PAYSTACK_SECRET_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        email: order.customer_email,
        amount: Math.round(amountToCharge * 100), // Paystack uses kobo
        currency: 'NGN',
        reference: paystackRef,
        callback_url: safeCallbackUrl(body.callback_url, env),
        metadata: {
          order_id: order.id,
          order_ref: order.order_ref,
          custom_fields: [
            { display_name: 'Order Ref', variable_name: 'order_ref', value: order.order_ref },
          ],
        },
      }),
    });
    paystackData = await paystackRes.json();
  } catch (e: any) {
    await undoWallet();
    return err('Payment initialization failed: ' + (e?.message || 'network error'), 502, request, env);
  }

  if (!paystackData?.status) {
    await undoWallet();
    return err('Payment initialization failed: ' + (paystackData?.message || 'Unknown error'), 500, request, env);
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

  const tx = event.data;
  const reference = tx.reference;

  // ── Idempotency: the unique index on payment_reference is the lock ──
  const { error: peErr } = await db.from('payment_events').insert({
    payment_reference: reference,
    status: tx.status,
    amount_ngn: tx.amount / 100, // kobo → NGN
    provider: 'paystack',
    raw_payload: tx,
  });
  if (peErr) {
    if (peErr.code === '23505') return new Response('Already processed', { status: 200 });
    console.error('Webhook: could not record payment event', peErr.message);
    return new Response('Retry', { status: 500 });
  }

  // Wallet top-ups (features/walletFunding.ts) aren't orders.
  if (tx?.metadata?.kind === TOPUP_KIND) {
    try {
      const r = await settleTopupTx(db, tx);
      if (r === 'missing' || r === 'mismatch') console.error(`Webhook: top-up ${reference} ${r}`);
    } catch (e: any) {
      console.error('Webhook top-up failed:', e?.message);
      await db.from('payment_events').delete().eq('payment_reference', reference);
      return new Response('Retry', { status: 500 });
    }
    return new Response('OK', { status: 200 });
  }

  const order = await findOrderForTx(db, tx);
  if (!order) {
    console.error(`Webhook: No order found for reference ${reference}`);
    return new Response('OK', { status: 200 });
  }

  // Fulfil before answering, so a failure gets a non-200 and Paystack retries.
  try {
    await settlePaystackPayment(db, order, tx, env);
  } catch (e: any) {
    console.error('Webhook fulfilment failed:', e?.message);
    // Release the idempotency row so the retry is processed.
    await db.from('payment_events').delete().eq('payment_reference', reference);
    return new Response('Retry', { status: 500 });
  }

  return new Response('OK', { status: 200 });
}


// ============================================================
// HANDLER: GET /v2/pay/verify?reference=xxx
// ============================================================
// Also settles the order, so a missed or delayed webhook doesn't leave a paid
// order pending. fulfillOrder is idempotent, so racing the webhook is safe.
async function handlePaystackVerify(
  db: SupabaseClient, reference: string | null, request: Request, env: Env,
): Promise<Response> {
  if (!reference) return err('Reference is required', 400, request, env);

  const tx = await paystackVerifyTx(reference, env);
  if (!tx || tx.status !== 'success') {
    return err('Payment not verified', 400, request, env);
  }

  const order = await findOrderForTx(db, tx);
  if (!order) return err('Payment received but no matching order was found. Contact support with your reference.', 404, request, env);

  let settled: string;
  try {
    settled = await settlePaystackPayment(db, order, tx, env);
  } catch (e: any) {
    console.error('Verify fulfilment failed:', e?.message);
    return err('Payment received but the order could not be updated. Contact support with your reference.', 500, request, env);
  }
  if (settled === 'mismatch') {
    return err('Payment amount does not match the order. Contact support with your reference.', 409, request, env);
  }
  await clearCartForPaidOrder(db, order);

  return ok({
    verified: true,
    order_ref: order.order_ref,
    status: 'paid',
    amount_ngn: tx.amount / 100,
    summary: await verifySummary(db, order),
  }, request, env);
}

// What the confirmation page shows. Anyone holding the Paystack reference can
// load this, so it carries the items and amounts but only a masked email and a
// first name. Best effort: a failed read returns null, never a failed verify.
async function verifySummary(db: SupabaseClient, order: any) {
  try {
    const { data: items } = await db.from('order_items')
      .select('product_name, billing_period, billing_type, duration_months, quantity, total_price_ngn, products(slug, domain, image_url, delivery_time)')
      .eq('order_id', order.id);
    const email = String(order.customer_email || '');
    const [user, host] = email.split('@');
    return {
      first_name: String(order.customer_name || '').trim().split(/\s+/)[0] || null,
      email_masked: user && host ? `${user.slice(0, 2)}${'•'.repeat(Math.max(1, Math.min(6, user.length - 2)))}@${host}` : null,
      paid_at: order.paid_at || new Date().toISOString(),
      currency: order.currency || 'NGN',
      fx_rate: Number(order.fx_rate) || 1,
      subtotal_ngn: Number(order.subtotal_ngn) || 0,
      discount_ngn: Number(order.discount_ngn) || 0,
      wallet_ngn: Number(order.wallet_ngn) || 0,
      total_ngn: Number(order.total_ngn) || 0,
      items: (items || []).map((i: any) => ({
        name: i.product_name,
        period: i.billing_period,
        billing_type: i.billing_type,
        months: i.duration_months,
        quantity: i.quantity,
        total_ngn: Number(i.total_price_ngn) || 0,
        slug: i.products?.slug ?? null,
        domain: i.products?.domain ?? null,
        image_url: i.products?.image_url ?? null,
        delivery_time: i.products?.delivery_time ?? null,
      })),
    };
  } catch {
    return null;
  }
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
  if (q) query = query.or(`order_ref.ilike.%${orSafe(q)}%,customer_email.ilike.%${orSafe(q)}%,customer_name.ilike.%${orSafe(q)}%`);

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
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

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
    const period = PERIOD_INFO[item.billing_period] || PERIOD_INFO['Quarterly'];
    const override = Number(item.unit_price_ngn);
    const unitPrice = item.unit_price_ngn != null && Number.isFinite(override) && override >= 0
      ? override
      : (product[period.field] ?? 0);

    const qty = Math.min(MAX_LINE_QTY, Math.max(1, parseInt(item.quantity) || 1));
    const lineTotal = unitPrice * qty;
    subtotalNGN += lineTotal;

    resolvedItems.push({
      product_id: product.id,
      product_name: product.name,
      category: product.category,
      billing_period: period.name,
      billing_type: product.billing_type || 'subscription',
      duration_months: Number(item.duration_months) > 0 ? Number(item.duration_months) : period.months,
      unit_price_ngn: unitPrice,
      quantity: qty,
      total_price_ngn: lineTotal,
    });
  }

  const discountNGN  = Math.min(subtotalNGN, Math.max(0, Number(body.discount_ngn) || 0));
  const taxNGN       = Math.max(0, Number(body.tax_ngn) || 0);
  const totalNGN     = Math.max(0, subtotalNGN - discountNGN + taxNGN);
  // 'paid' goes through fulfillOrder below, so it gets the same usage,
  // commission and receipt email as every other paid order.
  const orderStatus  = status === 'paid' ? 'paid' : 'pending_manual';

  // payment_method is a Postgres enum. Methods outside it (POS, coupon,
  // cashback…) are kept in the notes instead of failing the insert.
  const methodIsEnum = MANUAL_PAYMENT_METHODS.includes(payment_method);
  const storedMethod = methodIsEnum ? payment_method : null;
  const methodNote   = !methodIsEnum && payment_method ? `Paid via ${payment_method}` : null;
  const storedNotes  = [methodNote, notes].filter(Boolean).join('\n') || null;

  // Find or create customer
  const customerId = await findOrCreateCustomer(db, {
    email: customer_email,
    name: customer_name,
    phone: customer_phone,
    source: 'admin_manual',
  });

  // Generate order ref
  const { data: refData, error: refErr } = await db.rpc('generate_order_ref');
  if (refErr || !refData) return err('Could not generate an order reference', 500, request, env);
  const orderRef = refData as string;

  // Insert order
  const { data: order, error: oErr } = await db.from('orders').insert({
    order_ref:      orderRef,
    customer_id:    customerId,
    customer_name:  customer_name  || null,
    customer_email: String(customer_email).trim().toLowerCase(),
    customer_phone: customer_phone || null,
    status:         'pending_manual',
    payment_method: storedMethod,
    subtotal_ngn:   subtotalNGN,
    discount_ngn:   discountNGN,
    discount_code:  body.discount_code || null,
    tax_ngn:        taxNGN,
    total_ngn:      totalNGN,
    currency:       currency || 'NGN',
    fx_rate:        1,
    display_total:  totalNGN,
    notes:          storedNotes,
  }).select().single();

  if (oErr || !order) return err('Failed to create order: ' + (oErr?.message || 'unknown'), 500, request, env);

  // Insert order items
  const orderItemRows = resolvedItems.map(i => ({ ...i, order_id: order.id }));
  const { error: iErr } = await db.from('order_items').insert(orderItemRows);
  if (iErr) {
    await db.from('orders').delete().eq('id', order.id);
    return err('Failed to save order items: ' + iErr.message, 500, request, env);
  }

  await logEvent(db, 'order', order.id, 'created_manual', auth.userId, {
    order_ref: orderRef,
    total_ngn: totalNGN,
    item_count: resolvedItems.length,
  });

  if (orderStatus === 'paid') {
    try {
      await fulfillOrder(db, order.id, storedMethod, env, ['pending_manual']);
    } catch (e: any) {
      return err(`Order ${orderRef} created but could not be marked paid: ${e?.message}`, 500, request, env);
    }
  }

  return ok({ order_id: order.id, order_ref: orderRef, total_ngn: totalNGN, status: orderStatus }, request, env);
}


// ============================================================
// HANDLER: GET /v2/customers/search
// ============================================================
async function handleSearchCustomers(
  db: SupabaseClient, url: URL, request: Request, env: Env,
): Promise<Response> {
  // Customer PII: staff only (same data as /v2/admin/customers/search).
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const q = url.searchParams.get('q') || '';
  if (q.length < 2) return ok([], request, env);

  const { data, error } = await db.from('customers')
    .select('*')
    .or(`name.ilike.%${orSafe(q)}%,email.ilike.%${orSafe(q)}%,phone.ilike.%${orSafe(q)}%`)
    .limit(10);

  if (error) return err(error.message, 500, request, env);
  return ok(data, request, env);
}


// ============================================================
// FULFILLMENT PIPELINE
// ============================================================
// Idempotent: the status update is conditional, and only the caller that wins
// it runs the side effects (discount usage, commission, email). A second
// webhook, a verify call racing the webhook, or a double-clicked approve all
// return false and do nothing. Throws if the order can't be marked paid, so
// callers can report failure (and Paystack can retry).
async function fulfillOrder(
  db: SupabaseClient,
  orderId: string,
  paymentMethod: string | null, // null: keep the order's current payment_method
  env: Env,
  allowedFrom: string[] = ['pending', 'pending_manual', 'rejected_pending', 'cancelled', 'failed'],
): Promise<boolean> {
  // 1. Claim the transition to paid
  const { data: claimed, error: claimErr } = await db.from('orders').update({
    status: 'paid',
    ...(paymentMethod ? { payment_method: paymentMethod } : {}),
    paid_at: new Date().toISOString(),
  })
    .eq('id', orderId)
    .in('status', allowedFrom)
    .select('id');

  if (claimErr) throw new Error(`Could not mark order ${orderId} paid: ${claimErr.message}`);
  if (!claimed?.length) return false; // already paid, or not in a payable state

  // 2. Fetch full order with items
  const { data: order } = await db.from('orders')
    .select('*, order_items(*)')
    .eq('id', orderId)
    .single();

  if (!order) return true;

  console.log('Fulfillment started:', orderId);

  // Subscription end dates, for renewal reminders (features/renewals.ts).
  await setOrderExpiry(db, order);

  // 3. Discount usage. discount_usages is unique on (discount_id, customer_id);
  //    only count the use when that row is new.
  if (order.discount_code) {
    const { data: disc } = await db.from('discount_codes')
      .select('id').eq('code', order.discount_code).maybeSingle();
    if (disc) {
      let firstUse = true;
      if (order.customer_id) {
        const { error: usageErr } = await db.from('discount_usages').insert({
          discount_id: disc.id,
          order_id: order.id,
          customer_id: order.customer_id,
        });
        if (usageErr) firstUse = false;
      }
      if (firstUse) await db.rpc('increment_discount_usage', { p_discount_id: disc.id });
    }
  }

  // 4. Affiliate commission, on what the goods sold for after discount
  //    (wallet credit spent on the order still counts as a sale).
  if (order.affiliate_id) {
    const { data: aff } = await db.from('affiliates')
      .select('commission_rate, user_id, status')
      .eq('id', order.affiliate_id)
      .single();

    if (aff && aff.status === 'approved') {
      // Self-referral check: affiliate's user_id ≠ customer's user_id
      let isSelfReferral = false;
      if (order.customer_id && aff.user_id) {
        const { data: cust } = await db.from('customers')
          .select('user_id').eq('id', order.customer_id).single();
        if (cust?.user_id === aff.user_id) isSelfReferral = true;
      }

      if (!isSelfReferral) {
        const base = Math.max(0, Number(order.subtotal_ngn) - Number(order.discount_ngn || 0));
        // The partner's own rate, or their tier's when tiers are on (features/payouts.ts).
        const rate = await commissionRate(db, order.affiliate_id, Number(aff.commission_rate) || 0, order.id);
        const commissionAmount = base * (rate / 100);
        // One commission per order (unique index on order_id); a duplicate insert just fails.
        const { error: commErr } = await db.from('affiliate_commissions').insert({
          affiliate_id: order.affiliate_id,
          order_id: order.id,
          amount_ngn: Math.round(commissionAmount * 100) / 100,
          status: 'pending',
        });
        if (commErr) console.error('Commission insert failed:', commErr.message);
      }
    }
  }

  // 5. Send confirmation email (via Resend)
  try {
    await sendConfirmationEmail(order, env);
  } catch (e) {
    console.error('Email send failed:', e);
  }

  // 6. Inbox notice, and the refer-and-earn reward (both never throw)
  await notifyUser(db, await userIdForOrder(db, order), {
    kind: 'order',
    title: `Order ${order.order_ref} confirmed`,
    body: 'Payment received. Your receipt is in your email.',
    href: `/account/orders/${encodeURIComponent(order.order_ref)}`,
    dedupe: `order:${order.order_ref}:paid`,
  });
  await rewardReferral(db, order);

  // 7. Log fulfillment
  await logEvent(db, 'order', order.id, 'fulfilled', null, {
    order_ref: order.order_ref,
    payment_method: paymentMethod,
    total_ngn: order.total_ngn,
  });

  return true;
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
      <td style="padding:6px 0;color:#a0a0b0;font-size:13px;">Discount${order.discount_code && !(Number(order.volume_discount_ngn) > 0) ? ` (${escHtml(order.discount_code)})` : ''}</td>
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
  args: { to: string; ownerName: string; storeName: string; verifyUrl: string | null; existingAccount?: boolean },
  env: Env
): Promise<Response> {
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
              ${args.existingAccount
                ? 'You applied with your BuySub account, so there\'s nothing to set up. Once approved, the partner portal opens from your account menu, with your referral link, earnings and payout settings.'
                : `${args.verifyUrl
                  ? 'First, confirm this is your email address. We can only review applications with a verified email, and you can\'t sign in until it\'s done.'
                  : 'Before you can sign in, verify your email: on the login page, try to sign in and choose “Resend verification email”.'}
              Once approved, you can log in to your partner dashboard to view affiliate stats, track earnings, and manage your profile.`}
            </p>
            <div style="text-align:center;margin:24px 0 8px;">
              <a href="${escHtml(args.existingAccount ? `${env.FRONTEND_URL || 'https://app.buysub.ng'}/partner` : args.verifyUrl || 'https://app.buysub.ng/login')}" style="display:inline-block;padding:14px 28px;border-radius:10px;background:#7C5CFF;color:#fff;font-size:14px;font-weight:600;text-decoration:none;box-shadow:0 6px 20px rgba(124,92,255,0.35);">${args.existingAccount ? 'See your application' : args.verifyUrl ? 'Verify my email' : 'Go to login'}</a>
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
 
  return fetch('https://api.resend.com/emails', {
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

// Billing period → price column and duration. Public orders reject anything
// not in this table; months are derived here, never taken from the client.
const PERIOD_INFO: Record<string, { field: string; months: number | null; name: string }> = {
  'Quarterly': { field: 'price_3m', months: 3,    name: 'Quarterly' },
  'Biannual':  { field: 'price_6m', months: 6,    name: 'Biannual' },
  'Annual':    { field: 'price_1y', months: 12,   name: 'Annual' },
  'One-time':  { field: 'price_1m', months: null, name: 'One-time' }, // one-time products store same price in all fields
  'quarterly': { field: 'price_3m', months: 3,    name: 'Quarterly' },
  'biannual':  { field: 'price_6m', months: 6,    name: 'Biannual' },
  'annual':    { field: 'price_1y', months: 12,   name: 'Annual' },
  'one_time':  { field: 'price_1m', months: null, name: 'One-time' },
};

// Admin-writable product fields, shared by create and update. The three
// list fields are jsonb arrays (CHECKed in migration 07): features and
// how_it_works hold strings, faqs holds {q, a}. Blank entries are dropped so
// an empty editor row never reaches the product page.
const PRODUCT_WRITE_FIELDS = [
  'name', 'slug', 'status', 'stock_status', 'price_1m', 'price_3m', 'price_6m', 'price_1y',
  'category', 'tags', 'short_description', 'description', 'category_tagline',
  'domain', 'billing_type', 'billing_period', 'featured', 'sort_order', 'image_url',
  'whatsapp_group_url', 'social_links',
  'badge', 'delivery_time', 'delivery_method', 'region', 'seo_title', 'seo_description',
  'features', 'how_it_works', 'faqs', 'volume_tiers',
];

function pickProductFields(body: any): { ok: true; fields: Record<string, any> } | { ok: false; error: string } {
  const fields: Record<string, any> = {};
  for (const key of PRODUCT_WRITE_FIELDS) {
    if (body?.[key] !== undefined) fields[key] = body[key];
  }
  for (const key of ['features', 'how_it_works']) {
    if (fields[key] === undefined) continue;
    if (fields[key] === null) { fields[key] = []; continue; }
    if (!Array.isArray(fields[key])) return { ok: false, error: `${key} must be a list` };
    fields[key] = fields[key].map((s: any) => String(s ?? '').trim()).filter(Boolean).slice(0, 30);
  }
  if (fields.faqs !== undefined) {
    if (fields.faqs === null) fields.faqs = [];
    else if (!Array.isArray(fields.faqs)) return { ok: false, error: 'faqs must be a list' };
    else fields.faqs = fields.faqs
      .map((f: any) => ({ q: String(f?.q ?? '').trim(), a: String(f?.a ?? '').trim() }))
      .filter((f: { q: string; a: string }) => f.q && f.a)
      .slice(0, 30);
  }
  if (fields.volume_tiers !== undefined) {
    if (fields.volume_tiers !== null && !Array.isArray(fields.volume_tiers)) return { ok: false, error: 'volume_tiers must be a list' };
    fields.volume_tiers = normalizeVolumeTiers(fields.volume_tiers);
  }
  for (const key of ['badge', 'delivery_time', 'delivery_method', 'region', 'seo_title', 'seo_description']) {
    if (typeof fields[key] === 'string') fields[key] = fields[key].trim() || null;
  }
  return { ok: true, fields };
}

function getPriceField(billingPeriod: string): string {
  return PERIOD_INFO[billingPeriod]?.field || 'price_3m';
}



async function findOrCreateCustomer(
  db: SupabaseClient,
  info: { email: string; name?: string; phone?: string; source?: string },
): Promise<string | null> {
  // customers has a unique index on lower(email), so look up and insert case-insensitively.
  const email = info.email.trim().toLowerCase();
  const findExisting = async () => {
    const { data } = await db.from('customers')
      .select('id')
      .ilike('email', escapeLike(email))
      .limit(1);
    return data?.[0]?.id ?? null;
  };

  const existing = await findExisting();
  if (existing) return existing;

  const { data: created, error } = await db.from('customers').insert({
    name: info.name || email.split('@')[0],
    email,
    phone: info.phone || null,
    source: info.source || 'website',
  }).select('id').single();

  if (error || !created) {
    // Lost a race with a concurrent insert for the same email.
    const raced = await findExisting();
    if (raced) return raced;
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
  if (hex.length !== signature.length) return false;
  let diff = 0;
  for (let i = 0; i < hex.length; i++) diff |= hex.charCodeAt(i) ^ signature.charCodeAt(i);
  return diff === 0;
}


// ════════════════════════════════════════════════════════════════
// PHASE 3 HANDLER FUNCTIONS
// ════════════════════════════════════════════════════════════════



async function handleAdminStats(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const { data, error: rpcErr } = await db.rpc('admin_dashboard_stats');
  if (rpcErr) return err(rpcErr.message, 500, request, env);

  // Payout requests waiting on staff (migration 15), for the sidebar count.
  const { count: payouts } = await db.from('payout_requests').select('id', { count: 'exact', head: true }).eq('status', 'pending');
  // Support conversations waiting on a staff reply (migration 18).
  const support = await supportWaitingCount(db);
  return ok({ ...(data as any || {}), payouts_pending: payouts ?? 0, support_waiting: support }, request, env);
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

  if (q) query = query.or(`name.ilike.%${orSafe(q)}%,email.ilike.%${orSafe(q)}%,phone.ilike.%${orSafe(q)}%`);

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
    .or(`name.ilike.%${orSafe(q)}%,email.ilike.%${orSafe(q)}%,phone.ilike.%${orSafe(q)}%`)
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
  if (q) query = query.or(`name.ilike.%${orSafe(q)}%,category.ilike.%${orSafe(q)}%,tags.ilike.%${orSafe(q)}%`);

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

  const picked = pickProductFields(body);
  if (!picked.ok) return err(picked.error, 400, request, env);
  const updates: Record<string, any> = picked.fields;
  updates.updated_at = new Date().toISOString();

  const { data: before } = await db.from('products').select('stock_status').eq('id', productId).maybeSingle();

  const { data, error: dbErr } = await db
    .from('products')
    .update(updates)
    .eq('id', productId)
    .select()
    .single();

  if (dbErr) return err(dbErr.message, 500, request, env);

  // Back in stock: tell everyone who asked (features/stockAlerts.ts).
  let alerted = 0;
  if (before && before.stock_status !== 'in_stock' && data?.stock_status === 'in_stock' && data.status === 'active') {
    alerted = await sendBackInStock(db, env, data);
  }
  return ok(data, request, env, alerted ? { stock_alerts_sent: alerted } : undefined);
}

const MANUAL_PAYMENT_METHODS = ['whatsapp', 'bank_transfer', 'cash', 'wallet', 'free', 'paystack'];

async function handleAdminApproveOrderV2(
  db: SupabaseClient, ref: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json().catch(() => ({})) as any;

  const { data: order, error: findErr } = await db
    .from('orders')
    .select('id, status, order_ref, payment_method')
    .eq('order_ref', ref)
    .single();

  if (findErr || !order) return err('Order not found', 404, request, env);
  if (order.status !== 'pending_manual') return err(`Cannot approve — status is "${order.status}"`, 400, request, env);

  // Keep how the order was actually paid (cash, bank transfer…) unless the
  // admin says otherwise. This used to overwrite every order with 'whatsapp'.
  const paymentMethod = MANUAL_PAYMENT_METHODS.includes(body?.payment_method)
    ? body.payment_method
    : (order.payment_method || 'whatsapp');

  // Conditional on pending_manual inside fulfillOrder, so a double-click or two
  // admins approving at once fulfils (commission, usage, email) only once.
  let transitioned: boolean;
  try {
    transitioned = await fulfillOrder(db, order.id, paymentMethod, env, ['pending_manual']);
  } catch (e: any) {
    return err(e?.message || 'Failed to update order status', 500, request, env);
  }
  if (!transitioned) return err('Order was already approved or changed by someone else', 409, request, env);

  await logEvent(db, 'order', order.id, 'approved_manual_v2', auth.userId, {
    order_ref: order.order_ref,
    payment_method: paymentMethod,
  });

  return ok({ approved: true, order_ref: ref, status: 'paid' }, request, env);
}

async function handleAdminRejectOrder(
  db: SupabaseClient, ref: string, request: Request, env: Env
): Promise<Response> {
  const auth = await requireAdmin(db, request, env);
  if (!auth.ok) return auth.response;

  const body = await request.json().catch(() => ({})) as any;
  const reason = String(body.reason || '').trim();
  const confirmReject = body.confirm === true; // second-stage confirmation

  const { data: order, error: findErr } = await db
    .from('orders')
    .select('id, status, order_ref, notes, wallet_ngn, customer_id, customer_email')
    .eq('order_ref', ref)
    .single();

  if (findErr || !order) return err('Order not found', 404, request, env);

  const withReason = (notes: string | null) =>
    reason ? [notes, `Rejection reason: ${reason}`].filter(Boolean).join('\n') : notes;

  if (confirmReject && order.status === 'rejected_pending') {
    // Final rejection — move to cancelled
    const { data: moved } = await db.from('orders').update({
      status: 'cancelled',
      notes: withReason(order.notes),
      updated_at: new Date().toISOString(),
    }).eq('id', order.id).eq('status', 'rejected_pending').select('id');
    if (!moved?.length) return err('Order changed while rejecting — reload and try again', 409, request, env);

    // Give back any wallet balance this order had already taken.
    if (Number(order.wallet_ngn) > 0) {
      await refundWalletForOrder(db, order, Number(order.wallet_ngn), 'cancelled');
    }

    await notifyUser(db, await userIdForOrder(db, order), {
      kind: 'order',
      title: `Order ${order.order_ref} was cancelled`,
      body: Number(order.wallet_ngn) > 0
        ? `${naira(order.wallet_ngn)} from your wallet has been returned.${reason ? ` Reason: ${reason}` : ''}`
        : reason ? `Reason: ${reason}` : 'Contact support if you have questions.',
      href: `/account/orders/${encodeURIComponent(order.order_ref)}`,
      dedupe: `order:${order.order_ref}:cancelled`,
    });

    await logEvent(db, 'order', order.id, 'rejected_confirmed', auth.userId, {
      order_ref: order.order_ref, reason,
    });

    return ok({ rejected: true, confirmed: true, status: 'cancelled', order_ref: ref }, request, env);
  }

  if (['pending', 'pending_manual'].includes(order.status)) {
    // First-stage rejection — move to rejected_pending. The previous status and
    // notes go in the event log so undo-reject can put them back exactly.
    const { data: moved } = await db.from('orders').update({
      status: 'rejected_pending',
      notes: withReason(order.notes),
      updated_at: new Date().toISOString(),
    }).eq('id', order.id).eq('status', order.status).select('id');
    if (!moved?.length) return err('Order changed while rejecting — reload and try again', 409, request, env);

    await logEvent(db, 'order', order.id, 'rejected_pending', auth.userId, {
      order_ref: order.order_ref, reason,
      prev_status: order.status,
      prev_notes: order.notes,
    });

    return ok({ rejected: true, confirmed: false, status: 'rejected_pending', order_ref: ref }, request, env);
  }

  return err(`Cannot reject — status is "${order.status}"`, 400, request, env);
}

async function handleSubmitPartnerApplication(
  db: SupabaseClient, request: Request, env: Env
): Promise<Response> {
  const closed = await serviceBlocked(db, 'partner_applications', request, env);
  if (closed) return closed;
  const body = await request.json().catch(() => null) as any;
  if (!body) return err('Invalid request body', 400, request, env);
 
  // The short form (migration 23) asks for the store, the owner and where they
  // sell. Business address and contact, CAC details and payout setup are added
  // in the partner portal; payouts wait until payout details and the AML
  // declaration are in (features/payouts.ts). The old 4-step form still sends
  // everything, and that's still accepted.
  //
  // Signed in (an Authorization header): the application goes on that account.
  // No new login, no password, and the email is the account's own (already
  // verified, since unverified accounts can't sign in). One application per
  // account (unique index on user_id).
  let signedIn: { userId: string; email: string } | null = null;
  if (request.headers.get('Authorization')) {
    const a = await requireAuth(db, request, env);
    if (!a.ok) return a.response;
    if (!a.email) return err('Your account has no email address. Contact support to apply.', 400, request, env);
    signedIn = { userId: a.userId, email: a.email };
    const { data: already } = await db.from('partner_applications').select('id').eq('user_id', a.userId).maybeSingle();
    if (already) return err('You’ve already applied with this account. See its status in the partner portal.', 409, request, env);
  }

  const required = signedIn
    ? ['store_name', 'owner_name', 'owner_phone']
    : ['store_name', 'owner_name', 'owner_email', 'owner_phone', 'password'];
  for (const field of required) {
    if (!body[field]) return err(`Missing required field: ${field}`, 400, request, env);
  }
 
  if (!signedIn && (typeof body.password !== 'string' || body.password.length < 8)) {
    return err('Password must be at least 8 characters', 400, request, env);
  }
 
  if (body.payout_method === 'Bank Transfer' && (body.bank_name || body.account_name || body.account_number)) {
    if (!body.bank_name || !body.account_name || !body.account_number)
      return err('Bank details required for Bank Transfer', 400, request, env);
  }
  if (body.payout_method === 'Crypto' && (body.crypto_token || body.crypto_chain || body.wallet_address)) {
    if (!body.crypto_token || !body.crypto_chain || !body.wallet_address)
      return err('Crypto details required for Crypto payout', 400, request, env);
  }
  if (!body.privacy_accepted || !body.terms_accepted) {
    return err('Accept the partner terms and the privacy policy to apply', 400, request, env);
  }
 
  // Create the auth user UNCONFIRMED. This used to pass email_confirm: true,
  // so anyone could create a working account for an email they don't own.
  // The applicant verifies through the link in the welcome email below; until
  // then they can't sign in and an admin can't approve the application.
  const ownerEmail = (signedIn ? signedIn.email : String(body.owner_email)).trim().toLowerCase();
  let userId: string;
  if (signedIn) {
    userId = signedIn.userId;
  } else {
    const { data: signUp, error: signUpErr } = await db.auth.admin.createUser({
      email: ownerEmail,
      password: body.password,
      email_confirm: false,
      user_metadata: {
        full_name: body.owner_name,
        role: 'partner_applicant',
      },
    });

    if (signUpErr || !signUp?.user) {
      const msg = signUpErr?.message || 'Could not create account';
      if (/already|exists|registered/i.test(msg)) {
        // The web form offers "sign in to apply with it" on a signed-out 409.
        return err('An account already exists for this email. Sign in to apply with it.', 409, request, env);
      }
      return err(msg, 500, request, env);
    }
    userId = signUp.user.id;
  }
 
  const { data, error: dbErr } = await db
    .from('partner_applications')
    .insert({
      user_id: userId,
      legal_name: body.legal_name || null,
      store_name: body.store_name,
      address: body.address || null,
      lga: body.lga || null,
      state: body.state || null,
      business_phone: body.business_phone || null,
      alternate_phone: body.alternate_phone || null,
      business_email: body.business_email || null,
      cac_number: body.cac_number || null,
      registration_year: body.registration_year || null,
      social_media: body.social_media || null,
      owner_name: body.owner_name,
      owner_email: ownerEmail,
      owner_phone: body.owner_phone,
      gender: body.gender || null,
      owner_location: body.owner_location || null,
      contact_method: body.contact_method || null,
      payout_frequency: body.payout_frequency || null,
      payout_method: body.payout_method || null,
      bank_name: body.bank_name || null,
      account_name: body.account_name || null,
      account_number: body.account_number || null,
      crypto_token: body.crypto_token || null,
      crypto_chain: body.crypto_chain || null,
      wallet_address: body.wallet_address || null,
      aml_accepted: !!body.aml_accepted,
      privacy_accepted: body.privacy_accepted,
      terms_accepted: body.terms_accepted,
      status: 'pending_review',
    })
    .select('id, status')
    .single();
 
  if (dbErr) {
    // Roll back the auth user we just created (never an existing account).
    if (!signedIn) { try { await db.auth.admin.deleteUser(userId); } catch { /* ignore */ } }
    if (dbErr.code === '23505') return err('You’ve already applied with this account. See its status in the partner portal.', 409, request, env);
    return err(dbErr.message, 500, request, env);
  }
 
  await logEvent(db, 'partner_application', data.id, 'submitted', userId, {
    store_name: body.store_name,
  });
 
  // Verification link, sent from our own domain via Resend. Verifying a magic
  // link confirms the email and signs them in, landing in the partner portal.
  // /partner must be on the Supabase Auth redirect allow-list, or Supabase
  // falls back to the Site URL. A signed-in applicant is already verified.
  let verifyUrl: string | null = null;
  if (!signedIn) try {
    const { data: link, error: linkErr } = await db.auth.admin.generateLink({
      type: 'magiclink',
      email: ownerEmail,
      options: { redirectTo: `${env.FRONTEND_URL || 'https://app.buysub.ng'}/partner` },
    });
    if (linkErr) console.error('partner verify link failed:', linkErr.message);
    verifyUrl = link?.properties?.action_link ?? null;
  } catch (e) {
    console.error('partner verify link failed:', e);
  }

  let emailSent = false;
  try {
    const res = await sendPartnerSignupEmail({
      to: ownerEmail,
      ownerName: body.owner_name,
      storeName: body.store_name,
      verifyUrl,
      existingAccount: !!signedIn,
    }, env);
    emailSent = res.ok && !!verifyUrl;
  } catch (e) {
    console.error('partner welcome email failed:', e);
  }
 
  return jsonResponse(
    { ok: true, data: { id: data.id, status: data.status, user_id: userId, verification_email_sent: emailSent, existing_account: !!signedIn } },
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

  // Only approve applicants who have proved they own the email: approval
  // creates a referral code and payout details tied to this account.
  const { data: pending } = await db
    .from('partner_applications')
    .select('user_id')
    .eq('id', id)
    .maybeSingle();
  if (pending?.user_id) {
    const { data: authUser } = await db.auth.admin.getUserById(pending.user_id);
    if (!authUser?.user?.email_confirmed_at) {
      return err('The applicant has not verified their email yet. Ask them to use the link in their welcome email, or "Resend verification email" on the login page.', 409, request, env);
    }
  }
 
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
      // Referral codes are stored UPPER-case: every lookup (resolve, click,
      // order) upper-cases the incoming code before an exact match.
      const base = String(app.store_name || app.owner_name || 'partner')
        .toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 16) || 'partner';

      let created = false;
      let lastError = '';
      for (let attempt = 0; attempt < 5 && !created; attempt++) {
        const suffix = Math.random().toString(36).slice(2, 6);
        const { error: affErr } = await db.from('affiliates').insert({
          user_id: app.user_id,
          referral_code: `${base}-${suffix}`.toUpperCase(),
          status: 'approved',
          store_name: app.store_name || app.owner_name,
          business_name: app.legal_name,
          bank_name: app.bank_name,
          account_name: app.account_name,
          account_number: app.account_number,
          application_data: { partner_application_id: app.id },
        });
        if (!affErr) created = true;
        else if (affErr.code === '23505') lastError = affErr.message; // code collision: retry
        else { lastError = affErr.message; break; }
      }

      if (!created) {
        // Put the application back so approval can be retried, instead of
        // leaving an 'approved' partner with no referral code.
        await db.from('partner_applications').update({
          status: 'pending_review', reviewed_by: null, reviewed_at: null,
        }).eq('id', app.id);
        return err('Could not create the affiliate account: ' + lastError, 500, request, env);
      }
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
      .select('id, referral_code, status, store_name, business_name, commission_rate')
      .eq('user_id', user.id)
      .maybeSingle();
    // display_name is what the partner dashboard reads; there is no such column.
    affiliate = aff ? { ...aff, display_name: aff.store_name || aff.business_name } : null;
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
  // The AML declaration can be given here (it gates payouts) but not withdrawn.
  if (body.aml_accepted === true) updates.aml_accepted = true;
  // Registered-business details: the partner can fill them in once. Changing
  // them after that needs a review, so it goes through support.
  const fillOnce = ['legal_name', 'cac_number', 'registration_year'];
  if (fillOnce.some(k => body[k] !== undefined && body[k] !== null && body[k] !== '')) {
    const { data: cur } = await db.from('partner_applications')
      .select('legal_name, cac_number, registration_year').eq('user_id', user.id).maybeSingle();
    for (const k of fillOnce) {
      const v = body[k];
      if (v === undefined || v === null || v === '') continue;
      const was = (cur as any)?.[k];
      if (was && String(was) === String(v).trim()) continue;
      if (was) return err('Contact support to change your registered business details', 409, request, env);
      if (k === 'registration_year') {
        const y = parseInt(String(v), 10);
        if (!(y >= 1900 && y <= new Date().getFullYear())) return err('Enter a valid registration year', 400, request, env);
        updates[k] = y;
      } else {
        updates[k] = String(v).trim().slice(0, 200);
      }
    }
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
  // Approval copied legal_name to the affiliate as business_name; a partner who
  // adds it later gets it copied too.
  if (updates.legal_name) {
    await db.from('affiliates').update({ business_name: updates.legal_name })
      .eq('user_id', user.id).is('business_name', null);
  }
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

  // Extras for the /partner portal, computed here so the RPC (and anything
  // else reading it) is unchanged: commission owed but not yet paid, the
  // commission rate, and a 30-day daily series of clicks and conversions.
  const stats: any = { ...(data || {}) };
  const affiliateId = stats.affiliate_id;
  if (affiliateId) {
    const since = new Date(Date.now() - 29 * 86_400_000);
    since.setUTCHours(0, 0, 0, 0);
    const [aff, approved, clicks, convs] = await Promise.all([
      db.from('affiliates').select('commission_rate').eq('id', affiliateId).maybeSingle(),
      db.from('affiliate_commissions').select('amount_ngn').eq('affiliate_id', affiliateId).eq('status', 'approved'),
      db.from('affiliate_clicks').select('created_at').eq('affiliate_id', affiliateId).gte('created_at', since.toISOString()).limit(10000),
      db.from('affiliate_commissions').select('created_at, amount_ngn').eq('affiliate_id', affiliateId).gte('created_at', since.toISOString()).limit(10000),
    ]);
    const days: Record<string, { date: string; clicks: number; conversions: number; earned_ngn: number }> = {};
    for (let i = 0; i < 30; i++) {
      const d = new Date(since.getTime() + i * 86_400_000).toISOString().slice(0, 10);
      days[d] = { date: d, clicks: 0, conversions: 0, earned_ngn: 0 };
    }
    for (const c of clicks.data || []) { const d = String(c.created_at).slice(0, 10); if (days[d]) days[d].clicks++; }
    for (const c of convs.data || []) {
      const d = String(c.created_at).slice(0, 10)
      if (days[d]) { days[d].conversions++; days[d].earned_ngn += Number(c.amount_ngn) || 0 }
    }
    stats.commission_rate = aff.data?.commission_rate ?? null;
    // Tier progress, when partner tiers are on (features/payouts.ts).
    stats.tier = await tierInfo(db, affiliateId, Number(aff.data?.commission_rate) || 0);
    stats.approved_ngn = (approved.data || []).reduce((s: number, r: any) => s + (Number(r.amount_ngn) || 0), 0);
    stats.daily = Object.values(days);
  }
  return ok(stats, request, env);
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

    return ok(await withWalletPeople(db, data || []), request, env, {
      pagination: { page, limit, total: count || 0, pages: Math.ceil((count || 0) / limit) }
    });
  } catch (e: any) {
    // Table might not exist yet
    return ok([], request, env, {
      pagination: { page: 1, limit: 20, total: 0, pages: 0 }
    });
  }
}


// Names the wallet's owner (customer_name/_email) and, for staff changes, who
// made it (actor_name/_email; actor_id exists from migration 22). Best effort.
async function withWalletPeople(db: SupabaseClient, rows: any[]): Promise<any[]> {
  if (!rows.length) return rows;
  try {
    const walletIds = [...new Set(rows.map(r => r.wallet_id).filter(Boolean))];
    const { data: wallets } = await db.from('wallets').select('id, user_id').in('id', walletIds);
    const ownerOf = new Map((wallets || []).map((w: any) => [w.id, w.user_id]));
    const userIds = [...new Set([...ownerOf.values(), ...rows.map(r => r.actor_id)].filter(Boolean))];
    const [{ data: profiles }, { data: customers }] = await Promise.all([
      db.from('profiles').select('id, full_name, email').in('id', userIds),
      db.from('customers').select('user_id, name, email').in('user_id', userIds),
    ]);
    const person = new Map<string, { name: string | null; email: string | null }>();
    for (const c of customers || []) person.set(c.user_id, { name: c.name || null, email: c.email || null });
    for (const p of profiles || []) {
      const c = person.get(p.id);
      person.set(p.id, { name: p.full_name || c?.name || null, email: p.email || c?.email || null });
    }
    return rows.map(r => {
      const owner = person.get(ownerOf.get(r.wallet_id) as string);
      const actor = r.actor_id ? person.get(r.actor_id) : undefined;
      return {
        ...r,
        customer_name: owner?.name ?? null,
        customer_email: owner?.email ?? null,
        actor_name: actor?.name ?? null,
        actor_email: actor?.email ?? null,
      };
    });
  } catch (e: any) {
    console.error('withWalletPeople:', e?.message);
    return rows;
  }
}

// credit_wallet / debit_wallet with the staff member who made the change.
// Before migration 22 (or after its rollback) the functions don't take
// p_actor / p_source; retry without them so admin changes still work.
async function walletRpc(db: SupabaseClient, fn: 'credit_wallet' | 'debit_wallet', args: Record<string, any>, extra: Record<string, any>) {
  const first = await db.rpc(fn, { ...args, ...extra });
  if (first.error?.code === 'PGRST202') return db.rpc(fn, args);
  return first;
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
  amountNGN = Math.max(0, Math.min(amountNGN, subtotalNGN || 0));

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
    // Customer refer-and-earn codes are valid but their clicks aren't tracked.
    if (await resolveCustomerCode(db, code)) return ok({ tracked: false }, request, env);
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
 
  if (!affiliate) {
    // A customer's refer-and-earn code (features/referrals.ts).
    const friend = await resolveCustomerCode(db, code);
    if (friend) return ok({ valid: true, kind: 'customer', referral_code: code, store_name: friend.firstName || null }, request, env);
    return ok({ valid: false }, request, env);
  }
 
  return ok({
    valid: true,
    kind: 'partner',
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
      profiles!affiliates_user_id_fkey ( display_name:full_name, email )
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

  // Only change the rate when one is given; re-approving a suspended affiliate
  // used to reset a custom rate to 5%, and made 0% impossible.
  const updates: Record<string, any> = { status: 'approved', updated_at: new Date().toISOString() };
  if (body.commission_rate !== undefined && body.commission_rate !== null && body.commission_rate !== '') {
    const rate = Number(body.commission_rate);
    if (!Number.isFinite(rate) || rate < 0 || rate > 100) {
      return err('commission_rate must be between 0 and 100', 400, request, env);
    }
    updates.commission_rate = rate;
  }

  const { data, error: dbErr } = await db
    .from('affiliates')
    .update(updates)
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

  if (q) query = query.or(`slug.ilike.%${orSafe(q)}%,destination_url.ilike.%${orSafe(q)}%,tags.ilike.%${orSafe(q)}%`);

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

  // Same normalisation as create: the shortener matches slugs exactly.
  if (updates.slug !== undefined) {
    const slug = String(updates.slug || '').trim().toLowerCase();
    if (!slug) delete updates.slug;
    else {
      const { data: taken } = await db.from('short_links').select('id').eq('slug', slug).neq('id', id).limit(1);
      if (taken?.length) return err(`Slug "${slug}" is already taken`, 409, request, env);
      updates.slug = slug;
    }
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
  if (!Array.isArray(body.ad_ids) || body.ad_ids.length === 0) {
    return err('ad_ids array required', 400, request, env);
  }
  // Public endpoint: bound the work one request can cause.
  const adIds = [...new Set((body.ad_ids as unknown[]).filter((x): x is string => typeof x === 'string'))].slice(0, 20);

  // Atomic increment (increment_ad_views); fall back to per-ad read/write if
  // that RPC hasn't been applied yet.
  const { error: rpcErr } = await db.rpc('increment_ad_views', { p_ad_ids: adIds });
  if (rpcErr) {
    for (const adId of adIds) {
      const { data: ad } = await db.from('ads').select('view_count').eq('id', adId).maybeSingle();
      if (ad) {
        await db.from('ads').update({ view_count: (ad.view_count || 0) + 1 }).eq('id', adId);
      }
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

  // Restore what the first-stage reject recorded. Older rejections predate
  // that log metadata; fall back to pending_manual and leave notes alone.
  const { data: lastReject } = await db
    .from('event_logs')
    .select('metadata')
    .eq('entity', 'order')
    .eq('entity_id', order.id)
    .eq('action', 'rejected_pending')
    .order('created_at', { ascending: false })
    .limit(1);
  const meta = lastReject?.[0]?.metadata || {};
  const prevStatus = ['pending', 'pending_manual'].includes(meta.prev_status) ? meta.prev_status : 'pending_manual';

  const restore: Record<string, any> = {
    status: prevStatus,
    updated_at: new Date().toISOString(),
  };
  if ('prev_notes' in meta) restore.notes = meta.prev_notes;

  const { data: moved } = await db.from('orders').update(restore)
    .eq('id', order.id).eq('status', 'rejected_pending').select('id');
  if (!moved?.length) return err('Order changed while undoing — reload and try again', 409, request, env);

  await logEvent(db, 'order', order.id, 'rejection_undone', auth.userId, {
    order_ref: order.order_ref,
  });

  return ok({ undone: true, order_ref: ref, status: prevStatus }, request, env);
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

  const picked = pickProductFields(body);
  if (!picked.ok) return err(picked.error, 400, request, env);
  const insert: Record<string, any> = picked.fields;
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

  const limit = Math.min(200, Math.max(1, parseInt(url.searchParams.get('limit') || '100') || 100));
  // ?code= looks up one code exactly (the admin New Order form uses it).
  const code = url.searchParams.get('code')?.trim().toUpperCase();

  let query = db
    .from('discount_codes')
    .select('*')
    .order('created_at', { ascending: false })
    .limit(limit);
  if (code) query = query.eq('code', code);

  const { data, error: dbErr } = await query;

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

const SETTINGS_WRITE_FIELDS = ['facebook', 'instagram', 'phone', 'receipt_caption', 'tiktok', 'x']

async function handleGetSettings(db: any, request: Request, env: any) {
  const auth = await requireAdmin(db, request, env)
  if (!auth.ok) return auth.response

  const { data, error } = await db.from('settings').select('*').single()

  if (error) return err(error.message, 500, request, env)

  return ok(data, request, env)
}

async function handleUpdateSettings(db: any, request: Request, env: any) {
  const auth = await requireAdmin(db, request, env)
  if (!auth.ok) return auth.response

  const body: unknown = await request.json().catch(() => null)
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return err('Invalid request body', 400, request, env)
  }

  const updates: Record<string, unknown> = {}
  for (const key of SETTINGS_WRITE_FIELDS) {
    if ((body as any)[key] !== undefined) updates[key] = (body as any)[key]
  }
  if (Object.keys(updates).length === 0) return err('No updatable fields provided', 400, request, env)
  updates.updated_at = new Date().toISOString()

  // settings is a single row keyed by id = true.
  const { data, error } = await db
    .from('settings')
    .update(updates)
    .eq('id', true)
    .select()
    .single()

  if (error) return err(error.message, 500, request, env)

  return ok(data, request, env)
}

 
// ── POST /v2/auth/signup ─────────────────────────────────────────
// Completes the profile for the signed-in user. Identity comes from the token,
// never the body: this used to upsert any user_id from the body with
// role 'customer', which could demote an admin or claim another email's
// customer record. Idempotent — the web app calls it after sign-up and again
// at login (for sign-ups that needed email confirmation first). Values in the
// body overwrite; values from the sign-up metadata only fill empty fields.
async function handleCustomerSignup(db: any, request: Request, env: any): Promise<Response> {
  const token = (request.headers.get('Authorization') || '').replace('Bearer ', '').trim()
  if (!token) return err('Unauthorized', 401, request, env)
  const { data: userData, error: userErr } = await db.auth.getUser(token)
  const user = userData?.user
  if (userErr || !user) return err('Unauthorized', 401, request, env)

  const body = (await request.json().catch(() => null) as any) || {}
  const meta = user.user_metadata || {}
  const userId = user.id
  const email = String(user.email || '').toLowerCase()

  const { data: profileRows } = await db.from('profiles')
    .select('id, full_name, phone, gender, email').eq('id', userId).limit(1)
  const profile = profileRows?.[0]

  const pick = (field: 'full_name' | 'phone' | 'gender') => {
    if (body[field]) return body[field]
    if (!profile?.[field] && meta[field]) return meta[field]
    return undefined
  }
  const fields: Record<string, any> = {}
  for (const f of ['full_name', 'phone', 'gender'] as const) {
    const v = pick(f)
    if (v !== undefined) fields[f] = v
  }
  if (email && !profile?.email) fields.email = email

  // 1. Profile. The auth trigger normally creates it (role 'user'); role is never set here.
  if (profile) {
    if (Object.keys(fields).length) await db.from('profiles').update(fields).eq('id', userId)
  } else {
    await db.from('profiles').insert({ id: userId, role: 'customer', email, ...fields })
  }

  // 2. Customer row: already linked, else claim the guest row for this exact email, else create.
  const { data: linked } = await db.from('customers').select('id').eq('user_id', userId).limit(1)
  const customerPatch: Record<string, any> = {}
  if (fields.full_name) customerPatch.name = fields.full_name
  if (fields.phone) customerPatch.phone = fields.phone

  if (linked?.length) {
    if (Object.keys(customerPatch).length) await db.from('customers').update(customerPatch).eq('id', linked[0].id)
  } else if (email) {
    const { data: claimed } = await db.from('customers')
      .update({ user_id: userId, ...customerPatch })
      .ilike('email', escapeLike(email))
      .is('user_id', null)
      .select('id')
    if (!claimed?.length) {
      await db.from('customers').insert({
        user_id: userId,
        name: fields.full_name || email.split('@')[0],
        email,
        phone: fields.phone || null,
        source: 'customer_signup',
        is_active: true,
      })
    }
  }

  // 3. Wallet
  const { data: walletExists } = await db.from('wallets').select('id').eq('user_id', userId).limit(1)
  if (!walletExists?.length) {
    await db.from('wallets').insert({ user_id: userId, balance_ngn: 0 })
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
// `data` stays a plain array (app/dashboard reads it that way); paging is in
// meta.pagination. Optional ?status= takes a raw status or one of the account
// buckets (processing | completed | cancelled), ?q= matches the order ref.
const MY_ORDER_COLUMNS = `
  id, order_ref, status, total_ngn, subtotal_ngn, discount_ngn, wallet_ngn,
  discount_code, payment_method, currency, fx_rate, display_total,
  created_at, updated_at, paid_at,
  order_items (
    id, product_id, product_name, category, billing_period, billing_type,
    duration_months, quantity, unit_price_ngn, total_price_ngn, starts_at, expires_at,
    products ( slug, domain, image_url )
  )
`
const ORDER_BUCKETS: Record<string, string[]> = {
  processing: ['pending', 'pending_manual', 'rejected_pending'],
  completed: ['paid'],
  cancelled: ['failed', 'refunded', 'cancelled'],
}

async function myEmail(db: any, auth: { userId: string; email?: string }) {
  const { data: profile } = await db.from('profiles').select('email').eq('id', auth.userId).limit(1)
  return String(profile?.[0]?.email || auth.email || '').toLowerCase()
}

async function handleGetMyOrders(db: any, request: Request, env: any): Promise<Response> {
  const auth = await requireAuth(db, request, env)
  if (!auth.ok) return auth.response

  const url = new URL(request.url)
  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1') || 1)
  const limit = Math.min(100, Math.max(1, parseInt(url.searchParams.get('limit') || '50') || 50))
  const status = (url.searchParams.get('status') || '').trim()
  const q = (url.searchParams.get('q') || '').trim()
  const email = await myEmail(db, auth)

  let query = db
    .from('orders')
    .select(MY_ORDER_COLUMNS, { count: 'exact' })
    // Case-insensitive: checkout emails were stored as typed before being lower-cased.
    .ilike('customer_email', escapeLike(email))
    .order('created_at', { ascending: false })
    .range((page - 1) * limit, page * limit - 1)

  if (status) query = ORDER_BUCKETS[status] ? query.in('status', ORDER_BUCKETS[status]) : query.eq('status', status)
  if (q) query = query.ilike('order_ref', `%${escapeLike(q)}%`)

  const { data: orders, error, count } = await query
  if (error) return err(error.message, 500, request, env)
  return ok(orders || [], request, env, {
    pagination: { page, limit, total: count ?? 0, pages: Math.ceil((count || 0) / limit) },
  })
}

// ── GET /v2/me/orders/:ref ───────────────────────────────────────
// One of the caller's own orders. Matched by email like the list, so a ref
// belonging to someone else is a 404, not a 403 (refs are guessable).
async function handleGetMyOrder(db: any, ref: string, request: Request, env: any): Promise<Response> {
  const auth = await requireAuth(db, request, env)
  if (!auth.ok) return auth.response
  const email = await myEmail(db, auth)
  const { data, error } = await db
    .from('orders')
    .select(MY_ORDER_COLUMNS)
    .ilike('customer_email', escapeLike(email))
    .eq('order_ref', ref)
    .limit(1)
  if (error) return err(error.message, 500, request, env)
  if (!data?.length) return err('Order not found', 404, request, env)
  return ok(data[0], request, env)
}

// ── GET /v2/me/orders/:ref/confirmation ──────────────────────────
// The confirmation page for an order Paystack never saw (paid in full from
// the wallet). Same shape as /v2/pay/verify, but for the order's owner only.
async function handleMyOrderConfirmation(db: any, ref: string, request: Request, env: any): Promise<Response> {
  const auth = await requireAuth(db, request, env)
  if (!auth.ok) return auth.response
  const email = await myEmail(db, auth)
  const { data } = await db.from('orders').select('*')
    .ilike('customer_email', escapeLike(email)).eq('order_ref', ref).limit(1)
  const order = data?.[0]
  if (!order) return err('Order not found', 404, request, env)
  if (order.status !== 'paid') return err('This order isn’t paid yet.', 409, request, env)
  return ok({
    verified: true,
    order_ref: order.order_ref,
    status: order.status,
    amount_ngn: Number(order.total_ngn) || 0,
    summary: await verifySummary(db, order),
  }, request, env)
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
    if (!newWallet) return err('Could not create wallet', 500, request, env)
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
  
  const { error: rpcError } = await walletRpc(db, 'credit_wallet', {
    p_wallet_id: wallet[0].id,
    p_amount: Number(body.amount_ngn),
    p_reference: body.reference || body.source || 'admin topup',
    p_source: sourceMap[body.source] || 'admin',
  }, { p_actor: auth.userId })
  
  if (rpcError) return err(rpcError.message, 500, request, env)
 
  await logEvent(db, 'wallet', wallet[0].id, 'topup', auth.userId, {
    amount_ngn: body.amount_ngn,
    customer_id: customerId,
    new_balance: newBalance,
  })
  await notifyUser(db, userId, {
    kind: 'wallet',
    title: `${naira(body.amount_ngn)} added to your wallet`,
    body: body.source === 'refund' ? 'A refund from BuySub.' : 'A credit from BuySub.',
    href: '/account/wallet',
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

  const { error } = await walletRpc(db, 'debit_wallet', {
    p_wallet_id: wallet.id,
    p_amount: Number(amount),
    p_reference: reference || 'admin debit',
  }, { p_actor: auth.userId, p_source: 'admin' })

  if (error) return err(error.message, 500, request, env)
  await logEvent(db, 'wallet', wallet.id, 'debit', auth.userId, { amount_ngn: Number(amount), customer_id })

  return ok({ success: true }, request, env)
}


async function handleAdminToggleWallet(db: any, request: Request, env: any) {
  const auth = await requireAdmin(db, request, env)
  if (!auth.ok) return auth.response

  // /v2/admin/customers/:id/wallet/toggle — split the path, not the full URL
  // (request.url.split('/')[4] was 'admin', so every toggle 404'd).
  const customerId = new URL(request.url).pathname.split('/')[4]

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
      // discount_ngn includes any volume discount (migration 19), so name the
      // code only when it is the whole amount.
      `Discount${order.discount_code && !(Number(order.volume_discount_ngn) > 0) ? ' (' + order.discount_code + ')' : ''}`,
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