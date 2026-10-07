# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`@buysub/api` — the BuySub v2 backend: a single Cloudflare Worker (`buysub-api-v2`) that fronts a Supabase Postgres database. It serves the storefront at `https://app.buysub.ng` (subscription products sold in NGN, paid via Paystack or manually approved via WhatsApp), plus the admin dashboard, partner/affiliate portals, short links, and ads.

## Commands

```bash
npx wrangler dev        # local dev on http://localhost:8787 (npm run dev)
npx wrangler deploy     # deploy to Cloudflare (npm run deploy)
npx tsc --noEmit        # type check — the only verification step in this repo
```

`npm run build` is a deliberate no-op (Wrangler bundles `src/index.ts` directly). There is no test suite, no linter, and no build artifact. **`npx tsc --noEmit` currently passes clean — keep it that way**, since it's the only automated check.

Secrets are set with `npx wrangler secret put <NAME>`, not in `wrangler.toml`. Non-secret vars (`FRONTEND_URL`, `WHATSAPP_NUMBER`, `ALLOWED_ORIGINS`) live under `[vars]` in `wrangler.toml`; adding a frontend origin means editing that comma-separated `ALLOWED_ORIGINS` string. Required secrets: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `PAYSTACK_SECRET_KEY`, `PAYSTACK_PUBLIC_KEY`, `RESEND_API_KEY`, `WEBHOOK_SECRET`.

Routes in `wrangler.toml` are commented out — the Worker is reachable on its `workers.dev` subdomain, and every path is namespaced under `/v2/` so it can coexist with the older proxy workers.

## Architecture

The shape matters:

- `src/index.ts` (~4300 lines) — the router in `export default { fetch, scheduled }`, followed by the original handlers (products, orders, payments, admin, partners, links, ads, account) and a hand-written PDF writer.
- `src/http.ts` — `Env`, CORS, the `ok`/`err` envelope, `escapeLike`/`orSafe`, `logEvent`, `requireAuth`/`requireAdmin`. Everything imports these from here.
- `src/features/*.ts` — one module per later feature, each with its routes listed in its header comment:
  - `core.ts`: feature flags (`getFlags`, 30s per-isolate cache, `isOn`, `cfg`), the per-user inbox (`notifyUser`), `sendEmail`/`emailHtml`, `userIdForOrder`.
  - `status.ts`: `GET /v2/status`, `serviceBlocked()` (maintenance + per-service switches, enforced server-side), admin flag editing.
  - `inbox.ts`, `renewals.ts` (expiry dates + the daily cron), `walletFunding.ts` (Paystack top-ups), `reviews.ts` (reviews + sold/rating stats), `referrals.ts` (customer refer-and-earn), `stockAlerts.ts`, `saved.ts` (saved products on the account), `payouts.ts` (scheduled partner payouts by chosen frequency + tiers), `related.ts` (frequently bought together), `paystack.ts`.
- `src/shared/discount.ts` — the discount engine. Pure functions, no I/O.
- `src/shared/types.ts` — DB row and request/response types.

The tables these features use come from `../supabase-migrations/08`–`15`. They are all additive, so apply them **before** deploying this code (the old code ignores them; this code reads them).

### Router

There is no router library. `fetch` normalises the path (strips trailing slashes) and runs a top-to-bottom chain of `if (path === ... && method === ...)` / `path.match(/regex/)` checks, falling through to a 404. Two consequences worth remembering:

- **Order is significant.** `path.startsWith('/v2/admin/orders/') && method === 'GET'` sits above nothing that would shadow it today, but a new prefix match placed too early will swallow more specific routes below it. Add narrow regex routes above broad `startsWith` ones.
- Params are extracted inline — `path.split('/')[4]`, `url.pathname.split('/').at(-2)`, or a regex capture group. Some handlers take the id as an argument; others re-parse `request.url` internally (e.g. `handleAdminWalletTopup`, `handleAdminToggleWallet`). Match whichever convention the neighbouring handler uses.

A few endpoints (notifications) are implemented inline in the router body rather than as named handlers.

### Response envelope

Everything goes through `ok(data, request, env, meta?)` / `err(message, status, request, env)`, producing `{ ok, data?, error?, meta? }`. CORS headers are attached per-response by `corsHeaders(request, env)`, which echoes the Origin only if it's in `ALLOWED_ORIGINS`. **Never return a bare `new Response`** for an API route — it will ship without CORS headers and the frontend will fail. (The Paystack webhook is the intentional exception; it isn't browser-called.)

### Auth

Supabase JWT in `Authorization: Bearer <token>`, verified server-side via `db.auth.getUser(token)`:

- `requireAuth(db, request, env)` → any logged-in user. Returns `{ ok: true, userId, email }` or `{ ok: false, response }`.
- `requireAdmin(db, request, env)` → additionally requires `profiles.role` ∈ `admin | super_admin | support_agent`.

Both return a discriminated union; the calling convention is always `const auth = await requireAdmin(...); if (!auth.ok) return auth.response;` as the first lines of the handler. **Every new `/v2/admin/*` route must open with this guard** — the Worker holds the Supabase *service role* key, so `getSupabase(env)` bypasses RLS entirely and an unguarded handler is a full data leak. Public routes (products, discount validate, order creation, ads, affiliate click) are intentionally unauthenticated.

### Trust boundary

The frontend is never trusted for money. Both public order paths (`/v2/orders`, `/v2/orders/whatsapp`) go through `prepareOrder()`: it re-fetches every product, requires `status = active` and `in_stock`, maps the billing period through `PERIOD_INFO` (`Quarterly`→`price_3m`, `Biannual`→`price_6m`, `Annual`→`price_1y`, `One-time`→`price_1m`; unknown periods are rejected), rejects a price mismatch >₦1, requires an integer quantity 1–100, and rebuilds each item from the DB (price, name, category, billing type, months). Discount eligibility reads name/category, so those must never come from the client. `insertOrderWithItems()` then writes the order. Preserve this pattern in any new order path.

### Discount engine (`shared/discount.ts`)

`validateAndCalcDiscount(discount, items, isManualEntry)` runs a documented 8-step guard chain: active → auto-apply filter → active_from → expiry → usage limit → min order → per-item eligibility → amount. Rules that bite:

- Eligibility is per item; `getEligibleSubtotalNGN` sums only eligible items, and the discount is computed against that subtotal, never the full cart.
- **Exclusion beats inclusion.** Included lists act as allowlists only when non-empty. All list fields are comma-separated strings parsed by `splitList` and compared lowercased via `norm`.
- Manually entering an `auto_apply` code is rejected with a deliberately vague "Code not found or inactive."
- In `prepareOrder`, a promo code that no longer applies (expired, min order, one-use-per-customer via `discount_usages`, …) **fails the order with the reason**. It used to be dropped silently, which charged more than the cart showed.

### Order lifecycle

```
POST /v2/orders          → status 'pending'         → /v2/pay/init → Paystack
POST /v2/orders/whatsapp → status 'pending_manual'  → admin approve → paid
```

`fulfillOrder(db, orderId, paymentMethod, env, allowedFrom?)` is the single funnel into `paid` and must be used by any new payment path. It is **idempotent**: the status update is conditional (`.in('status', allowedFrom)`), it returns `false` if another caller already won, and only the winner runs the side effects. It throws if the order can't be marked paid. A `null` payment method keeps the order's existing one. It sets `paid_at`, increments discount usage (`increment_discount_usage` RPC + `discount_usages` row), writes one affiliate commission on `subtotal − discount` with a self-referral check (affiliate `user_id` vs customer `user_id`), sends the Resend confirmation email with a PDF receipt, and logs the event. Email failure is caught and swallowed — it must never block fulfillment.

Paystack webhook: signature is HMAC-SHA512-verified against `PAYSTACK_SECRET_KEY` via WebCrypto, then the `payment_events` insert is the lock (unique `payment_reference`; a 23505 means already processed). `settlePaystackPayment()` checks currency and amount against `total_ngn` before calling `fulfillOrder`. Fulfilment runs **before** responding: on failure the event row is deleted and the webhook returns 500 so Paystack retries. `/v2/pay/verify` settles too (a missed webhook can't strand a paid order), and `/v2/pay/init` checks whether a previous reference for the order was already paid. Orders are found by `paystack_ref`, falling back to `metadata.order_id`, since re-init overwrites the ref.

Rejection is two-stage: first `POST .../reject` moves `pending`/`pending_manual` → `rejected_pending`; a second call with `{ confirm: true }` moves it to `cancelled`. `POST .../undo-reject` reverses stage one, restoring the previous status and notes from the `rejected_pending` event log's metadata. Confirming a rejection refunds any `wallet_ngn` the order took. The `OrderStatus` union now matches the `order_status` enum, including `rejected_pending`.

`fulfillOrder` also sets `order_items.starts_at/expires_at` (renewal reminders), posts an inbox notice, pays the refer-and-earn reward (`rewardReferral`) and, with partner tiers on, uses the tier rate for the commission (`commissionRate`). The webhook routes transactions whose `metadata.kind` is `wallet_topup` to `settleTopupTx` instead of the order path.

Service switches (`feature_flags`, see `features/status.ts`): `/v2/pay/init` needs `paystack_checkout` unless the wallet covers the order, `/v2/orders/whatsapp` needs `whatsapp_checkout`, wallet use needs `wallet_enabled`, partner applications need `partner_applications`; `maintenance_mode` blocks all of them. A switch missing from the table counts as on.

Wallet: deducted at most once per order, and only for the signed-in owner of the order's customer record (the bearer token must match `customers.user_id`). `/v2/pay/init` claims it with a conditional update on `wallet_ngn = 0` before calling `debit_wallet`, reverts if the RPC fails, and refunds via `credit_wallet` if Paystack can't be started. Disabled wallets (`is_active = false`) are skipped. Full-wallet-coverage orders skip Paystack and call `fulfillOrder(..., 'wallet', ...)` directly.

### Supabase specifics

`getSupabase(env)` creates a fresh service-role client per request with sessions disabled. Balance mutations must go through the Postgres RPCs, never a read-modify-write on `wallets.balance_ngn`: `credit_wallet`, `debit_wallet`. Other RPCs the Worker depends on: `settle_wallet_topup`, `product_public_stats`, `grant_referral_reward`, `create_partner_payout`, `settle_partner_payout` (migrations 10–15), `generate_order_ref`, `increment_discount_usage`, `admin_dashboard_stats`, `partner_dashboard_stats`, `affiliate_dashboard_stats`, `short_link_stats`, `get_ads_by_placement`, `increment_ad_click`, `increment_ad_views` (the last two fall back to read/write if missing). Dashboard RPCs are `SECURITY DEFINER`: only `service_role` may execute them (see `../supabase-migrations/02_*`). The schema and these functions live in Supabase, not in this repo — there are no migrations here, so a new column or RPC has to be applied in the Supabase dashboard before the Worker code referencing it is deployed.

Two-table customer identity: `profiles` (keyed by the Supabase auth UUID, holds `role`) and `customers` (order-facing, may exist with `user_id = null` for guest checkout). The `handle_new_user` auth trigger creates the profile, wallet and customer row (claiming a guest customer with the same email). `handleCustomerSignup` takes identity from the bearer token only — never `user_id` from the body — and never sets `role`. It fills name/phone/gender and is idempotent. Customer emails are matched case-insensitively (`customers_email_unique_ci` is on `lower(email)`): use `escapeLike` + `.ilike`, never `.eq`. Search text going into `.or()` goes through `orSafe`. `handleUpdateMe` writes to both. Anything touching a customer must tolerate `customers.user_id` being null.

### Receipts (`buildReceiptPdf` + `PdfWriter`)

A hand-rolled minimal PDF writer at the bottom of `index.ts`, because Workers can't use Node PDF libraries. Single-page A4, bottom-left origin, base-14 Helvetica only. **Non-ASCII characters break the output** — that's why amounts render as `NGN 1,234.00` rather than `₦1,234.00` in the PDF (the HTML email does use `₦`). The result is base64'd via `bytesToBase64` and attached to the Resend email.

## Conventions

- Handlers are named `handle<Area><Action>`; signature is `(db, [param,] request, env)`, or `(db, url, request, env)` for list endpoints that read query params.
- List endpoints paginate with `page`/`limit` (limit clamped to 50) and return counts under `meta.pagination`.
- `logEvent(db, entity, entityId, action, actorId, metadata)` writes to `event_logs` and swallows its own errors — it's fire-and-forget audit, never in the critical path.
- Sensitive columns are stripped before returning rows; see `redactLink()`, which drops `password_hash` and replaces it with `has_password`.
- Admin write endpoints use an explicit allowlist of writable columns (`LINK_WRITE_FIELDS`, `RULE_WRITE_FIELDS`) rather than spreading the request body. Follow that when adding admin mutations.
- Typing is inconsistent by area: Phase 1–2 handlers use `SupabaseClient` and the shared types; later ones (customer auth, wallet, messages, settings) use `db: any, env: any`. Match the surrounding block rather than converting.
- The file is organised into commented "PHASE" blocks (products/orders/payments → admin/partners/receipts → affiliates/short links/ads → customer auth/wallet/messaging). New handlers go with their phase group, and the router entry goes in the matching section.
