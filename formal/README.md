# Formal models

These are models of the money paths in `src/index.ts`, checked with TLC (TLA+) and Lean 4.

The eight bugs they found are fixed by `../supabase-migrations/24_order_money_atomic.sql` and the API code that calls it. `test/*.bugs.test.ts` replays each counterexample against the real Worker, using an in-memory Supabase (`test/fakeDb.ts`) and a fake Paystack, and asserts the fixed outcome.

```bash
./formal/check.sh   # every TLC model + the Lean proofs; exit 0 = all results as expected
npm test            # the unit tests (vitest)
```

Requirements:

- Java. The script uses `/opt/homebrew/opt/openjdk` or `$JAVA`.
- `tla2tools.jar`. The script uses `~/.local/tla/` or `$TLA2TOOLS`.
- Lean 4, installed via elan.

## What is modelled

| File | What it covers | Step granularity |
|---|---|---|
| `tla/Checkout.tla` | One order. `handlePaystackInit` (wallet claim, debit, Paystack init, undo), the customer paying any open Paystack page, `settlePaystackPayment` (webhook or verify), and the admin's reject / confirm (refund) / undo-reject. Optionally, the wallet being spent on another order. | One PostgREST call or RPC per step, so concurrent requests interleave as they do on Workers. |
| `tla/DiscountUsage.tla` | One promo code: the `prepareOrder` checks (`times_used < max_uses`, `discount_usages` lookup), order insert, `fulfillOrder` usage recording, and cancel. | |
| `lean/Discount.lean` | `calcDiscountNGN` + `prepareOrder` totals against the web's `calcDiscountAmount` + `computeTotals`, plus `volumeTierFor`. | |

### Properties checked in `Checkout.tla`

Each property is checked only when no request is mid-flight and every captured payment has been settled.

| Property | Meaning |
|---|---|
| P1 `NoUnderpaidFulfilment` | A paid order cost the customer at least its price. |
| P2 `NoMoneyCreated` | The customer never ends up with more than they started with. |
| P3 `CancelledIsRefunded` | A cancelled order cost the customer nothing, unless something was logged for support. |
| P4 `NoSilentOvercharge` | Paying more than the price is always logged. |
| P5 `NoHonestMismatch` | Paying exactly what the Paystack page showed never gets "amount does not match", unless an admin cancelled the order. |

## Findings (all fixed)

| # | Bug (before the fix) | Found by | Unit test (now asserts the fix) |
|---|---|---|---|
| 1 | **Cancelled order fulfilled at a discount.** Confirm-reject refunded `wallet_ngn`, but `total_ngn` stayed as the card remainder. If the customer then paid the Paystack page that was still open, `fulfillOrder` accepted `cancelled` and the order was paid for the remainder only. | TLC P1 (`P1_noother`) | "a payment for the card remainder after the order was cancelled…" |
| 2 | **Double refund.** If Paystack init failed while an admin was confirming a reject, `undoWallet` and confirm-reject both refunded the wallet part. | TLC P2 (`minus_FixNoUndo`) | "the wallet part is returned once…" |
| 3 | **Refund of money never taken.** The claim on the order and `debit_wallet` were separate statements. A cancel between them refunded the claimed amount. If the debit then failed (the balance was spent elsewhere), the order was reset, and the refund stayed. | TLC P2/P3 | "a cancel racing the wallet step…" |
| 4 | **Silent overcharge.** An older full-price Paystack page (another tab) could be paid after a later attempt applied the wallet, or two pages could both be paid. Settle returned `ok`/`already` without logging the extra. | TLC P4 | "paying an older full-price page…", "a full-price page paid after the wallet covered…" |
| 5 | **Honest mismatch.** Tab 2 opened a page for the remainder while tab 1 held the wallet. Tab 1's Paystack init then failed and undid the wallet, so tab 2's payment no longer matched `total_ngn` and the order stayed pending. | TLC P5 | "a concurrent attempt whose Paystack init fails…" |
| 6 | **One use per customer not enforced.** `discount_usages` was written at payment. Two orders placed before either was paid both got the code. | TLC `OneUsePerCustomer` | "an order placed after an abandoned one takes over the use…", "…the second use is logged" |
| 7 | **`max_uses` not enforced.** It was checked at order creation and counted at payment, so pending orders overshot it. | TLC `WithinMaxUses` | "max_uses counts unpaid orders…", "cancelling an unpaid order frees its use" |
| 8 | **Negative discount.** PATCH `/v2/admin/discounts/:id` didn't validate `value`, and `discount_codes` had no CHECK constraint. A negative value made the API charge more than the subtotal, while the web clamps the discount at 0 and shows less. The admin form blocks values ≤ 0; the API didn't. | Lean: `parity` needs `0 ≤ raw`; `parity_fails_for_negative_value` | "PATCH … is refused" (six cases) |

**Proved in Lean** (no `sorry`):

- With a non-negative amount, the charged discount is between 0 and the eligible subtotal.
- The charged total is between 0 and the subtotal.
- The web and API totals agree.
- Buying more never lowers the volume-tier percent.
- A volume discount (≤ 90%) never exceeds its line.

## The fixes

Each `Fix*` constant in `Checkout.tla` is one of these changes. The `All_fixed*` configs check the code as it is now; the `P*_current` configs check the code before the fix.

| Constant | Implemented as |
|---|---|
| `FixAtomic` | `apply_order_wallet` RPC: locks the order and the wallet, debits, and sets `wallet_ngn` / `total_ngn` in one transaction. With Paystack off, it only applies a wallet that covers the whole order. |
| `FixNoUndo` | `/v2/pay/init` no longer hands the wallet part back when Paystack can't be started. A retry charges only the remainder, and cancelling returns it. |
| `FixCancel` | `cancel_rejected_order` RPC: cancels, sets `wallet_ngn = 0` and `total_ngn = total_ngn + wallet_ngn`, refunds the amount it took off, and releases the promo reservation. |
| `FixSettle` | `fulfillOrder`'s claim includes `total_ngn <= paid` (`opts.maxTotalNGN`), and records the paying reference (`opts.paystackRef`). |
| `FixDup` | `settlePaystackPayment` logs `payment_overpaid` (paid more than `total_ngn` at the claim) and `payment_duplicate` (the order was already paid by another reference or the wallet). The webhook and `/v2/pay/verify` settling the same reference is not a duplicate. |

**Checkout results:**

- With all five on, P1–P5 hold:
  - 2 attempts and 3 admin actions: 26k states.
  - Wallet ≥ price: 57k states.
  - 3 attempts and 4 admin actions: 2.4M states.
- Turning off any single fix breaks a property again (the `minus_*` configs).

**Promo codes** (`DiscountUsage.tla` with `FixReserve`):

- `reserve_discount_use` runs when an order is placed, and again at `/v2/pay/init`. A customer's newer unpaid order takes over the use held by their older one, so an abandoned checkout doesn't lock them out. `max_uses` counts uses held by unpaid orders.
- `record_discount_use` counts the use at payment.
- A payment that can't be refused is logged instead:
  - `discount_reused`: two Paystack pages were open at once and both were paid.
  - `discount_over_limit`: the order had no reservation, for example one placed before migration 24.

**Promo-code results** (max_uses 1 and 2, 4 orders, 2 customers):

- These all hold:
  - One use per customer and `max_uses`, counting everything except logged payments.
  - `NoWrongRefusal`: a customer is refused only when their use was spent or others fill `max_uses`.
  - `times_used` stays accurate.
- `D_reach_logged` shows the logged path is reachable, so those properties aren't vacuous.

**Discount values:**

- POST and PATCH on `/v2/admin/discounts` check:
  - value > 0
  - percentage ≤ 100
  - non-negative minimum and cap
  - whole `max_uses`
- Migration 24 adds matching CHECKs.

**Migration testing:**

- The SQL was run forward, back and forward on PGlite with a stub schema.
- 33 scenario checks passed: wallet apply, cancel, reserve, record, and the constraints.
- `test/fakeDb.ts` implements the same four functions for the unit tests.

## Limits

- **Bounded checks.** TLC checks small instances: one order, 2–3 attempts, a few admin actions, and amounts of 10 and 4 (or 12). The Lean proofs cover all integers but abstract away the 2-decimal rounding and the fx rate.
- **Hand-written models.** The models were written by hand from the code, so a drift between model and code is possible. The unit tests tie each finding to the real handlers, but they run against `test/fakeDb.ts`, whose RPCs re-implement the SQL in TypeScript. The SQL itself was only checked separately, on PGlite.
- **Not modelled:** wallet top-ups (`settle_wallet_topup` locks and is idempotent), partner payouts, and referral rewards (both unique-keyed in SQL). I read them and found nothing in the same class.
