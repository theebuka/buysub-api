# Formal models

These are models of the money paths in `src/index.ts`, checked with TLC (TLA+) and Lean 4. Every bug they found has a reproduction in `test/*.bugs.test.ts`, which runs the real Worker against an in-memory Supabase (`test/fakeDb.ts`) and a fake Paystack.

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

## Findings

| # | Bug | Found by | Unit test |
|---|---|---|---|
| 1 | **Cancelled order fulfilled at a discount.** Confirm-reject refunds `wallet_ngn`, but `total_ngn` stays as the card remainder. If the customer then pays the Paystack page that was still open, `fulfillOrder` accepts `cancelled` and the order is paid for the remainder only. | TLC P1 (`P1_noother`) | "a cancelled order is fulfilled for the card remainder…" |
| 2 | **Double refund.** If Paystack init fails while an admin is confirming a reject, `undoWallet` and confirm-reject both refund the wallet part. | TLC P2 (`minus_FixNoUndo`) | "the wallet part is refunded twice…" |
| 3 | **Refund of money never taken.** The claim on the order and `debit_wallet` are separate statements. A cancel between them refunds the claimed amount. If the debit then fails (the balance was spent elsewhere), the order is reset, and the refund stays. | TLC P2/P3 | "a cancel between the wallet claim and the debit…" |
| 4 | **Silent overcharge.** An older full-price Paystack page (another tab) can be paid after a later attempt applied the wallet, or two pages can both be paid. Settle returns `ok`/`already` without logging or refunding the extra. | TLC P4 | "paying an older full-price Paystack page…" |
| 5 | **Honest mismatch.** Tab 2 opens a page for the remainder while tab 1 holds the wallet. Tab 1's Paystack init then fails and undoes the wallet, so tab 2's payment no longer matches `total_ngn` and the order stays pending. | TLC P5 | "an honest customer gets 'amount does not match'…" |
| 6 | **One use per customer not enforced.** `discount_usages` is written at payment. Two orders placed before either is paid both get the code. | TLC `OneUsePerCustomer` | "one customer gets a 'one use per customer' code twice" |
| 7 | **`max_uses` not enforced.** It is checked at order creation and counted at payment, so pending orders overshoot it. | TLC `WithinMaxUses` | "a code with max_uses = 1 is used by two customers" |
| 8 | **Negative discount.** PATCH `/v2/admin/discounts/:id` doesn't validate `value`, and `discount_codes` has no CHECK constraint. A negative value makes the API charge more than the subtotal, while the web clamps the discount at 0 and shows less. The admin form blocks values ≤ 0; the API doesn't. | Lean: `parity` needs `0 ≤ raw`; `parity_fails_for_negative_value` | "PATCH accepts a negative value…" |

**Proved in Lean** (no `sorry`):

- With a non-negative amount, the charged discount is between 0 and the eligible subtotal.
- The charged total is between 0 and the subtotal.
- The web and API totals agree.
- Buying more never lowers the volume-tier percent.
- A volume discount (≤ 90%) never exceeds its line.

## Proposed fixes, as modelled

None of these is applied to the code. Each one is a `Fix*` constant in `Checkout.tla`:

| Constant | Change |
|---|---|
| `FixAtomic` | Apply the wallet in one RPC: lock the wallet and order rows, debit, and set `wallet_ngn` / `total_ngn` together. |
| `FixNoUndo` | When Paystack init fails, leave the wallet part on the order. The retry path already charges only the remainder, and cancelling returns the wallet part. |
| `FixCancel` | The cancelling UPDATE also sets `wallet_ngn = 0, total_ngn = total_ngn + wallet_ngn` and returns the old `wallet_ngn`; that returned value is what gets refunded. |
| `FixSettle` | The amount check is part of `fulfillOrder`'s conditional UPDATE (`... AND total_ngn <= paid`), not a comparison with an earlier read. |
| `FixDup` | A capture that pays more than `total_ngn` at that moment, or that lands on an already-paid order, is logged for refund. |

**Results with the fixes:**

- With all five on, P1–P5 hold:
  - 2 attempts and 3 admin actions: 26k states.
  - Wallet ≥ price: 57k states.
  - 3 attempts and 4 admin actions: 2.4M states.
- Turning off any single fix breaks a property again (the `minus_*` configs).
- `FixCancel` alone is not enough. TLC showed that settle's earlier read still lets the remainder through, which is why `FixSettle` exists.

**Discount usage fix (`FixReserve`):**

- Placing an order reserves the use, together with `times_used + held < max_uses`, in one statement.
- Cancelling releases the reservation.
- With `FixReserve` on, both discount properties hold.
- Trade-off: an abandoned pending order holds its use until it's cancelled.

**Negative discount fix:**

- Validate `value > 0` on PATCH, and `≤ 100` for percentage codes.
- Add `CHECK (value > 0)` on `discount_codes`.

## Limits

- **Bounded checks.** TLC checks small instances: one order, 2–3 attempts, a few admin actions, and amounts of 10 and 4 (or 12). The Lean proofs cover all integers but abstract away the 2-decimal rounding and the fx rate.
- **Hand-written models.** The models were written by hand from the code, so a drift between model and code is possible. The unit tests are what tie each finding back to the real handlers.
- **Not modelled:** wallet top-ups (`settle_wallet_topup` locks and is idempotent), partner payouts, and referral rewards (both unique-keyed in SQL). I read them and found nothing in the same class.
