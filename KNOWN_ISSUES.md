# Known issues

These are open problems that we know about and have deliberately left for later. Each entry says what happens, why it was left, and where to start when revisiting it.

## 1. Two Paystack pages for the same order, both paid

*Logged 2026-10-10, after migration 24 / API version `2064e121`.*

**What happens**

- A customer opens checkout for one order in two tabs, or starts a payment, goes back, and starts another. Each attempt creates its own Paystack page.
- If they pay both, Paystack collects both.
- The same applies when an older full-price page is paid after the wallet covered the order.
- If the order had a promo code and the customer placed two orders with it, then paid both pages, the code is used twice.

**What the code does now**

- The second payment can't be refused, because Paystack has already taken the money.
- `settlePaystackPayment` logs these events in `event_logs`, with the order, reference and amounts:
  - `payment_duplicate`: the order was already paid by another reference or by the wallet.
  - `payment_overpaid`: the order was paid for more than `total_ngn`.
- `fulfillOrder` logs `discount_reused` when the promo case happens. Before migration 24, none of this was logged (`formal/README.md`, findings 4 and 6).

**Still missing**

- Nobody is alerted, and the admin console doesn't show these events. Someone has to query `event_logs`.
- The refund is manual.

**Options**

- Show these events in the admin console, on the order and as a count in the sidebar, and/or email support.
- Credit the excess to the customer's wallet automatically: `credit_wallet` with source `refund`, keyed on the reference so it can't run twice. This decides a money policy, so agree on it before building it.
- Refund through Paystack's refund API instead of the wallet.
- Reduce how often it happens: when `/v2/pay/init` starts a new page, ask Paystack to cancel or expire the previous page for the order. Paystack has no general "cancel transaction" call, so check what it supports first.

**Where:** `settlePaystackPayment` and `fulfillOrder` (step 3) in `src/index.ts`, and `record_discount_use` (migration 24).

## 2. Wallet money held on an abandoned order

*Logged 2026-10-10, after migration 24 / API version `2064e121`.*

**What happens**

- The customer pays part of an order from the wallet, and then Paystack can't be started (timeout, Paystack down, or card payments switched off).
- The wallet amount stays on the pending order (`orders.wallet_ngn`).
- If they retry, they pay only the remainder.
- If they never come back, the money stays held until an admin rejects and confirms the order, which returns it through `cancel_rejected_order`.
- The customer can't release it themselves, and there is no expiry.

**Why it was left like this**

Returning the wallet straight away (the old `undoWallet`) raced two things:

- The admin's cancel, which caused a double refund.
- Any other open Paystack page for the remainder, which then failed with "amount does not match".

See `formal/README.md`, findings 2 and 5, and `FixNoUndo` in `formal/tla/Checkout.tla`. Keeping the money on the order is the simplest design the model shows to be correct.

**Options**

- A daily job, in the existing 08:00 cron, that releases the wallet part of `pending` orders older than N days. It must:
  - go through one locked RPC, like `cancel_rejected_order`: cancel or reset the order and refund in the same transaction;
  - only touch orders with no Paystack payment pending, for example by checking `paystack_ref` with Paystack's verify call first;
  - keep the amount check in `fulfillOrder`'s claim. That check is what stops a late payment for the remainder from paying for the whole order.
- Let the customer cancel their own unpaid order from `/account/orders/[ref]`, using the same RPC.
- Show the held amount on the customer's wallet page ("₦X held for order BS-…").

Whatever is chosen, add it to `formal/tla/Checkout.tla` as a new action and re-run `./formal/check.sh` before shipping.

**Where:** `/v2/pay/init` (`handlePaystackInit`) in `src/index.ts`; `apply_order_wallet` and `cancel_rejected_order` (migration 24).
