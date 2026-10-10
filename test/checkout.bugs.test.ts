// The checkout / wallet races found by formal/tla/Checkout.tla, fixed by
// migration 24 and the API changes that use it. Each test replays the
// counterexample and asserts the correct outcome; formal/README.md has what
// the code did before. The "harness" tests show the ordinary paths behave.

import { beforeEach, describe, expect, it, vi } from 'vitest';
vi.mock('@supabase/supabase-js', () => ({ createClient: () => (globalThis as any).__db }));

import { World, gate, tick, PRICE, WALLET } from './harness';

let w: World;
beforeEach(() => {
  vi.spyOn(console, 'error').mockImplementation(() => {});
  vi.spyOn(console, 'log').mockImplementation(() => {});
  w = new World();
});

describe('harness: ordinary paths', () => {
  it('card payment marks the order paid', async () => {
    const o = await w.createOrder();
    const init = await w.payInit(o.order_id, false);
    expect(init.status).toBe(200);
    await w.customerPays(init.body.data.reference);
    expect(w.order(o.order_id).status).toBe('paid');
    expect(w.cardPaid()).toBe(PRICE);
  });

  it('wallet + card: wallet is debited once, Paystack charges the rest', async () => {
    const o = await w.createOrder();
    const init = await w.payInit(o.order_id, true);
    expect(init.status).toBe(200);
    expect(w.balance()).toBe(0);
    expect(w.order(o.order_id).total_ngn).toBe(PRICE - WALLET);
    await w.customerPays(init.body.data.reference);
    expect(w.order(o.order_id).status).toBe('paid');
    expect(w.cardPaid() + (WALLET - w.balance())).toBe(PRICE);
  });

  it('cancelling an unpaid order returns its wallet part once', async () => {
    const o = await w.createOrder();
    await w.payInit(o.order_id, true);
    expect((await w.rejectOrder(o.order_ref)).status).toBe(200);
    expect((await w.rejectOrder(o.order_ref, true)).status).toBe(200);
    expect(w.order(o.order_id).status).toBe('cancelled');
    expect(w.balance()).toBe(WALLET);
  });
});

describe('fixed: money paths (formal/tla/Checkout.tla)', () => {
  // Was: TLC P1_noother. The cancel puts the wallet part back into total_ngn,
  // and the claim re-checks the amount, so the remainder page can't pay for
  // the whole order.
  it('a payment for the card remainder after the order was cancelled does not fulfil it', async () => {
    const o = await w.createOrder();
    const init = await w.payInit(o.order_id, true);       // ₦4,000 wallet + ₦6,000 Paystack page
    await w.rejectOrder(o.order_ref);
    await w.rejectOrder(o.order_ref, true);               // cancelled, ₦4,000 back in the wallet
    expect(w.balance()).toBe(WALLET);
    expect(w.order(o.order_id).total_ngn).toBe(PRICE);

    await w.customerPays(init.body.data.reference);       // customer finishes the open ₦6,000 page

    expect(w.order(o.order_id).status).toBe('cancelled');
    expect(w.events('payment_amount_mismatch')).toHaveLength(1); // support refunds the ₦6,000
  });

  // Was: TLC minus_FixNoUndo (two refunds).
  it('the wallet part is returned once when Paystack init fails while the order is being cancelled', async () => {
    const o = await w.createOrder();
    const g = gate<{ status: boolean; message?: string }>();
    w.onInitialize = () => { g.reached(); return g.opened; };

    const pending = w.payInit(o.order_id, true);           // applies ₦4,000, then waits on Paystack
    await g.arrived;
    expect(w.balance()).toBe(0);
    await w.rejectOrder(o.order_ref);
    await w.rejectOrder(o.order_ref, true);                // admin cancels: the one refund
    g.release({ status: false, message: 'Paystack is down' });
    const res = await pending;

    expect(res.status).toBe(500);
    expect(w.order(o.order_id).status).toBe('cancelled');
    expect(w.balance()).toBe(WALLET);
  });

  it('a failed Paystack init keeps the wallet part on the order, and the retry charges the remainder', async () => {
    const o = await w.createOrder();
    w.onInitialize = () => ({ status: false, message: 'timeout' });
    const first = await w.payInit(o.order_id, true);
    expect(first.status).toBe(500);
    expect(first.body.error).toMatch(/wallet amount already applied stays on this order/);
    expect(w.balance()).toBe(0);

    w.onInitialize = () => ({ status: true });
    await tick();
    const retry = await w.payInit(o.order_id, true);
    expect(retry.status).toBe(200);
    expect(w.txs.get(retry.body.data.reference)!.amount).toBe((PRICE - WALLET) * 100);
    await w.customerPays(retry.body.data.reference);
    expect(w.order(o.order_id).status).toBe('paid');
    expect(w.cardPaid() + (WALLET - w.balance())).toBe(PRICE);
  });

  // Was: TLC P2_current. apply_order_wallet claims and debits in one step, so
  // a cancel either happens before (nothing taken) or after (refunded once).
  it('a cancel racing the wallet step never refunds money that was not taken', async () => {
    const o = await w.createOrder();
    const g = gate();
    w.db.hooks.rpc = async fn => {
      if (fn === 'apply_order_wallet') { g.reached(); await g.opened; }
    };

    const pending = w.payInit(o.order_id, true);           // about to apply the wallet
    await g.arrived;
    await w.rejectOrder(o.order_ref);
    await w.rejectOrder(o.order_ref, true);                // nothing held yet: nothing refunded
    const spend = await w.db.rpc('debit_wallet', { p_wallet_id: 'wallet-ada', p_amount: 3_000, p_reference: 'BS-OTHER' });
    expect(spend.error).toBeNull();                         // another order spends ₦3,000
    g.release();
    const res = await pending;

    expect(res.status).toBe(404);                           // the order is no longer payable
    expect(w.order(o.order_id).status).toBe('cancelled');
    expect(w.balance() + 3_000).toBe(WALLET);
  });

  // Was: TLC P4_current (silent).
  it('paying an older full-price page after the wallet was applied, and then the newer page, is logged', async () => {
    const o = await w.createOrder();
    const a = await w.payInit(o.order_id, false);          // tab 1: ₦10,000 page
    await tick();
    const b = await w.payInit(o.order_id, true);           // tab 2: ₦4,000 wallet + ₦6,000 page
    expect(b.status).toBe(200);

    await w.customerPays(a.body.data.reference);           // pays tab 1: ₦4,000 more than owed
    await w.customerPays(b.body.data.reference);           // and tab 2: a second payment

    expect(w.order(o.order_id).status).toBe('paid');
    expect(w.order(o.order_id).paystack_ref).toBe(a.body.data.reference);
    expect(w.events('payment_overpaid').map(e => e.metadata.excess_kobo)).toEqual([WALLET * 100]);
    expect(w.events('payment_duplicate').map(e => e.metadata.reference)).toEqual([b.body.data.reference]);
  });

  it('the webhook and /v2/pay/verify settling the same payment is not a duplicate', async () => {
    const o = await w.createOrder();
    const init = await w.payInit(o.order_id, false);
    await w.customerPays(init.body.data.reference);
    const verify = await w.call('GET', `/v2/pay/verify?reference=${encodeURIComponent(init.body.data.reference)}`);
    expect(verify.status).toBe(200);
    expect(w.events('payment_duplicate')).toHaveLength(0);
    expect(w.events('payment_overpaid')).toHaveLength(0);
  });

  it('a full-price page paid after the wallet covered the whole order is logged', async () => {
    w.db.row('wallets', 'wallet-ada')!.balance_ngn = PRICE + 500;
    const o = await w.createOrder();
    const a = await w.payInit(o.order_id, false);          // tab 1: ₦10,000 page
    await tick();
    const b = await w.payInit(o.order_id, true);           // tab 2: wallet pays it all
    expect(b.body.data.fully_paid_by_wallet).toBe(true);
    await w.customerPays(a.body.data.reference);
    expect(w.events('payment_duplicate')).toHaveLength(1);
  });

  // Was: TLC P5_current.
  it('a concurrent attempt whose Paystack init fails no longer breaks the other tab’s payment', async () => {
    const o = await w.createOrder();
    const g = gate<{ status: boolean; message?: string }>();
    let calls = 0;
    w.onInitialize = () => (++calls === 1 ? (g.reached(), g.opened) : { status: true });

    const tab1 = w.payInit(o.order_id, true);              // applies ₦4,000, waits on Paystack
    await g.arrived;
    await tick();
    const tab2 = await w.payInit(o.order_id, true);        // sees the wallet applied: ₦6,000 page
    expect(tab2.status).toBe(200);
    g.release({ status: false, message: 'timeout' });
    await tab1;                                             // wallet part stays on the order

    await w.customerPays(tab2.body.data.reference);
    const verify = await w.call('GET', `/v2/pay/verify?reference=${encodeURIComponent(tab2.body.data.reference)}`);

    expect(verify.status).toBe(200);
    expect(w.order(o.order_id).status).toBe('paid');
    expect(w.cardPaid() + (WALLET - w.balance())).toBe(PRICE);
  });
});
