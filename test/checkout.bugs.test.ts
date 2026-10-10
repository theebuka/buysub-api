// Reproductions of the checkout / wallet races found by formal/tla/Checkout.tla.
//
// Each BUG test asserts what the code does TODAY, with the correct outcome in
// a comment. When a bug is fixed its test fails: flip it to the correct
// expectation then. The "harness" tests show the ordinary paths behave, so
// the BUG outcomes aren't artefacts of the fakes.

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

describe('BUG: money paths (TLA+ Checkout)', () => {
  // TLC: P1_noother. Wallet part refunded on cancel, then the open Paystack
  // page for the remainder is paid; fulfillOrder accepts 'cancelled'.
  it('a cancelled order is fulfilled for the card remainder after its wallet part was refunded', async () => {
    const o = await w.createOrder();
    const init = await w.payInit(o.order_id, true);       // ₦4,000 wallet + ₦6,000 Paystack page
    await w.rejectOrder(o.order_ref);
    await w.rejectOrder(o.order_ref, true);               // cancelled, ₦4,000 back in the wallet
    expect(w.balance()).toBe(WALLET);

    await w.customerPays(init.body.data.reference);       // customer finishes the open ₦6,000 page

    const netPaid = w.cardPaid() + (WALLET - w.balance());
    expect(w.order(o.order_id).status).toBe('paid');
    expect(netPaid).toBe(PRICE - WALLET);                  // correct: >= PRICE, or the order stays unpaid
    expect(w.events('payment_amount_mismatch')).toHaveLength(0);
  });

  // TLC: minus_FixNoUndo. undoWallet (Paystack init failed) and the admin's
  // confirm-reject both refund the same wallet part.
  it('the wallet part is refunded twice when Paystack init fails while the order is being cancelled', async () => {
    const o = await w.createOrder();
    const g = gate<{ status: boolean; message?: string }>();
    w.onInitialize = () => { g.reached(); return g.opened; };

    const pending = w.payInit(o.order_id, true);           // debits ₦4,000, then waits on Paystack
    await g.arrived;
    expect(w.balance()).toBe(0);
    await w.rejectOrder(o.order_ref);
    await w.rejectOrder(o.order_ref, true);                // admin cancels: refund #1
    g.release({ status: false, message: 'Paystack is down' });
    const res = await pending;                              // undoWallet: refund #2

    expect(res.status).toBe(500);
    expect(w.order(o.order_id).status).toBe('cancelled');
    expect(w.balance()).toBe(2 * WALLET);                   // correct: WALLET
  });

  // TLC: P2_current. The claim on the order and debit_wallet are separate
  // statements; a cancel between them refunds money that is then never debited.
  it('a cancel between the wallet claim and the debit refunds money that was never taken', async () => {
    const o = await w.createOrder();
    const g = gate();
    let first = true;
    w.db.hooks.rpc = async fn => {
      if (fn === 'debit_wallet' && first) { first = false; g.reached(); await g.opened; }
    };

    const pending = w.payInit(o.order_id, true);           // claims ₦4,000 on the order, about to debit
    await g.arrived;
    await w.rejectOrder(o.order_ref);
    await w.rejectOrder(o.order_ref, true);                // refunds the claimed ₦4,000: balance ₦8,000
    const spend = await w.db.rpc('debit_wallet', { p_wallet_id: 'wallet-ada', p_amount: 5_000, p_reference: 'BS-OTHER' });
    expect(spend.error).toBeNull();                         // another order spends ₦5,000
    g.release();
    const res = await pending;                              // debit of ₦4,000 fails (₦3,000 left); order reset

    expect(res.status).toBe(409);
    expect(w.order(o.order_id).status).toBe('cancelled');
    // Started with ₦4,000, spent ₦5,000 elsewhere, this order cost nothing.
    expect(w.balance() + 5_000).toBe(2 * WALLET);           // correct: WALLET
  });

  // TLC: P4_current. A full-price Paystack page from an earlier attempt is
  // paid after a later attempt took the wallet part; nothing is refunded or logged.
  it('paying an older full-price Paystack page after the wallet was applied overcharges silently', async () => {
    const o = await w.createOrder();
    const a = await w.payInit(o.order_id, false);          // tab 1: ₦10,000 page
    await tick();
    const b = await w.payInit(o.order_id, true);           // tab 2: ₦4,000 wallet + ₦6,000 page
    expect(b.status).toBe(200);

    await w.customerPays(a.body.data.reference);           // pays tab 1
    await w.customerPays(b.body.data.reference);           // and tab 2

    const netPaid = w.cardPaid() + (WALLET - w.balance());
    expect(w.order(o.order_id).status).toBe('paid');
    expect(netPaid).toBe(PRICE + PRICE);                    // ₦20,000 for a ₦10,000 order
    // correct: the extra ₦10,000 is refunded or at least logged for support
    expect(w.db.t('event_logs').filter(e => /mismatch|duplicate|overpa/i.test(e.action))).toHaveLength(0);
    expect(w.db.t('wallet_transactions').filter(t => t.type === 'credit')).toHaveLength(0);
  });

  // TLC: P5_current. Tab 2 starts a Paystack page for the remainder while
  // tab 1 still holds the wallet; tab 1's Paystack init then fails and undoes
  // the wallet, so the honest customer's payment no longer matches.
  it('an honest customer gets "amount does not match" after a concurrent attempt undid the wallet', async () => {
    const o = await w.createOrder();
    const g = gate<{ status: boolean; message?: string }>();
    let calls = 0;
    w.onInitialize = () => (++calls === 1 ? (g.reached(), g.opened) : { status: true });

    const tab1 = w.payInit(o.order_id, true);              // takes ₦4,000, waits on Paystack
    await g.arrived;
    await tick();
    const tab2 = await w.payInit(o.order_id, true);        // sees the wallet already applied: ₦6,000 page
    expect(tab2.status).toBe(200);
    g.release({ status: false, message: 'timeout' });
    await tab1;                                             // undoWallet: refund, total back to ₦10,000

    await w.customerPays(tab2.body.data.reference);        // pays exactly what the page showed
    const verify = await w.call('GET', `/v2/pay/verify?reference=${encodeURIComponent(tab2.body.data.reference)}`);

    expect(verify.status).toBe(409);                        // correct: 200, order paid
    expect(w.order(o.order_id).status).toBe('pending');
    expect(w.cardPaid()).toBe(PRICE - WALLET);
  });
});
