// Reproductions for formal/tla/DiscountUsage.tla and formal/lean/Discount.lean.
// BUG tests assert today's behaviour; the correct outcome is in the comment.

import { beforeEach, describe, expect, it, vi } from 'vitest';
vi.mock('@supabase/supabase-js', () => ({ createClient: () => (globalThis as any).__db }));

import { World, ADMIN, PRICE } from './harness';

let w: World;
beforeEach(() => {
  vi.spyOn(console, 'error').mockImplementation(() => {});
  vi.spyOn(console, 'log').mockImplementation(() => {});
  w = new World();
  w.db.t('discount_codes').push({
    id: 'disc-save10', code: 'SAVE10', type: 'percentage', value: 10, active: true, auto_apply: false,
    min_order_ngn: 0, max_uses: null, times_used: 0, max_discount_ngn: null, exclusive: false, scope: 'site_wide',
  });
});

async function placeAndPay(email?: string, code = 'SAVE10') {
  const o = await w.createOrder({ email, code });
  const init = await w.payInit(o.order_id, false);
  return { o, ref: init.body.data.reference as string };
}

describe('harness: discount paths', () => {
  it('a used code is refused on the next order', async () => {
    const { o, ref } = await placeAndPay();
    await w.customerPays(ref);
    expect(w.order(o.order_id).discount_ngn).toBe(1_000);
    await expect(w.createOrder({ code: 'SAVE10' })).rejects.toThrow(/already used promo code SAVE10/);
  });
});

describe('BUG: discount usage limits (TLA+ DiscountUsage)', () => {
  // OneUsePerCustomer: discount_usages is only written at payment, so two
  // orders placed before either is paid both pass the check in prepareOrder.
  it('one customer gets a "one use per customer" code twice', async () => {
    const first = await placeAndPay();
    const second = await placeAndPay();                    // placed before the first is paid
    await w.customerPays(first.ref);
    await w.customerPays(second.ref);

    const paidWithCode = w.db.t('orders').filter(o => o.status === 'paid' && o.discount_code === 'SAVE10');
    expect(paidWithCode).toHaveLength(2);                   // correct: 1 (second order refused, or charged in full)
    expect(paidWithCode.every(o => o.discount_ngn === 1_000)).toBe(true);
    expect(w.db.t('discount_usages')).toHaveLength(1);
    expect(w.db.row('discount_codes', 'disc-save10')!.times_used).toBe(1);
  });

  // WithinMaxUses: times_used is checked when the order is created and
  // incremented when it's paid.
  it('a code with max_uses = 1 is used by two customers', async () => {
    w.db.row('discount_codes', 'disc-save10')!.max_uses = 1;
    const a = await placeAndPay('ada@example.com');
    const b = await placeAndPay('bola@example.com');
    await w.customerPays(a.ref);
    await w.customerPays(b.ref);

    expect(w.db.t('orders').filter(o => o.status === 'paid' && o.discount_ngn > 0)).toHaveLength(2); // correct: 1
    expect(w.db.row('discount_codes', 'disc-save10')!.times_used).toBe(2);
  });
});

describe('BUG: negative discount value (Lean parity_fails_for_negative_value)', () => {
  // POST /v2/admin/discounts rejects value <= 0, PATCH doesn't, and
  // discount_codes has no CHECK constraint. The web clamps the discount at 0
  // (calcDiscountAmount), so checkout shows ₦10,000; the API charges more.
  it('PATCH accepts a negative value and the order total exceeds the subtotal', async () => {
    w.db.t('discount_codes').push({
      id: 'disc-flat', code: 'FLAT', type: 'fixed', value: 500, active: true, auto_apply: false,
      min_order_ngn: 0, max_uses: null, times_used: 0, max_discount_ngn: null, exclusive: false, scope: 'site_wide',
    });
    const patch = await w.call('PATCH', '/v2/admin/discounts/disc-flat', { value: -5_000 }, ADMIN.token);
    expect(patch.status).toBe(200);                          // correct: 400

    const o = await w.createOrder({ code: 'FLAT' });
    expect(o.discount_ngn).toBe(-5_000);
    expect(o.total_ngn).toBe(PRICE + 5_000);                 // correct: <= PRICE
  });
});
