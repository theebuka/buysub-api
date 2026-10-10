// Promo-code findings from formal/tla/DiscountUsage.tla and
// formal/lean/Discount.lean, fixed by migration 24 and the API changes.

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

describe('fixed: discount usage limits (formal/tla/DiscountUsage.tla)', () => {
  // Was: two unpaid orders both passed the check and both got the code.
  it('an order placed after an abandoned one takes over the use; the abandoned one can no longer be paid with it', async () => {
    const first = await placeAndPay();                     // Paystack page opened, then abandoned
    const second = await placeAndPay();                    // not locked out by the first
    await w.customerPays(second.ref);
    expect(w.order(second.o.order_id).status).toBe('paid');

    const retry = await w.payInit(first.o.order_id, false);
    expect(retry.status).toBe(409);
    expect(retry.body.error).toMatch(/already used promo code SAVE10.*Place a new order without it/);
    expect(w.db.row('discount_codes', 'disc-save10')!.times_used).toBe(1);
  });

  it('when both Paystack pages were open and both are paid, the second use is logged', async () => {
    const first = await placeAndPay();
    const second = await placeAndPay();
    await w.customerPays(first.ref);
    await w.customerPays(second.ref);                       // can't be refused once paid

    expect(w.events('discount_reused').map(e => e.entity_id)).toEqual([second.o.order_id]);
    expect(w.db.row('discount_codes', 'disc-save10')!.times_used).toBe(1);
  });

  // Was: max_uses checked at creation, counted at payment.
  it('max_uses counts unpaid orders, so a second customer is refused while the first holds the last use', async () => {
    w.db.row('discount_codes', 'disc-save10')!.max_uses = 1;
    const a = await placeAndPay('ada@example.com');
    await expect(w.createOrder({ email: 'bola@example.com', code: 'SAVE10' })).rejects.toThrow(/409.*reached its usage limit/);
    await w.customerPays(a.ref);
    expect(w.db.t('orders').filter(o => o.status === 'paid' && o.discount_ngn > 0)).toHaveLength(1);
  });

  it('cancelling an unpaid order frees its use', async () => {
    w.db.row('discount_codes', 'disc-save10')!.max_uses = 1;
    const a = await placeAndPay('ada@example.com');
    await w.rejectOrder(a.o.order_ref);
    await w.rejectOrder(a.o.order_ref, true);
    const b = await placeAndPay('bola@example.com');
    await w.customerPays(b.ref);
    expect(w.order(b.o.order_id).status).toBe('paid');
    expect(w.db.row('discount_codes', 'disc-save10')!.times_used).toBe(1);
  });
});

describe('fixed: discount values (formal/lean/Discount.lean)', () => {
  // Was: PATCH accepted value -5000 and the API charged ₦15,000 for ₦10,000.
  it.each([
    [{ value: -5_000 }, /positive/],
    [{ value: 0 }, /positive/],
    [{ type: 'percentage', value: 150 }, /more than 100/],
    [{ type: 'percentage' }, /more than 100/],             // FLAT is 500: as a percentage, too much
    [{ max_discount_ngn: -1 }, /Cap can’t be negative/],
    [{ min_order_ngn: -1 }, /Minimum order can’t be negative/],
  ])('PATCH %j is refused', async (patch, msg) => {
    w.db.t('discount_codes').push({
      id: 'disc-flat', code: 'FLAT', type: 'fixed', value: 500, active: true, auto_apply: false,
      min_order_ngn: 0, max_uses: null, times_used: 0, max_discount_ngn: null, exclusive: false, scope: 'site_wide',
    });
    const res = await w.call('PATCH', '/v2/admin/discounts/disc-flat', patch, ADMIN.token);
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(msg);
    expect(w.db.row('discount_codes', 'disc-flat')!.value).toBe(500);
  });

  it('POST refuses a percentage over 100', async () => {
    const res = await w.call('POST', '/v2/admin/discounts', { code: 'BIG', type: 'percentage', value: 120 }, ADMIN.token);
    expect(res.status).toBe(400);
  });

  it('a valid PATCH still works', async () => {
    const res = await w.call('PATCH', '/v2/admin/discounts/disc-save10', { value: 15 }, ADMIN.token);
    expect(res.status).toBe(200);
    const o = await w.createOrder({ code: 'SAVE10' });
    expect(o.total_ngn).toBe(PRICE - 1_500);
  });
});
