// Drives the real Worker (src/index.ts default export) against FakeDb and a
// fake Paystack. Test files must mock @supabase/supabase-js first:
//
//   vi.mock('@supabase/supabase-js', () => ({ createClient: () => (globalThis as any).__db }))

import { createHmac } from 'node:crypto';
import { vi } from 'vitest';
import { FakeDb } from './fakeDb';
import worker from '../src/index';

export const env = {
  SUPABASE_URL: 'https://fake.supabase.co',
  SUPABASE_SERVICE_ROLE_KEY: 'service',
  PAYSTACK_SECRET_KEY: 'sk_test_fake',
  PAYSTACK_PUBLIC_KEY: 'pk_test_fake',
  RESEND_API_KEY: '',
  WHATSAPP_NUMBER: '2340000000000',
  FRONTEND_URL: 'https://app.buysub.ng',
  WEBHOOK_SECRET: 'x',
  ALLOWED_ORIGINS: 'https://app.buysub.ng',
} as any;

const ctx = { waitUntil() {}, passThroughOnException() {} } as any;

export const PRICE = 10_000;     // Netflix, quarterly
export const WALLET = 4_000;     // the customer's starting balance
export const CUSTOMER = { token: 'tok-ada', id: 'user-ada', email: 'ada@example.com' };
export const ADMIN = { token: 'tok-admin', id: 'user-admin', email: 'ops@buysub.ng' };

/** A promise you can release later: pauses a request at a chosen statement. */
export function gate<T = void>() {
  let release!: (v: T) => void;
  let reached!: () => void;
  const opened = new Promise<T>(r => { release = r; });
  const arrived = new Promise<void>(r => { reached = r; });
  return { opened, arrived, release, reached };
}

type Tx = { reference: string; amount: number; currency: string; metadata: any; paid: boolean };

export class World {
  db = new FakeDb();
  txs = new Map<string, Tx>();
  /** Override to delay or fail Paystack /transaction/initialize. */
  onInitialize: (body: any) => Promise<{ status: boolean; message?: string }> | { status: boolean; message?: string } =
    () => ({ status: true });

  constructor() {
    (globalThis as any).__db = this.db;
    const db = this.db;
    db.users[CUSTOMER.token] = { id: CUSTOMER.id, email: CUSTOMER.email };
    db.users[ADMIN.token] = { id: ADMIN.id, email: ADMIN.email };
    db.t('profiles').push(
      { id: CUSTOMER.id, email: CUSTOMER.email, role: 'customer', full_name: 'Ada Obi' },
      { id: ADMIN.id, email: ADMIN.email, role: 'admin', full_name: 'Ops' },
    );
    db.t('customers').push({ id: 'cust-ada', user_id: CUSTOMER.id, email: CUSTOMER.email, name: 'Ada Obi' });
    db.t('wallets').push({ id: 'wallet-ada', user_id: CUSTOMER.id, balance_ngn: WALLET, is_active: true });
    db.t('products').push({
      id: 'prod-netflix', name: 'Netflix', slug: 'netflix', category: 'video streaming', status: 'active',
      stock_status: 'in_stock', billing_type: 'recurring', price_3m: PRICE, price_6m: 19_000, price_1y: 36_000,
      deleted_at: null, volume_tiers: null,
    });

    vi.stubGlobal('fetch', vi.fn(async (input: any, init?: any) => {
      const url = String(input?.url ?? input);
      if (url === 'https://api.paystack.co/transaction/initialize') {
        const body = JSON.parse(init.body);
        // Like Paystack: a reference can only be initialised once.
        if (this.txs.has(body.reference)) return json({ status: false, message: 'Duplicate Transaction Reference' });
        const res = await this.onInitialize(body);
        if (!res.status) return json({ status: false, message: res.message ?? 'Paystack unavailable' });
        this.txs.set(body.reference, { reference: body.reference, amount: body.amount, currency: body.currency, metadata: body.metadata, paid: false });
        return json({ status: true, data: { authorization_url: `https://checkout.paystack.com/${body.reference}`, access_code: 'ac' } });
      }
      const v = url.match(/^https:\/\/api\.paystack\.co\/transaction\/verify\/(.+)$/);
      if (v) {
        const tx = this.txs.get(decodeURIComponent(v[1]));
        if (!tx) return json({ status: false, message: 'Transaction reference not found' });
        return json({ status: true, data: { ...txData(tx), status: tx.paid ? 'success' : 'abandoned' } });
      }
      return json({ ok: true }); // Resend and anything else
    }));
  }

  // ── Requests ──
  async call(method: string, path: string, body?: any, token?: string) {
    const req = new Request(`https://api.buysub.ng${path}`, {
      method,
      headers: { 'Content-Type': 'application/json', Origin: env.FRONTEND_URL, ...(token ? { Authorization: `Bearer ${token}` } : {}) },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const res = await worker.fetch(req, env, ctx);
    const text = await res.text();
    let parsed: any = text;
    try { parsed = JSON.parse(text); } catch {}
    return { status: res.status, body: parsed };
  }

  async createOrder(opts: { email?: string; code?: string } = {}) {
    const r = await this.call('POST', '/v2/orders', {
      customer_email: opts.email ?? CUSTOMER.email,
      customer_name: 'Ada Obi',
      items: [{ product_id: 'prod-netflix', product_name: 'Netflix', quantity: 1, billing_period: 'Quarterly', unit_price_ngn: PRICE }],
      ...(opts.code ? { discount_code: opts.code } : {}),
    });
    if (r.status !== 200) throw new Error(`create order failed: ${r.status} ${JSON.stringify(r.body)}`);
    return r.body.data as { order_id: string; order_ref: string; total_ngn: number; discount_ngn: number };
  }

  payInit(orderId: string, useWallet: boolean) {
    return this.call('POST', '/v2/pay/init', { order_id: orderId, use_wallet: useWallet, callback_url: `${env.FRONTEND_URL}/order/verify` }, CUSTOMER.token);
  }

  rejectOrder(ref: string, confirm = false) {
    return this.call('POST', `/v2/admin/orders/${encodeURIComponent(ref)}/reject`, { reason: 'abandoned', ...(confirm ? { confirm: true } : {}) }, ADMIN.token);
  }

  /** The customer completes a Paystack page; Paystack then sends the signed webhook. */
  async customerPays(reference: string) {
    const tx = this.txs.get(reference);
    if (!tx) throw new Error(`no Paystack transaction ${reference}`);
    tx.paid = true;
    const body = JSON.stringify({ event: 'charge.success', data: { ...txData(tx), status: 'success' } });
    const signature = createHmac('sha512', env.PAYSTACK_SECRET_KEY).update(body).digest('hex');
    const res = await worker.fetch(new Request('https://api.buysub.ng/v2/pay/webhook', {
      method: 'POST', headers: { 'x-paystack-signature': signature, 'Content-Type': 'application/json' }, body,
    }), env, ctx);
    return { status: res.status, text: await res.text() };
  }

  // ── Reads ──
  order(id: string) { return this.db.row('orders', id)!; }
  balance() { return Number(this.db.row('wallets', 'wallet-ada')!.balance_ngn); }
  /** Card money Paystack actually collected, in NGN. */
  cardPaid() { return [...this.txs.values()].filter(t => t.paid).reduce((s, t) => s + t.amount / 100, 0); }
  events(action: string) { return this.db.t('event_logs').filter(e => e.action === action); }
  lastRef() { return [...this.txs.keys()].at(-1)!; }
}

/** Lets Date.now() move on, as it does between two tabs. */
export const tick = () => new Promise(r => setTimeout(r, 2));

function txData(tx: Tx) {
  return { reference: tx.reference, amount: tx.amount, currency: tx.currency, metadata: tx.metadata };
}

function json(o: any) {
  return new Response(JSON.stringify(o), { status: 200, headers: { 'Content-Type': 'application/json' } });
}
