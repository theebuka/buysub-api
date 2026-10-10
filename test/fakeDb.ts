// In-memory stand-in for the Supabase client, covering the PostgREST calls the
// order, payment, wallet and discount handlers make. Each awaited call is one
// "statement", so tests can interleave requests between statements the way
// concurrent Workers requests interleave in production.
//
// Unique constraints mirror the live schema (orders.order_ref,
// payment_events.payment_reference, discount_usages (discount_id, customer_id),
// affiliate_commissions.order_id, user_notifications dedupe, wallets.user_id).

type Row = Record<string, any>;
type Filter = (r: Row) => boolean;

const UNIQUE: Record<string, string[][]> = {
  orders: [['order_ref']],
  payment_events: [['payment_reference']],
  discount_usages: [['discount_id', 'customer_id']],
  discount_codes: [['code']],
  affiliate_commissions: [['order_id']],
  user_notifications: [['user_id', 'dedupe_key']],
  wallets: [['user_id']],
};

const same = (a: any, b: any) => (a === null || a === undefined || b === null || b === undefined)
  ? a == b
  : String(a) === String(b);

const singular = (t: string) => t.replace(/s$/, '');

export class FakeDb {
  tables: Record<string, Row[]> = {};
  users: Record<string, { id: string; email: string }> = {};
  /** Called before every RPC; a test can await a gate here to pause a request mid-flight. */
  hooks: { rpc?: (fn: string, args: any) => Promise<void> | void } = {};
  private seq = 0;

  auth = {
    getUser: async (token: string) => {
      const user = this.users[token];
      return user ? { data: { user }, error: null } : { data: { user: null }, error: { message: 'invalid token' } };
    },
  };

  nextId(t: string) { return `${singular(t)}-${++this.seq}`; }
  t(name: string): Row[] { return (this.tables[name] ??= []); }
  row(name: string, id: string): Row | undefined { return this.t(name).find(r => r.id === id); }

  from(table: string) { return new Query(this, table); }

  walletMove(fn: string, args: any): { data: any; error: any } {
    const w = this.row('wallets', args.p_wallet_id);
    if (!w) return { data: null, error: { message: 'Wallet not found' } };
    const amt = Number(args.p_amount);
    if (fn === 'debit_wallet' && Number(w.balance_ngn) < amt) {
      return { data: null, error: { message: `Insufficient wallet balance. Available: ${w.balance_ngn}, Requested: ${amt}` } };
    }
    w.balance_ngn = Number(w.balance_ngn) + (fn === 'credit_wallet' ? amt : -amt);
    this.t('wallet_transactions').push({
      id: this.nextId('wallet_transactions'), wallet_id: w.id, type: fn === 'credit_wallet' ? 'credit' : 'debit',
      amount_ngn: amt, source: args.p_source ?? 'order_payment', reference: args.p_reference, balance_after: w.balance_ngn,
    });
    return { data: w.balance_ngn, error: null };
  }

  async rpc(fn: string, args: any = {}): Promise<{ data: any; error: any }> {
    await this.hooks.rpc?.(fn, args);
    switch (fn) {
      case 'generate_order_ref':
        return { data: `BS-${1000 + ++this.seq}`, error: null };
      case 'credit_wallet':
      case 'debit_wallet':
        return this.walletMove(fn, args);
      case 'increment_discount_usage': {
        const d = this.row('discount_codes', args.p_discount_id);
        if (d) d.times_used = Number(d.times_used || 0) + 1;
        return { data: null, error: null };
      }
      // Migration 24. These mirror supabase-migrations/24_order_money_atomic.sql
      // statement for statement; each runs as one step here, as the SQL runs
      // under row locks in one transaction.
      case 'apply_order_wallet': {
        const o = this.row('orders', args.p_order_id);
        if (!o) return { data: { result: 'missing' }, error: null };
        if (o.status !== 'pending') return { data: { result: 'not_pending' }, error: null };
        if (Number(o.wallet_ngn || 0) > 0) return { data: { result: 'already', amount: o.wallet_ngn, total_ngn: o.total_ngn }, error: null };
        const w = this.row('wallets', args.p_wallet_id);
        if (!w || w.is_active === false || Number(w.balance_ngn) <= 0 || Number(o.total_ngn) <= 0) return { data: { result: 'empty' }, error: null };
        if (!this.t('customers').some(c => c.id === o.customer_id && c.user_id === w.user_id)) return { data: { result: 'not_owner' }, error: null };
        if (args.p_require_full && Number(w.balance_ngn) < Number(o.total_ngn)) return { data: { result: 'insufficient' }, error: null };
        const amount = Math.min(Number(w.balance_ngn), Number(o.total_ngn));
        const debit = await this.walletMove('debit_wallet', { p_wallet_id: w.id, p_amount: amount, p_reference: o.order_ref });
        if (debit.error) return debit;
        Object.assign(o, { wallet_ngn: amount, total_ngn: Number(o.total_ngn) - amount });
        return { data: { result: 'ok', amount, total_ngn: o.total_ngn }, error: null };
      }
      case 'cancel_rejected_order': {
        const o = this.row('orders', args.p_order_id);
        if (!o) return { data: { result: 'missing' }, error: null };
        if (o.status !== 'rejected_pending') return { data: { result: 'changed' }, error: null };
        const held = Number(o.wallet_ngn || 0);
        Object.assign(o, { status: 'cancelled', notes: args.p_notes, wallet_ngn: 0, total_ngn: Number(o.total_ngn) + held });
        this.tables.discount_usages = this.t('discount_usages').filter(u => u.order_id !== o.id);
        if (held > 0) {
          const c = this.t('customers').find(c => c.id === o.customer_id);
          const w = this.t('wallets').find(w => c && w.user_id === c.user_id);
          if (!w) return { data: null, error: { message: `cancel_rejected_order: no wallet for ${o.order_ref}` } };
          await this.walletMove('credit_wallet', { p_wallet_id: w.id, p_amount: held, p_reference: `${o.order_ref} cancelled`, p_source: 'refund' });
        }
        return { data: { result: 'ok', refunded: held }, error: null };
      }
      case 'reserve_discount_use':
      case 'record_discount_use': {
        const d = this.row('discount_codes', args.p_discount_id);
        if (!d) return { data: 'missing', error: null };
        const record = fn === 'record_discount_use';
        const status = (id: string) => this.row('orders', id)?.status ?? null;
        const held = () => this.t('discount_usages').filter(u => u.discount_id === d.id && status(u.order_id) !== 'paid').length;
        if (args.p_customer_id) {
          const u = this.t('discount_usages').find(u => u.discount_id === d.id && u.customer_id === args.p_customer_id);
          if (u) {
            if (u.order_id !== args.p_order_id) {
              const s = status(u.order_id);
              if (s === null || s === 'paid') return { data: record ? 'duplicate' : 'used', error: null };
              u.order_id = args.p_order_id;
            }
            if (!record) return { data: 'ok', error: null };
            d.times_used = Number(d.times_used || 0) + 1;
            return { data: 'counted', error: null };
          }
        }
        const full = d.max_uses != null && Number(d.times_used || 0) + held() >= Number(d.max_uses);
        if (!record && full) return { data: 'exhausted', error: null };
        if (args.p_customer_id) {
          this.t('discount_usages').push({ id: this.nextId('discount_usages'), discount_id: d.id, order_id: args.p_order_id, customer_id: args.p_customer_id });
        }
        if (!record) return { data: 'ok', error: null };
        d.times_used = Number(d.times_used || 0) + 1;
        return { data: full ? 'over_limit' : 'counted', error: null };
      }
      default:
        return { data: null, error: null };
    }
  }
}

class Query implements PromiseLike<any> {
  private op: 'select' | 'insert' | 'update' | 'delete' = 'select';
  private filters: Filter[] = [];
  private payload: any;
  private returning = false;
  private cols = '*';
  private countOpt: { count?: string; head?: boolean } = {};
  private mode: 'many' | 'single' | 'maybe' = 'many';
  private sort: { col: string; asc: boolean }[] = [];
  private lim?: number;
  private rng?: [number, number];

  constructor(private db: FakeDb, private table: string) {}

  select(cols = '*', opts: { count?: string; head?: boolean } = {}) {
    if (this.op === 'select') { this.cols = cols; this.countOpt = opts; } else this.returning = true;
    return this;
  }
  insert(rows: Row | Row[]) { this.op = 'insert'; this.payload = rows; return this; }
  upsert(rows: Row | Row[]) { return this.insert(rows); }
  update(patch: Row) { this.op = 'update'; this.payload = patch; return this; }
  delete() { this.op = 'delete'; return this; }

  private add(col: string, f: (v: any) => boolean) {
    if (col.includes('.')) return this; // filters on embedded tables aren't modelled
    this.filters.push(r => f(r[col]));
    return this;
  }
  eq(c: string, v: any) { return this.add(c, x => same(x, v)); }
  neq(c: string, v: any) { return this.add(c, x => !same(x, v)); }
  in(c: string, vs: any[]) { return this.add(c, x => vs.some(v => same(x, v))); }
  is(c: string, v: any) { return this.add(c, x => (v === null ? x === null || x === undefined : x === v)); }
  gt(c: string, v: any) { return this.add(c, x => x > v); }
  gte(c: string, v: any) { return this.add(c, x => x >= v); }
  lt(c: string, v: any) { return this.add(c, x => x < v); }
  lte(c: string, v: any) { return this.add(c, x => x <= v); }
  ilike(c: string, pattern: string) {
    const want = pattern.replace(/\\(.)/g, '$1').toLowerCase();
    return this.add(c, x => String(x ?? '').toLowerCase() === want);
  }
  /** 'a.is.null,a.eq.0' style disjunctions. */
  or(expr: string) {
    const parts = expr.split(',').map(p => {
      const [col, op, ...rest] = p.split('.');
      const v = rest.join('.');
      return (r: Row) => op === 'is' ? (v === 'null' ? r[col] == null : String(r[col]) === v)
        : op === 'eq' ? same(r[col], v)
        : op === 'neq' ? !same(r[col], v)
        : false;
    });
    this.filters.push(r => parts.some(p => p(r)));
    return this;
  }
  order(col: string, o: { ascending?: boolean } = {}) { this.sort.push({ col, asc: o.ascending !== false }); return this; }
  limit(n: number) { this.lim = n; return this; }
  range(a: number, b: number) { this.rng = [a, b]; return this; }
  single() { this.mode = 'single'; return this; }
  maybeSingle() { this.mode = 'maybe'; return this; }

  then<A = any, B = never>(ok?: ((v: any) => A | PromiseLike<A>) | null, bad?: ((e: any) => B | PromiseLike<B>) | null): PromiseLike<A | B> {
    return Promise.resolve().then(() => this.run()).then(ok, bad);
  }

  private match(r: Row) { return this.filters.every(f => f(r)); }

  private embed(r: Row): Row {
    const out = { ...r };
    for (const m of this.cols.matchAll(/(\w+)(?:!inner)?\(/g)) {
      const rel = m[1];
      const fk = `${singular(this.table)}_id`;
      const children = this.db.t(rel);
      if (children.some(c => fk in c) || rel.endsWith('_items')) out[rel] = children.filter(c => c[fk] === r.id).map(c => ({ ...c }));
      else out[rel] = this.db.row(rel, r[`${singular(rel)}_id`]) ?? null;
    }
    return out;
  }

  private shape(rows: Row[], countAll?: number) {
    const count = countAll ?? rows.length;
    if (this.countOpt.head) return { data: null, error: null, count };
    if (this.mode === 'many') return { data: rows, error: null, count };
    if (rows.length === 1) return { data: rows[0], error: null, count };
    if (rows.length === 0 && this.mode === 'maybe') return { data: null, error: null, count };
    return { data: null, error: { code: 'PGRST116', message: `expected one row, got ${rows.length}` }, count };
  }

  private run() {
    const rows = this.db.t(this.table);
    if (this.op === 'insert') {
      const batch = (Array.isArray(this.payload) ? this.payload : [this.payload])
        .map((r: Row) => ({ id: this.db.nextId(this.table), created_at: new Date().toISOString(), ...r }));
      for (const keys of UNIQUE[this.table] ?? []) {
        const seen = [...rows];
        for (const r of batch) {
          if (keys.some(k => r[k] == null)) continue;
          if (seen.some(s => keys.every(k => same(s[k], r[k])))) {
            return { data: null, error: { code: '23505', message: `duplicate key value violates unique constraint "${this.table}_${keys.join('_')}"` } };
          }
          seen.push(r);
        }
      }
      rows.push(...batch);
      return this.returning ? this.shape(batch.map(r => ({ ...r }))) : { data: null, error: null };
    }
    const hit = rows.filter(r => this.match(r));
    if (this.op === 'update') {
      for (const r of hit) Object.assign(r, this.payload);
      return this.returning ? this.shape(hit.map(r => ({ ...r }))) : { data: null, error: null };
    }
    if (this.op === 'delete') {
      this.db.tables[this.table] = rows.filter(r => !hit.includes(r));
      return { data: null, error: null };
    }
    let out = [...hit];
    for (const s of [...this.sort].reverse()) {
      out.sort((a, b) => (a[s.col] < b[s.col] ? -1 : a[s.col] > b[s.col] ? 1 : 0) * (s.asc ? 1 : -1));
    }
    const total = out.length;
    if (this.rng) out = out.slice(this.rng[0], this.rng[1] + 1);
    if (this.lim !== undefined) out = out.slice(0, this.lim);
    return this.shape(out.map(r => this.embed(r)), total);
  }
}
