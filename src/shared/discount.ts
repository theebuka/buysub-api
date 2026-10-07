// ============================================================
// BUYSUB — SHARED CONSTANTS & DISCOUNT ENGINE
// ============================================================
import type { CartItemPayload, DiscountCode, DiscountType, VolumeTier } from './types';

// ── Periods ──
export const PERIODS = {
  quarterly:  { months: 3,  field: 'price_3m' as const, label: '/ 3 mo', name: 'Quarterly' },
  biannual:   { months: 6,  field: 'price_6m' as const, label: '/ 6 mo', name: 'Biannual' },
  annual:     { months: 12, field: 'price_1y' as const, label: '/ yr',   name: 'Annual' },
} as const;

export const TAB_ORDER = [
  'all', 'music streaming', 'video streaming', 'security', 'ai',
  'productivity', 'sports', 'bundles', 'education', 'cloud',
  'gaming', 'services', 'coins', 'social media',
] as const;

// FX lives only in buysub-web/lib/constants.ts. The API charges in NGN and
// stores the display rate the storefront sent (orders.fx_rate).

// ── Formatting ──
export const formatNGN = (value: number): string => {
  const v = Math.ceil(value * 2) / 2;
  return `₦${v.toLocaleString('en-NG')}`;
};

export const formatAmount = (value: number, currency: string): string => {
  if (!value && value !== 0) return '—';
  const v = Math.ceil(value * 2) / 2;
  if (currency === 'NGN') return `₦${v.toLocaleString('en-NG')}`;
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency,
    maximumFractionDigits: 2,
  }).format(v);
};

// ── Comma-separated list parser ──
export const splitList = (raw: string | null | undefined): string[] =>
  String(raw || '')
    .split(',')
    .map(s => s.trim().toLowerCase())
    .filter(Boolean);

// ── Normalise for comparison ──
export const norm = (v: any): string =>
  String(v || '').trim().toLowerCase();

// ============================================================
// DISCOUNT ENGINE — 8-step validation guard chain
// ============================================================

/**
 * Step 1-7: Validate a discount code against the guard chain.
 * Returns null if valid, or an error string if rejected.
 * 
 * Guard Chain Order:
 * 1. Active check
 * 2. Auto-apply filter (reject manual entry of auto-apply codes)
 * 3. Active From check
 * 4. Expiry check
 * 5. Usage limit check
 * 6. Minimum order check
 * 7. Eligibility check (done per-item, reflected in eligible subtotal)
 */
export function validateDiscountGuardChain(
  discount: DiscountCode,
  eligibleSubtotalNGN: number,
  isManualEntry: boolean,
): string | null {
  // 1. Active check
  if (!discount.active) {
    return 'This discount code is not active.';
  }

  // 2. Auto-apply filter: reject if customer manually enters an auto-apply code
  if (isManualEntry && discount.auto_apply) {
    return 'Code not found or inactive.';
  }

  // 3. Active From check
  if (discount.active_from && new Date(discount.active_from) > new Date()) {
    return 'This code is not active yet.';
  }

  // 4. Expiry check
  if (discount.expires_at && new Date(discount.expires_at) < new Date()) {
    return 'This code has expired.';
  }

  // 5. Usage limit check
  if (discount.max_uses != null && discount.times_used >= discount.max_uses) {
    return 'This code has reached its usage limit.';
  }

  // 6. Minimum order check (against eligible subtotal in NGN)
  if (discount.min_order_ngn > 0 && eligibleSubtotalNGN < discount.min_order_ngn) {
    return `Minimum order of ₦${discount.min_order_ngn.toLocaleString()} required.`;
  }

  return null; // valid
}

/**
 * Check if a single cart item is eligible for a discount.
 * Exclusion > Inclusion.
 */
export function isItemEligibleForDiscount(
  item: CartItemPayload,
  discount: DiscountCode,
): boolean {
  const name = norm(item.product_name);
  const categories = splitList(item.category);

  const excludedProducts = splitList(discount.excluded_products);
  const excludedCategories = splitList(discount.excluded_categories);
  const includedProducts = splitList(discount.included_products);
  const includedCategories = splitList(discount.included_categories);

  // Exclusion checks first (take precedence)
  if (excludedProducts.length > 0 && excludedProducts.includes(name)) {
    return false;
  }
  if (excludedCategories.length > 0 && categories.some(c => excludedCategories.includes(c))) {
    return false;
  }

  // Inclusion checks (allowlist — if specified, item must be in it)
  if (includedProducts.length > 0 && !includedProducts.includes(name)) {
    return false;
  }
  if (includedCategories.length > 0 && !categories.some(c => includedCategories.includes(c))) {
    return false;
  }

  return true;
}

/**
 * Calculate the eligible subtotal in NGN for a given discount: what's left of
 * each eligible line after its volume discount, so a code never discounts
 * money a volume tier already took off.
 */
export function getEligibleSubtotalNGN(
  items: CartItemPayload[],
  discount: DiscountCode,
): number {
  return items.reduce((sum, item) => {
    if (!isItemEligibleForDiscount(item, discount)) return sum;
    const line = item.unit_price_ngn * item.quantity;
    return sum + Math.max(0, line - Math.min(line, Number(item.volume_discount_ngn) || 0));
  }, 0);
}

// ============================================================
// VOLUME DISCOUNTS (migration 19). Mirrored in buysub-web/lib/constants.ts:
// change one, change both.
// ============================================================

export const MAX_VOLUME_TIERS = 5;
export const MAX_VOLUME_PERCENT = 90;

/** Clean a product's volume_tiers: whole quantities of 2 or more, percents
 *  above 0 and at most 90, one tier per quantity, ascending. */
export function normalizeVolumeTiers(raw: unknown): VolumeTier[] {
  if (!Array.isArray(raw)) return [];
  const byQty = new Map<number, number>();
  for (const t of raw) {
    const q = Math.floor(Number((t as any)?.min_qty));
    const pct = Math.round(Number((t as any)?.percent) * 100) / 100;
    if (!Number.isFinite(q) || q < 2 || q > 1000) continue;
    if (!Number.isFinite(pct) || pct <= 0 || pct > MAX_VOLUME_PERCENT) continue;
    byQty.set(q, pct);
  }
  return [...byQty.entries()]
    .sort((a, b) => a[0] - b[0])
    .slice(0, MAX_VOLUME_TIERS)
    .map(([min_qty, percent]) => ({ min_qty, percent }));
}

/** The highest tier a quantity reaches, or null. */
export function volumeTierFor(tiers: VolumeTier[], quantity: number): VolumeTier | null {
  let best: VolumeTier | null = null;
  for (const t of tiers) if (quantity >= t.min_qty && (!best || t.percent > best.percent)) best = t;
  return best;
}

/** NGN taken off one line (unit price x quantity) by its volume tier. */
export function volumeDiscountNGN(unitPriceNGN: number, quantity: number, tiers: VolumeTier[]): number {
  const tier = volumeTierFor(tiers, quantity);
  if (!tier) return 0;
  return Math.round(unitPriceNGN * quantity * (tier.percent / 100) * 100) / 100;
}

/**
 * Step 8: Calculate the discount amount in NGN.
 * Based on eligible subtotal ONLY. Respects max_discount_ngn cap.
 */
export function calcDiscountNGN(
  eligibleSubtotalNGN: number,
  discount: DiscountCode,
): number {
  let amount = 0;

  if (discount.type === 'percentage') {
    amount = eligibleSubtotalNGN * (discount.value / 100);
  } else {
    // Fixed amount discount
    amount = discount.value;
  }

  // Respect max discount cap
  if (discount.max_discount_ngn != null && discount.max_discount_ngn > 0) {
    amount = Math.min(amount, discount.max_discount_ngn);
  }

  // Never discount more than the eligible subtotal
  amount = Math.min(amount, eligibleSubtotalNGN);

  return Math.round(amount * 100) / 100; // round to 2 decimal
}

/**
 * Build a human-readable discount label.
 */
export function buildDiscountDisplay(
  discount: DiscountCode,
  currency: string = 'NGN',
  fxRate: number = 1,
): string {
  let label = '';
  if (discount.type === 'percentage') {
    label = `${discount.value}% off`;
  } else {
    label = `${formatAmount(discount.value * fxRate, currency)} off`;
  }
  if (discount.max_discount_ngn != null) {
    label += ` · max ${formatAmount(discount.max_discount_ngn * fxRate, currency)}`;
  }
  return label;
}

/**
 * Full discount validation pipeline — used by the Workers API.
 * Runs all 8 steps and returns the result.
 */
export function validateAndCalcDiscount(
  discount: DiscountCode,
  items: CartItemPayload[],
  isManualEntry: boolean,
): {
  valid: boolean;
  error?: string;
  eligible_subtotal_ngn: number;
  discount_ngn: number;
  display: string;
} {
  const eligibleSubtotal = getEligibleSubtotalNGN(items, discount);

  // Run guard chain (steps 1-7)
  const error = validateDiscountGuardChain(discount, eligibleSubtotal, isManualEntry);
  if (error) {
    return { valid: false, error, eligible_subtotal_ngn: 0, discount_ngn: 0, display: '' };
  }

  // Step 8: Calculate
  const discountNGN = calcDiscountNGN(eligibleSubtotal, discount);
  const display = buildDiscountDisplay(discount);

  return {
    valid: true,
    eligible_subtotal_ngn: eligibleSubtotal,
    discount_ngn: discountNGN,
    display,
  };
}
