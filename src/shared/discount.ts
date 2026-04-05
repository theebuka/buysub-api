// ============================================================
// BUYSUB — SHARED CONSTANTS & DISCOUNT ENGINE
// ============================================================
import type { CartItemPayload, DiscountCode, DiscountType } from './types';

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

// ── FX (static fallback) ──
export const STATIC_FX: Record<string, number> = {
  NGN: 1,
  USD: 1 / 1300,
  GBP: 1 / 1860,
  CAD: 1 / 920,
};

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
 * Calculate the eligible subtotal in NGN for a given discount.
 */
export function getEligibleSubtotalNGN(
  items: CartItemPayload[],
  discount: DiscountCode,
): number {
  return items.reduce((sum, item) => {
    if (!isItemEligibleForDiscount(item, discount)) return sum;
    return sum + item.unit_price_ngn * item.quantity;
  }, 0);
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
