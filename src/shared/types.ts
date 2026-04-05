// ============================================================
// BUYSUB — SHARED TYPES
// ============================================================

// ── Enums ──
export type UserRole = 'user' | 'affiliate' | 'support_agent' | 'admin' | 'super_admin';
export type OrderStatus = 'pending' | 'pending_manual' | 'paid' | 'failed' | 'refunded' | 'cancelled';
export type PaymentMethod = 'paystack' | 'whatsapp' | 'bank_transfer' | 'cash' | 'wallet' | 'free';
export type DiscountType = 'percentage' | 'fixed';
export type DiscountScope = 'site_wide' | 'category';
export type AffiliateStatus = 'pending' | 'approved' | 'suspended' | 'rejected';
export type CommissionStatus = 'pending' | 'approved' | 'paid' | 'rejected';
export type ProductStock = 'in_stock' | 'out_of_stock' | 'preorder';
export type ProductStatus = 'active' | 'draft' | 'archived';
export type BillingType = 'subscription' | 'one_time';

// ── Products ──
export interface Product {
  id: string;
  name: string;
  slug: string;
  category: string | null;
  description: string | null;
  short_description: string | null;
  category_tagline: string | null;
  price_1m: number | null;
  price_3m: number | null;
  price_6m: number | null;
  price_1y: number | null;
  billing_type: BillingType;
  billing_period: string | null;
  tags: string | null;
  domain: string | null;
  stock_status: ProductStock;
  status: ProductStatus;
  image_url: string | null;
  sort_order: number;
  created_at: string;
  updated_at: string;
}

// ── Cart (frontend → API) ──
export interface CartItemPayload {
  product_id: string;
  product_name: string;
  category: string | null;
  billing_period: string;          // "Quarterly" | "Biannual" | "Annual" | "One-time"
  billing_type: string;            // "subscription" | "one_time"
  duration_months: number;
  unit_price_ngn: number;
  quantity: number;
}

// ── Orders ──
export interface Order {
  id: string;
  order_ref: string;
  customer_id: string | null;
  customer_email: string | null;
  customer_name: string | null;
  customer_phone: string | null;
  status: OrderStatus;
  payment_method: PaymentMethod | null;
  subtotal_ngn: number;
  discount_ngn: number;
  wallet_ngn: number;
  tax_ngn: number;
  total_ngn: number;
  currency: string;
  fx_rate: number;
  display_total: number | null;
  discount_code: string | null;
  affiliate_id: string | null;
  notes: string | null;
  paystack_ref: string | null;
  created_at: string;
  updated_at: string;
  paid_at: string | null;
  items?: OrderItem[];
}

export interface OrderItem {
  id: string;
  order_id: string;
  product_id: string | null;
  product_name: string;
  category: string | null;
  duration_months: number | null;
  billing_period: string | null;
  billing_type: string | null;
  unit_price_ngn: number;
  quantity: number;
  total_price_ngn: number;
}

// ── Discount Codes ──
export interface DiscountCode {
  id: string;
  code: string;
  type: DiscountType;
  value: number;
  min_order_ngn: number;
  max_discount_ngn: number | null;
  max_uses: number | null;
  times_used: number;
  active: boolean;
  active_from: string | null;
  expires_at: string | null;
  included_products: string | null;
  excluded_products: string | null;
  included_categories: string | null;
  excluded_categories: string | null;
  auto_apply: boolean;
  scope: DiscountScope;
  exclusive: boolean;
}

// ── API Request/Response ──
export interface CreateOrderRequest {
  customer_email: string;
  customer_name?: string;
  customer_phone?: string;
  items: CartItemPayload[];
  discount_code?: string;
  currency: string;
  fx_rate: number;
  payment_method: 'paystack' | 'whatsapp';
  use_wallet?: boolean;
  affiliate_code?: string;
}

export interface PaystackInitRequest {
  order_id: string;
  callback_url: string;
}

export interface PaystackInitResponse {
  authorization_url: string;
  access_code: string;
  reference: string;
}

export interface WhatsAppOrderResponse {
  order_ref: string;
  order_id: string;
  whatsapp_url: string;
  message: string;
}

export interface DiscountValidateRequest {
  code: string;
  items: CartItemPayload[];
  currency: string;
  fx_rate: number;
}

export interface DiscountValidateResponse {
  valid: boolean;
  error?: string;
  code?: string;
  type?: DiscountType;
  value?: number;
  display?: string;
  discount_ngn?: number;
  eligible_subtotal_ngn?: number;
  is_auto_apply?: boolean;
  is_exclusive?: boolean;
}

export interface AutoApplyResponse {
  discounts: Array<{
    code: string;
    type: DiscountType;
    value: number;
    display: string;
    max_discount_ngn: number | null;
    min_order_ngn: number;
    included_products: string | null;
    excluded_products: string | null;
    included_categories: string | null;
    excluded_categories: string | null;
    scope: DiscountScope;
    exclusive: boolean;
  }>;
}

export interface AdminApproveRequest {
  order_ref: string;
  payment_method: PaymentMethod;
  notes?: string;
}

// ── Affiliates ──
export interface Affiliate {
  id: string;
  user_id: string | null;
  business_name: string | null;
  store_name: string | null;
  status: AffiliateStatus;
  referral_code: string;
  commission_rate: number;
  bank_name: string | null;
  account_name: string | null;
  account_number: string | null;
  application_data: Record<string, any> | null;
  created_at: string;
}

// ── Customers ──
export interface Customer {
  id: string;
  user_id: string | null;
  name: string;
  email: string | null;
  phone: string | null;
  category: string | null;
  source: string | null;
  is_active: boolean;
  notes: string | null;
  created_at: string;
}

// ── Wallet ──
export interface Wallet {
  id: string;
  user_id: string;
  balance_ngn: number;
}

export interface WalletTransaction {
  id: string;
  wallet_id: string;
  type: 'credit' | 'debit';
  amount_ngn: number;
  source: 'admin' | 'refund' | 'order_payment';
  reference: string | null;
  balance_after: number;
  created_at: string;
}

// ── API Envelope ──
export interface ApiResponse<T = any> {
  ok: boolean;
  data?: T;
  error?: string;
  meta?: Record<string, any>;
}
