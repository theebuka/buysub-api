// ============================================================
// BUYSUB — Paystack calls shared by orders and wallet top-ups
// ============================================================

import type { Env } from '../http';

export async function paystackVerifyTx(reference: string, env: Env): Promise<any | null> {
  try {
    const res = await fetch(`https://api.paystack.co/transaction/verify/${encodeURIComponent(reference)}`, {
      headers: { Authorization: `Bearer ${env.PAYSTACK_SECRET_KEY}` },
    });
    const json = await res.json() as any;
    return json?.status ? json.data : null;
  } catch {
    return null;
  }
}

// Paystack redirects here after payment, so only allow our own origins.
export function safeCallbackUrl(requested: string | undefined, env: Env, fallbackPath = '/order/verify'): string {
  const fallback = `${env.FRONTEND_URL}${fallbackPath}`;
  if (!requested) return fallback;
  try {
    const u = new URL(requested);
    const allowed = [env.FRONTEND_URL, ...(env.ALLOWED_ORIGINS?.split(',') || [])]
      .map(s => s?.trim()).filter(Boolean)
      .map(s => { try { return new URL(s).origin; } catch { return ''; } });
    return allowed.includes(u.origin) ? u.toString() : fallback;
  } catch {
    return fallback;
  }
}

/** Starts a Paystack transaction. Amount in Naira. */
export async function paystackInitialize(env: Env, o: {
  email: string; amountNGN: number; reference: string; callbackUrl: string; metadata: Record<string, any>;
}): Promise<{ ok: true; authorization_url: string; access_code: string } | { ok: false; error: string }> {
  try {
    const res = await fetch('https://api.paystack.co/transaction/initialize', {
      method: 'POST',
      headers: { Authorization: `Bearer ${env.PAYSTACK_SECRET_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email: o.email,
        amount: Math.round(o.amountNGN * 100),
        currency: 'NGN',
        reference: o.reference,
        callback_url: o.callbackUrl,
        metadata: o.metadata,
      }),
    });
    const json = await res.json() as any;
    if (!json?.status) return { ok: false, error: json?.message || 'Unknown error' };
    return { ok: true, authorization_url: json.data.authorization_url, access_code: json.data.access_code };
  } catch (e: any) {
    return { ok: false, error: e?.message || 'network error' };
  }
}
