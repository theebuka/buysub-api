/-
  The discount arithmetic, as charged (src/shared/discount.ts + prepareOrder
  in src/index.ts) and as displayed (buysub-web/lib/constants.ts
  calcDiscountAmount + lib/checkout.ts computeTotals), at fx_rate = 1.

  Amounts are integers (kobo). `raw` is the code's amount before capping:
  eligible * value / 100 for a percentage code, value for a fixed one. The
  2-decimal rounding both sides apply is left out; it is below one kobo.

  Check with:  lean Discount.lean
-/

/-- max_discount_ngn: applied only when set and > 0, on both sides. -/
def capped (raw : Int) (cap : Option Int) : Int :=
  match cap with
  | some c => if c > 0 then min raw c else raw
  | none => raw

/-- API calcDiscountNGN: cap, then never more than the eligible subtotal. -/
def apiDiscount (raw elig : Int) (cap : Option Int) : Int :=
  min (capped raw cap) elig

/-- Web calcDiscountAmount: the same, then Math.max(0, amount). -/
def webDiscount (raw elig : Int) (cap : Option Int) : Int :=
  max 0 (min (capped raw cap) elig)

/-- prepareOrder: discount_ngn = min(subtotal, volume + code); total = subtotal - discount_ngn. -/
def apiTotal (sub vol d : Int) : Int := sub - min sub (vol + d)

/-- computeTotals: Math.max(0, subtotal - volume - discount). -/
def webTotal (sub vol d : Int) : Int := max 0 (sub - vol - d)

theorem capped_nonneg (raw : Int) (cap : Option Int) (h : 0 ≤ raw) : 0 ≤ capped raw cap := by
  unfold capped; split
  · split <;> omega
  · omega

/-- With a non-negative amount, the charged discount is between 0 and the eligible subtotal. -/
theorem apiDiscount_bounds (raw elig : Int) (cap : Option Int)
    (hr : 0 ≤ raw) (he : 0 ≤ elig) :
    0 ≤ apiDiscount raw elig cap ∧ apiDiscount raw elig cap ≤ elig := by
  have := capped_nonneg raw cap hr
  unfold apiDiscount; omega

/-- The total charged is never negative and never more than the subtotal. -/
theorem apiTotal_bounds (sub vol d : Int) (hs : 0 ≤ sub) (hv : 0 ≤ vol) (hd : 0 ≤ d) :
    0 ≤ apiTotal sub vol d ∧ apiTotal sub vol d ≤ sub := by
  unfold apiTotal; omega

/-- The two total formulas agree whenever they're given the same discount. -/
theorem totals_agree (sub vol d : Int) : apiTotal sub vol d = webTotal sub vol d := by
  unfold apiTotal webTotal; omega

/-- Displayed total = charged total, provided the code's amount is non-negative. -/
theorem parity (sub vol raw elig : Int) (cap : Option Int)
    (hr : 0 ≤ raw) (he : 0 ≤ elig) :
    apiTotal sub vol (apiDiscount raw elig cap) = webTotal sub vol (webDiscount raw elig cap) := by
  have := capped_nonneg raw cap hr
  have hw : webDiscount raw elig cap = apiDiscount raw elig cap := by
    unfold webDiscount apiDiscount; omega
  rw [hw]; exact totals_agree _ _ _

/-- Without that hypothesis parity fails. A fixed code of value -5,000 kobo
    (PATCH /v2/admin/discounts/:id doesn't check value, and discount_codes has
    no CHECK constraint) on a ₦100 cart: the API charges ₦150, checkout shows ₦100. -/
theorem parity_fails_for_negative_value :
    apiTotal 10000 0 (apiDiscount (-5000) 10000 none) = 15000 ∧
    webTotal 10000 0 (webDiscount (-5000) 10000 none) = 10000 := by
  decide

/-! Volume tiers (normalizeVolumeTiers / volumeTierFor): the best percent a
    quantity reaches. -/

/-- volumeTierFor: the highest percent among tiers with min_qty ≤ quantity (0 = none). -/
def bestPct : List (Nat × Nat) → Nat → Nat
  | [], _ => 0
  | (m, p) :: ts, q => if m ≤ q then max p (bestPct ts q) else bestPct ts q

/-- Buying more never lowers the percent off. -/
theorem bestPct_mono (ts : List (Nat × Nat)) (q₁ q₂ : Nat) (h : q₁ ≤ q₂) :
    bestPct ts q₁ ≤ bestPct ts q₂ := by
  induction ts with
  | nil => simp [bestPct]
  | cons t ts ih =>
    obtain ⟨m, p⟩ := t
    simp only [bestPct]
    split <;> split <;> omega

/-- percent ≤ 90 after normalisation, so a line's volume discount never exceeds
    the line, and the eligible amount left for a code is non-negative. -/
theorem volume_le_line (line pct : Nat) (hp : pct ≤ 90) : line * pct / 100 ≤ line := by
  have : line * pct ≤ line * 100 := Nat.mul_le_mul_left _ (by omega)
  have := Nat.div_le_div_right (c := 100) this
  simpa using this
