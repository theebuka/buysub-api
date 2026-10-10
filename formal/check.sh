#!/bin/sh
# Runs every TLC model and the Lean proofs, and says whether each result is
# the expected one. Needs Java + tla2tools.jar (TLA2TOOLS, default
# ~/.local/tla/tla2tools.jar) and Lean 4 (elan).
set -u
cd "$(dirname "$0")"
JAR="${TLA2TOOLS:-$HOME/.local/tla/tla2tools.jar}"
# /usr/bin/java on macOS is a stub when no JDK is installed; prefer Homebrew's.
if [ -z "${JAVA:-}" ]; then
  if [ -x /opt/homebrew/opt/openjdk/bin/java ]; then JAVA=/opt/homebrew/opt/openjdk/bin/java; else JAVA=java; fi
fi
LEAN="${LEAN:-$(command -v lean || echo "$HOME/.elan/bin/lean")}"
fail=0

tlc() { # spec cfg expect(ok|violated)
  out="tla/out_$2.txt"
  (cd tla && "$JAVA" -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -workers auto -config "$2.cfg" \
     -metadir "states/$2" "$1.tla") > "$out" 2>&1
  inv=$(grep -o 'Invariant [A-Za-z]* is violated' "$out" | head -1 | cut -d' ' -f2)
  if grep -q "No error has been found" "$out"; then got=ok
  elif [ -n "$inv" ]; then got="violated: $inv"
  else got="error: see $out"; fi
  case "$got" in "$3"*) mark=pass ;; *) mark=UNEXPECTED; fail=1 ;; esac
  printf '%-11s %-30s %s\n' "$mark" "$2" "$got"
}

echo "Checkout.tla, the code before migration 24 (each should be violated)"
for c in P1_current P1_noother P2_current P3_current P4_current P5_current; do tlc Checkout $c violated; done
echo "Checkout.tla, with the fixes as implemented (should hold)"
for c in All_fixed All_fixed_W12 All_fixed_big; do tlc Checkout $c ok; done
echo "Checkout.tla, all fixes but one (each should be violated)"
for c in minus_FixAtomic minus_FixNoUndo minus_FixCancel minus_FixDup minus_FixSettle; do tlc Checkout $c violated; done
echo "DiscountUsage.tla"
tlc DiscountUsage D_OneUsePerCustomer_current violated
tlc DiscountUsage D_WithinMaxUses_current violated
tlc DiscountUsage D_fixed_max1 ok
tlc DiscountUsage D_fixed_max2 ok
tlc DiscountUsage D_reach_logged violated   # the logged path is reachable

echo "Lean"
if "$LEAN" lean/Discount.lean; then echo "pass        Discount.lean"; else echo "UNEXPECTED  Discount.lean"; fail=1; fi
exit $fail
