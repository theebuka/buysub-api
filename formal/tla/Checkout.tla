---------------------------- MODULE Checkout ----------------------------
(***************************************************************************)
(* One Paystack order, the customer's wallet, and an admin, modelled at    *)
(* the granularity of the database statements in src/index.ts:            *)
(*                                                                         *)
(*   handlePaystackInit      Start, Claim, Debit, Check, Init, Undo1/2     *)
(*   (customer pays)         Pay                                           *)
(*   settlePaystackPayment   SettleRead, SettleDo  (webhook or verify)     *)
(*   handleAdminRejectOrder  Reject, ConfirmRead, ConfirmMove, Refund      *)
(*   handleAdminUndoReject   UndoReject                                    *)
(*   another order           OtherSpend (the wallet is spent elsewhere)    *)
(*                                                                         *)
(* Each step is one PostgREST call or RPC, so steps of concurrent requests *)
(* interleave the way Workers requests do. Amounts are whole naira.        *)
(*                                                                         *)
(* The Fix* constants switch on proposed fixes; with all FALSE this is the *)
(* code as it stands.                                                      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
  T,          \* order total (NGN)
  W,          \* customer's starting wallet balance
  N,          \* number of /v2/pay/init attempts (tabs, retries)
  MaxAdmin,   \* bound on admin actions
  FixAtomic,  \* claim + debit in one RPC (wallet row and order row locked together)
  FixNoUndo,  \* a failed Paystack init leaves the wallet part on the order
  FixCancel,  \* the cancelling UPDATE also takes the wallet part off the order
  FixDup,     \* a capture that pays more than total_ngn at fulfil time (incl. an
              \* already-paid order) is logged for refund
  FixSettle,  \* the amount check is part of fulfillOrder's conditional UPDATE
              \* (... WHERE status IN (...) AND total_ngn <= paid), not a prior read
  OtherOrders \* the customer may spend the wallet on another order meanwhile

Attempts == 1..N
Min(a, b) == IF a < b THEN a ELSE b

VARIABLES
  status,       \* orders.status
  oWallet,      \* orders.wallet_ngn
  oTotal,       \* orders.total_ngn (what Paystack must still collect)
  oRef,         \* orders.paystack_ref (attempt id, 0 = none)
  balance,      \* wallets.balance_ngn
  pc, snap, useW, amt, deducted, charge,   \* per-attempt locals of handlePaystackInit
  captured,     \* attempts the customer actually paid on Paystack
  spc, sTotal,  \* per-capture settle progress and the order total it read
  flagged,      \* an anomaly was logged (payment_amount_mismatch, or the FixDup event)
  mismatchSeen, everCancelled,
  apc, aSnapW, adminOps,
  otherSpent

vars == <<status, oWallet, oTotal, oRef, balance, pc, snap, useW, amt, deducted,
          charge, captured, spc, sTotal, flagged, mismatchSeen, everCancelled,
          apc, aSnapW, adminOps, otherSpent>>

RECURSIVE SumCharge(_)
SumCharge(S) == IF S = {} THEN 0
                ELSE LET x == CHOOSE x \in S : TRUE IN charge[x] + SumCharge(S \ {x})

CardPaid == SumCharge(captured)
\* What this order has cost the customer: wallet money gone (not spent
\* elsewhere) plus card money captured.
NetPaid == (W - otherSpent - balance) + CardPaid

Init ==
  /\ status = "pending" /\ oWallet = 0 /\ oTotal = T /\ oRef = 0 /\ balance = W
  /\ pc = [a \in Attempts |-> "idle"]
  /\ snap = [a \in Attempts |-> [w |-> 0, t |-> 0]]
  /\ useW = [a \in Attempts |-> FALSE]
  /\ amt = [a \in Attempts |-> 0]
  /\ deducted = [a \in Attempts |-> 0]
  /\ charge = [a \in Attempts |-> 0]
  /\ captured = {}
  /\ spc = [a \in Attempts |-> "none"]
  /\ sTotal = [a \in Attempts |-> 0]
  /\ flagged = FALSE /\ mismatchSeen = FALSE /\ everCancelled = FALSE
  /\ apc = "idle" /\ aSnapW = 0 /\ adminOps = 0
  /\ otherSpent = 0

Goto(a, l) == pc' = [pc EXCEPT ![a] = l]

\* fulfillOrder's claim: UPDATE ... SET status='paid' WHERE status IN allowed
Fulfil(allowed) == status' = IF status \in allowed THEN "paid" ELSE status
SettleAllowed == {"pending", "pending_manual", "rejected_pending", "cancelled", "failed"}

(* ---- handlePaystackInit --------------------------------------------- *)

\* SELECT * FROM orders WHERE id = ? AND status = 'pending', then the
\* "earlier attempt already paid" check on orders.paystack_ref.
Start(a) ==
  /\ pc[a] = "idle" /\ status = "pending"
  /\ \E u \in BOOLEAN : useW' = [useW EXCEPT ![a] = u]
  /\ snap' = [snap EXCEPT ![a] = [w |-> oWallet, t |-> oTotal]]
  /\ IF oRef # 0 /\ oRef \in captured
       THEN IF charge[oRef] >= oTotal
              THEN /\ Fulfil(SettleAllowed) /\ Goto(a, "done")
                   /\ UNCHANGED <<flagged, mismatchSeen>>
              ELSE /\ flagged' = TRUE /\ mismatchSeen' = TRUE /\ Goto(a, "claim")
                   /\ UNCHANGED status
       ELSE Goto(a, "claim") /\ UNCHANGED <<status, flagged, mismatchSeen>>
  /\ UNCHANGED <<oWallet, oTotal, oRef, balance, amt, deducted, charge, captured,
                 spc, sTotal, everCancelled, apc, aSnapW, adminOps, otherSpent>>

\* Wallet read + the conditional claim
\*   UPDATE orders SET wallet_ngn = x, total_ngn = t - x
\*   WHERE status = 'pending' AND (wallet_ngn IS NULL OR wallet_ngn = 0)
Claim(a) ==
  /\ pc[a] = "claim"
  /\ IF useW[a] /\ snap[a].w = 0 /\ balance > 0 /\ status = "pending" /\ oWallet = 0
       THEN LET x == Min(balance, snap[a].t) IN
            /\ oWallet' = x
            /\ oTotal' = snap[a].t - x
            /\ amt' = [amt EXCEPT ![a] = x]
            /\ IF FixAtomic
                 THEN /\ balance' = balance - x
                      /\ deducted' = [deducted EXCEPT ![a] = x]
                      /\ Goto(a, "check")
                 ELSE /\ UNCHANGED <<balance, deducted>>
                      /\ Goto(a, "debit")
       ELSE Goto(a, "check") /\ UNCHANGED <<oWallet, oTotal, amt, balance, deducted>>
  /\ UNCHANGED <<status, oRef, snap, useW, charge, captured, spc, sTotal, flagged,
                 mismatchSeen, everCancelled, apc, aSnapW, adminOps, otherSpent>>

\* rpc debit_wallet; on failure the order is reset unconditionally.
Debit(a) ==
  /\ pc[a] = "debit"
  /\ IF balance >= amt[a]
       THEN /\ balance' = balance - amt[a]
            /\ deducted' = [deducted EXCEPT ![a] = amt[a]]
            /\ Goto(a, "check")
            /\ UNCHANGED <<oWallet, oTotal>>
       ELSE /\ oWallet' = 0 /\ oTotal' = snap[a].t
            /\ Goto(a, "done")
            /\ UNCHANGED <<balance, deducted>>
  /\ UNCHANGED <<status, oRef, snap, useW, amt, charge, captured, spc, sTotal, flagged,
                 mismatchSeen, everCancelled, apc, aSnapW, adminOps, otherSpent>>

\* The 409 guard, the wallet-covers-everything branch, else go to Paystack.
Check(a) ==
  /\ pc[a] = "check"
  /\ LET ch == snap[a].t - deducted[a] IN
     IF useW[a] /\ deducted[a] = 0 /\ snap[a].w = 0
       THEN Goto(a, "done") /\ UNCHANGED <<status, charge>>
     ELSE IF ch <= 0
       THEN Fulfil({"pending"}) /\ Goto(a, "done") /\ UNCHANGED charge
     ELSE /\ charge' = [charge EXCEPT ![a] = ch]
          /\ Goto(a, "init")
          /\ UNCHANGED status
  /\ UNCHANGED <<oWallet, oTotal, oRef, balance, snap, useW, amt, deducted, captured,
                 spc, sTotal, flagged, mismatchSeen, everCancelled, apc, aSnapW,
                 adminOps, otherSpent>>

\* Paystack /transaction/initialize succeeds (save paystack_ref) or fails (undoWallet).
InitOk(a) ==
  /\ pc[a] = "init"
  /\ oRef' = a
  /\ Goto(a, "open")
  /\ UNCHANGED <<status, oWallet, oTotal, balance, snap, useW, amt, deducted, charge,
                 captured, spc, sTotal, flagged, mismatchSeen, everCancelled, apc,
                 aSnapW, adminOps, otherSpent>>

InitFail(a) ==
  /\ pc[a] = "init"
  /\ Goto(a, IF deducted[a] > 0 /\ ~FixNoUndo THEN "undo1" ELSE "done")
  /\ UNCHANGED <<status, oWallet, oTotal, oRef, balance, snap, useW, amt, deducted,
                 charge, captured, spc, sTotal, flagged, mismatchSeen, everCancelled,
                 apc, aSnapW, adminOps, otherSpent>>

\* undoWallet: refundWalletForOrder (rpc credit_wallet) ...
Undo1(a) ==
  /\ pc[a] = "undo1"
  /\ balance' = balance + deducted[a]
  /\ Goto(a, "undo2")
  /\ UNCHANGED <<status, oWallet, oTotal, oRef, snap, useW, amt, deducted, charge,
                 captured, spc, sTotal, flagged, mismatchSeen, everCancelled, apc,
                 aSnapW, adminOps, otherSpent>>

\* ... then UPDATE orders SET wallet_ngn = 0, total_ngn = original WHERE id = ?
Undo2(a) ==
  /\ pc[a] = "undo2"
  /\ oWallet' = 0 /\ oTotal' = snap[a].t
  /\ Goto(a, "done")
  /\ UNCHANGED <<status, oRef, balance, snap, useW, amt, deducted, charge, captured,
                 spc, sTotal, flagged, mismatchSeen, everCancelled, apc, aSnapW,
                 adminOps, otherSpent>>

(* ---- The customer pays an open Paystack page ------------------------ *)

Pay(a) ==
  /\ pc[a] = "open" /\ a \notin captured
  /\ captured' = captured \cup {a}
  /\ UNCHANGED <<status, oWallet, oTotal, oRef, balance, pc, snap, useW, amt, deducted,
                 charge, spc, sTotal, flagged, mismatchSeen, everCancelled, apc,
                 aSnapW, adminOps, otherSpent>>

(* ---- settlePaystackPayment (webhook or /v2/pay/verify) -------------- *)

\* findOrderForTx: SELECT * by reference or metadata.order_id (any status)
SettleRead(a) ==
  /\ a \in captured /\ spc[a] = "none"
  /\ sTotal' = [sTotal EXCEPT ![a] = oTotal]
  /\ spc' = [spc EXCEPT ![a] = "read"]
  /\ UNCHANGED <<status, oWallet, oTotal, oRef, balance, pc, snap, useW, amt, deducted,
                 charge, captured, flagged, mismatchSeen, everCancelled, apc, aSnapW,
                 adminOps, otherSpent>>

\* tx.amount < total_ngn -> 'mismatch' (logged); else fulfillOrder(default allowed).
SettleDo(a) ==
  /\ spc[a] = "read"
  /\ spc' = [spc EXCEPT ![a] = "done"]
  /\ IF charge[a] < (IF FixSettle THEN oTotal ELSE sTotal[a])
       THEN /\ flagged' = TRUE /\ mismatchSeen' = TRUE
            /\ UNCHANGED status
       ELSE IF status \in SettleAllowed
              THEN /\ status' = "paid"
                   \* FixDup: UPDATE ... RETURNING total_ngn, compared in the same step
                   /\ flagged' = (flagged \/ (FixDup /\ charge[a] > oTotal))
                   /\ UNCHANGED mismatchSeen
              ELSE \* 'already': returns quietly unless FixDup
                   /\ flagged' = (flagged \/ FixDup)
                   /\ UNCHANGED <<status, mismatchSeen>>
  /\ UNCHANGED <<oWallet, oTotal, oRef, balance, pc, snap, useW, amt, deducted, charge,
                 captured, sTotal, everCancelled, apc, aSnapW, adminOps, otherSpent>>

(* ---- Admin: two-stage reject, undo ---------------------------------- *)

AdminFree == apc = "idle" /\ adminOps < MaxAdmin

Reject ==
  /\ AdminFree /\ status = "pending"
  /\ status' = "rejected_pending"
  /\ adminOps' = adminOps + 1
  /\ UNCHANGED <<oWallet, oTotal, oRef, balance, pc, snap, useW, amt, deducted, charge,
                 captured, spc, sTotal, flagged, mismatchSeen, everCancelled, apc,
                 aSnapW, otherSpent>>

\* SELECT id, status, wallet_ngn, ... FROM orders WHERE order_ref = ?
ConfirmRead ==
  /\ AdminFree /\ status = "rejected_pending"
  /\ aSnapW' = oWallet
  /\ apc' = "moving"
  /\ adminOps' = adminOps + 1
  /\ UNCHANGED <<status, oWallet, oTotal, oRef, balance, pc, snap, useW, amt, deducted,
                 charge, captured, spc, sTotal, flagged, mismatchSeen, everCancelled,
                 otherSpent>>

\* UPDATE orders SET status='cancelled' WHERE status='rejected_pending'
\* (FixCancel: also SET wallet_ngn = 0, total_ngn = total_ngn + wallet_ngn,
\*  RETURNING the old wallet_ngn, which is what gets refunded)
ConfirmMove ==
  /\ apc = "moving"
  /\ IF status = "rejected_pending"
       THEN /\ status' = "cancelled" /\ everCancelled' = TRUE
            /\ apc' = "refund"
            /\ IF FixCancel
                 THEN /\ aSnapW' = oWallet /\ oWallet' = 0 /\ oTotal' = oTotal + oWallet
                 ELSE UNCHANGED <<aSnapW, oWallet, oTotal>>
       ELSE /\ apc' = "idle"
            /\ UNCHANGED <<status, everCancelled, aSnapW, oWallet, oTotal>>
  /\ UNCHANGED <<oRef, balance, pc, snap, useW, amt, deducted, charge, captured, spc,
                 sTotal, flagged, mismatchSeen, adminOps, otherSpent>>

\* refundWalletForOrder(order, wallet_ngn as read)
Refund ==
  /\ apc = "refund"
  /\ balance' = balance + aSnapW
  /\ apc' = "idle"
  /\ UNCHANGED <<status, oWallet, oTotal, oRef, pc, snap, useW, amt, deducted, charge,
                 captured, spc, sTotal, flagged, mismatchSeen, everCancelled, aSnapW,
                 adminOps, otherSpent>>

UndoReject ==
  /\ AdminFree /\ status = "rejected_pending"
  /\ status' = "pending"
  /\ adminOps' = adminOps + 1
  /\ UNCHANGED <<oWallet, oTotal, oRef, balance, pc, snap, useW, amt, deducted, charge,
                 captured, spc, sTotal, flagged, mismatchSeen, everCancelled, apc,
                 aSnapW, otherSpent>>

(* ---- The customer spends the wallet on another order ---------------- *)

OtherSpend ==
  /\ OtherOrders /\ otherSpent = 0 /\ balance > 0
  /\ \E x \in 1..balance : balance' = balance - x /\ otherSpent' = x
  /\ UNCHANGED <<status, oWallet, oTotal, oRef, pc, snap, useW, amt, deducted, charge,
                 captured, spc, sTotal, flagged, mismatchSeen, everCancelled, apc,
                 aSnapW, adminOps>>

Next ==
  \/ \E a \in Attempts :
       \/ Start(a) \/ Claim(a) \/ Debit(a) \/ Check(a) \/ InitOk(a) \/ InitFail(a)
       \/ Undo1(a) \/ Undo2(a) \/ Pay(a) \/ SettleRead(a) \/ SettleDo(a)
  \/ Reject \/ ConfirmRead \/ ConfirmMove \/ Refund \/ UndoReject
  \/ OtherSpend

Spec == Init /\ [][Next]_vars

(* ---- Properties ------------------------------------------------------ *)

\* No request is half-way through, and every captured payment has been settled.
Quiescent ==
  /\ \A a \in Attempts : pc[a] \in {"idle", "open", "done"}
  /\ apc = "idle"
  /\ \A a \in captured : spc[a] = "done"

\* P1. An order is only paid if the customer has paid its full price.
NoUnderpaidFulfilment == Quiescent => (status = "paid" => NetPaid >= T)

\* P2. The customer never ends up with money they didn't have (no double refund).
NoMoneyCreated == Quiescent => NetPaid >= 0

\* P3. A cancelled order has cost the customer nothing, or support was alerted.
CancelledIsRefunded == Quiescent => (status = "cancelled" => NetPaid = 0 \/ flagged)

\* P4. Paying more than the order is never silent.
NoSilentOvercharge == Quiescent => (NetPaid > T => flagged)

\* P5. A customer who pays exactly what Paystack shows them, on an order no
\* admin cancelled, never hits "amount does not match".
NoHonestMismatch == mismatchSeen => everCancelled

TypeOK ==
  /\ status \in {"pending", "rejected_pending", "cancelled", "paid"}
  /\ oWallet \in 0..T /\ oTotal \in 0..T
  /\ balance \in 0..(W + 2 * T)
=============================================================================
