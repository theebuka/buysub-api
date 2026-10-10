--------------------------- MODULE DiscountUsage ---------------------------
(***************************************************************************)
(* One promo code, several customers placing orders with it.              *)
(*                                                                         *)
(* FixReserve = FALSE: the code before migration 24.                       *)
(*   Check   prepareOrder: times_used < max_uses, and no discount_usages   *)
(*           row for this customer                                          *)
(*   Insert  the order is written                                          *)
(*   Pay     fulfillOrder: insert discount_usages (unique per customer),   *)
(*           times_used + 1 only when that row is new                      *)
(*                                                                         *)
(* FixReserve = TRUE: migration 24 + the API that uses it.                 *)
(*   Check   prepareOrder: times_used < max_uses, and the customer's row   *)
(*           (if any) isn't held by a paid order                           *)
(*   Insert  order written, then reserve_discount_use: the customer's      *)
(*           row moves to this order if held by an unpaid one; else a new  *)
(*           row if times_used + rows held by unpaid orders < max_uses     *)
(*   Init    /v2/pay/init: reserve_discount_use again before Paystack      *)
(*   Pay     record_discount_use: count the use; 'duplicate' if the row is *)
(*           held by another paid order, 'over_limit' if there was no row  *)
(*           and the code was full. Both are logged (the payment can't be  *)
(*           refused).                                                     *)
(*   Cancel  cancel_rejected_order deletes the order's row                 *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Customers, Orders, MaxUses, FixReserve, NoOrder

VARIABLES st, cust, ok, usage, holder, timesUsed, dup, over, wrongRefusal
vars == <<st, cust, ok, usage, holder, timesUsed, dup, over, wrongRefusal>>

Init ==
  /\ st = [o \in Orders |-> "none"]
  /\ cust = [o \in Orders |-> CHOOSE c \in Customers : TRUE]
  /\ ok = [o \in Orders |-> FALSE]
  /\ usage = {}                                  \* old code: customers with a row
  /\ holder = [c \in Customers |-> NoOrder]      \* fix: the order holding c's row
  /\ timesUsed = 0
  /\ dup = {} /\ over = {}                        \* orders logged discount_reused / over_limit
  /\ wrongRefusal = FALSE

HolderPaid(c) == holder[c] # NoOrder /\ st[holder[c]] = "paid"
HeldBy(S) == Cardinality({c \in S : holder[c] # NoOrder /\ st[holder[c]] # "paid"})
Held == HeldBy(Customers)

\* A refusal is justified when c's use went to a paid order, or other
\* customers' paid and held uses fill max_uses.
Justified(c) == HolderPaid(c) \/ timesUsed + HeldBy(Customers \ {c}) >= MaxUses

\* reserve_discount_use for order o of customer c: "ok" / "used" / "exhausted"
ReserveResult(o, c) ==
  IF holder[c] = o THEN "ok"
  ELSE IF holder[c] # NoOrder THEN (IF st[holder[c]] = "paid" THEN "used" ELSE "ok")
  ELSE IF timesUsed + Held >= MaxUses THEN "exhausted" ELSE "ok"

Reserve(o, c, next) ==
  IF ReserveResult(o, c) = "ok"
    THEN /\ holder' = [holder EXCEPT ![c] = o]
         /\ st' = [st EXCEPT ![o] = next]
         /\ UNCHANGED wrongRefusal
    ELSE /\ st' = [st EXCEPT ![o] = "refused"]
         /\ wrongRefusal' = (wrongRefusal \/ ~Justified(c))
         /\ UNCHANGED holder

Check(o) ==
  /\ st[o] = "none"
  /\ \E c \in Customers :
       /\ cust' = [cust EXCEPT ![o] = c]
       /\ ok' = [ok EXCEPT ![o] =
             IF FixReserve THEN timesUsed < MaxUses /\ ~HolderPaid(c)
             ELSE timesUsed < MaxUses /\ c \notin usage]
  /\ st' = [st EXCEPT ![o] = "checked"]
  /\ UNCHANGED <<usage, holder, timesUsed, dup, over, wrongRefusal>>

Insert(o) ==
  /\ st[o] = "checked"
  /\ IF ~ok[o]
       THEN /\ st' = [st EXCEPT ![o] = "refused"]
            /\ wrongRefusal' = (wrongRefusal \/ (FixReserve /\ ~Justified(cust[o])))
            /\ UNCHANGED holder
       ELSE IF FixReserve THEN Reserve(o, cust[o], "pending")
       ELSE /\ st' = [st EXCEPT ![o] = "pending"]
            /\ UNCHANGED <<holder, wrongRefusal>>
  /\ UNCHANGED <<cust, ok, usage, timesUsed, dup, over>>

\* /v2/pay/init: the Paystack page opens
StartPay(o) ==
  /\ st[o] = "pending"
  /\ IF FixReserve THEN Reserve(o, cust[o], "open")
     ELSE /\ st' = [st EXCEPT ![o] = "open"] /\ UNCHANGED <<holder, wrongRefusal>>
  /\ UNCHANGED <<cust, ok, usage, timesUsed, dup, over>>

Pay(o) ==
  /\ st[o] = "open"
  /\ st' = [st EXCEPT ![o] = "paid"]
  /\ LET c == cust[o] IN
     IF ~FixReserve
       THEN /\ IF c \notin usage
                 THEN usage' = usage \cup {c} /\ timesUsed' = timesUsed + 1
                 ELSE UNCHANGED <<usage, timesUsed>>
            /\ UNCHANGED <<holder, dup, over>>
     ELSE IF holder[c] = o
       THEN timesUsed' = timesUsed + 1 /\ UNCHANGED <<holder, dup, over, usage>>
     ELSE IF holder[c] # NoOrder
       THEN IF st[holder[c]] = "paid"
              THEN dup' = dup \cup {o} /\ UNCHANGED <<holder, timesUsed, over, usage>>
              ELSE /\ holder' = [holder EXCEPT ![c] = o]
                   /\ timesUsed' = timesUsed + 1
                   /\ UNCHANGED <<dup, over, usage>>
     ELSE /\ holder' = [holder EXCEPT ![c] = o]
          /\ timesUsed' = timesUsed + 1
          /\ over' = IF timesUsed + Held >= MaxUses THEN over \cup {o} ELSE over
          /\ UNCHANGED <<dup, usage>>
  /\ UNCHANGED <<cust, ok, wrongRefusal>>

Cancel(o) ==
  /\ st[o] \in {"pending", "open"}
  /\ st' = [st EXCEPT ![o] = "cancelled"]
  /\ holder' = IF FixReserve /\ holder[cust[o]] = o
                 THEN [holder EXCEPT ![cust[o]] = NoOrder] ELSE holder
  /\ UNCHANGED <<cust, ok, usage, timesUsed, dup, over, wrongRefusal>>

Next == \E o \in Orders : Check(o) \/ Insert(o) \/ StartPay(o) \/ Pay(o) \/ Cancel(o)
Spec == Init /\ [][Next]_vars

Counted == {o \in Orders : st[o] = "paid"} \ (dup \cup over)

\* One use per customer, apart from payments that were logged.
OneUsePerCustomer == \A c \in Customers : Cardinality({o \in Counted : cust[o] = c}) <= 1

\* max_uses is never exceeded, apart from payments that were logged.
WithinMaxUses == Cardinality(Counted) <= MaxUses

\* A customer is only refused when their use was spent, or others fill max_uses.
\* (An abandoned unpaid order doesn't lock its customer out.)
NoWrongRefusal == ~wrongRefusal

\* times_used counts every paid use except the logged duplicates.
TimesUsedAccurate ==
  timesUsed = Cardinality({o \in Orders : st[o] = "paid"} \ dup) \/ ~FixReserve

\* Expected to be violated with FixReserve: shows the logged paths are reachable,
\* so the properties above don't hold vacuously.
NothingLogged == dup = {} /\ over = {}
=============================================================================
