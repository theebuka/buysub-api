--------------------------- MODULE DiscountUsage ---------------------------
(***************************************************************************)
(* One promo code, several customers placing orders with it.              *)
(*                                                                         *)
(*   prepareOrder (index.ts)  Check: validateDiscountGuardChain step 5     *)
(*                            (times_used < max_uses) and the              *)
(*                            discount_usages lookup for this customer,    *)
(*                            then Insert: the order row is written        *)
(*   fulfillOrder             Pay: insert discount_usages (unique on       *)
(*                            discount_id + customer_id); increment        *)
(*                            times_used only when that row is new         *)
(*   admin reject + confirm   Cancel                                       *)
(*                                                                         *)
(* FixReserve: the order insert claims the use atomically (a reservation   *)
(* row, plus times_used < max_uses in the same statement); cancelling      *)
(* releases it.                                                            *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Customers, Orders, MaxUses, FixReserve

VARIABLES st, cust, ok, usage, timesUsed, reserved, held
vars == <<st, cust, ok, usage, timesUsed, reserved, held>>

Init ==
  /\ st = [o \in Orders |-> "none"]
  /\ cust = [o \in Orders |-> CHOOSE c \in Customers : TRUE]
  /\ ok = [o \in Orders |-> FALSE]
  /\ usage = {}            \* customers with a discount_usages row
  /\ timesUsed = 0
  /\ reserved = {}         \* FixReserve: customers holding a reservation
  /\ held = 0              \* FixReserve: uses held by pending orders

Check(o) ==
  /\ st[o] = "none"
  /\ \E c \in Customers :
       /\ cust' = [cust EXCEPT ![o] = c]
       /\ ok' = [ok EXCEPT ![o] = (timesUsed < MaxUses /\ c \notin usage)]
  /\ st' = [st EXCEPT ![o] = "checked"]
  /\ UNCHANGED <<usage, timesUsed, reserved, held>>

Insert(o) ==
  /\ st[o] = "checked"
  /\ IF FixReserve
       THEN IF ok[o] /\ cust[o] \notin reserved /\ timesUsed + held < MaxUses
              THEN /\ st' = [st EXCEPT ![o] = "pending"]
                   /\ reserved' = reserved \cup {cust[o]}
                   /\ held' = held + 1
              ELSE /\ st' = [st EXCEPT ![o] = "refused"]
                   /\ UNCHANGED <<reserved, held>>
       ELSE /\ st' = [st EXCEPT ![o] = IF ok[o] THEN "pending" ELSE "refused"]
            /\ UNCHANGED <<reserved, held>>
  /\ UNCHANGED <<cust, ok, usage, timesUsed>>

Pay(o) ==
  /\ st[o] = "pending"
  /\ st' = [st EXCEPT ![o] = "paid"]
  /\ IF cust[o] \notin usage
       THEN usage' = usage \cup {cust[o]} /\ timesUsed' = timesUsed + 1
       ELSE UNCHANGED <<usage, timesUsed>>
  /\ held' = IF FixReserve THEN held - 1 ELSE held
  /\ UNCHANGED <<cust, ok, reserved>>

Cancel(o) ==
  /\ st[o] = "pending"
  /\ st' = [st EXCEPT ![o] = "cancelled"]
  /\ reserved' = IF FixReserve THEN reserved \ {cust[o]} ELSE reserved
  /\ held' = IF FixReserve THEN held - 1 ELSE held
  /\ UNCHANGED <<cust, ok, usage, timesUsed>>

Next == \E o \in Orders : Check(o) \/ Insert(o) \/ Pay(o) \/ Cancel(o)
Spec == Init /\ [][Next]_vars

PaidWith(c) == {o \in Orders : st[o] = "paid" /\ cust[o] = c}

\* "One use per customer" (discount_usages is unique on discount_id + customer_id).
OneUsePerCustomer == \A c \in Customers : Cardinality(PaidWith(c)) <= 1

\* max_uses is never exceeded.
WithinMaxUses == Cardinality({o \in Orders : st[o] = "paid"}) <= MaxUses
=============================================================================
