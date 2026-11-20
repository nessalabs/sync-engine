---- MODULE CatalogueCollection ----
EXTENDS Naturals, FiniteSets
CONSTANTS Ids, Cold, MaxGeneration, UnsafeAtomic, UnsafeCursor, UnsafeLower,
          UnsafeGeneration, UnsafeAbsence, UnsafeLegacy, UnsafeFinish
VARIABLE s
vars == <<s>>
NoMsg == [kind |-> "None", id |-> 0, deleted |-> FALSE, g |-> 0,
          c |-> 0, target |-> 0, head |-> 0, floor |-> 0]
NoOffer == [valid |-> FALSE, head |-> 0, old |-> 0, want |-> 0]
Max(a,b) == IF a>b THEN a ELSE b
Init == s = [head |-> 2, dead |-> [i \in Ids |-> 0], served |-> Ids,
    floor |-> 0, floorMax |-> 0, seen |-> 0, live |-> IF Cold THEN {} ELSE Ids,
    fences |-> {}, proof |-> {}, pending |-> {}, todo |-> {},
    phase |-> "Idle", g |-> 0, c |-> IF Cold THEN 0 ELSE 2,
    target |-> IF Cold THEN 0 ELSE 2, required |-> IF Cold THEN {} ELSE Ids,
    msg |-> NoMsg, offer |-> NoOffer, auth |-> TRUE,
    badGen |-> FALSE, badFloor |-> FALSE,
    badEffect |-> FALSE, lastLegacy |-> 0]
Delete == \E i \in Ids: /\ s.dead[i]=0
    /\ s'=[s EXCEPT !.dead[i]=s.head+1, !.head=@+1]
Offer == /\ ~s.offer.valid /\ \E h \in s.floor..s.head: s'=[s EXCEPT !.offer=[valid |-> TRUE,head |-> s.head,old |-> s.floor,want |-> h]]
OfferValid == s.offer.valid /\ s.offer.head=s.head /\ s.offer.old=s.floor
Published(h,offerAfter) == [s EXCEPT
    !.served=@ \ {i \in Ids:s.dead[i]>0 /\ s.dead[i]<h},
    !.floor=IF UnsafeAtomic THEN s.floor ELSE h,
    !.floorMax=Max(@,IF UnsafeAtomic THEN s.floor ELSE h),!.offer=offerAfter]
Collect == /\ OfferValid /\ s'=Published(s.offer.want,NoOffer)
ConcurrentCollect == /\ s.offer.valid
    /\ OfferValid /\ s.floor<s.head /\ s.offer.want<s.head
    /\ s'=Published(s.head,s.offer)
StaleOffer == /\ s.offer.valid /\ ~OfferValid /\ s'=[s EXCEPT !.offer=NoOffer]
Authorize == s'=[s EXCEPT !.auth=~@]
Observe == /\ s.auth /\ s.floor>=s.seen /\ s'=[s EXCEPT !.seen=s.floor]
HeadFetch == /\ s.auth /\ s.msg.kind="None"
    /\ s'=[s EXCEPT !.msg=[kind |-> "Head",id |-> 0,deleted |-> FALSE,g |-> s.g,
                            c |-> s.c,target |-> s.target,head |-> s.head,floor |-> s.floor]]
HeadDeliver == /\ s.auth /\ s.msg.kind="Head" /\ s.msg.head>=s.c
    /\ (UnsafeLower \/ s.msg.floor>=s.seen)
    /\ s'=[s EXCEPT !.seen=s.msg.floor,!.badFloor=@ \/ s.msg.floor<s.seen,!.msg=NoMsg]
BeginIncremental == /\ s.auth /\ s.phase="Idle" /\ s.head>s.c
    /\ (IF UnsafeCursor THEN s.head ELSE s.c)>=s.floor
    /\ s'=[s EXCEPT !.target=s.head,!.todo=Ids,!.phase="Incr",!.seen=s.floor]
BeginReset == /\ s.auth /\ s.g<MaxGeneration
    /\ s'=[s EXCEPT !.g=@+1,!.target=s.head,!.c=0,!.seen=s.floor,
            !.required=s.pending \cup s.live \cup s.fences,!.pending=@ \cup s.live,
            !.live={},!.todo=Ids,!.phase="Enumerate"]
Point == IF s.phase="Incr" THEN (IF UnsafeCursor THEN s.target ELSE s.c) ELSE s.target
ActualPoint == IF s.phase="Incr" THEN s.c ELSE s.target
Active == s.phase \in {"Incr","Enumerate","Reconcile"}
Needed == IF s.phase="Reconcile" THEN s.pending ELSE s.todo
Eligible == IF s.phase="Reconcile" THEN s.pending ELSE s.todo \cap s.served
Fetch == /\ s.auth /\ Active /\ s.msg.kind="None" /\ Point>=s.floor
    /\ \E i \in Eligible: s'=[s EXCEPT !.msg=[kind |-> s.phase,id |-> i,deleted |-> s.dead[i]>0,
                          g |-> s.g,c |-> s.c,target |-> s.target,head |-> s.head,floor |-> s.floor]]
DataDeliver == /\ s.auth /\ Active /\ s.msg.kind=s.phase /\ s.msg.id \in Needed
    /\ (UnsafeGeneration \/ s.msg.g=s.g) /\ s.msg.c=s.c /\ s.msg.target=s.target
    /\ s.msg.head>=s.target /\ (UnsafeLower \/ s.msg.floor>=s.seen)
    /\ Point>=Max(s.seen,s.msg.floor) /\ (s.msg.deleted \/ s.msg.id \notin s.fences)
    /\ s'=[s EXCEPT
        !.live=IF s.msg.deleted THEN @ \ {s.msg.id} ELSE @ \cup {s.msg.id},
        !.fences=IF s.msg.deleted THEN @ \cup {s.msg.id} ELSE @,
        !.proof=IF s.msg.deleted THEN @ \cup {s.msg.id} ELSE @,
        !.todo=IF s.phase="Reconcile" THEN @ ELSE @ \ {s.msg.id},
        !.pending=@ \ {s.msg.id},!.seen=s.msg.floor,
        !.badGen=@ \/ s.msg.g#s.g,!.badFloor=@ \/ s.msg.floor<s.seen,
        !.badEffect=@ \/ ActualPoint<Max(s.seen,s.msg.floor),!.msg=NoMsg]
Skip == /\ s.auth /\ s.phase \in {"Incr","Enumerate"}
    /\ Point>=Max(s.floor,s.seen) /\ \E i \in s.todo:
       /\ (i \notin s.served \/ (s.phase="Incr" /\ (IF s.dead[i]>0 THEN s.dead[i] ELSE i)<=s.c))
       /\ s'=[s EXCEPT !.todo=@ \ {i},!.seen=s.floor,
                      !.badEffect=@ \/ ActualPoint<Max(s.floor,s.seen)]
Refuse == /\ s.auth /\ (Active \/ s.phase \in {"Finish","Check"})
    /\ (IF s.phase \in {"Finish","Check"} THEN s.c ELSE Point)<s.floor
    /\ s.floor>=s.seen /\ s'=[s EXCEPT !.seen=s.floor]
EndManifest == /\ s.auth /\ s.phase \in {"Incr","Enumerate"} /\ s.todo={}
    /\ Point>=s.seen
    /\ s'=[s EXCEPT !.phase=IF s.phase="Incr" THEN "Check" ELSE "Reconcile",
            !.c=IF s.phase="Incr" THEN s.target ELSE @,
            !.badEffect=@ \/ ActualPoint<s.seen]
EndCandidates == /\ s.auth /\ s.phase="Reconcile" /\ s.pending={} /\ s.target>=s.seen
    /\ s'=[s EXCEPT !.phase="Finish",!.badEffect=@ \/ s.target<s.seen]
Publish == /\ s.auth /\ s.phase="Finish" /\ s.c#s.target /\ s.target>=s.seen
    /\ s'=[s EXCEPT !.c=s.target,!.badEffect=@ \/ s.target<s.seen]
Finish == /\ s.auth /\ s.phase \in {"Finish","Check"} /\ s.c=s.target
    /\ s.msg.kind="Head" /\ s.msg.g=s.g /\ s.msg.c=s.c
    /\ s.msg.target=s.target /\ s.msg.head>=s.c /\ s.msg.floor>=s.seen
    /\ (UnsafeFinish \/ s.c>=Max(s.seen,s.msg.floor))
    /\ s'=[s EXCEPT !.seen=s.msg.floor,!.phase="Idle",!.msg=NoMsg,
                    !.badEffect=@ \/ s.c<Max(s.seen,s.msg.floor)]
Unavailable == /\ s.auth /\ s.phase="Reconcile" /\ s.pending#{}
    /\ \E i \in s.pending:s'=[s EXCEPT !.fences=IF UnsafeAbsence THEN @ \cup {i} ELSE @]
LegacyRead == /\ s.auth /\ (UnsafeLegacy \/ s.floor=0)
    /\ s'=[s EXCEPT !.lastLegacy=s.floor]
Drop == /\ s.msg.kind#"None" /\ s'=[s EXCEPT !.msg=NoMsg]
Crash == s'=[s EXCEPT !.msg=NoMsg,!.offer=NoOffer]
Next == Delete \/ Offer \/ Collect \/ ConcurrentCollect \/ StaleOffer \/ Authorize \/ Observe \/ HeadFetch \/ HeadDeliver
    \/ BeginIncremental \/ BeginReset \/ Fetch \/ DataDeliver \/ Skip \/ Refuse \/ EndManifest
    \/ EndCandidates \/ Publish \/ Finish \/ Unavailable \/ LegacyRead \/ Drop \/ Crash
TypeOK == /\ s.head \in 2..4 /\ s.dead \in [Ids -> 0..4] /\ s.floor \in 0..s.head
    /\ s.seen \in 0..s.head /\ s.c \in 0..s.head /\ s.target \in 0..s.head
    /\ s.live \subseteq Ids /\ s.fences \subseteq Ids /\ s.proof \subseteq Ids
    /\ s.pending \subseteq Ids /\ s.todo \subseteq Ids /\ s.required \subseteq Ids
    /\ s.phase \in {"Idle","Incr","Enumerate","Reconcile","Finish","Check"}
    /\ s.g \in 0..MaxGeneration /\ s.auth \in BOOLEAN
    /\ s.floorMax \in 0..s.head /\ s.lastLegacy \in 0..4
    /\ s.badGen \in BOOLEAN /\ s.badFloor \in BOOLEAN /\ s.badEffect \in BOOLEAN
    /\ s.msg.kind \in {"None","Head","Incr","Enumerate","Reconcile"}
    /\ s.msg.id \in Ids \cup {0} /\ s.msg.deleted \in BOOLEAN
    /\ s.msg.g \in 0..MaxGeneration /\ s.msg.c \in 0..4
    /\ s.msg.target \in 0..4 /\ s.msg.head \in 0..4 /\ s.msg.floor \in 0..4
    /\ s.offer.valid \in BOOLEAN /\ s.offer.head \in 0..4
    /\ s.offer.old \in 0..4 /\ s.offer.want \in 0..4
SourceMonotone == s.floor=s.floorMax
MarkersCovered == \A i \in Ids \ s.served:s.dead[i]>0 /\ s.dead[i]<s.floor
FenceProof == s.fences=s.proof /\ \A i \in s.fences:s.dead[i]>0
NoResurrection == s.live \cap s.fences={}
CoveredNotLive == \A i \in Ids:(s.dead[i]>0 /\ s.dead[i]<=s.c) => i \notin s.live
RequiredFenced == (s.c=s.target /\ s.c>0 /\ s.g>0) =>
    \A i \in s.required:(s.dead[i]>0 /\ s.dead[i]<=s.target) => i \in s.fences
NoEarlyCompletion == /\ (s.phase \in {"Enumerate","Reconcile"} => s.c=0)
    /\ ((s.c=s.target /\ s.c>0) => (s.todo={} /\ s.pending={}))
NoStaleApply == ~s.badGen
NoLowerHorizon == ~s.badFloor
NoExpiredEffects == ~s.badEffect
LegacyBeforeData == s.lastLegacy=0
====
