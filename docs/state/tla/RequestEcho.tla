---- MODULE RequestEcho ----
EXTENDS Naturals
VARIABLES scope, offset, accepted, acceptedScope, acceptedOffset
vars == <<scope,offset,accepted,acceptedScope,acceptedOffset>>
Init == /\ scope \in {1,2} /\ offset \in {1,2} /\ accepted = FALSE /\ acceptedScope = 0 /\ acceptedOffset = 0
Validate == /\ ~accepted /\ scope = 1 /\ offset = 1 /\ accepted' = TRUE
            /\ acceptedScope' = scope /\ acceptedOffset' = offset /\ UNCHANGED <<scope,offset>>
Next == Validate
ExactScope == accepted => acceptedScope = 1
ExactOffset == accepted => acceptedOffset = 1
====
