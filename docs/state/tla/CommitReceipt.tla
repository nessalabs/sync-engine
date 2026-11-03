---- MODULE CommitReceipt ----
EXTENDS Naturals
VARIABLES returned, accepted, captured
vars == <<returned,accepted,captured>>
Init == /\ returned \in {1,2} /\ accepted = FALSE /\ captured = 0
Accept == /\ ~accepted /\ returned = 1 /\ accepted' = TRUE /\ captured' = returned /\ UNCHANGED returned
Next == Accept
ExactReplacement == accepted => captured = 1
====
