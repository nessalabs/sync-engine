---- MODULE CataloguePage ----
EXTENDS Naturals
VARIABLES cursor, resolved, completed, applied, hadPayload, completionPage
vars == <<cursor,resolved,completed,applied,hadPayload,completionPage>>
Init == /\ cursor = 0 /\ resolved = FALSE /\ completed = 0 /\ applied = FALSE /\ hadPayload = TRUE /\ completionPage = 0
Resolve == /\ cursor < 2 /\ ~resolved /\ resolved' = TRUE /\ UNCHANGED <<cursor,completed,applied,hadPayload,completionPage>>
Apply == /\ cursor < 2 /\ resolved /\ cursor' = cursor + 1 /\ resolved' = FALSE
         /\ applied' = TRUE /\ hadPayload' = resolved
         /\ completed' = IF cursor = 1 THEN 2 ELSE 0
         /\ completionPage' = IF cursor = 1 THEN cursor + 1 ELSE completionPage
Next == Resolve \/ Apply
PayloadBeforeCursor == applied => hadPayload
FinalPageOnly == completed = 2 => completionPage = 2
====
