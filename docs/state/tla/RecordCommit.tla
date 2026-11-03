---- MODULE RecordCommit ----
EXTENDS Naturals
VARIABLES stored, checkpoint, local, committed, expectedAtCommit, priorAtCommit
vars == <<stored,checkpoint,local,committed,expectedAtCommit,priorAtCommit>>
Init == /\ stored = 0 /\ checkpoint = 0 /\ local = 0 /\ committed = FALSE /\ expectedAtCommit = 0 /\ priorAtCommit = 0
Apply == /\ checkpoint < 2 /\ local = checkpoint
         /\ stored' = stored + 1 /\ checkpoint' = checkpoint + 1
         /\ committed' = TRUE /\ expectedAtCommit' = local /\ priorAtCommit' = checkpoint
         /\ UNCHANGED local
Reload == /\ local' = checkpoint /\ UNCHANGED <<stored,checkpoint,committed,expectedAtCommit,priorAtCommit>>
Next == Apply \/ Reload
AtomicProgress == checkpoint = stored
ExpectedPosition == committed => expectedAtCommit = priorAtCommit
====
