---- MODULE HistoryFence ----
EXTENDS Naturals
VARIABLES generation, deleted, installed, atGeneration, atDeleted
vars == <<generation,deleted,installed,atGeneration,atDeleted>>
Init == /\ generation = 1 /\ deleted = FALSE /\ installed = FALSE /\ atGeneration = 0 /\ atDeleted = FALSE
Reset == /\ generation = 1 /\ generation' = 2 /\ UNCHANGED <<deleted,installed,atGeneration,atDeleted>>
Delete == /\ ~deleted /\ deleted' = TRUE /\ UNCHANGED <<generation,installed,atGeneration,atDeleted>>
Install == /\ ~installed /\ generation = 1 /\ ~deleted /\ installed' = TRUE
           /\ atGeneration' = generation /\ atDeleted' = deleted /\ UNCHANGED <<generation,deleted>>
Next == Reset \/ Delete \/ Install
CurrentGeneration == installed => atGeneration = 1
NoDeletedInstall == installed => ~atDeleted
====
