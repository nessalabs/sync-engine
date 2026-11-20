---- MODULE CatalogueReset ----
EXTENDS Naturals, FiniteSets
CONSTANTS Ids, Prune, UnsafeGeneration, UnsafeCompletion, UnsafeAbsence, UnsafeCancel
VARIABLES head, deadRev, served, live, fences, proved, required, pending, todo, phase, g, epoch, binding,
          h, completed, msg, badApply, crashes, failure, checked
vars == <<head,deadRev,served,live,fences,proved,required,pending,todo,phase,g,epoch,binding,h,completed,msg,badApply,crashes,failure,checked>>
NoMessage == [id |-> 0, deleted |-> FALSE, generation |-> 0, boundEpoch |-> 0, kind |-> "None"]
Init == /\ head = Cardinality(Ids) /\ deadRev = [i \in Ids |-> 0]
        /\ served = Ids /\ live = Ids /\ fences = {} /\ proved = {} /\ required = {} /\ pending = {} /\ todo = {}
        /\ phase = "Idle" /\ g = 0 /\ epoch = 0 /\ binding = 0
        /\ h = head /\ completed = head /\ msg = NoMessage /\ badApply = FALSE
        /\ crashes = 0 /\ failure = 0 /\ checked = head
Delete == \E i \in Ids: /\ deadRev[i] = 0 /\ deadRev' = [deadRev EXCEPT ![i] = head+1]
                            /\ head' = head+1
                            /\ UNCHANGED <<served,live,fences,proved,required,pending,todo,phase,g,epoch,binding,h,completed,msg,badApply,crashes,failure,checked>>
Collect == \E i \in served: /\ deadRev[i] > 0 /\ served' = served \ {i}
                              /\ UNCHANGED <<head,deadRev,live,fences,proved,required,pending,todo,phase,g,epoch,binding,h,completed,msg,badApply,crashes,failure,checked>>
Begin == /\ g < 3 /\ binding = epoch /\ g' = g+1 /\ h' = head /\ completed' = 0
         /\ required' = pending \cup live \cup fences /\ pending' = pending \cup live /\ live' = {} /\ todo' = Ids /\ phase' = "Enumerate"
         /\ UNCHANGED <<head,deadRev,served,fences,proved,epoch,binding,msg,badApply,crashes,failure,checked>>
Fetch == /\ msg.kind = "None" /\ epoch = binding
         /\ \E i \in (IF phase="Enumerate" THEN todo \cap served ELSE IF phase="Reconcile" THEN pending ELSE {}):
            /\ msg' = [id |-> i, deleted |-> deadRev[i]>0, generation |-> g, boundEpoch |-> binding, kind |-> phase]
            /\ UNCHANGED <<head,deadRev,served,live,fences,proved,required,pending,todo,phase,g,epoch,binding,h,completed,badApply,crashes,failure,checked>>
Eligible == /\ msg.kind # "None" /\ epoch = binding /\ msg.kind = phase
            /\ (UnsafeGeneration \/ (msg.generation=g /\ msg.boundEpoch=binding))
            /\ msg.id \in (IF phase="Enumerate" THEN todo ELSE IF phase="Reconcile" THEN pending ELSE {})
Deliver == /\ Eligible
           /\ live' = IF msg.deleted THEN live \ {msg.id} ELSE IF msg.id \in fences THEN live ELSE live \cup {msg.id}
           /\ fences' = IF msg.deleted THEN fences \cup {msg.id} ELSE fences
           /\ proved' = IF msg.deleted THEN proved \cup {msg.id} ELSE proved
           /\ todo' = IF phase="Enumerate" THEN todo \ {msg.id} ELSE todo
           /\ pending' = IF phase="Reconcile" \/ Prune THEN pending \ {msg.id} ELSE pending
           /\ badApply' = (badApply \/ msg.generation#g \/ msg.boundEpoch#binding)
           /\ msg' = NoMessage
           /\ UNCHANGED <<head,deadRev,served,required,phase,g,epoch,binding,h,completed,crashes,failure,checked>>
Drop == /\ msg.kind # "None" /\ ~Eligible /\ msg'=NoMessage
        /\ UNCHANGED <<head,deadRev,served,live,fences,proved,required,pending,todo,phase,g,epoch,binding,h,completed,badApply,crashes,failure,checked>>
Omit == /\ phase="Enumerate" /\ epoch=binding
        /\ \E i \in todo \ served: /\ todo'=todo \ {i}
           /\ UNCHANGED <<head,deadRev,served,live,fences,proved,required,pending,phase,g,epoch,binding,h,completed,msg,badApply,crashes,failure,checked>>
EndManifest == /\ phase="Enumerate" /\ todo={} /\ epoch=binding /\ phase'="Reconcile"
               /\ completed'=IF UnsafeCompletion THEN h ELSE completed
               /\ UNCHANGED <<head,deadRev,served,live,fences,proved,required,pending,todo,g,epoch,binding,h,msg,badApply,crashes,failure,checked>>
EndCandidates == /\ phase="Reconcile" /\ pending={} /\ epoch=binding /\ phase'="Finish"
                 /\ UNCHANGED <<head,deadRev,served,live,fences,proved,required,pending,todo,g,epoch,binding,h,completed,msg,badApply,crashes,failure,checked>>
Publish == /\ phase="Finish" /\ completed=0 /\ epoch=binding /\ completed'=h
           /\ UNCHANGED <<head,deadRev,served,live,fences,proved,required,pending,todo,phase,g,epoch,binding,h,msg,badApply,crashes,failure,checked>>
Check == /\ phase="Finish" /\ completed=h /\ epoch=binding /\ checked'=head /\ phase'="Idle"
         /\ UNCHANGED <<head,deadRev,served,live,fences,proved,required,pending,todo,g,epoch,binding,h,completed,msg,badApply,crashes,failure>>
Crash == /\ crashes=0 /\ crashes'=1 /\ msg'=NoMessage
         /\ UNCHANGED <<head,deadRev,served,live,fences,proved,required,pending,todo,phase,g,epoch,binding,h,completed,badApply,failure,checked>>
Narrow == /\ epoch=0 /\ epoch'=1
          /\ UNCHANGED <<head,deadRev,served,live,fences,proved,required,pending,todo,phase,g,binding,h,completed,msg,badApply,crashes,failure,checked>>
Unavailable == /\ phase="Reconcile" /\ failure=0 /\ epoch=binding
               /\ \E i \in pending: /\ failure'=1
                  /\ fences'=(IF UnsafeAbsence THEN fences \cup {i} ELSE fences)
                  /\ UNCHANGED <<head,deadRev,served,live,proved,required,pending,todo,phase,g,epoch,binding,h,completed,msg,badApply,crashes,checked>>
CancelScope == /\ epoch#binding /\ g<3 /\ binding'=epoch /\ g'=g+1
               /\ pending'=(IF UnsafeCancel THEN pending ELSE {}) /\ todo'={} /\ live'={} /\ required'={}
               /\ completed'=0 /\ phase'="Idle"
               /\ UNCHANGED <<head,deadRev,served,fences,proved,epoch,h,msg,badApply,crashes,failure,checked>>
Next == Delete \/ Collect \/ Begin \/ Fetch \/ Deliver \/ Drop \/ Omit \/ EndManifest \/ EndCandidates \/ Publish \/ Check \/ Crash \/ Narrow \/ Unavailable \/ CancelScope
TypeOK == /\ head \in 2..4 /\ h \in 2..4 /\ completed \in 0..4
          /\ deadRev \in [Ids -> 0..4] /\ served \subseteq Ids /\ live \subseteq Ids
          /\ fences \subseteq Ids /\ proved \subseteq Ids /\ required \subseteq Ids /\ pending \subseteq Ids /\ todo \subseteq Ids
          /\ phase \in {"Idle","Enumerate","Reconcile","Finish"} /\ g \in 0..3
          /\ epoch \in 0..1 /\ binding \in 0..1 /\ badApply \in BOOLEAN
          /\ crashes \in 0..1 /\ failure \in 0..1 /\ checked \in 2..4
          /\ msg \in [id: Ids \cup {0}, deleted: BOOLEAN, generation: 0..3, boundEpoch: 0..1, kind: {"None","Enumerate","Reconcile"}]
NoStaleApply == ~badApply
NoEarlyCompletion == completed=h => (phase \in {"Idle","Finish"} /\ pending={} /\ todo={})
FenceProof == fences = proved /\ fences \subseteq {i \in Ids: deadRev[i]>0}
NoResurrection == live \cap fences = {}
DeletedByBoundary == (g>0 /\ completed=h) => \A i \in required: (deadRev[i]>0 /\ deadRev[i]<=h) => (i \in fences /\ i \notin live)
IdleJournal == phase="Idle" => (pending={} /\ todo={} /\ (completed=0 => live={}))
FreshFinish == (g>0 /\ phase="Idle" /\ completed=h) => checked>=h
====
