---- MODULE OriginEpoch ----
EXTENDS Naturals

CONSTANTS UnsafeEpoch, UnsafeStamp, UnsafeScope, UnsafeReopen, UnsafeSplitCommit

(* A single receiver store and one independently trusted stable origin key.
   Grants model distinct full scopes for one stream, not writer epochs.
   Zero is used only for an absent observation or an unstamped result. *)
Epochs == 1..3
Grants == 1..2
Queue == [epoch : 0..3, grant : Grants, after : 0..1]

VARIABLES claimed, grant, floor, observed, queued, reopened,
          position, savedGrant, records, lastApplied
state == <<claimed, grant, floor, observed, queued, reopened,
           position, savedGrant, records, lastApplied>>

Init ==
    /\ claimed = 1
    /\ grant = 1
    /\ floor = 0
    /\ observed = 0
    /\ queued = <<>>
    /\ reopened = FALSE
    /\ position = 0
    /\ savedGrant = 0
    /\ records = 0
    /\ lastApplied = <<>>

ClaimNext ==
    /\ claimed < 3
    /\ claimed' = claimed + 1
    /\ UNCHANGED <<grant, floor, observed, queued, reopened,
                   position, savedGrant, records, lastApplied>>

ChangeGrant ==
    /\ grant < 2
    /\ grant' = grant + 1
    /\ UNCHANGED <<claimed, floor, observed, queued, reopened,
                   position, savedGrant, records, lastApplied>>

(* Only an independently authenticated and exactly correlated current head can
   raise the floor. Observe never grants permission to reset cached progress. *)
Observe ==
    /\ claimed > floor
    /\ floor' = claimed
    /\ observed' = claimed
    /\ UNCHANGED <<claimed, grant, queued, reopened,
                   position, savedGrant, records, lastApplied>>

Capture(e) ==
    /\ queued = <<>>
    /\ position < 2
    /\ queued' = [epoch |-> e, grant |-> grant, after |-> position]
    /\ UNCHANGED <<claimed, grant, floor, observed, reopened,
                   position, savedGrant, records, lastApplied>>

Discard ==
    /\ queued # <<>>
    /\ queued' = <<>>
    /\ UNCHANGED <<claimed, grant, floor, observed, reopened,
                   position, savedGrant, records, lastApplied>>

Apply ==
    /\ queued # <<>>
    /\ floor > 0
    /\ \/ queued.epoch = floor
       \/ UnsafeEpoch /\ queued.epoch \in 1..floor
       \/ UnsafeStamp /\ queued.epoch = 0
    /\ (UnsafeScope \/ queued.grant = grant)
    /\ queued.after = position
    /\ position = 0 \/ savedGrant = queued.grant
    /\ position' = position + 1
    /\ records' = IF UnsafeSplitCommit THEN records ELSE records + 1
    /\ savedGrant' = queued.grant
    /\ lastApplied' = [frameEpoch |-> queued.epoch, durableFloor |-> observed,
                       frameGrant |-> queued.grant, currentGrant |-> grant,
                       afterReopen |-> reopened,
                       previousEpoch |-> IF position = 0 THEN 0 ELSE lastApplied.frameEpoch]
    /\ queued' = <<>>
    /\ UNCHANGED <<claimed, grant, floor, observed, reopened>>

(* A result may be delayed on another handle; opening a fresh handle must
   consult the same durable floor rather than a fresh per-handle default. *)
Reopen ==
    /\ ~reopened
    /\ reopened' = TRUE
    /\ floor' = IF UnsafeReopen THEN 0 ELSE floor
    /\ UNCHANGED <<claimed, grant, observed, queued,
                   position, savedGrant, records, lastApplied>>

Next ==
    ClaimNext \/ ChangeGrant \/ Observe \/ Capture(claimed) \/ Capture(0)
    \/ Discard \/ Apply \/ Reopen

TypeOK ==
    /\ claimed \in Epochs
    /\ grant \in Grants
    /\ floor \in 0..3
    /\ observed \in 0..3
    /\ queued \in Queue \cup {<<>>}
    /\ reopened \in BOOLEAN
    /\ position \in 0..2
    /\ savedGrant \in 0..2
    /\ records \in 0..2
    /\ lastApplied \in
          [frameEpoch : 0..3, durableFloor : 0..3,
           frameGrant : Grants, currentGrant : Grants,
           afterReopen : BOOLEAN, previousEpoch : 0..3] \cup {<<>>}

DurableOriginFloor == floor = observed
AtomicCheckpoint == records = position
StampedApply == lastApplied # <<>> => lastApplied.frameEpoch > 0
NoStaleEpoch == lastApplied # <<>> =>
    lastApplied.frameEpoch = 0 \/ lastApplied.frameEpoch = lastApplied.durableFloor
ExactGrant == lastApplied # <<>> =>
    lastApplied.frameGrant = lastApplied.currentGrant

(* Negated reachability goals, used only by the lab to demonstrate that
   passing invariants did not prevent an authorized catch-up or reopen. *)
NoApplyAfterReopen == ~(position > 0 /\ lastApplied.afterReopen)
NoCatchupAtHigherEpoch ==
    ~(position = 2 /\ lastApplied.frameEpoch > lastApplied.previousEpoch)

====
