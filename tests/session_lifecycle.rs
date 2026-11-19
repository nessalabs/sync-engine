#![cfg(feature = "transport")]
use nessa_sync::replication::{
    application::MAX_RECORD_BYTES,
    domain::{Id, Scope},
    infrastructure::session::{
        BindingMetadata, ControlFrame, Correlation, LifecycleError, RequestIdentity, ScopeIdentity,
        SessionLifecycle, SessionLimits, SessionPhase,
    },
};

fn scope(stream: &str, epoch: &str) -> Scope {
    let id = |text| Id::new(text).unwrap();
    Scope::new(
        id("receiver"),
        id("origin"),
        id(stream),
        id("incarnation"),
        id("schema"),
        id(epoch),
    )
}
fn limits(bindings: u16) -> SessionLimits {
    SessionLimits {
        control_bytes: 8192,
        bindings,
    }
}
fn welcome(bindings: u16) -> ControlFrame {
    ControlFrame::Welcome {
        session: 7,
        limits: limits(bindings),
        record_bytes: MAX_RECORD_BYTES as u32,
    }
}
fn ready(bindings: u16, pending: u16) -> SessionLifecycle {
    let mut owner = SessionLifecycle::new(limits(bindings), pending).unwrap();
    let key = owner.connection_id();
    assert_eq!(
        owner
            .accept_welcome(&key, &welcome(bindings))
            .unwrap()
            .get(),
        7
    );
    owner
}
fn reply(request: &RequestIdentity, handle: u32, scope: Scope) -> ControlFrame {
    ControlFrame::Bound {
        correlation: request.correlation(),
        binding: BindingMetadata {
            handle,
            generation: 1,
            scope,
            head: 123,
        },
    }
}
fn bind(
    owner: &mut SessionLifecycle,
    handle: u32,
    scope: Scope,
) -> (RequestIdentity, ScopeIdentity) {
    let (request, frame) = owner.begin_bind(scope.clone()).unwrap();
    assert_eq!(
        frame,
        ControlFrame::Bind {
            correlation: request.correlation(),
            scope: scope.clone()
        }
    );
    let key = owner.connection_id();
    let identity = owner
        .accept_bound(&key, &reply(&request, handle, scope))
        .unwrap();
    (request, identity)
}

#[test]
fn constructor_and_connecting_refuse_unadmitted_work() {
    for (offer, pending) in [
        (limits(0), 1),
        (limits(33), 1),
        (limits(2), 0),
        (limits(2), 3),
        (
            SessionLimits {
                control_bytes: 8193,
                bindings: 2,
            },
            1,
        ),
    ] {
        assert!(matches!(
            SessionLifecycle::new(offer, pending),
            Err(LifecycleError::InvalidLimits)
        ));
    }
    let mut owner = SessionLifecycle::new(limits(2), 1).unwrap();
    assert_eq!(owner.phase(), SessionPhase::Connecting);
    assert_eq!(owner.hello().unwrap(), ControlFrame::Hello(limits(2)));
    assert_eq!(
        owner.begin_bind(scope("a", "epoch")),
        Err(LifecycleError::NotReady)
    );
    assert_eq!(owner.retained_slots(), 0);
    owner.close();
    owner.close();
    assert_eq!(owner.hello(), Err(LifecycleError::Closed));
    let key = owner.connection_id();
    assert_eq!(
        owner.accept_welcome(&key, &welcome(2)),
        Err(LifecycleError::Closed)
    );
}

#[test]
fn welcome_intersection_and_phase_are_enforced() {
    let mut owner = SessionLifecycle::new(limits(3), 3).unwrap();
    let key = owner.connection_id();
    let selected = ControlFrame::Welcome {
        session: 9,
        limits: SessionLimits {
            control_bytes: 2048,
            bindings: 1,
        },
        record_bytes: 1,
    };
    assert_eq!(owner.accept_welcome(&key, &selected).unwrap().get(), 9);
    let (_, identity) = bind(&mut owner, 1, scope("a", "epoch"));
    assert_eq!(identity.session().get(), 9);
    assert_eq!(
        owner.begin_bind(scope("b", "epoch")),
        Err(LifecycleError::BudgetExhausted)
    );
    assert_eq!(
        owner.accept_welcome(&key, &selected),
        Err(LifecycleError::UnexpectedReply)
    );
    assert_eq!(owner.phase(), SessionPhase::Closed);
    assert_eq!(owner.check_scope(&identity), Err(LifecycleError::Closed));
    for frame in [
        ControlFrame::Welcome {
            session: 1,
            limits: limits(3),
            record_bytes: 1,
        },
        ControlFrame::Welcome {
            session: 1,
            limits: SessionLimits {
                control_bytes: 8192,
                bindings: 1,
            },
            record_bytes: 1,
        },
        ControlFrame::Welcome {
            session: 1,
            limits: SessionLimits {
                control_bytes: 2048,
                bindings: 1,
            },
            record_bytes: 0,
        },
        ControlFrame::Welcome {
            session: 1,
            limits: SessionLimits {
                control_bytes: 2048,
                bindings: 1,
            },
            record_bytes: (MAX_RECORD_BYTES + 1) as u32,
        },
    ] {
        let mut owner = SessionLifecycle::new(
            SessionLimits {
                control_bytes: 2048,
                bindings: 2,
            },
            1,
        )
        .unwrap();
        let key = owner.connection_id();
        assert_eq!(
            owner.accept_welcome(&key, &frame),
            Err(LifecycleError::IncompatibleWelcome)
        );
        assert_eq!(owner.phase(), SessionPhase::Closed);
    }
    let mut owner = SessionLifecycle::new(limits(2), 1).unwrap();
    let key = owner.connection_id();
    let frame = ControlFrame::Welcome {
        session: 0,
        limits: limits(2),
        record_bytes: 1,
    };
    assert_eq!(
        owner.accept_welcome(&key, &frame),
        Err(LifecycleError::InvalidIdentity)
    );
}

#[test]
fn out_of_order_exact_bindings_remain_separate() {
    let mut owner = ready(4, 2);
    let key = owner.connection_id();
    let a = scope("a", "epoch");
    let b = scope("b", "epoch");
    let (ra, _) = owner.begin_bind(a.clone()).unwrap();
    let (rb, _) = owner.begin_bind(b.clone()).unwrap();
    assert_eq!(ra.correlation().request, 1);
    assert_eq!(rb.correlation().request, 2);
    let ib = owner.accept_bound(&key, &reply(&rb, 2, b.clone())).unwrap();
    let ia = owner.accept_bound(&key, &reply(&ra, 1, a.clone())).unwrap();
    assert_eq!(ia.scope(), &a);
    assert_eq!(ib.scope(), &b);
    assert_eq!(ia.handle(), 1);
    assert_eq!(ib.generation(), 1);
    assert_eq!(owner.pending_count(), 0);
    assert_eq!(owner.check_scope(&ia), Ok(()));
    assert_eq!(owner.check_scope(&ib), Ok(()));
    assert_eq!(
        owner.accept_bound(&key, &reply(&ra, 1, a)),
        Err(LifecycleError::TerminalRequest)
    );
    assert_eq!(owner.phase(), SessionPhase::Ready);
}

#[test]
fn pending_and_lifetime_budgets_do_not_reuse_identity() {
    let mut owner = ready(3, 1);
    let key = owner.connection_id();
    let a = scope("a", "epoch");
    let (ra, _) = owner.begin_bind(a.clone()).unwrap();
    assert_eq!(
        owner.begin_bind(scope("b", "epoch")),
        Err(LifecycleError::BudgetExhausted)
    );
    owner.cancel_bind(&ra).unwrap();
    owner.cancel_bind(&ra).unwrap();
    assert_eq!(owner.pending_count(), 0);
    assert_eq!(owner.retained_slots(), 1);
    assert_eq!(
        owner.begin_bind(a.clone()),
        Err(LifecycleError::StaleIdentity)
    );
    let (rb, ib) = bind(&mut owner, 2, scope("b", "epoch"));
    assert_eq!(rb.correlation().request, 2); // Failed admission consumed no ID.
    assert_eq!(owner.cancel_bind(&rb), Err(LifecycleError::TerminalRequest));
    assert_eq!(
        owner.accept_bound(&key, &reply(&ra, 1, a)),
        Err(LifecycleError::TerminalRequest)
    );
    assert_eq!(owner.check_scope(&ib), Ok(()));
    let (rc, _) = owner.begin_bind(scope("c", "epoch")).unwrap();
    owner.cancel_bind(&rc).unwrap();
    assert_eq!(
        owner.begin_bind(scope("d", "epoch")),
        Err(LifecycleError::BudgetExhausted)
    );
    assert_eq!(owner.retained_slots(), 3);
}

#[test]
fn stale_connection_events_and_tokens_cannot_touch_reused_wire_numbers() {
    let mut old = ready(3, 2);
    let old_key = old.connection_id();
    let (old_request, old_identity) = bind(&mut old, 1, scope("a", "epoch"));
    let old_reply = reply(&old_request, 1, scope("a", "epoch"));
    old.close();
    let mut new = ready(3, 2);
    let new_key = new.connection_id();
    assert_ne!(old_key, new_key);
    let (new_request, _) = new.begin_bind(scope("a", "epoch")).unwrap();
    assert_eq!(old_request.correlation(), new_request.correlation());
    assert_ne!(old_request, new_request);
    assert_eq!(
        new.accept_bound(&old_key, &old_reply),
        Err(LifecycleError::WrongConnection)
    );
    assert_eq!(
        new.cancel_bind(&old_request),
        Err(LifecycleError::WrongConnection)
    );
    assert_eq!(
        new.check_scope(&old_identity),
        Err(LifecycleError::WrongConnection)
    );
    assert_eq!(
        new.invalidate_scope(&old_identity),
        Err(LifecycleError::WrongConnection)
    );
    assert_eq!(new.pending_count(), 1);
    assert_eq!(new.phase(), SessionPhase::Ready);
    let identity = new
        .accept_bound(&new_key, &reply(&new_request, 1, scope("a", "epoch")))
        .unwrap();
    assert_eq!(new.check_scope(&identity), Ok(()));
}

#[test]
fn invalidation_is_explicit_and_handle_tombstones_remain() {
    let mut owner = ready(3, 2);
    let (_, identity) = bind(&mut owner, 1, scope("a", "old"));
    assert_eq!(
        owner.begin_bind(scope("a", "new")),
        Err(LifecycleError::ScopeConflict)
    );
    owner.invalidate_scope(&identity).unwrap();
    assert_eq!(
        owner.check_scope(&identity),
        Err(LifecycleError::StaleIdentity)
    );
    assert_eq!(
        owner.begin_bind(scope("a", "old")),
        Err(LifecycleError::StaleIdentity)
    );
    let (request, _) = owner.begin_bind(scope("a", "new")).unwrap();
    let key = owner.connection_id();
    let mut changed = reply(&request, 1, scope("a", "new"));
    if let ControlFrame::Bound { binding, .. } = &mut changed {
        binding.generation = 2;
    }
    assert_eq!(
        owner.accept_bound(&key, &changed),
        Err(LifecycleError::StaleIdentity)
    );
    assert_eq!(owner.phase(), SessionPhase::Closed);
}

#[test]
fn each_exact_scope_component_is_checked_before_context_installation() {
    let expected = scope("a", "epoch");
    let fields = ["receiver", "origin", "a", "incarnation", "schema", "epoch"];
    for changed in 0..fields.len() {
        let mut parts = fields.map(|s| Id::new(s).unwrap());
        parts[changed] = Id::new("other").unwrap();
        let [r, o, s, i, f, e] = parts;
        let mut owner = ready(2, 1);
        let key = owner.connection_id();
        let (request, _) = owner.begin_bind(expected.clone()).unwrap();
        assert_eq!(
            owner.accept_bound(&key, &reply(&request, 1, Scope::new(r, o, s, i, f, e))),
            Err(LifecycleError::StaleIdentity)
        );
        assert_eq!(owner.phase(), SessionPhase::Closed);
        assert_eq!(owner.retained_slots(), 0);
    }
}

#[test]
fn malformed_current_replies_close_without_partial_state() {
    for case in 0..6 {
        let mut owner = ready(2, 1);
        let key = owner.connection_id();
        let (request, _) = owner.begin_bind(scope("a", "epoch")).unwrap();
        let mut frame = reply(&request, 1, scope("a", "epoch"));
        let mut expected = LifecycleError::InvalidIdentity;
        if let ControlFrame::Bound {
            correlation,
            binding,
        } = &mut frame
        {
            match case {
                0 => {
                    correlation.session += 1;
                    expected = LifecycleError::StaleIdentity;
                }
                1 => {
                    correlation.request += 1;
                    expected = LifecycleError::UnknownRequest;
                }
                2 => binding.handle = 0,
                3 => binding.generation = 0,
                4 => {
                    correlation.request = 0;
                    expected = LifecycleError::UnknownRequest;
                }
                _ => {
                    frame = ControlFrame::Hello(limits(1));
                    expected = LifecycleError::UnexpectedReply;
                }
            }
        }
        assert_eq!(owner.accept_bound(&key, &frame), Err(expected));
        assert_eq!(owner.phase(), SessionPhase::Closed);
        assert_eq!(owner.pending_count(), 0);
    }
}

#[test]
fn wrong_connection_welcome_leaves_new_owner_connecting() {
    let old = ready(1, 1);
    let mut owner = SessionLifecycle::new(limits(1), 1).unwrap();
    assert_eq!(
        owner.accept_welcome(&old.connection_id(), &welcome(1)),
        Err(LifecycleError::WrongConnection)
    );
    assert_eq!(owner.phase(), SessionPhase::Connecting);
    let key = owner.connection_id();
    owner.accept_welcome(&key, &welcome(1)).unwrap();
    assert_eq!(owner.phase(), SessionPhase::Ready);
    assert_eq!(
        owner.accept_bound(
            &key,
            &ControlFrame::Bound {
                correlation: Correlation {
                    session: 7,
                    request: 99
                },
                binding: BindingMetadata {
                    handle: 1,
                    generation: 1,
                    scope: scope("a", "epoch"),
                    head: 0
                }
            }
        ),
        Err(LifecycleError::UnknownRequest)
    );
}

#[test]
fn unsolicited_bound_before_welcome_closes_only_its_current_connection() {
    let mut old = ready(1, 1);
    let (request, _) = old.begin_bind(scope("a", "epoch")).unwrap();
    let frame = reply(&request, 1, scope("a", "epoch"));
    let mut owner = SessionLifecycle::new(limits(1), 1).unwrap();
    assert_eq!(
        owner.accept_bound(&old.connection_id(), &frame),
        Err(LifecycleError::WrongConnection)
    );
    assert_eq!(owner.phase(), SessionPhase::Connecting);
    let key = owner.connection_id();
    assert_eq!(
        owner.accept_bound(&key, &frame),
        Err(LifecycleError::UnexpectedReply)
    );
    assert_eq!(owner.phase(), SessionPhase::Closed);
}

#[test]
fn hard_lifetime_slot_limit_is_bounded_under_cancel_churn() {
    let mut owner = ready(32, 1);
    for number in 0..32 {
        let (request, _) = owner
            .begin_bind(scope(&format!("stream-{number}"), "epoch"))
            .unwrap();
        owner.cancel_bind(&request).unwrap();
        assert_eq!(owner.retained_slots(), number + 1);
        assert_eq!(owner.pending_count(), 0);
    }
    assert_eq!(
        owner.begin_bind(scope("one-more", "epoch")),
        Err(LifecycleError::BudgetExhausted)
    );
    owner.close();
    assert_eq!(owner.retained_slots(), 0);
}

#[test]
fn explicitly_invalidated_target_can_bind_a_new_scope_with_a_fresh_handle() {
    let mut owner = ready(3, 2);
    let key = owner.connection_id();
    let (old_request, old_identity) = bind(&mut owner, 1, scope("a", "old"));
    owner.invalidate_scope(&old_identity).unwrap();
    let (_, new_identity) = bind(&mut owner, 2, scope("a", "new"));
    assert_eq!(new_identity.scope(), &scope("a", "new"));
    assert_eq!(owner.check_scope(&new_identity), Ok(()));
    assert_eq!(
        owner.check_scope(&old_identity),
        Err(LifecycleError::StaleIdentity)
    );
    assert_eq!(
        owner.accept_bound(&key, &reply(&old_request, 1, scope("a", "old"))),
        Err(LifecycleError::TerminalRequest)
    );
    assert_eq!(owner.check_scope(&new_identity), Ok(()));
    assert_eq!(owner.retained_slots(), 2);
}

#[test]
fn active_handle_cannot_be_assigned_to_another_scope() {
    let mut owner = ready(3, 2);
    let (_, first) = bind(&mut owner, 1, scope("a", "epoch"));
    let (second, _) = owner.begin_bind(scope("b", "epoch")).unwrap();
    let key = owner.connection_id();
    assert_eq!(
        owner.accept_bound(&key, &reply(&second, 1, scope("b", "epoch"))),
        Err(LifecycleError::StaleIdentity)
    );
    assert_eq!(owner.phase(), SessionPhase::Closed);
    assert_eq!(owner.check_scope(&first), Err(LifecycleError::Closed));
}
