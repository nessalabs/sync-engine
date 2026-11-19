//! Benign in-memory control lifecycle lab. It opens no source, store or socket.
use nessa_sync::replication::{
    application::MAX_RECORD_BYTES,
    domain::{Id, Scope},
    infrastructure::session::{
        read_control, write_control, BindingMetadata, ControlFrame, LifecycleError,
        RequestIdentity, SessionLifecycle, SessionLimits, SessionPhase, MAX_CONTROL_BYTES,
    },
};

fn scope(stream: &str) -> Scope {
    let id = || Id::new("fixture").unwrap();
    Scope::new(id(), id(), Id::new(stream).unwrap(), id(), id(), id())
}
fn wire(frame: &ControlFrame) -> ControlFrame {
    let mut bytes = Vec::new();
    write_control(&mut bytes, frame, MAX_CONTROL_BYTES).unwrap();
    read_control(&mut bytes.as_slice(), MAX_CONTROL_BYTES).unwrap()
}
fn reply(request: &RequestIdentity, handle: u32, stream: &str) -> ControlFrame {
    wire(&ControlFrame::Bound {
        correlation: request.correlation(),
        binding: BindingMetadata {
            handle,
            generation: 1,
            scope: scope(stream),
            head: 99,
        },
    })
}
fn main() {
    let limits = SessionLimits {
        control_bytes: MAX_CONTROL_BYTES as u32,
        bindings: 4,
    };
    let welcome = wire(&ControlFrame::Welcome {
        session: 7,
        limits,
        record_bytes: MAX_RECORD_BYTES as u32,
    });
    let mut owner = SessionLifecycle::new(limits, 2).unwrap();
    let connection = owner.connection_id();
    assert_eq!(wire(&owner.hello().unwrap()), ControlFrame::Hello(limits));
    owner.accept_welcome(&connection, &welcome).unwrap();
    let (a, frame) = owner.begin_bind(scope("a")).unwrap();
    assert_eq!(wire(&frame), frame);
    let (b, frame) = owner.begin_bind(scope("b")).unwrap();
    assert_eq!(wire(&frame), frame);
    // B finishes before A; the retained exact scopes remain separate.
    let ib = owner.accept_bound(&connection, &reply(&b, 2, "b")).unwrap();
    let ia = owner.accept_bound(&connection, &reply(&a, 1, "a")).unwrap();
    assert_eq!(ia.scope(), &scope("a"));
    assert_eq!(ib.scope(), &scope("b"));
    owner.invalidate_scope(&ia).unwrap();
    assert_eq!(owner.check_scope(&ia), Err(LifecycleError::StaleIdentity));
    let (c, _) = owner.begin_bind(scope("c")).unwrap();
    owner.cancel_bind(&c).unwrap();
    assert_eq!(
        owner.accept_bound(&connection, &reply(&c, 3, "c")),
        Err(LifecycleError::TerminalRequest)
    );
    assert_eq!(owner.check_scope(&ib), Ok(()));
    assert_eq!(owner.retained_slots(), 3);
    owner.close();
    assert_eq!(owner.check_scope(&ib), Err(LifecycleError::Closed));
    assert_eq!(owner.phase(), SessionPhase::Closed);
    let mut replacement = SessionLifecycle::new(limits, 2).unwrap();
    let new_connection = replacement.connection_id();
    replacement
        .accept_welcome(&new_connection, &welcome)
        .unwrap();
    assert_eq!(
        replacement.check_scope(&ib),
        Err(LifecycleError::WrongConnection)
    );
    assert_eq!(
        replacement.accept_bound(&connection, &reply(&a, 1, "a")),
        Err(LifecycleError::WrongConnection)
    );
    assert_eq!(replacement.phase(), SessionPhase::Ready);
    println!("{{\"accepted_bindings\":2,\"reordered_replies\":true,\"invalidated_scope_refused\":true,\"cancelled_reply_refused\":true,\"closed_context_refused\":true,\"old_connection_refused\":true}}");
}
