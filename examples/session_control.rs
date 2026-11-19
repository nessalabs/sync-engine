//! In-memory benign control vocabulary lab; no socket or access grant.
use nessa_sync::replication::{
    application::MAX_RECORD_BYTES,
    domain::{Id, Scope},
    infrastructure::session::{
        read_control, write_control, BindingMetadata, ControlFrame, Correlation, SessionLimits,
        MAX_CONTROL_BYTES, MAX_SESSION_BINDINGS,
    },
};
use std::io::Cursor;

fn main() {
    let id = || Id::new("fixture").unwrap();
    let scope = Scope::new(id(), id(), id(), id(), id(), id());
    let limits = SessionLimits {
        control_bytes: MAX_CONTROL_BYTES as u32,
        bindings: MAX_SESSION_BINDINGS,
    };
    let correlation = Correlation {
        session: 1,
        request: 1,
    };
    let frames = [
        ControlFrame::Hello(limits),
        ControlFrame::Welcome {
            session: 1,
            limits,
            record_bytes: MAX_RECORD_BYTES as u32,
        },
        ControlFrame::Bind {
            correlation,
            scope: scope.clone(),
        },
        ControlFrame::Bound {
            correlation,
            binding: BindingMetadata {
                handle: 1,
                generation: 1,
                scope,
                head: 3,
            },
        },
    ];
    let mut wire = Vec::new();
    for frame in &frames {
        write_control(&mut wire, frame, MAX_CONTROL_BYTES).unwrap();
    }
    let bytes = wire.len();
    let mut reader = Cursor::new(wire);
    for frame in frames {
        assert_eq!(read_control(&mut reader, MAX_CONTROL_BYTES).unwrap(), frame);
    }
    assert_eq!(reader.position(), bytes as u64);
    println!("{{\"control_frames\":4,\"wire_bytes\":{bytes},\"authorization_mode\":\"per_operation\",\"grants_created\":0,\"source_reads\":0,\"commits\":0}}");
}
