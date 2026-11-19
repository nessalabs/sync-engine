#![cfg(feature = "transport")]

use nessa_sync::replication::{
    application::MAX_RECORD_BYTES,
    domain::{Id, Scope, MAX_ID_BYTES},
    infrastructure::session::{
        read_control, write_control, BindingMetadata, ControlError, ControlFrame, Correlation,
        SessionLimits, MAX_CONTROL_BYTES, MAX_SESSION_BINDINGS,
    },
};
use std::io::{self, Cursor, Read, Write};

fn scope(text: &str) -> Scope {
    let id = || Id::new(text).unwrap();
    Scope::new(id(), id(), id(), id(), id(), id())
}
fn limits() -> SessionLimits {
    SessionLimits {
        control_bytes: MAX_CONTROL_BYTES as u32,
        bindings: MAX_SESSION_BINDINGS,
    }
}
fn frames() -> Vec<ControlFrame> {
    let correlation = Correlation {
        session: 17,
        request: 2,
    };
    vec![
        ControlFrame::Hello(limits()),
        ControlFrame::Welcome {
            session: 17,
            limits: limits(),
            record_bytes: MAX_RECORD_BYTES as u32,
        },
        ControlFrame::Bind {
            correlation,
            scope: scope("scope"),
        },
        ControlFrame::Bound {
            correlation,
            binding: BindingMetadata {
                handle: 1,
                generation: 1,
                scope: scope("scope"),
                head: 9,
            },
        },
    ]
}
fn encoded(frame: &ControlFrame) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_control(&mut bytes, frame, MAX_CONTROL_BYTES).unwrap();
    bytes
}

#[test]
fn golden_hello_pins_version_mode_and_field_order() {
    let bytes = b"\0\0\0\x0eNSC1\0\x01\x01\x01\0\0\x20\0\0\x20";
    assert_eq!(encoded(&ControlFrame::Hello(limits())), bytes);
    assert_eq!(
        read_control(&mut bytes.as_slice(), MAX_CONTROL_BYTES).unwrap(),
        ControlFrame::Hello(limits())
    );
}

#[test]
fn coalesced_frames_keep_exact_boundaries_and_metadata() {
    let frames = frames();
    let mut wire = Vec::new();
    let mut ends = Vec::new();
    for frame in &frames {
        write_control(&mut wire, frame, MAX_CONTROL_BYTES).unwrap();
        ends.push(wire.len() as u64);
    }
    let mut reader = Cursor::new(wire);
    for (expected, end) in frames.into_iter().zip(ends) {
        assert_eq!(
            read_control(&mut reader, MAX_CONTROL_BYTES).unwrap(),
            expected
        );
        assert_eq!(reader.position(), end);
    }
}

#[test]
fn complete_identity_at_owner_bound_is_preserved() {
    for text in [
        "x".repeat(MAX_ID_BYTES),
        "é".repeat(MAX_ID_BYTES / 2),
        "id\0with spaces".to_owned(),
    ] {
        let frame = ControlFrame::Bind {
            correlation: Correlation {
                session: u64::MAX,
                request: u64::MAX,
            },
            scope: scope(&text),
        };
        assert_eq!(
            read_control(&mut encoded(&frame).as_slice(), 2048).unwrap(),
            frame
        );
    }
}

struct ShortReader<'a>(&'a [u8]);
impl Read for ShortReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let count = out.len().min(2);
        self.0.read(&mut out[..count])
    }
}
#[derive(Default)]
struct ShortWriter(Vec<u8>);
impl Write for ShortWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = bytes.len().min(2);
        self.0.extend_from_slice(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn short_io_completes_without_changing_frame_semantics() {
    for frame in frames() {
        let mut writer = ShortWriter::default();
        write_control(&mut writer, &frame, MAX_CONTROL_BYTES).unwrap();
        assert_eq!(
            read_control(&mut ShortReader(&writer.0), MAX_CONTROL_BYTES).unwrap(),
            frame
        );
    }
}

#[test]
fn oversize_header_is_refused_before_body_read() {
    for length in [2049_u32, (MAX_CONTROL_BYTES + 1) as u32, u32::MAX] {
        let mut bytes = length.to_be_bytes().to_vec();
        bytes.extend_from_slice(b"body must remain unread");
        let mut reader = Cursor::new(bytes);
        assert!(matches!(
            read_control(&mut reader, 2048),
            Err(ControlError::TooLarge)
        ));
        assert_eq!(reader.position(), 4);
    }
}

#[test]
fn unsupported_vocabulary_and_trailing_fields_are_fatal() {
    let hello = encoded(&ControlFrame::Hello(limits()));
    for (offset, value) in [(4, b'X'), (9, 2), (10, 255), (11, 2)] {
        let mut bytes = hello.clone();
        bytes[offset] = value;
        assert!(matches!(
            read_control(&mut bytes.as_slice(), MAX_CONTROL_BYTES),
            Err(ControlError::Incompatible)
        ));
    }
    let mut bytes = hello;
    bytes.push(0);
    let length = (bytes.len() - 4) as u32;
    bytes[..4].copy_from_slice(&length.to_be_bytes());
    assert!(matches!(
        read_control(&mut bytes.as_slice(), MAX_CONTROL_BYTES),
        Err(ControlError::Malformed)
    ));
}

#[test]
fn every_truncated_prefix_is_refused() {
    for frame in frames() {
        let bytes = encoded(&frame);
        for end in 0..bytes.len() {
            assert!(
                read_control(&mut &bytes[..end], MAX_CONTROL_BYTES).is_err(),
                "{frame:?}/{end}"
            );
        }
    }
}

#[test]
fn invalid_values_never_write_any_bytes() {
    let mut invalid = vec![
        ControlFrame::Hello(SessionLimits {
            control_bytes: 8193,
            bindings: 1,
        }),
        ControlFrame::Hello(SessionLimits {
            control_bytes: 2047,
            bindings: 1,
        }),
        ControlFrame::Hello(SessionLimits {
            control_bytes: 2048,
            bindings: 0,
        }),
        ControlFrame::Hello(SessionLimits {
            control_bytes: 2048,
            bindings: 33,
        }),
        ControlFrame::Welcome {
            session: 0,
            limits: limits(),
            record_bytes: 1,
        },
        ControlFrame::Welcome {
            session: 1,
            limits: limits(),
            record_bytes: 0,
        },
        ControlFrame::Welcome {
            session: 1,
            limits: limits(),
            record_bytes: (MAX_RECORD_BYTES + 1) as u32,
        },
        ControlFrame::Bind {
            correlation: Correlation {
                session: 0,
                request: 1,
            },
            scope: scope("scope"),
        },
        ControlFrame::Bind {
            correlation: Correlation {
                session: 1,
                request: 0,
            },
            scope: scope("scope"),
        },
    ];
    for (handle, generation) in [(0, 1), (1, 0)] {
        invalid.push(ControlFrame::Bound {
            correlation: Correlation {
                session: 1,
                request: 1,
            },
            binding: BindingMetadata {
                handle,
                generation,
                scope: scope("scope"),
                head: 0,
            },
        });
    }
    for frame in invalid {
        let mut writer = Vec::new();
        assert!(write_control(&mut writer, &frame, MAX_CONTROL_BYTES).is_err());
        assert!(writer.is_empty());
    }
}

#[test]
fn invalid_incoming_identity_and_numerical_values_are_refused() {
    // Fixed offsets follow the documented v1 control header, not the decoder.
    for (index, start, count) in [
        (1, 11, 8),
        (1, 26, 4),
        (2, 11, 8),
        (2, 19, 8),
        (3, 27, 4),
        (3, 31, 8),
    ] {
        let mut bytes = encoded(&frames()[index]);
        bytes[start..start + count].fill(0);
        assert!(matches!(
            read_control(&mut bytes.as_slice(), MAX_CONTROL_BYTES),
            Err(ControlError::Malformed)
        ));
    }
    let mut bytes = encoded(&frames()[2]);
    bytes[27] = 0; // First scope identity length.
    assert!(matches!(
        read_control(&mut bytes.as_slice(), MAX_CONTROL_BYTES),
        Err(ControlError::Malformed)
    ));
    for limit in [0, 2047, MAX_CONTROL_BYTES + 1, usize::MAX] {
        let mut reader = Cursor::new(encoded(&frames()[0]));
        assert!(matches!(
            read_control(&mut reader, limit),
            Err(ControlError::InvalidLimits)
        ));
        assert_eq!(reader.position(), 0);
        let mut writer = Vec::new();
        assert!(matches!(
            write_control(&mut writer, &frames()[0], limit),
            Err(ControlError::InvalidLimits)
        ));
        assert!(writer.is_empty());
    }
}

#[test]
fn incoming_limits_and_id_overflow_use_their_owners() {
    for (start, bytes) in [
        (12, 2047_u32.to_be_bytes().to_vec()),
        (12, 8193_u32.to_be_bytes().to_vec()),
        (16, 0_u16.to_be_bytes().to_vec()),
        (16, 33_u16.to_be_bytes().to_vec()),
    ] {
        let mut wire = encoded(&ControlFrame::Hello(limits()));
        wire[start..start + bytes.len()].copy_from_slice(&bytes);
        assert!(matches!(
            read_control(&mut wire.as_slice(), MAX_CONTROL_BYTES),
            Err(ControlError::InvalidLimits)
        ));
    }
    let frame = ControlFrame::Bind {
        correlation: Correlation {
            session: 1,
            request: 1,
        },
        scope: scope(&"a".repeat(MAX_ID_BYTES)),
    };
    let mut wire = encoded(&frame);
    wire[27] = (MAX_ID_BYTES + 1) as u8;
    assert!(matches!(
        read_control(&mut wire.as_slice(), MAX_CONTROL_BYTES),
        Err(ControlError::Malformed)
    ));
}

#[test]
fn golden_bind_pins_distinct_scope_field_order() {
    let frame = ControlFrame::Bind {
        correlation: Correlation {
            session: 17,
            request: 2,
        },
        scope: Scope::new(
            Id::new("r").unwrap(),
            Id::new("o").unwrap(),
            Id::new("t").unwrap(),
            Id::new("i").unwrap(),
            Id::new("s").unwrap(),
            Id::new("e").unwrap(),
        ),
    };
    let bytes = b"\0\0\0\x23NSC1\0\x01\x03\0\0\0\0\0\0\0\x11\0\0\0\0\0\0\0\x02\x01r\x01o\x01t\x01i\x01s\x01e";
    assert_eq!(encoded(&frame), bytes);
    assert_eq!(
        read_control(&mut bytes.as_slice(), MAX_CONTROL_BYTES).unwrap(),
        frame
    );
}
