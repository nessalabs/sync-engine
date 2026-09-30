//! Identifier byte contract and input/retained storage ownership regressions.
use nessa_sync::replication::domain::{Id, ValidationError, MAX_ID_BYTES};
use std::cell::Cell;

struct Borrowed<'a> {
    text: &'a str,
    reads: &'a Cell<usize>,
}
impl AsRef<str> for Borrowed<'_> {
    fn as_ref(&self) -> &str {
        self.reads.set(self.reads.get() + 1);
        self.text
    }
}
impl From<Borrowed<'_>> for String {
    fn from(_: Borrowed<'_>) -> Self {
        panic!("identifier validation must not take owning input conversion")
    }
}

#[test]
fn exact_and_multibyte_limits_consume_the_published_utf8_byte_contract() {
    for text in [
        "x".repeat(MAX_ID_BYTES),
        "é".repeat(MAX_ID_BYTES / 2) + &"x".repeat(MAX_ID_BYTES % 2),
    ] {
        assert_eq!(text.len(), MAX_ID_BYTES);
        assert_eq!(Id::new(&text).unwrap().as_str(), text);
        assert_eq!(Id::new(text + "x"), Err(ValidationError::InvalidId));
    }
    assert_eq!(Id::new("  preserved  ").unwrap().as_str(), "  preserved  ");
    for blank in ["", " ", "\t\n", "\u{2003}"] {
        assert_eq!(Id::new(blank), Err(ValidationError::InvalidId));
    }
}

#[test]
fn oversized_borrowed_input_is_consulted_once_without_owning_conversion() {
    let oversized = "x".repeat(MAX_ID_BYTES + 1);
    let reads = Cell::new(0);
    assert_eq!(
        Id::new(Borrowed {
            text: &oversized,
            reads: &reads
        }),
        Err(ValidationError::InvalidId)
    );
    assert_eq!(reads.get(), 1);
    reads.set(0);
    assert_eq!(
        Id::new(Borrowed {
            text: "accepted",
            reads: &reads
        })
        .unwrap()
        .as_str(),
        "accepted"
    );
    assert_eq!(reads.get(), 1);
}

#[test]
fn owned_spare_allocation_is_replaced_by_compact_immutable_text() {
    let mut supplied = String::with_capacity(MAX_ID_BYTES * 1024);
    supplied.push_str("retained-id");
    let original = supplied.as_ptr();
    let id = Id::new(supplied).unwrap();
    assert_eq!(id.as_str(), "retained-id");
    assert_ne!(
        id.as_str().as_ptr(),
        original,
        "input's oversized allocation must not be retained"
    );
    assert_eq!(std::mem::size_of_val(&id), std::mem::size_of::<Box<str>>());
    assert_eq!(id.clone(), id);
}
