//! Constructor retained-allocation acquisition counts, without an unsafe allocator.
use super::{Id, ValidationError, MAX_ID_BYTES};
use std::cell::Cell;

thread_local! { static ALLOCATIONS: Cell<usize> = const { Cell::new(0) }; }

pub(super) fn allocated() {
    ALLOCATIONS.with(|count| count.set(count.get() + 1));
}

#[test]
fn invalid_borrowed_identity_does_not_acquire_retained_storage() {
    ALLOCATIONS.with(|count| count.set(0));
    let oversized = "x".repeat(MAX_ID_BYTES + 1);
    for text in [oversized.as_str(), "", "\u{2003}"] {
        assert_eq!(Id::new(text), Err(ValidationError::InvalidId));
        assert_eq!(ALLOCATIONS.with(Cell::get), 0);
    }
    assert_eq!(Id::new("accepted").unwrap().as_str(), "accepted");
    assert_eq!(ALLOCATIONS.with(Cell::get), 1);
}
