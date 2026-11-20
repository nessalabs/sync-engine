//! One locally owned attempt, not a store transaction or a transport request.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

const PENDING: u8 = 0;
const CANCELLED: u8 = 1;
const CLAIMED: u8 = 2;
const FINISHED: u8 = 3;

/// Single-use apply admission for one cancellable follower page step.
/// Consuming this handle prevents reuse for another step.
pub struct PageStepAttempt(Arc<AtomicU8>);

/// Independent cancellation handle for exactly one [PageStepAttempt].
/// It does not acquire a transport, policy, or store lock.
#[derive(Clone)]
pub struct PageStepCancel(Arc<AtomicU8>);

/// Local decision made by [PageStepCancel::cancel].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageStepCancelResult {
    /// Cancellation owns the apply gate. This attempt cannot call apply.
    /// Its in-progress read may finish normally; repeated cancellation is safe.
    CancelledBeforeApply,
    /// Apply already owns the gate. This does not confirm success, failure
    /// or rollback; observe the step result and reload if it was uncertain.
    TooLate,
    /// The attempt ended or was dropped without claiming apply.
    FinishedWithoutApply,
}

/// Completion of a cancellable follower page step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancellablePageStep {
    /// The ordinary step completed or was already at its captured target.
    Finished {
        /// The pass has reached its captured target.
        complete: bool,
    },
    /// The apply gate was cancelled; no plan for this attempt was applied.
    CancelledBeforeApply,
}

impl PageStepAttempt {
    /// Create one pending attempt and its independent cancellation handle.
    pub fn pair() -> (Self, PageStepCancel) {
        let state = Arc::new(AtomicU8::new(PENDING));
        (Self(state.clone()), PageStepCancel(state))
    }

    pub(super) fn cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire) == CANCELLED
    }

    pub(super) fn claim(&self) -> bool {
        self.complete_as(CLAIMED)
    }

    pub(super) fn finish_without_apply(&self) -> bool {
        self.complete_as(FINISHED)
    }

    fn complete_as(&self, state: u8) -> bool {
        self.0
            .compare_exchange(PENDING, state, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

impl Drop for PageStepAttempt {
    fn drop(&mut self) {
        // Keep whichever terminal decision already won.
        self.finish_without_apply();
    }
}

impl PageStepCancel {
    /// Try to own the local apply gate. This does not wait for, interrupt,
    /// or acknowledge remote work or any already admitted store transaction.
    pub fn cancel(&self) -> PageStepCancelResult {
        match self
            .0
            .compare_exchange(PENDING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) | Err(CANCELLED) => PageStepCancelResult::CancelledBeforeApply,
            Err(CLAIMED) => PageStepCancelResult::TooLate,
            Err(_) => PageStepCancelResult::FinishedWithoutApply,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_one_terminal_result_can_own_an_attempt() {
        let (attempt, cancel) = PageStepAttempt::pair();
        assert_eq!(cancel.cancel(), PageStepCancelResult::CancelledBeforeApply);
        assert!(!attempt.claim());
        drop(attempt);
        assert_eq!(cancel.cancel(), PageStepCancelResult::CancelledBeforeApply);

        let (attempt, cancel) = PageStepAttempt::pair();
        assert!(attempt.claim());
        assert_eq!(cancel.cancel(), PageStepCancelResult::TooLate);
        drop(attempt);
        assert_eq!(cancel.cancel(), PageStepCancelResult::TooLate);

        let (attempt, cancel) = PageStepAttempt::pair();
        drop(attempt);
        assert_eq!(cancel.cancel(), PageStepCancelResult::FinishedWithoutApply);
    }
}
