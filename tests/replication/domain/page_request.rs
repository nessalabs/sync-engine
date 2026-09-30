//! Public pure request admission and shared receiver validation ownership.
use super::scope;
use nessa_sync::replication::domain::{
    validate_page, validate_page_request, Checkpoint, Limits, Page, PageRequest, ValidationError,
};

fn request() -> PageRequest {
    PageRequest {
        scope: scope("request-receiver"),
        after: 4,
        target: 7,
        max_records: 2,
        max_payload_bytes: 8,
        max_record_bytes: 8,
    }
}

#[test]
fn exact_limits_and_maximum_range_are_admissible_without_receiver_state() {
    let limits = Limits::new(2, 8, 8).unwrap();
    let mut request = request();
    assert_eq!(validate_page_request(&request, limits), Ok(()));
    request.after = u64::MAX - 1;
    request.target = u64::MAX;
    request.max_records = 1;
    request.max_payload_bytes = 1;
    request.max_record_bytes = 1;
    assert_eq!(validate_page_request(&request, limits), Ok(()));
}

#[test]
fn request_admission_and_receiver_page_validation_share_range_and_budget_refusals() {
    let limits = Limits::new(2, 8, 8).unwrap();
    let original = request();
    let mutations: [fn(&mut PageRequest); 8] = [
        |r| r.target = r.after,
        |r| r.target = r.after - 1,
        |r| r.max_records = 0,
        |r| r.max_records += 1,
        |r| r.max_payload_bytes = 0,
        |r| r.max_payload_bytes += 1,
        |r| r.max_record_bytes = 0,
        |r| r.max_record_bytes += 1,
    ];
    for mutate in mutations {
        let mut request = original.clone();
        mutate(&mut request);
        assert_eq!(
            validate_page_request(&request, limits),
            Err(ValidationError::InvalidRange)
        );
        let checkpoint = Checkpoint::new(request.scope.clone(), request.after);
        let page = Page {
            request: request.clone(),
            records: vec![],
        };
        assert_eq!(
            validate_page(&checkpoint, &request, page, limits),
            Err(ValidationError::InvalidRange)
        );
    }
}

#[test]
fn checkpoint_correlation_remains_with_receiver_page_validation() {
    let request = request();
    let limits = Limits::new(2, 8, 8).unwrap();
    assert_eq!(validate_page_request(&request, limits), Ok(()));
    for checkpoint in [
        Checkpoint::new(scope("another-receiver"), request.after),
        Checkpoint::new(request.scope.clone(), request.after + 1),
    ] {
        assert_eq!(
            validate_page(
                &checkpoint,
                &request,
                Page {
                    request: request.clone(),
                    records: vec![]
                },
                limits
            ),
            Err(ValidationError::InvalidRange)
        );
    }
}

#[test]
fn record_budget_may_exceed_page_budget_without_moving_response_validation() {
    let mut request = request();
    request.max_payload_bytes = 1;
    assert_eq!(
        validate_page_request(&request, Limits::new(2, 8, 8).unwrap()),
        Ok(())
    );
}
