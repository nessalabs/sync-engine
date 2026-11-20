use nessa_sync::replication::domain::{
    Id, RetentionError, RetentionKind, RetentionPin, RetentionPosition, RetentionScope,
    RetentionWindow, Scope,
};

fn pos(n: u64) -> RetentionPosition {
    RetentionPosition::new(n)
}

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn scope(kind: RetentionKind, origin: &str, stream: &str, incarnation: &str) -> RetentionScope {
    RetentionScope::from_scope(
        kind,
        &Scope::new(
            id("receiver"),
            id(origin),
            id(stream),
            id(incarnation),
            id("schema"),
            id("access"),
        ),
    )
}

fn pin(holder: &str, scope: &RetentionScope, completed: u64, expiry: u64) -> RetentionPin {
    RetentionPin::for_completed(id(holder), scope.clone(), pos(completed), expiry)
}

fn window(floor: u64, preferred: u64, forced: Option<u64>) -> RetentionWindow {
    RetentionWindow::new(pos(100), pos(floor), pos(preferred), forced.map(pos)).unwrap()
}

#[test]
fn oldest_active_durable_position_protects_history_for_all_holders() {
    let scope = scope(RetentionKind::Records, "origin", "a", "one");
    let phone = pin("phone", &scope, 40, 80);
    let backup = pin("backup", &scope, 60, 100);
    let w = window(10, 90, None);

    assert_eq!(phone.holder(), &id("phone"));
    assert_eq!(
        w.cutoff(&scope, 79, &[backup.clone(), phone.clone()]),
        Ok(pos(40))
    );
    assert_eq!(
        w.cutoff(&scope, 79, &[phone.clone(), backup.clone()]),
        Ok(pos(40))
    );
    assert_eq!(
        w.cutoff(&scope, 80, &[phone.clone(), backup.clone()]),
        Ok(pos(60))
    );
    assert_eq!(w.cutoff(&scope, 100, &[phone, backup]), Ok(pos(90)));
    assert_eq!(w.cutoff(&scope, 0, &[]), Ok(pos(90)));
}

#[test]
fn hard_pressure_overrides_pin_but_cannot_regress_existing_floor() {
    let scope = scope(RetentionKind::Records, "origin", "a", "one");
    let pins = [pin("phone", &scope, 40, 80)];
    assert_eq!(
        window(10, 90, Some(70)).cutoff(&scope, 0, &pins),
        Ok(pos(70))
    );
    assert_eq!(
        window(10, 20, Some(70)).cutoff(&scope, 0, &pins),
        Ok(pos(70))
    );
    assert_eq!(
        window(75, 20, Some(70)).cutoff(&scope, 0, &pins),
        Ok(pos(75))
    );
    assert_eq!(
        window(10, 20, Some(15)).cutoff(&scope, 0, &pins),
        Ok(pos(20))
    );
    assert_eq!(
        window(10, 90, Some(100)).cutoff(&scope, 0, &pins),
        Ok(pos(100))
    );
}

#[test]
fn only_the_exact_source_family_resource_and_incarnation_are_selected() {
    let selected = scope(RetentionKind::Records, "origin", "a", "one");
    let pins = [
        pin(
            "catalogue",
            &scope(RetentionKind::Catalogue, "origin", "a", "one"),
            1,
            30,
        ),
        pin(
            "old-origin",
            &scope(RetentionKind::Records, "old", "a", "one"),
            2,
            30,
        ),
        pin(
            "other-stream",
            &scope(RetentionKind::Records, "origin", "b", "one"),
            3,
            30,
        ),
        pin(
            "old-incarnation",
            &scope(RetentionKind::Records, "origin", "a", "old"),
            4,
            30,
        ),
        pin("current", &selected, 60, 30),
    ];
    assert_eq!(window(0, 90, None).cutoff(&selected, 0, &pins), Ok(pos(60)));
}

#[test]
fn receivers_share_retention_but_not_replica_authority() {
    let left = Scope::new(
        id("phone"),
        id("origin"),
        id("a"),
        id("one"),
        id("schema"),
        id("grant-a"),
    );
    let right = Scope::new(
        id("tablet"),
        id("origin"),
        id("a"),
        id("one"),
        id("schema"),
        id("grant-b"),
    );
    assert_ne!(left, right);
    assert_eq!(
        RetentionScope::from_scope(RetentionKind::Records, &left),
        RetentionScope::from_scope(RetentionKind::Records, &right)
    );
}

#[test]
fn rejects_impossible_active_bounds_even_under_hard_pressure() {
    assert_eq!(
        RetentionWindow::new(pos(100), pos(101), pos(0), None),
        Err(RetentionError::FloorAboveHead)
    );
    assert_eq!(
        RetentionWindow::new(pos(100), pos(0), pos(101), None),
        Err(RetentionError::PreferredAboveHead)
    );
    assert_eq!(
        RetentionWindow::new(pos(100), pos(0), pos(0), Some(pos(101))),
        Err(RetentionError::ForcedAboveHead)
    );
    let scope = scope(RetentionKind::Catalogue, "origin", "catalogue", "one");
    let invalid = pin("phone", &scope, 101, 20);
    let w = window(20, 80, Some(90));
    assert_eq!(
        w.cutoff(&scope, 19, std::slice::from_ref(&invalid)),
        Err(RetentionError::PinAboveHead)
    );
    assert_eq!(w.cutoff(&scope, 20, &[invalid]), Ok(pos(90)));
}

#[test]
fn zero_and_maximum_positions_need_no_arithmetic() {
    let scope = scope(RetentionKind::Catalogue, "origin", "catalogue", "one");
    let zero = RetentionWindow::new(pos(0), pos(0), pos(0), None).unwrap();
    assert_eq!(zero.cutoff(&scope, 0, &[]), Ok(pos(0)));
    let max = pos(u64::MAX);
    assert_eq!(max.get(), u64::MAX);
    let last = RetentionWindow::new(max, max, max, Some(max)).unwrap();
    let pins = [RetentionPin::for_completed(
        id("backup"),
        scope.clone(),
        max,
        u64::MAX,
    )];
    assert_eq!(last.cutoff(&scope, u64::MAX - 1, &pins), Ok(max));
}
