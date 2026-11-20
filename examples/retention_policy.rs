use nessa_sync::replication::domain::{
    Id, RetentionKind, RetentionPin, RetentionPosition as Position, RetentionScope,
    RetentionWindow, Scope,
};

fn main() {
    let id = |value| Id::new(value).expect("valid fixture identifier");
    let scope = RetentionScope::from_scope(
        RetentionKind::Records,
        &Scope::new(
            id("phone"),
            id("origin"),
            id("history"),
            id("first"),
            id("schema"),
            id("access"),
        ),
    );
    let p = Position::new;
    let phone = RetentionPin::for_completed(id("phone"), scope.clone(), p(40), 100);
    let backup = RetentionPin::for_completed(id("backup"), scope.clone(), p(60), 120);
    let pins = [phone, backup];
    let normal = RetentionWindow::new(p(100), p(10), p(90), None).unwrap();
    let pressure = RetentionWindow::new(p(100), p(10), p(90), Some(p(70))).unwrap();
    let active = normal.cutoff(&scope, 99, &pins).unwrap().get();
    let hard_cap = pressure.cutoff(&scope, 99, &pins).unwrap().get();
    let oldest_expired = normal.cutoff(&scope, 100, &pins).unwrap().get();
    let all_expired = normal.cutoff(&scope, 120, &pins).unwrap().get();
    assert_eq!(
        [active, hard_cap, oldest_expired, all_expired],
        [40, 70, 60, 90]
    );
    println!(
        "{{\"active\":{active},\"hard_pressure\":{hard_cap},\"oldest_expired\":{oldest_expired},\"all_expired\":{all_expired}}}"
    );
}
