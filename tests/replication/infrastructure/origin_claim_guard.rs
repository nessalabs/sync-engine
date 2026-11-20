use std::fs;
use std::time::SystemTime;

use super::*;
use crate::replication::domain::Id;

#[test]
fn claim_and_capture_serialize_in_the_shared_ledger() {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("nessa-origin-guard-{}-{now}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("authority");
    let key = OriginAuthorityKey::new(Id::new("trusted-origin").unwrap());
    let mut first = SqliteOriginAuthority::provision(&path).unwrap();
    let mut contender = SqliteOriginAuthority::open(&path).unwrap();
    contender.conn.busy_timeout(Duration::ZERO).unwrap();
    let e1 = first.compare_and_claim(&key, None).unwrap();

    let observed = first.with_current_claim(&key, e1, || {
        // Attempt a real SQLite claim while the other connection still owns
        // the write lock, without scheduler timing or a sleep.
        assert_eq!(
            contender.compare_and_claim(&key, Some(e1)),
            Err(OriginClaimError::Unavailable),
        );
        contender.current(&key).unwrap()
    });
    assert_eq!(observed, Ok(Some(e1)));

    let e2 = contender.compare_and_claim(&key, Some(e1)).unwrap();
    let attempted = first.with_current_claim(&key, e1, || panic!("stale guard ran capture"));
    assert_eq!(
        attempted,
        Err::<(), _>(OriginClaimError::Stale { actual: Some(e2) })
    );
    drop((contender, first));
    fs::remove_dir_all(directory).unwrap();
}
