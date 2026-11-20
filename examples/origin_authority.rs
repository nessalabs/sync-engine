use nessa_sync::replication::domain::Id;
use nessa_sync::replication::infrastructure::{OriginClaimError, SqliteOriginAuthority};
use nessa_sync::replication::origin_epoch::OriginAuthorityKey;

fn main() {
    let path = std::env::args_os()
        .nth(1)
        .expect("pass a fresh disposable authority database path");
    let authority = OriginAuthorityKey::new(Id::new("fixture-origin").unwrap());
    let mut first = SqliteOriginAuthority::provision(&path).unwrap();
    let mut second = SqliteOriginAuthority::open(&path).unwrap();
    let expected = second.current(&authority).unwrap();
    let epoch = first.compare_and_claim(&authority, expected).unwrap();
    assert_eq!(
        second.compare_and_claim(&authority, expected),
        Err(OriginClaimError::Stale {
            actual: Some(epoch)
        })
    );
    drop(first);
    drop(second);
    let mut reopened = SqliteOriginAuthority::open(&path).unwrap();
    assert_eq!(reopened.current(&authority), Ok(Some(epoch)));
    let next = reopened.compare_and_claim(&authority, Some(epoch)).unwrap();
    println!(
        "{{\"first_epoch\":{},\"stale_claims\":1,\"reopened_next_epoch\":{}}}",
        epoch.get(),
        next.get()
    );
}
