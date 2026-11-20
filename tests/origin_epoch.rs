use nessa_sync::replication::origin_epoch::OriginEpoch;

#[test]
fn epoch_is_positive_and_never_wraps() {
    assert_eq!(OriginEpoch::new(0), None);
    assert_eq!(OriginEpoch::new(1), Some(OriginEpoch::FIRST));
    assert_eq!(OriginEpoch::FIRST.successor(), OriginEpoch::new(2));
    assert_eq!(OriginEpoch::new(u64::MAX).unwrap().successor(), None);
}
