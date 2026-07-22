use super::*;

#[test]
fn failure_injector_fires_only_at_its_configured_point() {
    let injector = MessageFailureInjector::fail_at(MessageFailurePoint::AfterReceipt);

    assert!(injector.check(MessageFailurePoint::BeforeIntent).is_ok());
    assert!(injector.check(MessageFailurePoint::AfterIntent).is_ok());
    assert!(injector.check(MessageFailurePoint::AfterReceipt).is_err());
    assert!(injector.check(MessageFailurePoint::BeforeEnqueue).is_ok());
    assert!(
        injector
            .check(MessageFailurePoint::AfterEnqueueBeforeAck)
            .is_ok()
    );
}

#[test]
fn no_op_failure_injector_never_fires() {
    let injector = MessageFailureInjector::none();
    for point in [
        MessageFailurePoint::BeforeIntent,
        MessageFailurePoint::AfterIntent,
        MessageFailurePoint::AfterReceipt,
        MessageFailurePoint::BeforeEnqueue,
        MessageFailurePoint::AfterEnqueueBeforeAck,
    ] {
        assert!(injector.check(point).is_ok());
    }
}
