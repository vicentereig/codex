use super::*;

fn thread_id(value: u128) -> ThreadId {
    ThreadId::from_string(&uuid::Uuid::from_u128(value).to_string())
        .expect("valid uuid string parses into ThreadId")
}

#[test]
fn default_control_is_disabled() {
    let control = CoordinationControl::default();
    assert!(control.as_enabled().is_none());
}

#[test]
fn state_absent_when_constructed_without_a_state_db() {
    let state = CoordinationState::new_for_tests(/* state_db */ None);
    let err = state
        .ensure_root_usable(thread_id(1))
        .expect_err("no state_db must fail before any controlled side effect");
    assert_eq!(err, RootCoordinationError::StateAbsent);
}

#[test]
fn poisoned_root_reports_poisoned_distinctly_from_state_absent() {
    // Constructed with no `state_db`, so every root would otherwise fail `StateAbsent`. Poison
    // is checked first and independently, so a poisoned root must report `Poisoned` specifically
    // rather than being masked by the coarser absent check, while an unrelated root still
    // reports plain `StateAbsent`.
    let state = CoordinationState::new_for_tests(/* state_db */ None);
    let poisoned_root = thread_id(2);
    let other_root = thread_id(3);
    state.mark_root_poisoned_for_tests(poisoned_root);

    assert_eq!(
        state.ensure_root_usable(poisoned_root),
        Err(RootCoordinationError::Poisoned)
    );
    assert_eq!(
        state.ensure_root_usable(other_root),
        Err(RootCoordinationError::StateAbsent)
    );
}

#[test]
fn enabled_for_tests_round_trips_through_as_enabled() {
    let state = CoordinationState::new_for_tests(None);
    let control = CoordinationControl::enabled_for_tests(Arc::clone(&state));
    assert!(Arc::ptr_eq(
        control.as_enabled().expect("control should be enabled"),
        &state
    ));
}
