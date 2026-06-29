mod common;

use crate::common::{
    basic::can_achieve_basic_broker_functionality,
    invariant_tests::offsets_are_monotonically_increasing, join::run_basic_join_test,
    map::can_implement_basic_map, reduce::can_implement_basic_reduce, subscription::*,
    topography::can_achieve_basic_topography_retrieval, windowing::basic_windowing,
};

// Basic positive tests.
#[test]
fn basic_test() {
    can_achieve_basic_broker_functionality();
}

#[test]
fn basic_join() {
    run_basic_join_test();
}

#[test]
fn basic_map() {
    can_implement_basic_map();
}
#[test]
fn basic_reduce() {
    can_implement_basic_reduce();
}

#[test]
fn basic_topography() {
    can_achieve_basic_topography_retrieval();
}

#[test]
fn basic_window() {
    basic_windowing();
}

#[test]
fn basic_subscription() {
    can_retrieve_data_from_subscription();
}

#[test]
fn basic_subscription_multiple_clients() {
    subscription_works_with_multiple_clients();
}

#[test]
fn basic_subscription_multiple_values() {
    can_update_subscription_with_multiple_values();
}

#[test]
fn basic_subscription_update_after_creation() {
    can_update_subscription_after_created();
}

// General Invariants.
#[test]
fn monotonically_increasing_offsets() {
    offsets_are_monotonically_increasing();
}
