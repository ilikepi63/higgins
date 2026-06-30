mod common;

use crate::common::{
    basic::can_achieve_basic_broker_functionality,
    invariant_tests::{
        acknowledge_out_of_order_is_rejected, offsets_are_monotonically_increasing,
        partition_offsets_are_independent, produce_to_nonexistent_stream_does_not_crash_server,
        query_nonexistent_offset_does_not_crash_server,
        records_in_different_partitions_do_not_cross_contaminate,
        subscription_does_not_redeliver_after_acknowledge,
        subscription_redelivers_after_visibility_timeout,
        topography_is_idempotent_across_multiple_restarts,
    },
    join::run_basic_join_test,
    map::can_implement_basic_map,
    reduce::can_implement_basic_reduce,
    subscription::*,
    topography::can_achieve_basic_topography_retrieval,
    windowing::basic_windowing,
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

#[test]
fn partition_records_dont_cross() {
    records_in_different_partitions_do_not_cross_contaminate();
}

#[test]
fn independent_partition_offsets() {
    partition_offsets_are_independent();
}

#[test]
fn reject_out_of_order_events() {
    acknowledge_out_of_order_is_rejected();
}

#[test]
fn idempotent_topography() {
    topography_is_idempotent_across_multiple_restarts();
}

#[test]
fn nonexistent_stream_produce_results_in_error() {
    produce_to_nonexistent_stream_does_not_crash_server();
}

#[test]
fn nonexistent_offset_query_results_in_error() {
    query_nonexistent_offset_does_not_crash_server();
}

#[test]
fn subscription_visibility() {
    subscription_redelivers_after_visibility_timeout();
    subscription_does_not_redeliver_after_acknowledge();
}
