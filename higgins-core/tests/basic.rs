mod common;

use crate::common::{
    basic::can_achieve_basic_broker_functionality,
    concurrency_tests::{
        concurrent_produces_to_different_partitions_do_not_interfere,
        concurrent_produces_to_same_partition_are_serialised,
    },
    init_tracing,
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
    pipeline_tests::every_stream_type_answers_its_subscription,
    reduce::can_implement_basic_reduce,
    subscription::*,
    topography::can_achieve_basic_topography_retrieval,
    windowing::basic_windowing,
};

// Basic positive tests.
#[test]
fn basic_test() {
    init_tracing();
    can_achieve_basic_broker_functionality();
}

#[test]
fn basic_join() {
    init_tracing();
    run_basic_join_test();
}

#[test]
fn basic_map() {
    init_tracing();
    can_implement_basic_map();
}
#[test]
fn basic_reduce() {
    init_tracing();
    can_implement_basic_reduce();
}

#[test]
fn basic_topography() {
    init_tracing();
    can_achieve_basic_topography_retrieval();
}

#[test]
fn basic_window() {
    init_tracing();
    basic_windowing();
}

#[test]
fn basic_subscription() {
    init_tracing();
    can_retrieve_data_from_subscription();
}

#[test]
fn basic_subscription_multiple_clients() {
    init_tracing();
    subscription_works_with_multiple_clients();
}

#[test]
fn basic_subscription_multiple_values() {
    init_tracing();
    can_update_subscription_with_multiple_values();
}

#[test]
fn basic_subscription_update_after_creation() {
    init_tracing();
    can_update_subscription_after_created();
}

// General Invariants.
#[test]
fn monotonically_increasing_offsets() {
    init_tracing();
    offsets_are_monotonically_increasing();
}

#[test]
fn partition_records_dont_cross() {
    init_tracing();
    records_in_different_partitions_do_not_cross_contaminate();
}

#[test]
fn independent_partition_offsets() {
    init_tracing();
    partition_offsets_are_independent();
}

#[test]
fn reject_out_of_order_events() {
    init_tracing();
    acknowledge_out_of_order_is_rejected();
}

#[test]
fn idempotent_topography() {
    init_tracing();
    topography_is_idempotent_across_multiple_restarts();
}

#[test]
fn nonexistent_stream_produce_results_in_error() {
    init_tracing();
    produce_to_nonexistent_stream_does_not_crash_server();
}

#[test]
fn nonexistent_offset_query_results_in_error() {
    init_tracing();
    query_nonexistent_offset_does_not_crash_server();
}

#[test]
fn subscription_visibility() {
    init_tracing();
    subscription_redelivers_after_visibility_timeout();
    subscription_does_not_redeliver_after_acknowledge();
}

// Concurrency.
#[test]
fn concurrent_same_partition_produces_are_serialised() {
    init_tracing();
    concurrent_produces_to_same_partition_are_serialised();
}

#[test]
// #[ignore = "fails: concurrent produces to different partitions cross-contaminate (partition isolation race in the produce path); sequential isolation passes"]
fn concurrent_different_partition_produces_are_isolated() {
    init_tracing();
    concurrent_produces_to_different_partitions_do_not_interfere();
}

#[test]
fn full_pipeline_subscriptions_are_answered() {
    init_tracing();
    every_stream_type_answers_its_subscription();
}
