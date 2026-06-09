mod common;

use crate::common::{
    basic::can_achieve_basic_broker_functionality, join::run_basic_join_test,
    map::can_implement_basic_map, reduce::can_implement_basic_reduce, subscription::*,
    topography::can_achieve_basic_topography_retrieval, windowing::basic_windowing,
};

#[test]
fn basic_test() {
    tracing_subscriber::fmt::init();

    can_achieve_basic_broker_functionality();

    run_basic_join_test();
    can_implement_basic_map();
    can_implement_basic_reduce();
    can_achieve_basic_topography_retrieval();
    basic_windowing();
    can_retrieve_data_from_subscription();
    subscription_works_with_multiple_clients();
    can_update_subscription_with_multiple_values();
    can_update_subscription_after_created();
}
