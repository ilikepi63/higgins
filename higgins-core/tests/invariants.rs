//! End-to-end invariant tests for the Higgins stream processing engine.
//!
//! Each test boots an isolated in-process server on a random port and exercises
//! the system through the public client library, validating a specific system
//! invariant. See `INTEGRATION_TESTS.md` for the rationale behind each test.
//!
//! These run as independent `#[test]` functions (in parallel), separate from the
//! sequential `basic.rs` harness.

mod common;

use std::time::Duration;

use common::client_utils::{acknowledge, create_subscription, recv_until_take};
use common::harness::*;
use common::schema::customer_schema;
use higgins_client::ResponseBody;
use std::sync::Arc;

use crate::common::get_random_port;

// ---------------------------------------------------------------------------
// 1. Core stream lifecycle
// ---------------------------------------------------------------------------

/// 1.2 — `query_latest` always returns the highest committed offset.
#[test]
fn query_latest_always_returns_highest_offset() {
    let port = get_random_port();
    let dir = unique_dir();
    let (handle, mut client) = setup_server(dir.clone(), port);

    upload_config(&mut client, BASIC_CONFIG);

    for age in [10, 20, 30] {
        produce_await(
            &mut client,
            "update_customer",
            &customer_json("1", age),
            Arc::new(customer_schema()),
        );
    }

    client
        .query_latest(b"update_customer", &partition("1"))
        .unwrap();
    let batch = decode_index_batch(client.recv(Some(Duration::from_secs(5))).unwrap());
    assert_eq!(age_of(&batch), 30, "query_latest returned a stale offset");

    handle.close();
    let _ = std::fs::remove_dir_all(dir);
}

// NOTE: tests for rejecting invalid configurations (unknown schema reference /
// unknown base stream) are intentionally omitted here: the engine currently
// does NOT validate these cases. An unknown base stream is silently accepted,
// and an unknown schema reference can crash the server. These are tracked as
// robustness bugs in INTEGRATION_TESTS.md rather than asserted as passing
// invariants.

// ---------------------------------------------------------------------------
// 2. Multi-partition correctness
// ---------------------------------------------------------------------------

/// 2.1 — Records in different partitions are isolated from each other.
#[test]
fn records_in_different_partitions_do_not_cross_contaminate() {
    let port = get_random_port();
    let dir = unique_dir();
    let (handle, mut client) = setup_server(dir.clone(), port);

    upload_config(&mut client, BASIC_CONFIG);

    produce_await(
        &mut client,
        "update_customer",
        r#"{"id":"1","first_name":"Alice","last_name":"A","age":1}"#,
        Arc::new(customer_schema()),
    );
    produce_await(
        &mut client,
        "update_customer",
        r#"{"id":"2","first_name":"Bob","last_name":"B","age":2}"#,
        Arc::new(customer_schema()),
    );

    client
        .query_at(b"update_customer", &partition("1"), 0)
        .unwrap();
    let b1 = decode_index_batch(client.recv(Some(Duration::from_secs(5))).unwrap());
    assert_eq!(string_of(&b1, "first_name"), "Alice");

    client
        .query_at(b"update_customer", &partition("2"), 0)
        .unwrap();
    let b2 = decode_index_batch(client.recv(Some(Duration::from_secs(5))).unwrap());
    assert_eq!(string_of(&b2, "first_name"), "Bob");

    handle.close();
    let _ = std::fs::remove_dir_all(dir);
}

/// 2.2 — Each partition has its own independent offset counter.
#[test]
fn partition_offsets_are_independent() {
    let port = get_random_port();
    let dir = unique_dir();
    let (handle, mut client) = setup_server(dir.clone(), port);

    upload_config(&mut client, BASIC_CONFIG);

    for age in 0..3 {
        produce_await(
            &mut client,
            "update_customer",
            &customer_json("A", age),
            Arc::new(customer_schema()),
        );
    }
    produce_await(
        &mut client,
        "update_customer",
        &customer_json("B", 99),
        Arc::new(customer_schema()),
    );

    // Partition A has offset 2.
    client
        .query_at(b"update_customer", &partition("A"), 2)
        .unwrap();
    let a2 = decode_index_batch(client.recv(Some(Duration::from_secs(5))).unwrap());
    assert_eq!(age_of(&a2), 2);

    // Partition B has offset 0.
    client
        .query_at(b"update_customer", &partition("B"), 0)
        .unwrap();
    let b0 = decode_index_batch(client.recv(Some(Duration::from_secs(5))).unwrap());
    assert_eq!(age_of(&b0), 99);

    // Partition B has no offset 1 — the server returns nothing.
    client
        .query_at(b"update_customer", &partition("B"), 1)
        .unwrap();
    assert!(
        client.recv(Some(Duration::from_millis(1200))).is_err(),
        "partition B must not have a record at offset 1"
    );

    handle.close();
    let _ = std::fs::remove_dir_all(dir);
}

// ---------------------------------------------------------------------------
// 7. Subscription management
// ---------------------------------------------------------------------------

/// 7.2 — An invalid / out-of-order acknowledgement is rejected.
#[test]
fn acknowledge_out_of_order_is_rejected() {
    let port = get_random_port();
    let dir = unique_dir();
    let (handle, mut client) = setup_server(dir.clone(), port);

    upload_config(&mut client, BASIC_CONFIG);

    let sub_id = create_subscription(&mut client, "update_customer");

    // Produce two records so offsets 0 and 1 exist.
    produce_await(
        &mut client,
        "update_customer",
        &customer_json("1", 0),
        Arc::new(customer_schema()),
    );
    produce_await(
        &mut client,
        "update_customer",
        &customer_json("1", 1),
        Arc::new(customer_schema()),
    );

    // Take one record so the subscription registers the partition.
    client.take(sub_id.clone(), b"update_customer", 1).unwrap();
    let _ = recv_until_take(&mut client).unwrap();

    // Acknowledge a far-future offset (5) while earlier offsets remain
    // unacknowledged — this skips offsets and must be rejected.
    let response = acknowledge(
        "update_customer",
        &sub_id,
        vec![(partition("1"), 5u64..5u64)],
        &mut client,
    );

    assert!(
        !response.error.is_empty(),
        "out-of-order acknowledgement should be rejected with an error"
    );

    handle.close();
    let _ = std::fs::remove_dir_all(dir);
}

// ---------------------------------------------------------------------------
// 8. Persistence (topography)
// ---------------------------------------------------------------------------

/// 8.4 — The topography is reconstructed identically across multiple restarts.
///
/// Note: stream *data* uses in-memory storage in these tests and therefore does
/// not survive a restart; only the topography (an append-only JSONL log) does.
#[test]
fn topography_is_idempotent_across_multiple_restarts() {
    let dir = unique_dir();
    let mut snapshots: Vec<Vec<u8>> = Vec::new();

    for (i, _) in (0..3).enumerate() {
        let port = get_random_port();
        let (handle, mut client) = setup_server(dir.clone(), port);

        if i == 0 {
            upload_config(&mut client, BASIC_CONFIG);
        }

        client.get_current_topography().unwrap();
        let data = match client.recv(Some(Duration::from_secs(5))).unwrap().body {
            ResponseBody::GetCurrentTopography(t) => t.data,
            other => panic!("expected GetCurrentTopography response, got {:?}", other),
        };
        snapshots.push(data);

        handle.close();
        std::thread::sleep(Duration::from_millis(150));
    }

    assert_eq!(
        snapshots[0], snapshots[1],
        "topography changed after 1 restart"
    );
    assert_eq!(
        snapshots[1], snapshots[2],
        "topography changed after 2 restarts"
    );

    let _ = std::fs::remove_dir_all(dir);
}

// ---------------------------------------------------------------------------
// 9. Error handling and resilience
// ---------------------------------------------------------------------------

/// 9.1 — Producing to an unknown stream does not crash the server.
#[test]
fn produce_to_nonexistent_stream_does_not_crash_server() {
    let port = get_random_port();
    let dir = unique_dir();
    let (handle, mut client) = setup_server(dir.clone(), port);

    upload_config(&mut client, BASIC_CONFIG);

    // Produce to a stream that was never configured.
    client
        .produce_json(
            "ghost_stream",
            customer_json("1", 1).as_bytes(),
            Arc::new(customer_schema()),
        )
        .unwrap();

    // The server does not respond to an invalid produce.
    assert!(
        client.recv(Some(Duration::from_millis(1000))).is_err(),
        "server should not send a produce response for an unknown stream"
    );

    // The server is still alive and responsive.
    client.ping().unwrap();
    assert!(matches!(
        client.recv(Some(Duration::from_secs(5))).unwrap().body,
        ResponseBody::Pong(_)
    ));

    handle.close();
    let _ = std::fs::remove_dir_all(dir);
}

/// 9.2 — Querying a non-existent offset does not crash the server.
#[test]
fn query_nonexistent_offset_does_not_crash_server() {
    let port = get_random_port();
    let dir = unique_dir();
    let (handle, mut client) = setup_server(dir.clone(), port);

    upload_config(&mut client, BASIC_CONFIG);

    // No records have been produced yet.
    client
        .query_at(b"update_customer", &partition("1"), 0)
        .unwrap();
    assert!(
        client.recv(Some(Duration::from_millis(1000))).is_err(),
        "querying an empty stream should not return a record"
    );

    // The server is still alive and responsive.
    client.ping().unwrap();
    assert!(matches!(
        client.recv(Some(Duration::from_secs(5))).unwrap().body,
        ResponseBody::Pong(_)
    ));

    handle.close();
    let _ = std::fs::remove_dir_all(dir);
}

// ---------------------------------------------------------------------------
// Visibility timeout (subscription redelivery)
// ---------------------------------------------------------------------------

/// A taken-but-unacknowledged record is redelivered after the visibility
/// timeout elapses.
#[test]
fn subscription_redelivers_after_visibility_timeout() {
    let port = get_random_port();
    let dir = unique_dir();
    let visibility_timeout = Duration::from_millis(700);
    let (handle, mut client) = setup_server_with_visibility(dir.clone(), port, visibility_timeout);

    upload_config(&mut client, BASIC_CONFIG);

    let sub_id = create_subscription(&mut client, "update_customer");

    // Express standing demand before any data exists.
    client.take(sub_id.clone(), b"update_customer", 1).unwrap();

    // Produce a single record; it will be pushed to the waiting consumer.
    client
        .produce_json(
            "update_customer",
            customer_json("1", 42).as_bytes(),
            Arc::new(customer_schema()),
        )
        .unwrap();

    // First delivery.
    let first = recv_until_take(&mut client).unwrap();
    assert_eq!(
        first.records.first().unwrap().offset,
        0,
        "first delivery should be offset 0"
    );

    // Deliberately do NOT acknowledge. After the visibility timeout the offset
    // must be reset and redelivered automatically.
    let redelivered = recv_until_take(&mut client).unwrap();
    assert_eq!(
        redelivered.records.first().unwrap().offset,
        0,
        "offset 0 should be redelivered after the visibility timeout"
    );

    handle.close();
    let _ = std::fs::remove_dir_all(dir);
}

/// Once a record is acknowledged it is NOT redelivered, even after the
/// visibility timeout elapses.
#[test]
fn subscription_does_not_redeliver_after_acknowledge() {
    let port = get_random_port();
    let dir = unique_dir();
    let visibility_timeout = Duration::from_millis(700);
    let (handle, mut client) = setup_server_with_visibility(dir.clone(), port, visibility_timeout);

    upload_config(&mut client, BASIC_CONFIG);

    let sub_id = create_subscription(&mut client, "update_customer");

    client.take(sub_id.clone(), b"update_customer", 1).unwrap();

    client
        .produce_json(
            "update_customer",
            customer_json("1", 42).as_bytes(),
            Arc::new(customer_schema()),
        )
        .unwrap();

    let first = recv_until_take(&mut client).unwrap();
    assert_eq!(first.records.first().unwrap().offset, 0);

    // Acknowledge offset 0. A `ProduceResponse` from the earlier produce may
    // still be in flight, so drain until we observe the AcknowledgeResponse.
    client
        .acknowledge(
            "update_customer",
            &sub_id,
            vec![(partition("1"), 0u64..0u64)],
        )
        .unwrap();
    let ack = loop {
        match client.recv(Some(Duration::from_secs(5))).unwrap().body {
            ResponseBody::Acknowledge(a) => break a,
            ResponseBody::Produce(_) | ResponseBody::TakeRecords(_) => continue,
            other => panic!("expected Acknowledge response, got {:?}", other),
        }
    };
    assert!(
        ack.error.is_empty(),
        "acknowledging offset 0 should succeed: {}",
        ack.error
    );

    // Wait well past the visibility timeout — nothing should be redelivered.
    std::thread::sleep(visibility_timeout * 2 + Duration::from_millis(300));
    assert!(
        client.recv(Some(Duration::from_millis(500))).is_err(),
        "an acknowledged offset must not be redelivered"
    );

    handle.close();
    let _ = std::fs::remove_dir_all(dir);
}
