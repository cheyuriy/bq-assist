#[path = "helpers/mod.rs"]
mod helpers;

use assert_cmd::Command;
use helpers::{assert_partitioning, get_test_env};
use predicates::prelude::PredicateBooleanExt;
use predicates::str::contains;

const TABLE: &str = "test_partitioning";

#[tokio::test]
async fn test_partitioning_lifecycle() {
    let Some(env) = get_test_env().await else {
        eprintln!("Skipping integration test: BQ_TEST_PROJECT not set");
        return;
    };

    let table_ref = format!("{}.{}.{TABLE}", env.project, env.dataset);

    // 1. No partitioning initially
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "partitioning"])
        .assert()
        .success()
        .stdout(contains("No partitioning set on this table."));

    // 2. Add time unit column partitioning on the TIMESTAMP field (defaults: timestamp, day)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "partitioning", "add", "time", "event_ts"])
        .assert()
        .success();

    // 3. Verify partitioning is reflected in BigQuery metadata
    assert_partitioning(env, TABLE, Some("DATE(event_ts)")).await;

    // 4. partitioning list shows the clause
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "partitioning"])
        .assert()
        .success()
        .stdout(contains("PARTITION BY").and(contains("event_ts")));

    // 5. Replace with integer range partitioning on the INT64 field
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args([
            "table", &table_ref, "partitioning", "add",
            "range", "value", "0", "1000", "100",
        ])
        .assert()
        .success();

    // 6. Verify range partitioning in BigQuery metadata
    assert_partitioning(env, TABLE, Some("RANGE_BUCKET(value,")).await;

    // 7. partitioning list shows the range clause
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "partitioning"])
        .assert()
        .success()
        .stdout(contains("PARTITION BY").and(contains("RANGE_BUCKET")));

    // TODO: uncomment after fixing `table partitioning add ingestion` command
    // // 8. Replace with ingestion time partitioning (default granularity: day)
    // Command::cargo_bin("bq-assist")
    //     .unwrap()
    //     .args(["table", &table_ref, "partitioning", "add", "ingestion"])
    //     .assert()
    //     .success();

    // // 9. Verify ingestion time partitioning in BigQuery metadata
    // assert_partitioning(env, TABLE, Some("_PARTITIONDATE")).await;

    // // 10. partitioning list shows the ingestion time clause
    // Command::cargo_bin("bq-assist")
    //     .unwrap()
    //     .args(["table", &table_ref, "partitioning"])
    //     .assert()
    //     .success()
    //     .stdout(contains("PARTITION BY").and(contains("_PARTITIONTIME")));

    // 11. Remove all partitioning
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "partitioning", "remove"])
        .assert()
        .success();

    // 12. Verify partitioning is gone from BigQuery metadata
    assert_partitioning(env, TABLE, None).await;

    // 13. partitioning list shows no partitioning
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "partitioning"])
        .assert()
        .success()
        .stdout(contains("No partitioning set on this table."));

    // Restore fixture so the test is idempotent across runs
    env.recreate_table("partitioning").await;
}
