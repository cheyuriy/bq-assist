#[path = "helpers/mod.rs"]
mod helpers;

use assert_cmd::Command;
use chrono::Utc;
use helpers::get_test_env;
use predicates::str::contains;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_snapshot_lifecycle() {
    let Some(env) = get_test_env().await else {
        eprintln!("Skipping integration test: BQ_TEST_PROJECT not set");
        return;
    };
    let table = "test_snapshot";
    let table_ref = format!("{}.{}.{table}", env.project, env.dataset);
    let snap_rewind = "test_snapshot_rewind";
    let snap_timestamp = "test_snapshot_timestamp";
    let snap_untracked = "test_snapshot_untracked";

    // Step 2: verify no snapshots tracked yet
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "snapshots"])
        .assert()
        .success()
        .stdout(contains("No snapshots are tracked for this table."));

    // Sleep to establish a valid time-travel point for --rewind and --timestamp
    sleep(Duration::from_secs(15)).await;

    // Capture a point 15 s in the past for the --timestamp snapshot
    let ts_15s_ago = (Utc::now() - chrono::Duration::seconds(15))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();

    // Step 3: add four snapshots
    // 3a. Default options (auto-generated name, tracked)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "snapshots", "add"])
        .assert()
        .success();

    // 3b. Named, with --rewind 15s (tracked)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "snapshots", "add", snap_rewind, "--rewind", "15s"])
        .assert()
        .success();

    // 3c. Named, with --timestamp set to 15 s ago (tracked)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args([
            "table", &table_ref, "snapshots", "add", snap_timestamp,
            "--timestamp", &ts_15s_ago,
        ])
        .assert()
        .success();

    // 3d. Untracked with special name (easier to drop manually later)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "snapshots", "add", snap_untracked, "--no-track"])
        .assert()
        .success();

    // BQ DDL quota: 5 ops / 10 s rolling window
    sleep(Duration::from_secs(12)).await;

    // Step 4: verify exactly 3 tracked snapshots (3a + 3b + 3c; 3d is untracked)
    let labeled_sql = format!(
        "SELECT table_name \
         FROM `{}.{}.INFORMATION_SCHEMA.TABLE_OPTIONS` \
         WHERE option_name = 'labels' \
           AND CONTAINS_SUBSTR(option_value, 'bq_assist_snapshot_id')",
        env.project, env.dataset
    );
    let labeled = env.run_string_col_query(labeled_sql).await;
    assert_eq!(
        3,
        labeled.len(),
        "Expected 3 tracked snapshots after add, got {}: {:?}",
        labeled.len(),
        labeled
    );

    // Step 5: remove the rewind snapshot
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "snapshots", "remove", snap_rewind])
        .assert()
        .success();

    // Step 6: verify exactly 2 tracked snapshots remain
    let labeled_sql2 = format!(
        "SELECT table_name \
         FROM `{}.{}.INFORMATION_SCHEMA.TABLE_OPTIONS` \
         WHERE option_name = 'labels' \
           AND CONTAINS_SUBSTR(option_value, 'bq_assist_snapshot_id')",
        env.project, env.dataset
    );
    let labeled2 = env.run_string_col_query(labeled_sql2).await;
    assert_eq!(
        2,
        labeled2.len(),
        "Expected 2 tracked snapshots after remove, got {}: {:?}",
        labeled2.len(),
        labeled2
    );

    // Step 7: direct BQ query — exactly 3 physical snapshots (2 tracked + 1 untracked)
    let tables_sql = format!(
        "SELECT table_name \
         FROM `{}.{}.INFORMATION_SCHEMA.TABLES` \
         WHERE table_name LIKE 'test_snapshot_%' AND table_type = 'SNAPSHOT'",
        env.project, env.dataset
    );
    let tables = env.run_string_col_query(tables_sql).await;
    assert_eq!(
        3,
        tables.len(),
        "Expected 3 physical snapshots (2 tracked + 1 untracked), got {}: {:?}",
        tables.len(),
        tables
    );
    assert!(
        tables.contains(&snap_untracked.to_string()),
        "Expected {snap_untracked} to still exist in BigQuery, got: {:?}",
        tables
    );

    // Step 8: cleanup
    // Remove remaining tracked snapshots by name (resolved from the labeled2 query result)
    for name in &labeled2 {
        Command::cargo_bin("bq-assist")
            .unwrap()
            .args(["table", &table_ref, "snapshots", "remove", name])
            .assert()
            .success();
    }

    // Drop the untracked snapshot directly (not tracked, so snapshots remove cannot find it)
    env.run_ddl(format!(
        "DROP SNAPSHOT TABLE IF EXISTS `{}.{}.{snap_untracked}`",
        env.project, env.dataset
    ))
    .await;

    // Restore source table fixture
    env.recreate_table("snapshot").await;
}
