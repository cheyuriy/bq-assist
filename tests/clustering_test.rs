#[path = "helpers/mod.rs"]
mod helpers;

use assert_cmd::Command;
use helpers::{assert_clustering, get_test_env};
use predicates::prelude::PredicateBooleanExt;
use predicates::str::contains;

const TABLE: &str = "test_clustering";

#[tokio::test]
async fn test_clustering_lifecycle() {
    let Some(env) = get_test_env().await else {
        eprintln!("Skipping integration test: BQ_TEST_PROJECT not set");
        return;
    };

    let table_ref = format!("{}.{}.{TABLE}", env.project, env.dataset);

    // 1. No clustering initially
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "clustering"])
        .assert()
        .success()
        .stdout(contains("No clustering set on this table."));

    // 2. Add 2 fields
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "clustering", "add", "name", "category"])
        .assert()
        .success();

    assert_clustering(env, TABLE, &["name", "category"]).await;

    // 3. clustering list shows the 2 fields
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "clustering"])
        .assert()
        .success()
        .stdout(contains("name").and(contains("category")));

    // 4. Replace with 2 different fields
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "clustering", "add", "id", "score"])
        .assert()
        .success();

    assert_clustering(env, TABLE, &["id", "score"]).await;

    // 5. Attempt to set 5 fields — clap rejects (num_args = 1..=4)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args([
            "table", &table_ref, "clustering", "add",
            "name", "category", "id", "score", "active",
        ])
        .assert()
        .failure();

    // 6. Remove clustering
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "clustering", "remove"])
        .assert()
        .success();

    assert_clustering(env, TABLE, &[]).await;

    // Restore fixture so the test is idempotent across runs
    env.recreate_table("clustering").await;
}
