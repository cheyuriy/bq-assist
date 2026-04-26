#[path = "helpers/mod.rs"]
mod helpers;

use assert_cmd::Command;
use chrono::Utc;
use helpers::get_test_env;
use predicates::prelude::PredicateBooleanExt;
use predicates::str::contains;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_queries_read_and_modify() {
    let Some(env) = get_test_env().await else {
        eprintln!("Skipping integration test: BQ_TEST_PROJECT not set");
        return;
    };

    let table = "test_queries";
    let join_table = "test_queries_join";
    let table_ref = format!("{}.{}.{table}", env.project, env.dataset);

    // Step 1: record the start of the observation window
    let from_ts = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // Step 2a: CREATE TABLE test_queries  (modify job #1)
    env.run_ddl(format!(
        "CREATE OR REPLACE TABLE `{}.{}.{table}` (
            id    INT64   NOT NULL,
            name  STRING  NOT NULL,
            value FLOAT64
        )",
        env.project, env.dataset
    ))
    .await;

    // Step 2b: CREATE TABLE test_queries_join  (destination = join table, not counted)
    env.run_ddl(format!(
        "CREATE OR REPLACE TABLE `{}.{}.{join_table}` (
            id       INT64  NOT NULL,
            category STRING
        )",
        env.project, env.dataset
    ))
    .await;

    // Step 3: INSERT into test_queries  (modify job #2)
    env.run_ddl(format!(
        "INSERT INTO `{}.{}.{table}` (id, name, value) VALUES
            (1, 'alpha', 10.0),
            (2, 'beta',  20.0)",
        env.project, env.dataset
    ))
    .await;

    // (seed join table data — destination = join table, not counted for test_queries)
    env.run_ddl(format!(
        "INSERT INTO `{}.{}.{join_table}` (id, category) VALUES
            (1, 'cat_a'),
            (2, 'cat_b')",
        env.project, env.dataset
    ))
    .await;

    // Step 4: single-table SELECT  (read job #1 — ARRAY_LENGTH(referenced_tables) = 1)
    env.run_ddl(format!(
        "SELECT id, name, value FROM `{}.{}.{table}`",
        env.project, env.dataset
    ))
    .await;

    // Step 5: JOIN SELECT  (read job #2 — referenced_tables includes both tables)
    env.run_ddl(format!(
        "SELECT t.id, t.name, t.value, j.category
         FROM `{}.{}.{table}` t
         JOIN `{}.{}.{join_table}` j ON t.id = j.id",
        env.project, env.dataset, env.project, env.dataset
    ))
    .await;

    // Step 6: wait for INFORMATION_SCHEMA.JOBS propagation
    sleep(Duration::from_secs(15)).await;
    let to_ts = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // Step 7: queries read --period → 2 SELECT queries
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "queries", "read", "--period", "5m"])
        .assert()
        .success()
        .stdout(contains("SELECT").and(contains(join_table)));

    // Step 8: queries read --from --to → same 2 results
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args([
            "table", &table_ref, "queries", "read",
            "--from", &from_ts,
            "--to", &to_ts,
        ])
        .assert()
        .success()
        .stdout(contains("SELECT").and(contains(join_table)));

    // Step 9: queries read --single → 1 result (only the single-table SELECT)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args([
            "table", &table_ref, "queries", "read",
            "--period", "5m", "--single",
        ])
        .assert()
        .success()
        .stdout(contains("SELECT").and(contains(join_table).not()));

    // Step 10: queries modify --period → 2 queries (CREATE_TABLE + INSERT)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "queries", "modify", "--period", "5m"])
        .assert()
        .success()
        .stdout(contains("CREATE_TABLE").and(contains("INSERT")));

    // Step 11: queries modify --from --to → same 2 results
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args([
            "table", &table_ref, "queries", "modify",
            "--from", &from_ts,
            "--to", &to_ts,
        ])
        .assert()
        .success()
        .stdout(contains("CREATE_TABLE").and(contains("INSERT")));

    // Step 12: queries modify --query-type CREATE_TABLE → 1 result (DDL only)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args([
            "table", &table_ref, "queries", "modify",
            "--period", "5m",
            "--query-type", "CREATE_TABLE",
        ])
        .assert()
        .success()
        .stdout(contains("CREATE_TABLE").and(contains("INSERT").not()));

    // Cleanup
    env.run_ddl(format!(
        "DROP TABLE IF EXISTS `{}.{}.{table}`",
        env.project, env.dataset
    ))
    .await;
    env.run_ddl(format!(
        "DROP TABLE IF EXISTS `{}.{}.{join_table}`",
        env.project, env.dataset
    ))
    .await;
}
