#[path = "helpers/mod.rs"]
mod helpers;

use assert_cmd::Command;
use helpers::{
    assert_column_has_default, assert_column_nullable, assert_column_type, assert_columns,
    get_test_env,
};
use predicates::prelude::PredicateBooleanExt;
use predicates::str::contains;
use tokio::time::{Duration, sleep};

macro_rules! skip_or_env {
    () => {{
        let Some(env) = get_test_env().await else {
            eprintln!("Skipping integration test: BQ_TEST_PROJECT not set");
            return;
        };
        env
    }};
}

#[tokio::test]
async fn test_columns_remove_existing_column() {
    let Some(env) = get_test_env().await else {
        eprintln!("Skipping integration test: BQ_TEST_PROJECT not set");
        return;
    };

    assert_columns(env, "test_columns_remove", &["column_x", "column_y"]).await;

    let table_ref = format!("{}.{}.test_columns_remove", env.project, env.dataset);
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns", "remove", "column_x"])
        .assert()
        .success();

    assert_columns(env, "test_columns_remove", &["column_y"]).await;

    env.recreate_table("columns_remove").await;
}

// ── test_columns_list ─────────────────────────────────────────────────────────
// Uses test_columns_lifecycle (read-only — no modifications, no recreate needed).

#[tokio::test]
async fn test_columns_list() {
    let env = skip_or_env!();
    let table_ref = format!("{}.{}.test_columns_lifecycle", env.project, env.dataset);

    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns"])
        .assert()
        .success()
        .stdout(contains("created_at").and(contains("CURRENT_TIMESTAMP")));

    assert_column_nullable(env, "test_columns_lifecycle", "id", false).await;
    assert_column_nullable(env, "test_columns_lifecycle", "label", false).await;
    assert_column_nullable(env, "test_columns_lifecycle", "value", true).await;
    assert_column_has_default(env, "test_columns_lifecycle", "created_at", true).await;
    assert_column_has_default(env, "test_columns_lifecycle", "label", false).await;
}

// ── test_columns_add_remove ───────────────────────────────────────────────────
// Owns test_columns_add_remove exclusively.

#[tokio::test]
async fn test_columns_add_remove() {
    let env = skip_or_env!();
    let table = "test_columns_add_remove";
    let table_ref = format!("{}.{}.{table}", env.project, env.dataset);

    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns", "add", "new_tag", "string", "'default_tag'"])
        .assert()
        .success();

    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns", "add", "rating", "float64", "0.0"])
        .assert()
        .success();

    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns", "add", "score", "numeric", "0.0"])
        .assert()
        .success();

    assert_columns(
        env,
        table,
        &["id", "label", "value", "amount", "metadata", "is_active", "created_at",
          "new_tag", "rating", "score"],
    )
    .await;

    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns", "remove", "new_tag"])
        .assert()
        .success();

    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns", "remove", "rating"])
        .assert()
        .success();

    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns", "remove", "score"])
        .assert()
        .success();

    assert_columns(
        env,
        table,
        &["id", "label", "value", "amount", "metadata", "is_active", "created_at"],
    )
    .await;

    env.recreate_table("columns_add_remove").await;
}

// ── test_columns_rename ───────────────────────────────────────────────────────
// Owns test_columns_rename_lifecycle exclusively.

#[tokio::test]
async fn test_columns_rename() {
    let env = skip_or_env!();
    let table = "test_columns_rename_lifecycle";
    let table_ref = format!("{}.{}.{table}", env.project, env.dataset);

    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns", "rename", "label", "display_name"])
        .assert()
        .success();

    assert_columns(
        env,
        table,
        &["id", "display_name", "value", "amount", "metadata", "is_active", "created_at"],
    )
    .await;

    env.recreate_table("columns_rename_lifecycle").await;
}

// ── test_columns_cast ─────────────────────────────────────────────────────────
// Owns test_columns_cast exclusively.
// Slow-path casts sleep 10 s internally; 12 s gaps between invocations keep
// BigQuery's 5-DDL-ops-per-10s rolling quota from being exceeded.

#[tokio::test]
async fn test_columns_cast() {
    let env = skip_or_env!();
    let table = "test_columns_cast";
    let table_ref = format!("{}.{}.{table}", env.project, env.dataset);

    // NUMERIC → FLOAT64 (fast path: single ALTER COLUMN SET DATA TYPE)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns", "cast", "amount", "float64"])
        .assert()
        .success();

    sleep(Duration::from_secs(12)).await;

    // FLOAT64 → INT64 (slow path)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns", "cast", "value", "int64"])
        .assert()
        .success();

    sleep(Duration::from_secs(12)).await;

    // Verify updated types directly via INFORMATION_SCHEMA
    assert_column_type(env, table, "amount", "FLOAT64").await;
    assert_column_type(env, table, "value", "INT64").await;

    // Confirm the `table columns` command output reflects the new types
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns"])
        .assert()
        .success()
        .stdout(
            contains("INT64")
                .and(contains("FLOAT64"))
                .and(contains("STRING"))
                .and(contains("TIMESTAMP")),
        );

    env.recreate_table("columns_cast").await;
}
