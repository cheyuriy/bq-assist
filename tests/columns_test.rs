#[path = "helpers/mod.rs"]
mod helpers;

use assert_cmd::Command;
use helpers::{assert_columns, get_test_env};

#[tokio::test]
async fn test_columns_remove_existing_column() {
    let Some(env) = get_test_env().await else {
        eprintln!("Skipping integration test: BQ_TEST_PROJECT not set");
        return;
    };

    // Pre-condition: fixture table has both columns
    assert_columns(env, "test_columns_remove", &["column_x", "column_y"]).await;

    let table_ref = format!(
        "{}.{}.test_columns_remove",
        env.project, env.dataset
    );
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "columns", "remove", "column_x"])
        .assert()
        .success();

    // Post-condition: only column_y remains
    assert_columns(env, "test_columns_remove", &["column_y"]).await;

    // Restore fixture so the next test in this file starts from a clean state
    env.recreate_table("columns_remove").await;
}
