#[path = "helpers/mod.rs"]
mod helpers;

use assert_cmd::Command;
use chrono::Utc;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;
use helpers::{
    assert_no_rows_where, assert_table_exists, assert_table_not_exists, assert_table_option,
    get_test_env,
};
use std::time::Duration;

#[tokio::test]
async fn test_table_rename() {
    let Some(env) = get_test_env().await else {
        eprintln!("Skipping integration test: BQ_TEST_PROJECT not set");
        return;
    };

    let table = "test_rename";
    let table_new = "test_rename_new";
    let table_ref = format!("{}.{}.{table}", env.project, env.dataset);

    // 1. Table exists before rename
    assert_table_exists(env, table).await;

    // 2. Rename the table
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "rename", table_new])
        .assert()
        .success();

    // 3. Old name is gone, new name exists
    assert_table_not_exists(env, table).await;
    assert_table_exists(env, table_new).await;

    // Cleanup: drop renamed table and restore fixture
    let drop_sql = format!("DROP TABLE IF EXISTS `{}.{}.{table_new}`", env.project, env.dataset);
    env.run_ddl(drop_sql).await;
    env.recreate_table("rename").await;
}

#[tokio::test]
async fn test_table_options() {
    let Some(env) = get_test_env().await else {
        eprintln!("Skipping integration test: BQ_TEST_PROJECT not set");
        return;
    };

    let table = "test_options";
    let table_ref = format!("{}.{}.{table}", env.project, env.dataset);
    let expiration_ts = (Utc::now() + chrono::Duration::hours(1))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();

    // 1. Set expiration_timestamp
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "options", "expiration_timestamp", &expiration_ts])
        .assert()
        .success();

    // 2. Set labels
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "options", "labels", "tag_one:x,tag_two:y"])
        .assert()
        .success();

    // 3. Set description
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "options", "description", "some test description"])
        .assert()
        .success();

    // 4. Verify options are reflected in BigQuery metadata
    assert_table_option(env, table, "expiration_timestamp", &expiration_ts[..10]).await;
    assert_table_option(env, table, "description", "some test description").await;

    // 5. Wrong expiration_timestamp format fails client-side validation
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "options", "expiration_timestamp", "not-a-timestamp"])
        .assert()
        .failure();

    // 6. Malformed labels value (embedded quote breaks generated SQL) fails at BigQuery level
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "options", "labels", r#"k:val"ue"#])
        .assert()
        .failure();

    // Restore fixture
    env.recreate_table("options").await;
}

#[tokio::test]
async fn test_table_rewind() {
    let Some(env) = get_test_env().await else {
        eprintln!("Skipping integration test: BQ_TEST_PROJECT not set");
        return;
    };

    let table = "test_rewind";
    let table_ref = format!("{}.{}.{table}", env.project, env.dataset);

    // 1. Wait to establish a time-travel point before the insert
    tokio::time::sleep(Duration::from_secs(15)).await;

    // 2. Insert a new row after the time-travel point
    let insert_sql = format!(
        "INSERT INTO `{}.{}.{table}` (id, label) VALUES (999, 'inserted_after')",
        env.project, env.dataset
    );
    let req = QueryRequest { query: insert_sql, ..Default::default() };
    let mut iter = env.client.query::<Row>(&env.project, req).await.unwrap();
    while iter.next().await.unwrap().is_some() {}

    // 3. Wait briefly so the insert is committed
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 4. Rewind the table 15 seconds back (before the insert)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "restore", "--rewind", "15s"])
        .assert()
        .success();

    // 5. The inserted row must no longer be present
    assert_no_rows_where(env, table, "id = 999").await;

    // Restore fixture
    env.recreate_table("rewind").await;
}
