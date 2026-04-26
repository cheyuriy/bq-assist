#[path = "helpers/mod.rs"]
mod helpers;

use assert_cmd::Command;
use helpers::get_test_env;
use predicates::str::contains;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_copy_lifecycle() {
    let Some(env) = get_test_env().await else {
        eprintln!("Skipping integration test: BQ_TEST_PROJECT not set");
        return;
    };
    let table = "test_copy";
    let table_ref = format!("{}.{}.{table}", env.project, env.dataset);
    let copy_named = "test_copy_named";
    let copy_untracked = "test_copy_untracked";

    // Step 2: verify no copies tracked yet
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "copy"])
        .assert()
        .success()
        .stdout(contains("No copies are tracked for this table."));

    // Step 3: add three copies
    // 3a. default options (auto-generated name, tracked)
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "copy", "add"])
        .assert()
        .success();

    // 3b. named tracked copy
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "copy", "add", copy_named])
        .assert()
        .success();

    // 3c. untracked copy with special name
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "copy", "add", copy_untracked, "--no-track"])
        .assert()
        .success();

    // BQ DDL quota: 5 ops / 10 s rolling window
    sleep(Duration::from_secs(12)).await;

    // Step 4: verify exactly 2 tracked copies via INFORMATION_SCHEMA
    let labeled_sql = format!(
        "SELECT table_name \
         FROM `{}.{}.INFORMATION_SCHEMA.TABLE_OPTIONS` \
         WHERE option_name = 'labels' \
           AND CONTAINS_SUBSTR(option_value, 'bq_assist_copy_id')",
        env.project, env.dataset
    );
    let labeled = env.run_string_col_query(labeled_sql).await;
    assert_eq!(
        2,
        labeled.len(),
        "Expected 2 tracked copies after add, got {}: {:?}",
        labeled.len(),
        labeled
    );

    // Step 5: remove the named tracked copy
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "copy", "remove", copy_named])
        .assert()
        .success();

    // Step 6: verify exactly 1 tracked copy remains
    let labeled_sql2 = format!(
        "SELECT table_name \
         FROM `{}.{}.INFORMATION_SCHEMA.TABLE_OPTIONS` \
         WHERE option_name = 'labels' \
           AND CONTAINS_SUBSTR(option_value, 'bq_assist_copy_id')",
        env.project, env.dataset
    );
    let labeled2 = env.run_string_col_query(labeled_sql2).await;
    assert_eq!(
        1,
        labeled2.len(),
        "Expected 1 tracked copy after remove, got {}: {:?}",
        labeled2.len(),
        labeled2
    );

    // Step 7: direct BQ query — exactly 2 physical copies (1 tracked + 1 untracked)
    let tables_sql = format!(
        "SELECT table_name \
         FROM `{}.{}.INFORMATION_SCHEMA.TABLES` \
         WHERE table_name LIKE 'test_copy_%'",
        env.project, env.dataset
    );
    let tables = env.run_string_col_query(tables_sql).await;
    assert_eq!(
        2,
        tables.len(),
        "Expected 2 physical copies (1 tracked + 1 untracked), got {}: {:?}",
        tables.len(),
        tables
    );
    assert!(
        tables.contains(&copy_untracked.to_string()),
        "Expected {copy_untracked} to still exist in BigQuery, got: {:?}",
        tables
    );

    // Step 8: cleanup
    // Remove the remaining tracked copy by name (resolved from the labeled2 query result)
    let remaining_tracked = labeled2.first().expect("expected 1 remaining tracked copy");
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["table", &table_ref, "copy", "remove", remaining_tracked])
        .assert()
        .success();

    // Drop the untracked copy directly (not tracked, so copy remove cannot find it)
    env.run_ddl(format!(
        "DROP TABLE IF EXISTS `{}.{}.{copy_untracked}`",
        env.project, env.dataset
    ))
    .await;

    // Restore source table fixture
    env.recreate_table("copy").await;
}
