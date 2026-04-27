#[path = "helpers/mod.rs"]
mod helpers;

use assert_cmd::Command;
use helpers::{assert_dataset_option, assert_dataset_option_absent, get_test_env};
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_dataset_options() {
    let Some(env) = get_test_env().await else {
        eprintln!("Skipping integration test: BQ_TEST_PROJECT not set");
        return;
    };

    let dataset_ref = format!("{}.{}", env.project, env.dataset);

    // 1. Set description
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["dataset", &dataset_ref, "options", "description", "test description"])
        .assert()
        .success();

    // 2. Set labels
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["dataset", &dataset_ref, "options", "labels", "tag_one:x,tag_two:y"])
        .assert()
        .success();

    sleep(Duration::from_secs(15)).await;

    // 3. Verify both are reflected in BigQuery
    assert_dataset_option(env, "description", "test description").await;
    assert_dataset_option(env, "labels", "tag_one").await;
    assert_dataset_option(env, "labels", "tag_two").await;

    // 4. Unset description and labels via "null"
    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["dataset", &dataset_ref, "options", "description", "null"])
        .assert()
        .success();

    Command::cargo_bin("bq-assist")
        .unwrap()
        .args(["dataset", &dataset_ref, "options", "labels", "null"])
        .assert()
        .success();

    // 5. Verify both options are gone
    assert_dataset_option_absent(env, "description").await;
    assert_dataset_option_absent(env, "labels").await;
}
