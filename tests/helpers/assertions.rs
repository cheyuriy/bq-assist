#![allow(dead_code)]

use super::environment::TestEnvironment;

/// Assert that `table` has exactly the given columns (order-insensitive).
pub async fn assert_columns(env: &TestEnvironment, table: &str, expected: &[&str]) {
    let sql = format!(
        "SELECT column_name FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` \
         WHERE table_name = '{table}' ORDER BY ordinal_position",
        env.project, env.dataset
    );
    let actual = env.run_string_col_query(sql).await;
    let mut actual_sorted = actual.clone();
    actual_sorted.sort();
    let mut expected_sorted: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
    expected_sorted.sort();
    assert_eq!(
        expected_sorted, actual_sorted,
        "Column mismatch for `{table}`: expected {expected_sorted:?}, got {actual_sorted:?}"
    );
}

/// Assert that `table` exists in the test dataset.
pub async fn assert_table_exists(env: &TestEnvironment, table: &str) {
    let names = list_tables(env).await;
    assert!(
        names.contains(&table.to_string()),
        "Expected table `{table}` to exist in `{}.{}`",
        env.project,
        env.dataset
    );
}

/// Assert that `table` does NOT exist in the test dataset.
pub async fn assert_table_not_exists(env: &TestEnvironment, table: &str) {
    let names = list_tables(env).await;
    assert!(
        !names.contains(&table.to_string()),
        "Expected table `{table}` to not exist in `{}.{}`",
        env.project,
        env.dataset
    );
}

async fn list_tables(env: &TestEnvironment) -> Vec<String> {
    let sql = format!(
        "SELECT table_name FROM `{}.{}.INFORMATION_SCHEMA.TABLES`",
        env.project, env.dataset
    );
    env.run_string_col_query(sql).await
}
