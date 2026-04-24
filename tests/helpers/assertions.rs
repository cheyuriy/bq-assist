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

/// Assert that `table` has exactly the given clustering fields in the given order.
pub async fn assert_clustering(env: &TestEnvironment, table: &str, expected: &[&str]) {
    let sql = format!(
        "SELECT column_name FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` \
         WHERE table_name = '{table}' AND clustering_ordinal_position IS NOT NULL \
         ORDER BY clustering_ordinal_position",
        env.project, env.dataset
    );
    let actual = env.run_string_col_query(sql).await;
    let expected_vec: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
    assert_eq!(
        expected_vec, actual,
        "Clustering mismatch for `{table}`: expected {expected_vec:?}, got {actual:?}"
    );
}

/// Assert the table's DDL contains (or lacks) a PARTITION BY clause.
/// Pass `Some(needle)` to check that the clause includes that substring;
/// pass `None` to assert no partitioning is present.
pub async fn assert_partitioning(env: &TestEnvironment, table: &str, expected: Option<&str>) {
    let sql = format!(
        "SELECT ddl FROM `{}.{}.INFORMATION_SCHEMA.TABLES` WHERE table_name = '{table}'",
        env.project, env.dataset
    );
    let rows = env.run_string_col_query(sql).await;
    let ddl = rows.first().unwrap_or_else(|| panic!("table `{table}` not found"));
    match expected {
        None => assert!(
            !ddl.to_uppercase().contains("PARTITION BY"),
            "Expected no PARTITION BY in `{table}` DDL, got:\n{ddl}"
        ),
        Some(needle) => assert!(
            ddl.contains(needle),
            "Expected `{table}` DDL to contain `{needle}`, got:\n{ddl}"
        ),
    }
}

async fn list_tables(env: &TestEnvironment) -> Vec<String> {
    let sql = format!(
        "SELECT table_name FROM `{}.{}.INFORMATION_SCHEMA.TABLES`",
        env.project, env.dataset
    );
    env.run_string_col_query(sql).await
}
