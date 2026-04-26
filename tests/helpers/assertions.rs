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

/// Assert that `table` has a BigQuery option `option_name` whose value contains `expected_fragment`.
pub async fn assert_table_option(
    env: &TestEnvironment,
    table: &str,
    option_name: &str,
    expected_fragment: &str,
) {
    let sql = format!(
        "SELECT option_value \
         FROM `{}.{}.INFORMATION_SCHEMA.TABLE_OPTIONS` \
         WHERE table_name = '{table}' AND option_name = '{option_name}'",
        env.project, env.dataset
    );
    let rows = env.run_string_col_query(sql).await;
    let value = rows.first().unwrap_or_else(|| {
        panic!(
            "Option `{option_name}` not found for table `{table}` in `{}.{}`",
            env.project, env.dataset
        )
    });
    assert!(
        value.contains(expected_fragment),
        "Option `{option_name}` for `{table}`: expected value to contain `{expected_fragment}`, got: {value}"
    );
}

/// Assert that no rows in `table` match `where_clause`.
pub async fn assert_no_rows_where(env: &TestEnvironment, table: &str, where_clause: &str) {
    let sql = format!(
        "SELECT CAST(id AS STRING) FROM `{}.{}.{table}` WHERE {where_clause}",
        env.project, env.dataset
    );
    let rows = env.run_string_col_query(sql).await;
    assert!(
        rows.is_empty(),
        "Expected no rows in `{table}` matching `{where_clause}`, but got: {rows:?}"
    );
}

/// Assert that `column` in `table` has the given nullability.
pub async fn assert_column_nullable(env: &TestEnvironment, table: &str, column: &str, nullable: bool) {
    let sql = format!(
        "SELECT is_nullable FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` \
         WHERE table_name = '{table}' AND column_name = '{column}'",
        env.project, env.dataset
    );
    let rows = env.run_string_col_query(sql).await;
    let actual = rows
        .first()
        .unwrap_or_else(|| panic!("column `{column}` not found in `{table}`"));
    let expected_str = if nullable { "YES" } else { "NO" };
    assert_eq!(
        expected_str,
        actual.as_str(),
        "Nullable mismatch for `{table}.{column}`: expected {expected_str}, got {actual}"
    );
}

/// Assert whether `column` in `table` has a DEFAULT expression set.
pub async fn assert_column_has_default(env: &TestEnvironment, table: &str, column: &str, has_default: bool) {
    let sql = format!(
        "SELECT IFNULL(column_default, 'NULL') FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` \
         WHERE table_name = '{table}' AND column_name = '{column}'",
        env.project, env.dataset
    );
    let rows = env.run_string_col_query(sql).await;
    let actual = rows
        .first()
        .unwrap_or_else(|| panic!("column `{column}` not found in `{table}`"));
    let actual_has_default = actual != "NULL";
    assert_eq!(
        has_default,
        actual_has_default,
        "Default mismatch for `{table}.{column}`: expected has_default={has_default}, got column_default={actual}"
    );
}

/// Assert that `column` in `table` has the given BigQuery `data_type` (e.g. "STRING", "INT64").
pub async fn assert_column_type(env: &TestEnvironment, table: &str, column: &str, expected: &str) {
    let sql = format!(
        "SELECT data_type FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` \
         WHERE table_name = '{table}' AND column_name = '{column}'",
        env.project, env.dataset
    );
    let rows = env.run_string_col_query(sql).await;
    let actual = rows
        .first()
        .unwrap_or_else(|| panic!("column `{column}` not found in `{table}`"));
    assert_eq!(
        expected,
        actual.as_str(),
        "Type mismatch for `{table}.{column}`: expected {expected}, got {actual}"
    );
}

async fn list_tables(env: &TestEnvironment) -> Vec<String> {
    let sql = format!(
        "SELECT table_name FROM `{}.{}.INFORMATION_SCHEMA.TABLES`",
        env.project, env.dataset
    );
    env.run_string_col_query(sql).await
}
