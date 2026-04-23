use crate::bigquery::client;
use crate::bigquery::executor;
use crate::bigquery::queries;
use crate::bigquery::validators;
use crate::models::bigquery::stats::{
    BillingMode, DatasetBasicInfo, DatasetContentInfo, DatasetExpiryInfo, DatasetStatsData,
    OtherOption, TableSizeEntry,
};
use crate::models::config::AppConfig;
use crate::models::bigquery::references::DatasetRef;
use chrono::{DateTime, Utc};
use google_cloud_bigquery::client::Client;

const EXCLUDED_OPTIONS: &[&str] = &[
    "storage_billing_model",
    "is_primary",
    "primary_replica",
    "default_partition_expiration_days",
    "default_table_expiration_days",
    "max_time_travel_hours",
    "location",
];

pub async fn report(
    config: AppConfig,
    dataset_ref: &DatasetRef,
) -> Result<DatasetStatsData, Box<dyn std::error::Error>> {
    let region = config.region.clone();
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = dataset_ref
        .project
        .as_deref()
        .unwrap_or(&project_id)
        .to_string();
    let dataset = dataset_ref.dataset.clone();

    validators::ensure_dataset_exists(&bq_client, &project, &dataset).await?;

    let fqn = format!("{}.{}", project, dataset);

    let info_sql = queries::StatsQueries::dataset_info(&region, &project, &dataset);
    let (creation_ms, last_modified_ms, location) =
        fetch_dataset_info(&bq_client, &project_id, info_sql).await?;

    let options_sql = queries::StatsQueries::dataset_options(&region, &project, &dataset);
    let raw_options = fetch_options(&bq_client, &project_id, options_sql).await?;

    let tables_sql = queries::StatsQueries::dataset_tables(&project, &dataset);
    let type_counts = fetch_table_counts(&bq_client, &project_id, tables_sql).await?;

    let storage_sql = queries::StatsQueries::dataset_storage_aggregate(&region, &project, &dataset);
    let storage = fetch_storage_aggregate(&bq_client, &project_id, storage_sql).await?;

    let sizes_sql = queries::StatsQueries::dataset_table_sizes(&region, &project, &dataset);
    let table_sizes = fetch_table_sizes(&bq_client, &project_id, sizes_sql).await?;

    // Parse options
    let mut billing_mode = BillingMode::Logical;
    let mut is_primary: Option<bool> = None;
    let mut primary_replica: Option<String> = None;
    let mut default_partition_expiration_days: Option<f64> = None;
    let mut default_table_expiration_days: Option<f64> = None;
    let mut time_travel_hours: Option<i64> = None;
    let mut other_options: Vec<OtherOption> = Vec::new();

    for (name, value) in &raw_options {
        match name.as_str() {
            "storage_billing_model" => {
                billing_mode = BillingMode::parse(value);
            }
            "is_primary" => {
                is_primary = Some(value.trim_matches('"').eq_ignore_ascii_case("true"));
            }
            "primary_replica" => {
                primary_replica = Some(value.trim_matches('"').to_string());
            }
            "default_partition_expiration_days" => {
                default_partition_expiration_days = value.trim_matches('"').parse().ok();
            }
            "default_table_expiration_days" => {
                default_table_expiration_days = value.trim_matches('"').parse().ok();
            }
            "max_time_travel_hours" => {
                time_travel_hours = value.trim_matches('"').parse().ok();
            }
            _ if EXCLUDED_OPTIONS.contains(&name.as_str()) => {}
            _ => {
                other_options.push(OtherOption {
                    name: name.clone(),
                    value: value.clone(),
                });
            }
        }
    }

    let get_count = |label: &str| -> i64 {
        type_counts
            .iter()
            .find(|(t, _)| t.eq_ignore_ascii_case(label))
            .map(|(_, c)| *c)
            .unwrap_or(0)
    };
    let tables = get_count("BASE TABLE");
    let views = get_count("VIEW");
    let materialized_views = get_count("MATERIALIZED VIEW");
    let clones = get_count("CLONE");
    let snapshots = get_count("SNAPSHOT");
    let external = get_count("EXTERNAL");
    let total = tables + views + materialized_views + clones + snapshots + external;

    let content_last_modified = storage
        .last_modified_ms
        .filter(|&ms| ms > 0)
        .and_then(DateTime::<Utc>::from_timestamp_millis);

    Ok(DatasetStatsData {
        basic: DatasetBasicInfo {
            fqn,
            location,
            created: DateTime::<Utc>::from_timestamp_millis(creation_ms),
            updated: DateTime::<Utc>::from_timestamp_millis(last_modified_ms),
            billing_mode,
            is_primary,
            primary_replica,
        },
        expiry: DatasetExpiryInfo {
            default_partition_expiration_days,
            default_table_expiration_days,
            time_travel_hours,
        },
        content: DatasetContentInfo {
            total,
            tables,
            views,
            materialized_views,
            clones,
            snapshots,
            external,
            last_modified: content_last_modified,
            active_logical_bytes: storage.active_logical_bytes,
            long_term_logical_bytes: storage.long_term_logical_bytes,
            total_logical_bytes: storage.total_logical_bytes,
            active_physical_bytes: storage.active_physical_bytes,
            long_term_physical_bytes: storage.long_term_physical_bytes,
            total_physical_bytes: storage.total_physical_bytes,
        },
        table_sizes,
        other_options,
    })
}

async fn fetch_dataset_info(
    client: &Client,
    project_id: &str,
    sql: String,
) -> Result<(i64, i64, String), Box<dyn std::error::Error>> {
    let result = executor::query_first(client, project_id, sql, |row| {
        let creation_ms = row.column::<i64>(0).unwrap_or(0);
        let last_modified_ms = row.column::<i64>(1).unwrap_or(0);
        let location = row.column::<String>(2).unwrap_or_default();
        (creation_ms, last_modified_ms, location)
    })
    .await?;
    Ok(result.unwrap_or((0, 0, String::new())))
}

async fn fetch_options(
    client: &Client,
    project_id: &str,
    sql: String,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    executor::query_collect(client, project_id, sql, |row| {
        (
            row.column::<String>(0).unwrap_or_default(),
            row.column::<String>(1).unwrap_or_default(),
        )
    })
    .await
    .map_err(Into::into)
}

async fn fetch_table_counts(
    client: &Client,
    project_id: &str,
    sql: String,
) -> Result<Vec<(String, i64)>, Box<dyn std::error::Error>> {
    executor::query_collect(client, project_id, sql, |row| {
        let table_type = row.column::<String>(0).unwrap_or_default();
        let count = row.column::<i64>(1).unwrap_or(0);
        (table_type, count)
    })
    .await
    .map_err(Into::into)
}

struct StorageAggregate {
    active_logical_bytes: i64,
    long_term_logical_bytes: i64,
    total_logical_bytes: i64,
    active_physical_bytes: i64,
    long_term_physical_bytes: i64,
    total_physical_bytes: i64,
    last_modified_ms: Option<i64>,
}

async fn fetch_storage_aggregate(
    client: &Client,
    project_id: &str,
    sql: String,
) -> Result<StorageAggregate, Box<dyn std::error::Error>> {
    let result = executor::query_first(client, project_id, sql, |row| StorageAggregate {
        active_logical_bytes: row.column::<i64>(0).unwrap_or(0),
        long_term_logical_bytes: row.column::<i64>(1).unwrap_or(0),
        total_logical_bytes: row.column::<i64>(2).unwrap_or(0),
        active_physical_bytes: row.column::<i64>(3).unwrap_or(0),
        long_term_physical_bytes: row.column::<i64>(4).unwrap_or(0),
        total_physical_bytes: row.column::<i64>(5).unwrap_or(0),
        last_modified_ms: row.column::<i64>(6).ok(),
    })
    .await?;
    Ok(result.unwrap_or(StorageAggregate {
        active_logical_bytes: 0,
        long_term_logical_bytes: 0,
        total_logical_bytes: 0,
        active_physical_bytes: 0,
        long_term_physical_bytes: 0,
        total_physical_bytes: 0,
        last_modified_ms: None,
    }))
}

async fn fetch_table_sizes(
    client: &Client,
    project_id: &str,
    sql: String,
) -> Result<Vec<TableSizeEntry>, Box<dyn std::error::Error>> {
    executor::query_collect(client, project_id, sql, |row| TableSizeEntry {
        table_name: row.column::<String>(0).unwrap_or_default(),
        logical_bytes: row.column::<i64>(1).unwrap_or(0),
        physical_bytes: row.column::<i64>(2).unwrap_or(0),
    })
    .await
    .map_err(Into::into)
}
