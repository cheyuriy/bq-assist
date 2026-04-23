use crate::bigquery::client;
use crate::bigquery::executor;
use crate::bigquery::queries;
use crate::bigquery::validators;
use crate::cli::TimeBins;
use crate::errors::ValidationError;
use crate::models::bigquery::stats::{
    BasicInfo, BillingMode, BinCount, CategoryStats, ClusteringInfo, ColumnMetaInfo, DeepStats,
    ExternalInfo, OtherOption, PartitioningInfo, SizeInfo, TableStatsData,
};
use crate::models::config::AppConfig;
use crate::models::bigquery::references::TableRef;
use chrono::{DateTime, Utc};
use google_cloud_bigquery::client::Client;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;
use regex::Regex;
use std::io::{self, Write as IoWrite};

const STORAGE_TYPES: &[&str] = &["BASE TABLE", "CLONE", "SNAPSHOT", "MATERIALIZED VIEW"];
const NON_STORAGE_TYPES: &[&str] = &["VIEW", "EXTERNAL"];

pub async fn report(config: AppConfig, table_ref: &TableRef, with_ddl: bool) -> Result<TableStatsData, Box<dyn std::error::Error>> {
    let region = config.region.clone();
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref
        .project
        .as_deref()
        .unwrap_or(&project_id)
        .to_string();
    validators::ensure_table_exists(&bq_client, &project, &table_ref.dataset, &table_ref.table).await?;
    let dataset = table_ref.dataset.clone();
    let table = table_ref.table.clone();
    let fqn = format!("{}.{}.{}", project, dataset, table);

    let info_sql = queries::StatsQueries::table_info(&project, &dataset, &table);
    let (table_type, creation_time_ms, ddl) = fetch_table_info(&bq_client, &project_id, info_sql)
        .await?
        .ok_or_else(|| ValidationError(format!("Table `{fqn}` not found")))?;

    let options_sql = queries::StatsQueries::table_options(&project, &dataset, &table);
    let raw_options = fetch_options(&bq_client, &project_id, options_sql).await?;

    let mut require_partition_filter: Option<bool> = None;
    let mut other_options: Vec<OtherOption> = Vec::new();
    for (name, value) in &raw_options {
        if name == "require_partition_filter" {
            require_partition_filter = Some(value.trim_matches('"').eq_ignore_ascii_case("true"));
        } else {
            other_options.push(OtherOption {
                name: name.clone(),
                value: value.clone(),
            });
        }
    }

    let snap_sql = queries::StatsQueries::child_snapshots(&region, &project, &dataset, &table);
    let child_snapshots = fetch_table_list(&bq_client, &project_id, snap_sql).await?;
    let clones_sql = queries::StatsQueries::child_clones(&project, &dataset, &table);
    let child_clones = fetch_table_list(&bq_client, &project_id, clones_sql).await?;

    let origin = if matches!(table_type.as_str(), "CLONE" | "SNAPSHOT") {
        Regex::new(r"(?i)\bCLONE\s+`([^`]+)`")
            .unwrap()
            .captures(&ddl)
            .map(|c| c[1].to_string())
    } else {
        None
    };

    let external = if table_type == "EXTERNAL" {
        Some(extract_external(&ddl, &raw_options))
    } else {
        None
    };

    let mut basic = BasicInfo {
        fqn: fqn.clone(),
        table_type: table_type.clone(),
        created: DateTime::<Utc>::from_timestamp_millis(creation_time_ms),
        updated: None,
        origin,
        external,
        snapshots: child_snapshots,
        clones: child_clones,
    };

    let size: Option<SizeInfo> = if STORAGE_TYPES.contains(&table_type.as_str()) {
        let storage_sql = queries::StatsQueries::table_storage(&region, &project, &dataset, &table);
        let storage = fetch_storage(&bq_client, &project_id, storage_sql).await?;
        let billing_sql = queries::StatsQueries::dataset_billing_mode(&region, &project, &dataset);
        let billing = fetch_billing_mode(&bq_client, &project_id, billing_sql).await?;

        if let Some(s) = storage {
            basic.updated = DateTime::<Utc>::from_timestamp_millis(s.storage_last_modified_ms);
            Some(SizeInfo {
                total_rows: s.total_rows,
                active_logical: s.active_logical,
                long_term_logical: s.long_term_logical,
                total_logical: s.total_logical,
                active_physical: s.active_physical,
                long_term_physical: s.long_term_physical,
                total_physical: s.total_physical,
                time_travel: s.time_travel,
                billing_mode: billing.unwrap_or(BillingMode::Logical),
            })
        } else {
            None
        }
    } else {
        None
    };

    let partitioning = if !NON_STORAGE_TYPES.contains(&table_type.as_str()) {
        let part_clause = Regex::new(r"(?i)PARTITION\s+BY\s+([^\n;]+)")
            .unwrap()
            .captures(&ddl)
            .map(|c| format!("PARTITION BY {}", c[1].trim()));
        let column = part_clause.as_deref().and_then(extract_partition_column);

        let (partitions_count, total_bytes) = if part_clause.is_some() {
            let parts_sql = queries::StatsQueries::partitions(&project, &dataset, &table);
            fetch_partitions(&bq_client, &project_id, parts_sql).await?
        } else {
            (None, None)
        };
        let avg_partition_bytes = match (partitions_count, total_bytes) {
            (Some(n), Some(b)) if n > 0 => Some((b as f64 / n as f64) as i64),
            _ => None,
        };

        Some(PartitioningInfo {
            column,
            clause: part_clause,
            partitions_count,
            avg_partition_bytes,
            require_partition_filter,
        })
    } else {
        None
    };

    let clustering = if !NON_STORAGE_TYPES.contains(&table_type.as_str()) {
        let fields = Regex::new(r"(?i)CLUSTER\s+BY\s+([^\n;]+)")
            .unwrap()
            .captures(&ddl)
            .map(|c| {
                c[1].split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Some(ClusteringInfo { fields })
    } else {
        None
    };

    Ok(TableStatsData {
        basic,
        size,
        partitioning,
        clustering,
        other_options,
        ddl: if with_ddl { Some(ddl) } else { None },
    })
}

async fn fetch_table_info(
    client: &Client,
    project_id: &str,
    sql: String,
) -> Result<Option<(String, i64, String)>, Box<dyn std::error::Error>> {
    executor::query_first(client, project_id, sql, |row| {
        (
            row.column::<String>(0).unwrap(),
            row.column::<i64>(1).unwrap(),
            row.column::<String>(2).unwrap(),
        )
    })
    .await
    .map_err(Into::into)
}

async fn fetch_options(
    client: &Client,
    project_id: &str,
    sql: String,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    executor::query_collect(client, project_id, sql, |row| {
        (
            row.column::<String>(0).unwrap(),
            row.column::<String>(1).unwrap(),
        )
    })
    .await
    .map_err(Into::into)
}

async fn fetch_table_list(client: &Client, project_id: &str, sql: String) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    executor::query_collect(client, project_id, sql, |row| {
        let p = row.column::<String>(0).unwrap();
        let d = row.column::<String>(1).unwrap();
        let t = row.column::<String>(2).unwrap();
        format!("{p}.{d}.{t}")
    })
    .await
    .map_err(Into::into)
}

struct StorageRow {
    total_rows: i64,
    active_logical: i64,
    long_term_logical: i64,
    total_logical: i64,
    active_physical: i64,
    long_term_physical: i64,
    total_physical: i64,
    time_travel: i64,
    storage_last_modified_ms: i64,
}

async fn fetch_storage(client: &Client, project_id: &str, sql: String) -> Result<Option<StorageRow>, Box<dyn std::error::Error>> {
    executor::query_first(client, project_id, sql, |row| StorageRow {
        total_rows: row.column::<i64>(0).unwrap(),
        active_logical: row.column::<i64>(1).unwrap(),
        long_term_logical: row.column::<i64>(2).unwrap(),
        total_logical: row.column::<i64>(3).unwrap(),
        active_physical: row.column::<i64>(4).unwrap(),
        long_term_physical: row.column::<i64>(5).unwrap(),
        total_physical: row.column::<i64>(6).unwrap(),
        time_travel: row.column::<i64>(7).unwrap(),
        storage_last_modified_ms: row.column::<i64>(8).unwrap(),
    })
    .await
    .map_err(Into::into)
}

async fn fetch_billing_mode(
    client: &Client,
    project_id: &str,
    sql: String,
) -> Result<Option<BillingMode>, Box<dyn std::error::Error>> {
    executor::query_first(client, project_id, sql, |row| {
        BillingMode::parse(&row.column::<String>(0).unwrap())
    })
    .await
    .map_err(Into::into)
}

async fn fetch_partitions(
    client: &Client,
    project_id: &str,
    sql: String,
) -> Result<(Option<u64>, Option<i64>), Box<dyn std::error::Error>> {
    let result = executor::query_first(client, project_id, sql, |row| {
        let count = row.column::<i64>(0).unwrap();
        let bytes = row.column::<i64>(2).unwrap();
        (Some(count as u64), Some(bytes))
    })
    .await?;
    Ok(result.unwrap_or((None, None)))
}

fn extract_external(ddl: &str, raw_options: &[(String, String)]) -> ExternalInfo {
    let format_from_opts = raw_options
        .iter()
        .find(|(n, _)| n == "format" || n == "file_format")
        .map(|(_, v)| v.trim_matches('"').to_string());
    let format_from_ddl = Regex::new(r#"(?i)\bformat\s*=\s*"([^"]+)""#)
        .unwrap()
        .captures(ddl)
        .map(|c| c[1].to_string());
    let file_format = format_from_opts.or(format_from_ddl);

    let uris_from_opts = raw_options
        .iter()
        .find(|(n, _)| n == "uris")
        .map(|(_, v)| parse_uri_array(v));
    let uris_from_ddl = Regex::new(r#"(?is)\buris\s*=\s*\[(.*?)\]"#)
        .unwrap()
        .captures(ddl)
        .map(|c| parse_uri_array(&c[1]));

    ExternalInfo {
        file_format,
        uris: uris_from_opts.or(uris_from_ddl).unwrap_or_default(),
    }
}

fn parse_uri_array(s: &str) -> Vec<String> {
    Regex::new(r#""([^"]+)""#)
        .unwrap()
        .captures_iter(s)
        .map(|c| c[1].to_string())
        .collect()
}

fn extract_partition_column(clause: &str) -> Option<String> {
    let body = clause.trim_start_matches(|c: char| c.is_alphabetic() || c.is_whitespace());
    let inner = body.strip_prefix("BY").unwrap_or(body).trim();
    let first_token: String = inner
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    let rest = inner[first_token.len()..].trim_start();

    if rest.starts_with('(') {
        let inside = rest
            .trim_start_matches('(')
            .split([',', ')'])
            .next()
            .unwrap_or("")
            .trim();
        if inside.is_empty() {
            None
        } else {
            Some(inside.to_string())
        }
    } else if first_token.is_empty() {
        None
    } else {
        Some(first_token)
    }
}

// ─── Column stats ─────────────────────────────────────────────────────────────

pub async fn column(
    config: AppConfig,
    table_ref: &TableRef,
    column_name: &str,
    deep: bool,
    bins_number: u32,
    time_bins: TimeBins,
    as_category: bool,
    distribution_limit: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref
        .project
        .as_deref()
        .unwrap_or(&project_id)
        .to_string();
    validators::ensure_table_exists(&bq_client, &project, &table_ref.dataset, &table_ref.table).await?;
    let dataset = table_ref.dataset.clone();
    let table = table_ref.table.clone();
    let fqn = format!("{}.{}.{}", project, dataset, table);

    // Phase 1: metadata — zero table scans
    let info_sql = queries::StatsQueries::table_info(&project, &dataset, &table);
    let (_, _, ddl) = fetch_table_info(&bq_client, &project_id, info_sql)
        .await?
        .ok_or_else(|| ValidationError(format!("Table `{fqn}` not found")))?;

    let col_sql = queries::StatsQueries::column_info(&project, &dataset, &table, column_name);
    let meta = fetch_column_info(&bq_client, &project_id, col_sql, column_name, &ddl).await?;

    crate::output::render_column_meta(column_name, &fqn, &meta);

    // Phase 2: cost gate
    if !deep {
        use colored::Colorize;
        print!(
            "\n  {} {}\n  {}\n",
            "⚠".yellow().bold(),
            "Deep scan reads the full table and may incur significant costs with on-demand billing."
                .yellow(),
            "Continue? [y/N]: ".bold()
        );
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if !matches!(input.trim().to_lowercase().as_str(), "y" | "yes") {
            return Ok(());
        }
    }

    // Phase 3: deep scan — 1 table scan
    let base_type = base_type_name(&meta.data_type);
    let deep_stats = fetch_deep_stats(
        &bq_client,
        &project_id,
        &project,
        &dataset,
        &table,
        column_name,
        base_type,
        bins_number,
        &time_bins,
    )
    .await?;

    println!();
    crate::output::render_deep_stats(&deep_stats);

    // Phase 4: category scan — 1 additional table scan (if requested and not boolean)
    let is_bool = matches!(base_type, "BOOL" | "BOOLEAN");
    if as_category && !is_bool {
        let freq_sql =
            queries::StatsQueries::column_frequency(&project, &dataset, &table, column_name);
        let cat = fetch_category(&bq_client, &project_id, freq_sql, distribution_limit).await?;
        println!();
        crate::output::render_category_stats(&cat, distribution_limit);
    }
    Ok(())
}

fn base_type_name(data_type: &str) -> &str {
    // Strip parameterized suffixes like NUMERIC(10,2) → NUMERIC
    let upper = data_type.trim();
    if let Some(pos) = upper.find('(') {
        &data_type[..pos]
    } else {
        upper
    }
}

async fn fetch_column_info(
    client: &Client,
    project_id: &str,
    sql: String,
    column_name: &str,
    ddl: &str,
) -> Result<ColumnMetaInfo, Box<dyn std::error::Error>> {
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await?;
    let row = iter
        .next()
        .await?
        .ok_or_else(|| ValidationError(format!("Column `{column_name}` not found")))?;

    let data_type = row.column::<String>(0).unwrap();
    let is_nullable = row
        .column::<String>(1)
        .map(|s| s.eq_ignore_ascii_case("YES"))
        .unwrap_or(true);
    let clustering_position = row.column::<i64>(2).ok();

    let part_col = Regex::new(r"(?i)PARTITION\s+BY\s+([^\n;]+)")
        .unwrap()
        .captures(ddl)
        .map(|c| format!("PARTITION BY {}", c[1].trim()))
        .as_deref()
        .and_then(extract_partition_column);

    let is_partitioned = part_col
        .as_deref()
        .map(|c| c.eq_ignore_ascii_case(column_name))
        .unwrap_or(false);

    let partition_clause = Regex::new(r"(?i)PARTITION\s+BY\s+([^\n;]+)")
        .unwrap()
        .captures(ddl)
        .map(|c| format!("PARTITION BY {}", c[1].trim()));

    Ok(ColumnMetaInfo {
        data_type,
        is_nullable,
        clustering_position,
        is_partitioned,
        partition_clause,
    })
}

async fn fetch_deep_stats(
    client: &Client,
    project_id: &str,
    project: &str,
    dataset: &str,
    table: &str,
    column: &str,
    base_type: &str,
    bins_number: u32,
    time_bins: &TimeBins,
) -> Result<DeepStats, Box<dyn std::error::Error>> {
    let upper = base_type.to_uppercase();
    match upper.as_str() {
        "INT64" | "INT" | "SMALLINT" | "INTEGER" | "BIGINT" | "TINYINT" | "BYTEINT"
        | "FLOAT64" | "FLOAT" | "NUMERIC" | "BIGNUMERIC" | "DECIMAL" | "BIGDECIMAL" => {
            fetch_numeric(client, project_id, project, dataset, table, column, bins_number).await
        }
        "STRING" | "BYTES" => {
            fetch_string(client, project_id, project, dataset, table, column).await
        }
        "DATETIME" | "TIMESTAMP" | "DATE" => {
            let (trunc_fn, is_time) = match upper.as_str() {
                "DATETIME" => ("DATETIME_TRUNC", false),
                "TIMESTAMP" => ("TIMESTAMP_TRUNC", false),
                _ => ("DATE_TRUNC", false),
            };
            let bins_str = time_bins_to_sql(time_bins);
            fetch_datetime(
                client, project_id, project, dataset, table, column, bins_str, is_time, trunc_fn,
            )
            .await
        }
        "TIME" => {
            fetch_datetime(
                client, project_id, project, dataset, table, column, "HOUR", true, "",
            )
            .await
        }
        "BOOL" | "BOOLEAN" => {
            fetch_boolean(client, project_id, project, dataset, table, column).await
        }
        _ => fetch_generic(client, project_id, project, dataset, table, column).await,
    }
}

fn time_bins_to_sql(tb: &TimeBins) -> &'static str {
    match tb {
        TimeBins::Hour => "HOUR",
        TimeBins::Day => "DAY",
        TimeBins::Week => "WEEK",
        TimeBins::Month => "MONTH",
        TimeBins::Year => "YEAR",
    }
}

async fn fetch_numeric(
    client: &Client,
    project_id: &str,
    project: &str,
    dataset: &str,
    table: &str,
    column: &str,
    bins_number: u32,
) -> Result<DeepStats, Box<dyn std::error::Error>> {
    let sql = queries::StatsQueries::column_numeric(project, dataset, table, column, bins_number);
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await?;
    let mut total = 0i64;
    let mut null_count = 0i64;
    let mut min = 0f64;
    let mut max = 0f64;
    let mut avg = 0f64;
    let mut raw_bins: Vec<(i64, i64)> = Vec::new();
    let mut first = true;

    while let Some(row) = iter.next().await? {
        if first {
            total = row.column::<i64>(0).unwrap_or(0);
            null_count = row.column::<i64>(1).unwrap_or(0);
            min = row.column::<f64>(2).unwrap_or(0.0);
            max = row.column::<f64>(3).unwrap_or(0.0);
            avg = row.column::<f64>(4).unwrap_or(0.0);
            first = false;
        }
        let bucket = row.column::<i64>(5).unwrap_or(0);
        let count = row.column::<i64>(6).unwrap_or(0);
        raw_bins.push((bucket, count));
    }

    let null_pct = if total > 0 {
        null_count as f64 / total as f64 * 100.0
    } else {
        0.0
    };

    let range = max - min;
    let bin_width = if bins_number > 0 && range > 0.0 {
        range / bins_number as f64
    } else {
        0.0
    };

    let bins = raw_bins
        .into_iter()
        .map(|(bucket, count)| {
            let lower = min + bucket as f64 * bin_width;
            let upper = lower + bin_width;
            BinCount { lower, upper, count }
        })
        .collect();

    Ok(DeepStats::Numeric {
        null_pct,
        null_count,
        total,
        min,
        max,
        avg,
        bins,
    })
}

async fn fetch_string(
    client: &Client,
    project_id: &str,
    project: &str,
    dataset: &str,
    table: &str,
    column: &str,
) -> Result<DeepStats, Box<dyn std::error::Error>> {
    let sql = queries::StatsQueries::column_string(project, dataset, table, column);
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await?;
    if let Some(row) = iter.next().await? {
        let total = row.column::<i64>(0).unwrap_or(0);
        let null_count = row.column::<i64>(1).unwrap_or(0);
        let min_len = row.column::<i64>(2).unwrap_or(0);
        let max_len = row.column::<i64>(3).unwrap_or(0);
        let avg_len = row.column::<f64>(4).unwrap_or(0.0);
        let null_pct = if total > 0 {
            null_count as f64 / total as f64 * 100.0
        } else {
            0.0
        };
        Ok(DeepStats::Str { null_pct, null_count, total, min_len, max_len, avg_len })
    } else {
        Ok(DeepStats::Str {
            null_pct: 0.0,
            null_count: 0,
            total: 0,
            min_len: 0,
            max_len: 0,
            avg_len: 0.0,
        })
    }
}

async fn fetch_datetime(
    client: &Client,
    project_id: &str,
    project: &str,
    dataset: &str,
    table: &str,
    column: &str,
    time_bins: &str,
    is_time: bool,
    trunc_fn: &str,
) -> Result<DeepStats, Box<dyn std::error::Error>> {
    let sql = queries::StatsQueries::column_datetime(
        project, dataset, table, column, time_bins, is_time, trunc_fn,
    );
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await?;
    let mut total = 0i64;
    let mut null_count = 0i64;
    let mut earliest = String::new();
    let mut latest = String::new();
    let mut distribution: Vec<(String, i64)> = Vec::new();
    let mut first = true;

    while let Some(row) = iter.next().await? {
        if first {
            total = row.column::<i64>(0).unwrap_or(0);
            null_count = row.column::<i64>(1).unwrap_or(0);
            earliest = row.column::<String>(2).unwrap_or_default();
            latest = row.column::<String>(3).unwrap_or_default();
            first = false;
        }
        let bucket = row.column::<String>(4).unwrap_or_default();
        let count = row.column::<i64>(5).unwrap_or(0);
        distribution.push((bucket, count));
    }

    let null_pct = if total > 0 {
        null_count as f64 / total as f64 * 100.0
    } else {
        0.0
    };

    Ok(DeepStats::Datetime { null_pct, null_count, total, earliest, latest, distribution })
}

async fn fetch_boolean(
    client: &Client,
    project_id: &str,
    project: &str,
    dataset: &str,
    table: &str,
    column: &str,
) -> Result<DeepStats, Box<dyn std::error::Error>> {
    let sql = queries::StatsQueries::column_boolean(project, dataset, table, column);
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await?;
    if let Some(row) = iter.next().await? {
        let total = row.column::<i64>(0).unwrap_or(0);
        let null_count = row.column::<i64>(1).unwrap_or(0);
        let true_count = row.column::<i64>(2).unwrap_or(0);
        let non_null_count = row.column::<i64>(3).unwrap_or(0);
        let null_pct = if total > 0 {
            null_count as f64 / total as f64 * 100.0
        } else {
            0.0
        };
        let true_pct = if non_null_count > 0 {
            true_count as f64 / non_null_count as f64 * 100.0
        } else {
            0.0
        };
        Ok(DeepStats::Boolean { null_pct, null_count, total, true_pct })
    } else {
        Ok(DeepStats::Boolean { null_pct: 0.0, null_count: 0, total: 0, true_pct: 0.0 })
    }
}

async fn fetch_generic(
    client: &Client,
    project_id: &str,
    project: &str,
    dataset: &str,
    table: &str,
    column: &str,
) -> Result<DeepStats, Box<dyn std::error::Error>> {
    let sql = queries::StatsQueries::column_generic(project, dataset, table, column);
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await?;
    if let Some(row) = iter.next().await? {
        let total = row.column::<i64>(0).unwrap_or(0);
        let null_count = row.column::<i64>(1).unwrap_or(0);
        let null_pct = if total > 0 {
            null_count as f64 / total as f64 * 100.0
        } else {
            0.0
        };
        Ok(DeepStats::Generic { null_pct, null_count, total })
    } else {
        Ok(DeepStats::Generic { null_pct: 0.0, null_count: 0, total: 0 })
    }
}

async fn fetch_category(
    client: &Client,
    project_id: &str,
    sql: String,
    distribution_limit: u64,
) -> Result<CategoryStats, Box<dyn std::error::Error>> {
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await?;
    let mut rows: Vec<(String, i64)> = Vec::new();
    let mut distinct_count = 0i64;
    let mut first = true;

    while let Some(row) = iter.next().await? {
        let value = row.column::<String>(0).unwrap_or_default();
        let count = row.column::<i64>(1).unwrap_or(0);
        if first {
            distinct_count = row.column::<i64>(2).unwrap_or(0);
            first = false;
        }
        rows.push((value, count));
    }

    let frequency = if distinct_count <= distribution_limit as i64 {
        Some(rows.into_iter().take(distribution_limit as usize).collect())
    } else {
        None
    };

    Ok(CategoryStats { distinct_count, frequency })
}
