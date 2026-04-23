use crate::bigquery::client;
use crate::bigquery::executor;
use crate::bigquery::queries;
use crate::bigquery::validators;
use crate::cli::TimeBins;
use crate::errors::ValidationError;
use crate::models::bigquery::queries::format_bytes;
use crate::models::bigquery::stats::{
    BasicInfo, BillingMode, ClusteringInfo, ExternalInfo, OtherOption, PartitioningInfo, SizeInfo,
};
use crate::models::config::AppConfig;
use crate::models::bigquery::references::TableRef;
use chrono::{DateTime, Utc};
use colored::Colorize;
use google_cloud_bigquery::client::Client;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;
use regex::Regex;
use std::io::{self, Write as IoWrite};

const STORAGE_TYPES: &[&str] = &["BASE TABLE", "CLONE", "SNAPSHOT", "MATERIALIZED VIEW"];
const NON_STORAGE_TYPES: &[&str] = &["VIEW", "EXTERNAL"];

pub async fn report(config: AppConfig, table_ref: &TableRef, with_ddl: bool) -> Result<(), Box<dyn std::error::Error>> {
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

    let size_info: Option<SizeInfo> = if STORAGE_TYPES.contains(&table_type.as_str()) {
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

    let partitioning_info = if !NON_STORAGE_TYPES.contains(&table_type.as_str()) {
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

    let clustering_info = if !NON_STORAGE_TYPES.contains(&table_type.as_str()) {
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

    render(
        &basic,
        size_info.as_ref(),
        partitioning_info.as_ref(),
        clustering_info.as_ref(),
        &other_options,
        if with_ddl { Some(ddl.as_str()) } else { None },
    );
    Ok(())
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

fn render(
    basic: &BasicInfo,
    size: Option<&SizeInfo>,
    partitioning: Option<&PartitioningInfo>,
    clustering: Option<&ClusteringInfo>,
    other_options: &[OtherOption],
    ddl: Option<&str>,
) {
    render_basic(basic);

    if STORAGE_TYPES.contains(&basic.table_type.as_str())
        && let Some(s) = size
    {
        println!();
        render_rows_and_avg(s);
        println!();
        render_size(s, &basic.table_type);
    }

    if !NON_STORAGE_TYPES.contains(&basic.table_type.as_str()) {
        println!();
        render_partitioning(partitioning);
        println!();
        render_clustering(clustering);
    }

    println!();
    render_other_options(other_options);

    if let Some(ddl) = ddl {
        println!();
        render_ddl(ddl);
    }
}

fn section(title: &str) {
    let bar = "━".repeat(60);
    println!("{}", bar.cyan());
    println!("  {}", title.bold().cyan());
    println!("{}", bar.cyan());
}

fn info_line(label: &str, value: &str) {
    println!("  {} {}: {}", "ℹ".cyan(), label.bold(), value);
}

fn render_basic(basic: &BasicInfo) {
    section("Basic Information");
    info_line("Name", &format!("`{}`", basic.fqn).green().to_string());
    info_line("Type", &basic.table_type.yellow().to_string());
    info_line(
        "Created",
        &basic
            .created
            .map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "-".into()),
    );
    info_line(
        "Last modified",
        &basic
            .updated
            .map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "-".into()),
    );

    if let Some(origin) = &basic.origin {
        info_line("Origin", &format!("`{}`", origin).green().to_string());
    }

    if let Some(ext) = &basic.external {
        info_line("Format", ext.file_format.as_deref().unwrap_or("-"));
        if ext.uris.is_empty() {
            info_line("URIs", "-");
        } else {
            println!("  {} {}:", "ℹ".cyan(), "URIs".bold());
            for u in &ext.uris {
                println!("    {} {}", "•".cyan(), u);
            }
        }
    }

    if basic.snapshots.is_empty() {
        info_line("Snapshots", &format!("{} none", "✗".red()));
    } else {
        println!(
            "  {} {}: {}",
            "✓".green(),
            "Snapshots".bold(),
            basic.snapshots.len()
        );
        for s in &basic.snapshots {
            println!("    {} `{}`", "•".green(), s);
        }
    }

    if basic.clones.is_empty() {
        info_line("Clones", &format!("{} none", "✗".red()));
    } else {
        println!(
            "  {} {}: {}",
            "✓".green(),
            "Clones".bold(),
            basic.clones.len()
        );
        for c in &basic.clones {
            println!("    {} `{}`", "•".green(), c);
        }
    }
}

fn render_rows_and_avg(size: &SizeInfo) {
    section("Rows");
    info_line("Row count", &format_number(size.total_rows).to_string());
    let reference_bytes = match size.billing_mode {
        BillingMode::Physical => size.active_physical,
        BillingMode::Logical => size.active_logical,
    };
    let avg_size = if size.total_rows > 0 {
        format_bytes(reference_bytes / size.total_rows)
    } else {
        "-".to_string()
    };
    info_line("Average row size", &avg_size);
}

fn render_size(size: &SizeInfo, table_type: &str) {
    section("Size");
    if matches!(table_type, "CLONE" | "SNAPSHOT" | "MATERIALIZED VIEW") {
        println!(
            "  {} {}",
            "ℹ".yellow(),
            format!(
                "Note: {} tables use efficient storage and may not reflect origin table size.",
                table_type
            )
            .yellow()
        );
    }

    let logical_highlight = size.billing_mode == BillingMode::Logical;
    let physical_highlight = size.billing_mode == BillingMode::Physical;
    let long_term_pct_logical = percent(size.long_term_logical, size.total_logical);
    let long_term_pct_physical = percent(size.long_term_physical, size.total_physical);

    render_size_row(
        "Logical — total",
        size.total_logical,
        None,
        logical_highlight,
    );
    render_size_row(
        "Logical — active",
        size.active_logical,
        None,
        logical_highlight,
    );
    render_size_row(
        "Logical — long-term",
        size.long_term_logical,
        Some(long_term_pct_logical),
        logical_highlight,
    );

    render_size_row(
        "Physical — total",
        size.total_physical,
        None,
        physical_highlight,
    );
    render_size_row(
        "Physical — active",
        size.active_physical,
        None,
        physical_highlight,
    );
    render_size_row(
        "Physical — long-term",
        size.long_term_physical,
        Some(long_term_pct_physical),
        physical_highlight,
    );

    render_size_row("Time travel", size.time_travel, None, false);

    let mode = match size.billing_mode {
        BillingMode::Logical => "LOGICAL".green().bold(),
        BillingMode::Physical => "PHYSICAL".green().bold(),
    };
    println!("  {} {}: {}", "ℹ".cyan(), "Billing mode".bold(), mode);
}

fn render_size_row(label: &str, bytes: i64, percent_suffix: Option<f64>, highlight: bool) {
    let size_str = format_bytes(bytes);
    let value = match percent_suffix {
        Some(p) => format!("{} ({:.1}%)", size_str, p),
        None => size_str,
    };
    let marker = if highlight { "➤".green() } else { " ".normal() };
    let label_styled = if highlight {
        label.bold().green()
    } else {
        label.bold()
    };
    println!("  {} {}: {}", marker, label_styled, value);
}

fn percent(part: i64, total: i64) -> f64 {
    if total <= 0 {
        0.0
    } else {
        (part as f64 / total as f64) * 100.0
    }
}

fn render_partitioning(p: Option<&PartitioningInfo>) {
    section("Partitioning");
    let Some(p) = p else {
        info_line("Column", &format!("{} None", "✗".red()));
        return;
    };
    match (&p.column, &p.clause) {
        (Some(col), _) => {
            info_line("Column", &col.green().to_string());
        }
        (None, None) => {
            info_line("Column", &format!("{} None", "✗".red()));
        }
        (None, Some(_)) => {
            info_line("Column", &"(see clause)".dimmed().to_string());
        }
    }

    if let Some(clause) = &p.clause {
        info_line("Clause", &clause.dimmed().to_string());
    }

    if let Some(n) = p.partitions_count {
        info_line("Partitions count", &format_number(n as i64));
    }
    if let Some(b) = p.avg_partition_bytes {
        info_line("Avg partition size", &format_bytes(b));
    }

    match p.require_partition_filter {
        Some(true) => println!(
            "  {} {}",
            "✓".green(),
            "require_partition_filter is ON".green()
        ),
        Some(false) => println!(
            "  {} {}",
            "✗".red(),
            "require_partition_filter is OFF".dimmed()
        ),
        None => {}
    }
}

fn render_clustering(c: Option<&ClusteringInfo>) {
    section("Clustering");
    let Some(c) = c else {
        info_line("Fields", &format!("{} None", "✗".red()));
        return;
    };
    if c.fields.is_empty() {
        info_line("Fields", &format!("{} None", "✗".red()));
    } else {
        let list = c
            .fields
            .iter()
            .map(|f| f.green().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        info_line("Fields", &list);
    }
}

fn render_other_options(options: &[OtherOption]) {
    section("Options");
    if options.is_empty() {
        println!("  {} {}", "✗".red(), "no options set".dimmed());
        return;
    }
    for o in options {
        println!("  {} {} = {}", "•".cyan(), o.name.bold(), o.value);
    }
}

fn render_ddl(ddl: &str) {
    section("DDL");
    println!("{}", ddl);
}

fn format_number(n: i64) -> String {
    let s = n.abs().to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    let start = bytes.len() % 3;
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && i >= start && (i - start) % 3 == 0 {
            out.push(',');
        }
        out.push(*b as char);
    }
    if n < 0 {
        format!("-{}", out)
    } else {
        out
    }
}

// ─── Column stats ────────────────────────────────────────────────────────────

struct ColumnMetaInfo {
    data_type: String,
    is_nullable: bool,
    clustering_position: Option<i64>,
    is_partitioned: bool,
    partition_clause: Option<String>,
}

struct BinCount {
    lower: f64,
    upper: f64,
    count: i64,
}

enum DeepStats {
    Numeric {
        null_pct: f64,
        null_count: i64,
        total: i64,
        min: f64,
        max: f64,
        avg: f64,
        bins: Vec<BinCount>,
    },
    Str {
        null_pct: f64,
        null_count: i64,
        total: i64,
        min_len: i64,
        max_len: i64,
        avg_len: f64,
    },
    Datetime {
        null_pct: f64,
        null_count: i64,
        total: i64,
        earliest: String,
        latest: String,
        distribution: Vec<(String, i64)>,
    },
    Boolean {
        null_pct: f64,
        null_count: i64,
        total: i64,
        true_pct: f64,
    },
    Generic {
        null_pct: f64,
        null_count: i64,
        total: i64,
    },
}

struct CategoryStats {
    distinct_count: i64,
    frequency: Option<Vec<(String, i64)>>,
}

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

    render_column_meta(column_name, &fqn, &meta);

    // Phase 2: cost gate
    if !deep {
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
    render_deep_stats(&deep_stats);

    // Phase 4: category scan — 1 additional table scan (if requested and not boolean)
    let is_bool = matches!(base_type, "BOOL" | "BOOLEAN");
    if as_category && !is_bool {
        let freq_sql =
            queries::StatsQueries::column_frequency(&project, &dataset, &table, column_name);
        let cat = fetch_category(&bq_client, &project_id, freq_sql, distribution_limit).await?;
        println!();
        render_category_stats(&cat, distribution_limit);
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
    let mut raw_bins: Vec<(i64, i64)> = Vec::new(); // (bucket, count)
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
            BinCount {
                lower,
                upper,
                count,
            }
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
        Ok(DeepStats::Str {
            null_pct,
            null_count,
            total,
            min_len,
            max_len,
            avg_len,
        })
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

    Ok(DeepStats::Datetime {
        null_pct,
        null_count,
        total,
        earliest,
        latest,
        distribution,
    })
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
        Ok(DeepStats::Boolean {
            null_pct,
            null_count,
            total,
            true_pct,
        })
    } else {
        Ok(DeepStats::Boolean {
            null_pct: 0.0,
            null_count: 0,
            total: 0,
            true_pct: 0.0,
        })
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
        Ok(DeepStats::Generic {
            null_pct,
            null_count,
            total,
        })
    } else {
        Ok(DeepStats::Generic {
            null_pct: 0.0,
            null_count: 0,
            total: 0,
        })
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

    Ok(CategoryStats {
        distinct_count,
        frequency,
    })
}

fn render_column_meta(column_name: &str, fqn: &str, meta: &ColumnMetaInfo) {
    section("Column Information");
    info_line("Table", &format!("`{}`", fqn).green().to_string());
    info_line("Column", &column_name.green().bold().to_string());
    info_line("Type", &meta.data_type.yellow().to_string());
    let nullable_str = if meta.is_nullable {
        "YES".dimmed().to_string()
    } else {
        format!("{} NOT NULL", "✓".green())
    };
    info_line("Nullable", &nullable_str);

    match meta.clustering_position {
        Some(pos) => info_line(
            "Clustering",
            &format!("{} position #{}", "✓".green(), pos)
                .green()
                .to_string(),
        ),
        None => info_line("Clustering", &format!("{} not a clustering key", "✗".red())),
    }

    if meta.is_partitioned {
        let clause = meta
            .partition_clause
            .as_deref()
            .unwrap_or("(see table DDL)");
        info_line(
            "Partitioning",
            &format!("{} {}", "✓".green(), clause).green().to_string(),
        );
    } else {
        info_line(
            "Partitioning",
            &format!("{} not the partition column", "✗".red()),
        );
    }
}

fn render_deep_stats(stats: &DeepStats) {
    match stats {
        DeepStats::Numeric {
            null_pct,
            null_count,
            total,
            min,
            max,
            avg,
            bins,
        } => {
            render_null_section(*null_pct, *null_count, *total);
            println!();
            section("Numeric Statistics");
            info_line("Min", format!("{:.6}", min).trim_end_matches('0').trim_end_matches('.'));
            info_line("Max", format!("{:.6}", max).trim_end_matches('0').trim_end_matches('.'));
            info_line("Average", format!("{:.6}", avg).trim_end_matches('0').trim_end_matches('.'));
            if !bins.is_empty() {
                println!();
                render_histogram(bins);
            }
        }
        DeepStats::Str {
            null_pct,
            null_count,
            total,
            min_len,
            max_len,
            avg_len,
        } => {
            render_null_section(*null_pct, *null_count, *total);
            println!();
            section("String Statistics");
            info_line("Min length", &format_number(*min_len));
            info_line("Max length", &format_number(*max_len));
            info_line("Average length", &format!("{:.1}", avg_len));
        }
        DeepStats::Datetime {
            null_pct,
            null_count,
            total,
            earliest,
            latest,
            distribution,
        } => {
            render_null_section(*null_pct, *null_count, *total);
            println!();
            section("Datetime Statistics");
            info_line("Earliest", earliest);
            info_line("Latest", latest);
            if !distribution.is_empty() {
                println!();
                render_time_distribution(distribution);
            }
        }
        DeepStats::Boolean {
            null_pct,
            null_count,
            total,
            true_pct,
        } => {
            render_null_section(*null_pct, *null_count, *total);
            println!();
            section("Boolean Statistics");
            info_line("TRUE proportion", &format!("{:.1}%", true_pct));
        }
        DeepStats::Generic {
            null_pct,
            null_count,
            total,
        } => {
            render_null_section(*null_pct, *null_count, *total);
        }
    }
}

fn render_null_section(null_pct: f64, null_count: i64, total: i64) {
    section("Null Values");
    let marker = if null_pct == 0.0 {
        "✓".green().to_string()
    } else if null_pct > 50.0 {
        "✗".red().to_string()
    } else {
        "ℹ".yellow().to_string()
    };
    println!(
        "  {} {}: {:.1}% ({} of {} rows)",
        marker,
        "NULL proportion".bold(),
        null_pct,
        format_number(null_count),
        format_number(total)
    );
}

fn render_histogram(bins: &[BinCount]) {
    section("Distribution");
    let max_count = bins.iter().map(|b| b.count).max().unwrap_or(1).max(1);
    let total: i64 = bins.iter().map(|b| b.count).sum();
    const BAR_WIDTH: usize = 20;

    for bin in bins {
        let bar_len = (bin.count as f64 / max_count as f64 * BAR_WIDTH as f64) as usize;
        let bar = "█".repeat(bar_len);
        let pct = if total > 0 {
            bin.count as f64 / total as f64 * 100.0
        } else {
            0.0
        };
        let lower = format_float(bin.lower);
        let upper = format_float(bin.upper);
        println!(
            "  [{:>12} – {:<12})  {:<20}  {:>10}  ({:.1}%)",
            lower,
            upper,
            bar.cyan(),
            format_number(bin.count),
            pct
        );
    }
}

fn render_time_distribution(distribution: &[(String, i64)]) {
    section("Distribution");
    let max_count = distribution
        .iter()
        .map(|(_, c)| *c)
        .max()
        .unwrap_or(1)
        .max(1);
    let total: i64 = distribution.iter().map(|(_, c)| c).sum();
    const BAR_WIDTH: usize = 20;

    for (bucket, count) in distribution {
        let bar_len = (*count as f64 / max_count as f64 * BAR_WIDTH as f64) as usize;
        let bar = "█".repeat(bar_len);
        let pct = if total > 0 {
            *count as f64 / total as f64 * 100.0
        } else {
            0.0
        };
        println!(
            "  {:<24}  {:<20}  {:>10}  ({:.1}%)",
            bucket,
            bar.cyan(),
            format_number(*count),
            pct
        );
    }
}

fn render_category_stats(cat: &CategoryStats, distribution_limit: u64) {
    section("Category Distribution");
    info_line(
        "Distinct values",
        &format_number(cat.distinct_count).to_string(),
    );
    match &cat.frequency {
        None => {
            println!(
                "  {} {}",
                "ℹ".cyan(),
                format!(
                    "Too many distinct values to show frequency table (limit: {}).",
                    distribution_limit
                )
                .dimmed()
            );
        }
        Some(rows) => {
            println!();
            let max_count = rows.iter().map(|(_, c)| *c).max().unwrap_or(1).max(1);
            let total: i64 = rows.iter().map(|(_, c)| c).sum();
            const BAR_WIDTH: usize = 20;
            for (value, count) in rows {
                let bar_len = (*count as f64 / max_count as f64 * BAR_WIDTH as f64) as usize;
                let bar = "█".repeat(bar_len);
                let pct = if total > 0 {
                    *count as f64 / total as f64 * 100.0
                } else {
                    0.0
                };
                println!(
                    "  {:<30}  {:<20}  {:>10}  ({:.1}%)",
                    value,
                    bar.cyan(),
                    format_number(*count),
                    pct
                );
            }
        }
    }
}

fn format_float(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{:.0}", v)
    } else {
        let s = format!("{:.6}", v);
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    }
}
