use crate::bigquery::client;
use crate::bigquery::queries;
use crate::models::bigquery::queries::format_bytes;
use crate::models::bigquery::stats::{
    BasicInfo, BillingMode, ClusteringInfo, ExternalInfo, OtherOption, PartitioningInfo, SizeInfo,
};
use crate::models::config::AppConfig;
use crate::models::schema::TableRef;
use chrono::{DateTime, Utc};
use colored::Colorize;
use google_cloud_bigquery::client::Client;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;
use regex::Regex;

const STORAGE_TYPES: &[&str] = &["BASE TABLE", "CLONE", "SNAPSHOT", "MATERIALIZED VIEW"];
const NON_STORAGE_TYPES: &[&str] = &["VIEW", "EXTERNAL"];

pub async fn report(config: AppConfig, table_ref: &TableRef, with_ddl: bool) {
    let region = config.region.clone();
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };
    let project = table_ref
        .project
        .as_deref()
        .unwrap_or(&project_id)
        .to_string();
    let dataset = table_ref.dataset.clone();
    let table = table_ref.table.clone();
    let fqn = format!("{}.{}.{}", project, dataset, table);

    let info_sql = queries::StatsQueries::table_info(&project, &dataset, &table);
    let (table_type, creation_time_ms, ddl) = fetch_table_info(&bq_client, &project_id, info_sql)
        .await
        .unwrap_or_else(|| panic!("Table `{fqn}` not found"));

    let options_sql = queries::StatsQueries::table_options(&project, &dataset, &table);
    let raw_options = fetch_options(&bq_client, &project_id, options_sql).await;

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
    let child_snapshots = fetch_table_list(&bq_client, &project_id, snap_sql).await;
    let clones_sql = queries::StatsQueries::child_clones(&project, &dataset, &table);
    let child_clones = fetch_table_list(&bq_client, &project_id, clones_sql).await;

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
        let storage = fetch_storage(&bq_client, &project_id, storage_sql).await;
        let billing_sql = queries::StatsQueries::dataset_billing_mode(&region, &project, &dataset);
        let billing = fetch_billing_mode(&bq_client, &project_id, billing_sql).await;

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
            fetch_partitions(&bq_client, &project_id, parts_sql).await
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
}

async fn fetch_table_info(
    client: &Client,
    project_id: &str,
    sql: String,
) -> Option<(String, i64, String)> {
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await.unwrap();
    iter.next().await.unwrap().map(|row| {
        (
            row.column::<String>(0).unwrap(),
            row.column::<i64>(1).unwrap(),
            row.column::<String>(2).unwrap(),
        )
    })
}

async fn fetch_options(
    client: &Client,
    project_id: &str,
    sql: String,
) -> Vec<(String, String)> {
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await.unwrap();
    let mut out = Vec::new();
    while let Some(row) = iter.next().await.unwrap() {
        out.push((
            row.column::<String>(0).unwrap(),
            row.column::<String>(1).unwrap(),
        ));
    }
    out
}

async fn fetch_table_list(client: &Client, project_id: &str, sql: String) -> Vec<String> {
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await.unwrap();
    let mut out = Vec::new();
    while let Some(row) = iter.next().await.unwrap() {
        let p = row.column::<String>(0).unwrap();
        let d = row.column::<String>(1).unwrap();
        let t = row.column::<String>(2).unwrap();
        out.push(format!("{p}.{d}.{t}"));
    }
    out
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

async fn fetch_storage(client: &Client, project_id: &str, sql: String) -> Option<StorageRow> {
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await.unwrap();
    iter.next().await.unwrap().map(|row| StorageRow {
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
}

async fn fetch_billing_mode(
    client: &Client,
    project_id: &str,
    sql: String,
) -> Option<BillingMode> {
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await.unwrap();
    iter.next()
        .await
        .unwrap()
        .map(|row| BillingMode::parse(&row.column::<String>(0).unwrap()))
}

async fn fetch_partitions(
    client: &Client,
    project_id: &str,
    sql: String,
) -> (Option<u64>, Option<i64>) {
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await.unwrap();
    if let Some(row) = iter.next().await.unwrap() {
        let count = row.column::<i64>(0).unwrap();
        let bytes = row.column::<i64>(2).unwrap();
        (Some(count as u64), Some(bytes))
    } else {
        (None, None)
    }
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
