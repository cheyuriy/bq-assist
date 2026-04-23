use crate::models::bigquery::{
    columns::ColumnMetadata,
    copy::CopyMetadata,
    queries::{format_bytes, QueryJobMetadata},
    snapshot::SnapshotMetadata,
    stats::{
        BasicInfo, BillingMode, BinCount, CategoryStats, ClusteringInfo, ColumnMetaInfo, DeepStats,
        OtherOption, PartitioningInfo, SizeInfo, TableStatsData,
    },
};
use colored::Colorize;
use std::io::{self, Write as IoWrite};
use tabled::{settings::Width, Table};
use terminal_size::{terminal_size, Width as TermWidth};

const STORAGE_TYPES: &[&str] = &["BASE TABLE", "CLONE", "SNAPSHOT", "MATERIALIZED VIEW"];
const NON_STORAGE_TYPES: &[&str] = &["VIEW", "EXTERNAL"];

pub fn confirm(msg: &str) {
    println!("{} {}", "✓".green(), msg);
}

pub fn print_clustering(fields: &[String]) {
    if fields.is_empty() {
        println!("No clustering set on this table.");
    } else {
        for (i, f) in fields.iter().enumerate() {
            println!("  {}. {}", i + 1, f.green());
        }
    }
}

pub fn print_columns(columns: &[ColumnMetadata]) {
    println!("{}", Table::new(columns));
}

pub fn print_copies(copies: &[CopyMetadata]) {
    if copies.is_empty() {
        println!("No copies are tracked for this table.");
    } else {
        println!("{}", Table::new(copies));
    }
}

pub fn print_snapshots(snapshots: &[SnapshotMetadata]) {
    if snapshots.is_empty() {
        println!("No snapshots are tracked for this table.");
    } else {
        println!("{}", Table::new(snapshots));
    }
}

pub fn print_partitioning(clause: Option<&str>) {
    match clause {
        None => println!("No partitioning set on this table."),
        Some(c) => println!("{}", c.cyan()),
    }
}

pub fn print_queries(jobs: &[QueryJobMetadata]) {
    if jobs.is_empty() {
        println!("No queries found for this table.");
        return;
    }
    let width = terminal_size()
        .map(|(TermWidth(w), _)| w as usize)
        .unwrap_or(120);
    let mut table = Table::new(jobs);
    table.with(Width::wrap(width));
    println!("{}", table);
}

pub fn print_cast_waiting() {
    print!("Waiting 10 s for BigQuery DDL rate limits...");
    io::stdout().flush().ok();
}

pub fn print_cast_done() {
    println!(" Done.");
}

// ─── Stats renderers ─────────────────────────────────────────────────────────

fn section(title: &str) {
    let bar = "━".repeat(60);
    println!("{}", bar.cyan());
    println!("  {}", title.bold().cyan());
    println!("{}", bar.cyan());
}

fn info_line(label: &str, value: &str) {
    println!("  {} {}: {}", "ℹ".cyan(), label.bold(), value);
}

pub fn render_table_stats(data: &TableStatsData) {
    render_basic(&data.basic);

    if STORAGE_TYPES.contains(&data.basic.table_type.as_str())
        && let Some(s) = &data.size
    {
        println!();
        render_rows_and_avg(s);
        println!();
        render_size(s, &data.basic.table_type);
    }

    if !NON_STORAGE_TYPES.contains(&data.basic.table_type.as_str()) {
        println!();
        render_partitioning_info(data.partitioning.as_ref());
        println!();
        render_clustering_info(data.clustering.as_ref());
    }

    println!();
    render_other_options(&data.other_options);

    if let Some(ddl) = &data.ddl {
        println!();
        render_ddl(ddl);
    }
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

    render_size_row("Logical — total", size.total_logical, None, logical_highlight);
    render_size_row("Logical — active", size.active_logical, None, logical_highlight);
    render_size_row(
        "Logical — long-term",
        size.long_term_logical,
        Some(long_term_pct_logical),
        logical_highlight,
    );
    render_size_row("Physical — total", size.total_physical, None, physical_highlight);
    render_size_row("Physical — active", size.active_physical, None, physical_highlight);
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
    let label_styled = if highlight { label.bold().green() } else { label.bold() };
    println!("  {} {}: {}", marker, label_styled, value);
}

fn percent(part: i64, total: i64) -> f64 {
    if total <= 0 {
        0.0
    } else {
        (part as f64 / total as f64) * 100.0
    }
}

fn render_partitioning_info(p: Option<&PartitioningInfo>) {
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

fn render_clustering_info(c: Option<&ClusteringInfo>) {
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

// ─── Column stats renderers ───────────────────────────────────────────────────

pub fn render_column_meta(column_name: &str, fqn: &str, meta: &ColumnMetaInfo) {
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
        let clause = meta.partition_clause.as_deref().unwrap_or("(see table DDL)");
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

pub fn render_deep_stats(stats: &DeepStats) {
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
            info_line(
                "Min",
                format!("{:.6}", min).trim_end_matches('0').trim_end_matches('.'),
            );
            info_line(
                "Max",
                format!("{:.6}", max).trim_end_matches('0').trim_end_matches('.'),
            );
            info_line(
                "Average",
                format!("{:.6}", avg).trim_end_matches('0').trim_end_matches('.'),
            );
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

pub fn render_category_stats(cat: &CategoryStats, distribution_limit: u64) {
    section("Category Distribution");
    info_line("Distinct values", &format_number(cat.distinct_count));
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

// ─── Helpers ─────────────────────────────────────────────────────────────────

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
    if n < 0 { format!("-{}", out) } else { out }
}

fn format_float(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{:.0}", v)
    } else {
        let s = format!("{:.6}", v);
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    }
}
