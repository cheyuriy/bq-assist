use crate::errors::ArgumentsParsingError;
use crate::models::bigquery;
use crate::models::schema::{DatasetRef, TableRef};
use chrono;
use clap::{Parser, Subcommand, ValueEnum};
use std::str::FromStr;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(version, about = "CLI to run complex operations in BigQuery", long_about = None, disable_help_subcommand = true)]
pub struct CLI {
    #[command(subcommand)]
    pub commands: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Operations over a specific table
    Table {
        /// Table reference (e.g. <project>.<dataset>.<table> or <dataset>.<table>)
        #[arg(value_name = "TABLE")]
        table_ref: TableRef,

        #[command(subcommand)]
        table_subcommands: TableSubcommands,
    },

    /// Operation over a specific dataset
    Dataset {
        /// Dataset reference (e.g. <project>.<dataset> or <dataset>)
        #[arg(value_name = "DATASET")]
        dataset_ref: DatasetRef,

        #[command(subcommand)]
        dataset_subcommands: DatasetSubcommands,
    },

    /// Merge two different tables using one of the typical patterns
    Merge {
        /// Left table reference (e.g. <project>.<dataset>.<table> or <dataset>.<table>) on the left side of the merge (destination by default)
        #[arg(value_name = "LEFT")]
        left_ref: TableRef,

        /// Right table reference (e.g. <project>.<dataset>.<table> or <dataset>.<table>) on the right side of the merge (source by default)
        #[arg(value_name = "RIGHT")]
        right_ref: TableRef,

        /// Table reference (e.g. <project>.<dataset>.<table> or <dataset>.<table>) to store the result instead of using left-table as a default one
        #[arg(value_name = "DESTINATION")]
        destination_ref: Option<TableRef>,

        #[command(subcommand)]
        merge_subcommands: MergeSubcommands,
    },

    /// Compare two tables and show report about differences. Can also work with tracked copies and snapshots.
    Compare {
        /// Left table reference (e.g. <project>.<dataset>.<table> or <dataset>.<table>) for comparing (base table)
        #[arg(value_name = "LEFT")]
        left_ref: TableRef,

        /// Right table reference (e.g. <project>.<dataset>.<table> or <dataset>.<table>) for comparing (changed table)
        #[arg(value_name = "RIGHT")]
        right_ref: TableRef,

        /// Tracked copy name or ID on the left
        #[arg(long, value_name = "ID")]
        left_copy: Option<String>,

        /// Tracked snapshot or ID of the table on the left
        #[arg(long, value_name = "ID")]
        left_snapshot: Option<String>,

        /// Tracked copy or ID on the right
        #[arg(long, value_name = "ID")]
        right_copy: Option<String>,

        /// Tracked snapshot or ID on the right
        #[arg(long, value_name = "ID")]
        right_snapshot: Option<String>,
    },

    /// Run different sanity checks on the table(s)
    Checks,

    /// Setup wizard
    Init,
}

#[derive(Subcommand, Debug)]
#[command(disable_help_subcommand = true)]
pub enum TableSubcommands {
    /// Clustering settings. Skip command name to see current settings.
    Clustering {
        #[command(subcommand)]
        command: Option<ClusteringSubcommands>,
    },

    /// Partitioning settings. Skip command name to see current settings.
    Partitioning {
        #[command(subcommand)]
        command: Option<PartitioningSubcommands>,
    },

    /// Adding, deleting or changing type of columns. Skip command name to see current schema.
    Columns {
        #[command(subcommand)]
        command: Option<ColumnsSubcommands>,
    },

    #[command(about = "\
Restore table.\n\
You can restore it using:\n\
- time-travel (if set beforehand)\n\
- existing and tracked copy or snapshot\n\
- archive")]
    Restore {
        /// Seconds to rewind table state using time-travel
        #[arg(long, value_parser = parse_duration)]
        rewind: Option<Duration>,

        /// Name or ID of the tracked copy to restore table from
        #[arg(long)]
        copy: Option<String>,

        /// Name or ID of the tracked snapshot to restore table from
        #[arg(long)]
        snapshot: Option<String>,

        /// Restore from the archive, set for the table
        #[arg(long)]
        archive: Option<bool>,
    },

    /// Manage and track snapshots. Skip command name to see all tracked snapshots
    Snapshots {
        #[command(subcommand)]
        command: Option<SnapshotsSubcommands>,
    },

    /// Set table-specific options (like expiration time, description etc)
    Options {
        /// Name of the option.
        /// Known options will be checked for value correctness. Unknown options still can be processed and passed without any checks.
        /// Known options are: expiration_timestamp, partition_expiration_days, require_partition_filter, kms_key_name, friendly_name, description, labels, default_rounding_mode, enable_change_history, max_staleness, enable_fine_grained_mutations, storage_uri, file_format, table_format, tags
        #[arg(value_parser = bigquery::options::TableOption::from_str)]
        option: bigquery::options::TableOption,

        /// Value to set (use value `NULL` to remove option, if supported)
        value: String,
    },

    /// Manage and track copies
    Copy {
        #[command(subcommand)]
        command: Option<CopySubcommands>,
    },

    /// List queries associated with the table
    Queries {
        #[command(subcommand)]
        command: QueriesSubcommand,
    },

    /// Show different table statistics and information. Skip command name to see basic report
    Stats {
        /// Include the table DDL in the report
        #[arg(long)]
        with_ddl: bool,

        #[command(subcommand)]
        command: Option<StatsSubcommands>,
    },

    /// Manage table archivation
    Archive {
        #[command(subcommand)]
        command: Option<ArchiveSubcommands>,
    },

    /// Rename table
    Rename {
        /// New name
        new_name: String,
    },
}

#[derive(Subcommand, Debug)]
#[command(disable_help_subcommand = true)]
pub enum ClusteringSubcommands {
    /// Add field(s) to clustering, or replacing existing with a new one
    Add {
        /// Fields to be added to clustering. Order will be preserved. No more then 4 fields.
        #[arg(num_args = 1..=4, required = true, value_name = "CLUSTERING FIELDS")]
        fields: Vec<String>,
    },

    /// Remove clustering
    Remove,
}

#[derive(Subcommand, Debug)]
#[command(disable_help_subcommand = true)]
pub enum PartitioningSubcommands {
    /// Add field to partitioning, or replacing existing with a new one
    #[command(subcommand, name = "add")]
    Partitioning(bigquery::partitioning::Partitioning),

    /// Remove partitioning
    Remove,
}

#[derive(Subcommand, Debug)]
#[command(disable_help_subcommand = true)]
pub enum ColumnsSubcommands {
    /// Add a column
    Add {
        /// Column name
        name: String,

        /// Type
        #[arg(ignore_case = true)]
        field_type: bigquery::columns::Type,

        /// Default value
        default_value: Option<String>,
    },

    /// Rename a column
    Rename {
        /// Current name
        name: String,

        /// New name
        new_name: String,
    },

    #[command(long_about = "\
Replace a column with a new one with the same name, but a new type. Supposes, that casting between types is possible.\n\
Can lose precision (when casting between different numeric types) or lose values (when casting strings into BOOLEAN)")]
    Cast {
        /// Column name
        name: String,

        /// Name of the new column
        #[arg(ignore_case = true)]
        field_type: bigquery::columns::Type,
    },

    /// Delete a column
    Remove {
        /// Column name
        name: String,
    },
}

#[derive(Subcommand, Debug)]
#[command(disable_help_subcommand = true)]
pub enum SnapshotsSubcommands {
    /// Create a new snapshot
    Add {
        /// Snapshot name. If not present, then it will have default name `<table>_YYYY_DD_MMThh_mm_ss`
        name: Option<String>,

        /// Dataset to store snapshot. If not specified, then snapshot will be created in the same dataset as the table
        #[arg(long)]
        dataset: Option<DatasetRef>,

        /// Seconds to rewind table state (time travel) before creating the snapshot
        #[arg(long, value_parser = parse_duration)]
        rewind: Option<Duration>,

        /// Exact timestamp of table state (time travel) before creating snapshot. Supports RFC3339 format (i.e. `2026-01-01 01:02:03Z`).
        #[arg(long, value_parser = parse_datetime)]
        timestamp: Option<chrono::DateTime<chrono::Utc>>,

        /// If you don't want to track this snapshot and allow to use it faster in other commands
        #[arg(long)]
        no_track: bool,
    },

    /// Delete a snapshot. Snapshot should be tracked. Otherwise, delete it manually.
    Remove {
        /// Snapshot name or ID. Special value `*` deletes all tracked snapshots.
        name: String,
    },
}

#[derive(Subcommand, Debug)]
#[command(disable_help_subcommand = true)]
pub enum CopySubcommands {
    /// Create a new copy
    Add {
        /// Copy name. If not present, then it will have default name `<table>_YYYY_DD_MMThh_mm_ss`
        name: Option<String>,

        /// Dataset to store snapshot. If not specified, then snapshot will be created in the same dataset as the table
        #[arg(long)]
        dataset: Option<DatasetRef>,

        /// If you don't want to track this copy and allow to use it faster in other commands
        #[arg(long)]
        no_track: bool,
    },

    /// Delete a copy. Copy should be tracked. Otherwise, delete it manually.
    Remove {
        /// Copy name or ID. Special value `*` deletes all tracked copies.
        name: String,
    },
}

#[derive(Subcommand, Debug)]
#[command(disable_help_subcommand = true)]
pub enum QueriesSubcommand {
    /// List queries which read from the table (SQL)
    Read {
        /// If set, then returned queries will reference only this table
        #[arg(long, default_value = "false")]
        single: bool,

        /// Filters queries made by specific user email
        #[arg(long)]
        user: Option<String>,

        #[arg(long, help="\
Length of the period to retrieve queries.\n\
If `from` and `to` are not present, then we use current time to get period.\n\
If either `from` or `to` are present, then we use them to as a boundary for period.\n\
If both `from` and `to` are present, then we ignore this argument.\
        ", value_parser = parse_duration, default_value="1h")]
        period: Option<Duration>,

        /// Timestamp of period's start. Supports RFC3339 format (i.e. `2026-01-01 01:02:03Z`).
        #[arg(long, value_parser = parse_datetime)]
        from: Option<chrono::DateTime<chrono::Utc>>,

        /// Timestamp of period's end. Supports RFC3339 format (i.e. `2026-01-01 01:02:03Z`).
        #[arg(long, value_parser = parse_datetime)]
        to: Option<chrono::DateTime<chrono::Utc>>,

        /// Maximum number of results to return
        #[arg(long, default_value = "50")]
        limit: u64,
    },

    /// List queries which modify the table (DDL or DML)
    Modify {
        #[arg(long)]
        query_type: Option<String>,

        /// Filters queries made by specific user email
        #[arg(long)]
        user: Option<String>,

        #[arg(long, help="\
Length of the period to retrieve queries.\n\
If `from` and `to` are not present, then we use current time to get period.\n\
If either `from` or `to` are present, then we use them to as a boundary for period.\n\
If both `from` and `to` are present, then we ignore this argument.\
        ", value_parser = parse_duration, default_value="1h")]
        period: Option<Duration>,

        /// Timestamp of period's start. Supports RFC3339 format (i.e. `2026-01-01 01:02:03Z`).
        #[arg(long, value_parser = parse_datetime)]
        from: Option<chrono::DateTime<chrono::Utc>>,

        /// Timestamp of period's end. Supports RFC3339 format (i.e. `2026-01-01 01:02:03Z`).
        #[arg(long, value_parser = parse_datetime)]
        to: Option<chrono::DateTime<chrono::Utc>>,

        /// Maximum number of results to return
        #[arg(long, default_value = "50")]
        limit: u64,

        /// If set, also return queries which reference the table (not only those targeting it)
        #[arg(long, default_value = "false")]
        related: bool,
    },
}

#[derive(ValueEnum, Clone, Debug, Default)]
pub enum TimeBins {
    Hour,
    Day,
    Week,
    #[default]
    Month,
    Year,
}

#[derive(Subcommand, Debug)]
#[command(disable_help_subcommand = true)]
pub enum StatsSubcommands {
    /// Show statistics for a specific column
    #[command(name = "column")]
    Column {
        /// Column name
        name: String,
        /// Skip cost confirmation and run deep scan immediately
        #[arg(long)]
        deep: bool,
        /// Number of equal-width buckets for numeric distributions
        #[arg(long, default_value = "10")]
        bins_number: u32,
        /// Time granularity for datetime distributions
        #[arg(long, default_value = "month", ignore_case = true)]
        time_bins: TimeBins,
        /// Treat column values as categories (distinct count + frequency table)
        #[arg(long)]
        as_category: bool,
        /// Max distinct values to show frequency table
        #[arg(long, default_value = "20")]
        distribution_limit: u64,
    },
}

#[derive(Subcommand, Debug)]
#[command(disable_help_subcommand = true)]
pub enum ArchiveSubcommands {
    /// Archivate table\n
    /// Possible to create periodic archivation, or one-time\n
    /// Make sure that you have appropriate rights for the chosen type of archivation
    #[command(about = "\
Archivate table.\n\
It's possible to create periodic archivation, or one-time.\n\
Make sure that you have appropriate rights for the chosen type of archivation\
    ")]
    Add {
        /// Type of archivation to use
        archive_type: Option<String>,

        /// Frequency of archivation, if we want to have it periodically. Otherwise, it will be one-timer.
        #[arg(long, value_parser = parse_duration)]
        frequency: Option<Duration>,

        /// Time to start archivation, if we want it periodically. If not set, then we'll use 00:00:00 in UTC for the current day.
        #[arg(long, value_parser = parse_datetime)]
        start_time: Option<chrono::DateTime<chrono::Utc>>,

        /// If set, then table will be deleted after archivation.
        #[arg(long, default_value = "false")]
        delete_after: bool,
    },
}

#[derive(Subcommand, Debug)]
#[command(disable_help_subcommand = true)]
pub enum DatasetSubcommands {
    /// Set dataset-specific options(like time-travel window)
    Options {
        /// Name of the option.
        /// Known options will be checked for value correctness. Unknown options still can be processed and passed without any checks.
        /// Known options are: default_kms_key_name, default_partition_expiration_days, default_rounding_mode, default_table_expiration_days, description, failover_reservation, friendly_name, is_case_insensitive, is_primary, labels, max_time_travel_hours, primary_replica, storage_billing_model, tags
        #[arg(value_parser = bigquery::options::DatasetOption::from_str)]
        option: bigquery::options::DatasetOption,

        /// Value to set (use value `NULL` to remove option, if supported)
        value: String,
    },

    /// Show different dataset statistics and information
    Stats,
}

#[derive(Subcommand, Debug)]
#[command(disable_help_subcommand = true)]
pub enum MergeSubcommands {
    #[command(about = "\
Keep all values on the left without change and add only new values from the right.\n\
Like UPSERT but without UPDATE, or APPEND new records only, or FULL OUTER JOIN with selecting values from the left in case of the same key.\
    ")]
    Insert {
        /// Column name to be used as a key. Should be present in both tables, otherwise use `left_key` and `right_key` options.
        key: Option<String>,

        /// Column name to be used as a key in the left table.
        left_key: Option<String>,

        /// Column name to be used as a key in the right table.
        right_key: Option<String>,

        /// Optional filter on the left table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        left_filter: Option<String>,

        /// Optional filter on the right table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        right_filter: Option<String>,
    },

    #[command(about = "\
Keep all values on the left updating them in case of presence on the right, and add new values from the right.\n\
Like FULL OUTER JOIN with selecting values from the right in case of the same key.\
    ")]
    Upsert {
        /// Column name to be used as a key. Should be present in both tables, otherwise use `left_key` and `right_key` options.
        key: Option<String>,

        /// Column name to be used as a key in the left table.
        left_key: Option<String>,

        /// Column name to be used as a key in the right table.
        right_key: Option<String>,

        /// Optional filter on the left table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        left_filter: Option<String>,

        /// Optional filter on the right table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        right_filter: Option<String>,
    },

    #[command(about = "\
Keep all values from the left updating them in case of presence on the right, BUT not adding new values from the right.\n\
Like LEFT JOIN with selecting values from the right in case of the same key, or UPSERT without INSERT.\
    ")]
    Update {
        /// Column name to be used as a key. Should be present in both tables, otherwise use `left_key` and `right_key` options.
        key: Option<String>,

        /// Column name to be used as a key in the left table.
        left_key: Option<String>,

        /// Column name to be used as a key in the right table.
        right_key: Option<String>,

        /// Optional filter on the left table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        left_filter: Option<String>,

        /// Optional filter on the right table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        right_filter: Option<String>,
    },

    #[command(about = "\
Keep those values from the left for whose we encounter key on the right.\n\
Like INNER JOIN with selecting values from the left in case of the same key.\
    ")]
    InnerLeft {
        /// Column name to be used as a key. Should be present in both tables, otherwise use `left_key` and `right_key` options.
        key: Option<String>,

        /// Column name to be used as a key in the left table.
        left_key: Option<String>,

        /// Column name to be used as a key in the right table.
        right_key: Option<String>,

        /// Optional filter on the left table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        left_filter: Option<String>,

        /// Optional filter on the right table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        right_filter: Option<String>,
    },

    #[command(about = "\
Keep those values from the right for whose we encounter key on the left.\n\
Like INNER JOIN with selecting values from the right in case of the same key.\
    ")]
    InnerRight {
        /// Column name to be used as a key. Should be present in both tables, otherwise use `left_key` and `right_key` options.
        key: Option<String>,

        /// Column name to be used as a key in the left table.
        left_key: Option<String>,

        /// Column name to be used as a key in the right table.
        right_key: Option<String>,

        /// Optional filter on the left table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        left_filter: Option<String>,

        /// Optional filter on the right table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        right_filter: Option<String>,
    },

    #[command(about = "\
Keep only those values from the left and right, which don't share the same key.\n\
Also known as Symmetric Difference, or can be thought as FULL ANTI JOIN.\
    ")]
    Diff {
        /// Column name to be used as a key. Should be present in both tables, otherwise use `left_key` and `right_key` options.
        key: Option<String>,

        /// Column name to be used as a key in the left table.
        left_key: Option<String>,

        /// Column name to be used as a key in the right table.
        right_key: Option<String>,

        /// Optional filter on the left table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        left_filter: Option<String>,

        /// Optional filter on the right table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        right_filter: Option<String>,
    },

    #[command(about = "\
Keep only those values from the left, for whose we don't encounter key on the right.\n\
Also known as (LEFT) ANTI JOIN.\
    ")]
    DiffLeft {
        /// Column name to be used as a key. Should be present in both tables, otherwise use `left_key` and `right_key` options.
        key: Option<String>,

        /// Column name to be used as a key in the left table.
        left_key: Option<String>,

        /// Column name to be used as a key in the right table.
        right_key: Option<String>,

        /// Optional filter on the left table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        left_filter: Option<String>,

        /// Optional filter on the right table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        right_filter: Option<String>,
    },

    #[command(about = "\
Keep only those values from the right, for whose we don't encounter key on the left.\n\
Also known as RIGHT ANTI JOIN.\
    ")]
    DiffRight {
        /// Column name to be used as a key. Should be present in both tables, otherwise use `left_key` and `right_key` options.
        key: Option<String>,

        /// Column name to be used as a key in the left table.
        left_key: Option<String>,

        /// Column name to be used as a key in the right table.
        right_key: Option<String>,

        /// Optional filter on the left table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        left_filter: Option<String>,

        /// Optional filter on the right table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        right_filter: Option<String>,
    },

    #[command(about = "\
Keep all values from the left and right. Can introduce duplicates in the key.\n\
Also known as UNION ALL.\
    ")]
    Union {
        /// Column name to be used as a key. Should be present in both tables, otherwise use `left_key` and `right_key` options.
        key: Option<String>,

        /// Column name to be used as a key in the left table.
        left_key: Option<String>,

        /// Column name to be used as a key in the right table.
        right_key: Option<String>,

        /// Optional filter on the left table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        left_filter: Option<String>,

        /// Optional filter on the right table. Should be a predicates to be used in WHERE-clause (i.e. `X = 1 AND Y = 2`)
        right_filter: Option<String>,
    },
}

fn parse_duration(s: &str) -> Result<Duration, ArgumentsParsingError> {
    let result = humantime::parse_duration(s)?;
    Ok(result)
}

fn parse_datetime(s: &str) -> Result<chrono::DateTime<chrono::Utc>, ArgumentsParsingError> {
    let result =
        chrono::DateTime::parse_from_rfc3339(s).map(|dt| dt.with_timezone(&chrono::Utc))?;
    Ok(result)
}
