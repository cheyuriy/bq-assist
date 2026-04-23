pub mod partitioning {
    use std::fmt::Display;

    use clap::{Args, Subcommand, ValueEnum};

    #[derive(Clone, Debug, Subcommand)]
    pub enum Partitioning {
        /// Integer range (bucket) partitioning
        Range(IntegerRangePartitioning),

        /// Time unit column partitioning
        Time(TimeUnitColumnPartitioning),

        /// Ingestion time partitioning
        Ingestion(IngestionTimePartitioning),
    }

    #[derive(Clone, Debug, Args)]
    pub struct IntegerRangePartitioning {
        /// Name of the column with integer values
        pub column: String,

        /// Minimum value of the range
        pub from: u64,

        /// Maximum value of the range
        pub to: u64,

        /// Range (bucket) size
        pub interval: u64,
    }

    #[derive(Clone, Debug, Args)]
    pub struct TimeUnitColumnPartitioning {
        /// Name of the column to be used for partitioning
        pub column: String,

        /// Type of values
        #[arg(default_value = "timestamp", ignore_case = true)]
        pub column_type: ColumnType,

        /// Partition granularity
        #[arg(default_value = "day", ignore_case = true)]
        pub granularity: Granularity,
    }

    #[derive(Clone, Debug, Args)]
    pub struct IngestionTimePartitioning {
        /// Partition granularity
        #[arg(default_value = "day", ignore_case = true)]
        pub granularity: Granularity,
    }

    #[derive(Clone, Debug, ValueEnum)]
    pub enum Granularity {
        #[value(alias = "h")]
        Hour,

        #[value(alias = "d")]
        Day,

        #[value(alias = "m")]
        Month,

        #[value(alias = "y")]
        Year,
    }

    impl Display for Granularity {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let s = match self {
                Granularity::Month => "MONTH",
                Granularity::Day => "DAY",
                Granularity::Hour => "HOUR",
                Granularity::Year => "YEAR",
            };
            write!(f, "{s}")
        }
    }

    #[derive(Clone, Debug, ValueEnum)]
    #[value(rename_all = "lower")]
    pub enum ColumnType {
        Date,

        #[value(alias = "date-time")]
        DateTime,

        Timestamp,
    }
}

pub mod columns {
    use clap::ValueEnum;
    use std::fmt;
    use std::str::FromStr;
    use tabled::Tabled;
    use tabled::derive::display;

    #[derive(Clone, Debug, ValueEnum, PartialEq)]
    #[value(rename_all = "lower")]
    pub enum Type {
        #[value(alias = "int64", alias = "int")]
        Integer,

        #[value(alias = "float64")]
        Float,

        #[value(alias = "decimal")]
        Numeric,

        #[value(alias = "bigdecimal", alias = "big-numeric")]
        BigNumeric,

        #[value(alias = "bool", alias = "logical")]
        Boolean,

        #[value(alias = "str")]
        String,

        Bytes,
        Date,

        #[value(alias = "date-time")]
        DateTime,

        Time,
        Timestamp,
        Struct,

        #[value(alias = "geo")]
        Geography,

        #[allow(clippy::upper_case_acronyms)]
        JSON,
        Range,
    }

    impl FromStr for Type {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            match s.to_uppercase().as_str() {
                "INT" | "INT64" | "INTEGER" | "SMALLINT" | "BIGINT" | "TINYINT" | "BYTEINT" => {
                    Ok(Type::Integer)
                }
                "FLOAT" | "FLOAT64" => Ok(Type::Float),
                "NUMERIC" | "DECIMAL" => Ok(Type::Numeric),
                "BIGNUMERIC" | "BIGDECIMAL" => Ok(Type::BigNumeric),
                "BOOL" | "BOOLEAN" | "LOGICAL" => Ok(Type::Boolean),
                "STRING" => Ok(Type::String),
                "BYTES" => Ok(Type::Bytes),
                "DATE" => Ok(Type::Date),
                "DATETIME" => Ok(Type::DateTime),
                "TIME" => Ok(Type::Time),
                "TIMESTAMP" => Ok(Type::Timestamp),
                "JSON" => Ok(Type::JSON),
                "GEOGRAPHY" => Ok(Type::Geography),
                other => {
                    if other.starts_with("STRUCT") {
                        Ok(Type::Struct)
                    } else if other.starts_with("RANGE") {
                        Ok(Type::Range)
                    } else {
                        Err(format!("Unknown data type: {}", s))
                    }
                }
            }
        }
    }

    impl fmt::Display for Type {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let s = match self {
                Type::Integer => "INT64",
                Type::Float => "FLOAT64",
                Type::Numeric => "NUMERIC",
                Type::BigNumeric => "BIGNUMERIC",
                Type::Boolean => "BOOLEAN",
                Type::String => "STRING",
                Type::Bytes => "BYTES",
                Type::Date => "DATE",
                Type::DateTime => "DATETIME",
                Type::Time => "TIME",
                Type::Timestamp => "TIMESTAMP",
                Type::Struct => "STRUCT",
                Type::Geography => "GEOGRAPHY",
                Type::JSON => "JSON",
                Type::Range => "RANGE",
            };
            write!(f, "{s}")
        }
    }

    #[derive(Tabled, Debug)]
    pub struct ColumnMetadata {
        #[tabled(rename = "Name", order = 1)]
        pub name: String,

        #[tabled(rename = "#", order = 0)]
        pub ordinal_position: u8,

        #[tabled(display("display::bool", "+", ""), rename = "Nullable?")]
        pub is_nullable: bool,

        #[tabled(rename = "Type", order = 2)]
        pub data_type: Type,

        #[tabled(display("display::bool", "+", ""), rename = "Hidden?")]
        pub is_hidden: bool,

        #[tabled(display("display::bool", "+", ""), rename = "Partitioned?", order = 3)]
        pub is_partitioning_column: bool,

        #[tabled(display("display::option", ""), rename = "Cluster #", order = 4)]
        pub clustering_ordinal_position: Option<u8>,

        #[tabled(display("display::option", ""), rename = "Default")]
        pub column_default: Option<String>,
    }

    impl ColumnMetadata {
        #[allow(clippy::too_many_arguments)]
        pub fn new(
            name: &str,
            ordinal_position: u8,
            is_nullable: &str,
            data_type: &str,
            is_hidden: &str,
            is_partitioning_column: &str,
            clustering_ordinal_position: Option<u8>,
            column_default: Option<String>,
        ) -> Self {
            ColumnMetadata {
                name: name.to_string(),
                ordinal_position,
                is_nullable: is_nullable == "YES",
                data_type: <Type as FromStr>::from_str(data_type).unwrap(),
                is_hidden: is_hidden == "YES",
                is_partitioning_column: is_partitioning_column == "YES",
                clustering_ordinal_position,
                column_default,
            }
        }
    }
}

pub mod options {
    use std::fmt;
    use std::str::FromStr;

    #[derive(Clone, Debug, PartialEq)]
    pub enum TableOption {
        ExpirationTimestamp,
        PartitionExpirationDays,
        RequirePartitionFilter,
        KMSKeyName,
        FriendlyName,
        Description,
        Labels,
        DefaultRoundingMode,
        EnableChangeHistory,
        MaxStaleness,
        EnableFineGrainedMutations,
        StorageURI,
        FileFormat,
        TableFormat,
        Tags,
        Unknown(String),
    }

    impl FromStr for TableOption {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            match s.to_lowercase().as_str() {
                "expiration_timestamp" => Ok(TableOption::ExpirationTimestamp),
                "partition_expiration_days" => Ok(TableOption::PartitionExpirationDays),
                "require_partition_filter" => Ok(TableOption::RequirePartitionFilter),
                "kms_key_name" => Ok(TableOption::KMSKeyName),
                "friendly_name" => Ok(TableOption::FriendlyName),
                "description" => Ok(TableOption::Description),
                "labels" => Ok(TableOption::Labels),
                "default_rounding_mode" => Ok(TableOption::DefaultRoundingMode),
                "enable_change_history" => Ok(TableOption::EnableChangeHistory),
                "max_staleness" => Ok(TableOption::MaxStaleness),
                "enable_fine_grained_mutations" => Ok(TableOption::EnableFineGrainedMutations),
                "storage_uri" => Ok(TableOption::StorageURI),
                "file_format" => Ok(TableOption::FileFormat),
                "table_format" => Ok(TableOption::TableFormat),
                "tags" => Ok(TableOption::Tags),
                other => Ok(TableOption::Unknown(other.to_string())),
            }
        }
    }

    impl fmt::Display for TableOption {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let s = match self {
                TableOption::ExpirationTimestamp => "expiration_timestamp",
                TableOption::PartitionExpirationDays => "partition_expiration_days",
                TableOption::RequirePartitionFilter => "require_partition_filter",
                TableOption::KMSKeyName => "kms_key_name",
                TableOption::FriendlyName => "friendly_name",
                TableOption::Description => "description",
                TableOption::Labels => "labels",
                TableOption::DefaultRoundingMode => "default_rounding_mode",
                TableOption::EnableChangeHistory => "enable_change_history",
                TableOption::MaxStaleness => "max_staleness",
                TableOption::EnableFineGrainedMutations => "enable_fine_grained_mutations",
                TableOption::StorageURI => "storage_uri",
                TableOption::FileFormat => "file_format",
                TableOption::TableFormat => "table_format",
                TableOption::Tags => "tags",
                TableOption::Unknown(s) => s,
            };
            write!(f, "{s}")
        }
    }

    impl TableOption {
        pub fn validate_value(&self, value: &str) -> Result<(), String> {
            if value.to_lowercase() == "null" {
                return Ok(());
            }

            match self {
                TableOption::ExpirationTimestamp => match chrono::DateTime::parse_from_rfc3339(value).map(|dt| dt.with_timezone(&chrono::Utc)) {
                    Ok(_) => Ok(()),
                    Err(_) => Err("Invalid value for `expiration_timestamp` option. Supports RFC3339 format (i.e. `2026-01-01 01:02:03Z`).".to_string())
                }
                TableOption::PartitionExpirationDays => match value.parse::<u32>() {
                    Ok(_) => Ok(()),
                    Err(_) => Err("Invalid value for `partition_expiration_days` option. Only integer value allowed".to_string())
                }
                TableOption::RequirePartitionFilter => match value.parse::<bool>() {
                    Ok(_) => Ok(()),
                    Err(_) => Err("Invalid value for `require_partition_filter` option. Only boolean value allowed.".to_string())
                }
                TableOption::KMSKeyName => Ok(()),
                TableOption::FriendlyName => Ok(()),
                TableOption::Description => Ok(()),
                TableOption::Labels => Ok(()), //TODO: implement
                TableOption::DefaultRoundingMode => if (["ROUND_HALF_AWAY_FROM_ZERO", "ROUND_HALF_EVEN"]).contains(&value.to_uppercase().as_str()) {
                    Ok(())
                } else {
                    Err("Invalid value for `default_rounding_mode` option. Supported values: ROUND_HALF_AWAY_FROM_ZERO or ROUND_HALF_EVEN".to_string())
                }
                TableOption::EnableChangeHistory => match value.parse::<bool>() {
                    Ok(_) => Ok(()),
                    Err(_) => Err("Invalid value for `enable_change_history` option. Only boolean value allowed.".to_string())
                }
                TableOption::MaxStaleness => Ok(()), //TODO: implement check for INTERVAL
                TableOption::EnableFineGrainedMutations => match value.parse::<bool>() {
                    Ok(_) => Ok(()),
                    Err(_) => Err("Invalid value for `enable_fine_grained_mutations` option. Only boolean value allowed.".to_string())
                }
                TableOption::StorageURI => Ok(()),
                TableOption::FileFormat => if value.to_uppercase().as_str() == "PARQUET" {
                    Ok(())
                } else {
                    Err("Invalid value for `file_format` option. Only value PARQUET allowed.".to_string())
                }
                TableOption::TableFormat => if value.to_uppercase().as_str() == "ICEBERG" {
                    Ok(())
                } else {
                    Err("Invalid value for `table_format` option. Only value ICEBERG allowed.".to_string())
                }
                TableOption::Tags => Ok(()), //TODO: implement
                TableOption::Unknown(_) => Ok(())
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    pub enum DatasetOption {
        DefaultKMSKeyName,
        DefaultPartitionExpirationDays,
        DefaultRoundingMode,
        DefaultTableExpirationDays,
        Description,
        FailoverReservation,
        FriendlyName,
        IsCaseSensitive,
        IsPrimary,
        Labels,
        MaxTimeTravelHours,
        PrimaryReplica,
        StorageBillingModel,
        Tags,
        Unknown(String),
    }

    impl FromStr for DatasetOption {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            match s.to_lowercase().as_str() {
                "default_kms_key_name" => Ok(DatasetOption::DefaultKMSKeyName),
                "default_partition_expiration_days" => {
                    Ok(DatasetOption::DefaultPartitionExpirationDays)
                }
                "default_rounding_mode" => Ok(DatasetOption::DefaultRoundingMode),
                "default_table_expiration_days" => Ok(DatasetOption::DefaultTableExpirationDays),
                "description" => Ok(DatasetOption::Description),
                "failover_reservation" => Ok(DatasetOption::FailoverReservation),
                "friendly_name" => Ok(DatasetOption::FriendlyName),
                "is_case_insensitive" => Ok(DatasetOption::IsCaseSensitive),
                "is_primary" => Ok(DatasetOption::IsPrimary),
                "labels" => Ok(DatasetOption::Labels),
                "max_time_travel_hours" => Ok(DatasetOption::MaxTimeTravelHours),
                "primary_replica" => Ok(DatasetOption::PrimaryReplica),
                "storage_billing_model" => Ok(DatasetOption::StorageBillingModel),
                "tags" => Ok(DatasetOption::Tags),
                other => Ok(DatasetOption::Unknown(other.to_string())),
            }
        }
    }

    impl fmt::Display for DatasetOption {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let s = match self {
                DatasetOption::DefaultKMSKeyName => "default_kms_key_name",
                DatasetOption::DefaultPartitionExpirationDays => {
                    "default_partition_expiration_days"
                }
                DatasetOption::DefaultRoundingMode => "default_rounding_mode",
                DatasetOption::DefaultTableExpirationDays => "default_table_expiration_days",
                DatasetOption::Description => "description",
                DatasetOption::FailoverReservation => "failover_reservation",
                DatasetOption::FriendlyName => "friendly_name",
                DatasetOption::IsCaseSensitive => "is_case_insensitive",
                DatasetOption::IsPrimary => "is_primary",
                DatasetOption::Labels => "labels",
                DatasetOption::MaxTimeTravelHours => "max_time_travel_hours",
                DatasetOption::PrimaryReplica => "primary_replica",
                DatasetOption::StorageBillingModel => "storage_billing_model",
                DatasetOption::Tags => "tags",
                DatasetOption::Unknown(s) => s,
            };
            write!(f, "{s}")
        }
    }

    impl DatasetOption {
        pub fn validate_value(&self, value: &str) -> Result<(), String> {
            if value.to_lowercase() == "null" {
                return Ok(());
            }

            match self {
                DatasetOption::DefaultKMSKeyName => Ok(()),
                DatasetOption::DefaultPartitionExpirationDays => match value.parse::<u32>() {
                    Ok(_) => Ok(()),
                    Err(_) => Err("Invalid value for `default_partition_expiration_days` option. Only integer value allowed".to_string())
                }
                DatasetOption::DefaultRoundingMode => if (["ROUND_HALF_AWAY_FROM_ZERO", "ROUND_HALF_EVEN"]).contains(&value.to_uppercase().as_str()) {
                    Ok(())
                } else {
                    Err("Invalid value for `default_rounding_mode` option. Supported values: ROUND_HALF_AWAY_FROM_ZERO or ROUND_HALF_EVEN".to_string())
                }
                DatasetOption::DefaultTableExpirationDays => match value.parse::<u32>() {
                    Ok(_) => Ok(()),
                    Err(_) => Err("Invalid value for `default_table_expiration_days` option. Only integer value allowed".to_string())
                }
                DatasetOption::Description => Ok(()),
                DatasetOption::FailoverReservation => Ok(()),
                DatasetOption::FriendlyName => Ok(()),
                DatasetOption::IsCaseSensitive => match value.parse::<bool>() {
                    Ok(_) => Ok(()),
                    Err(_) => Err("Invalid value for `is_case_insensitive` option. Only boolean value allowed.".to_string())
                }
                DatasetOption::IsPrimary => match value.parse::<bool>() {
                    Ok(_) => Ok(()),
                    Err(_) => Err("Invalid value for `is_primary` option. Only boolean value allowed.".to_string())
                }
                DatasetOption::Labels => Ok(()), //TODO: implement
                DatasetOption::MaxTimeTravelHours => match value.parse::<u32>() {
                    Ok(_) => Ok(()),
                    Err(_) => Err("Invalid value for `max_time_travel_hours` option. Only integer value allowed".to_string())
                }
                DatasetOption::PrimaryReplica => Ok(()),
                DatasetOption::StorageBillingModel => if (["PHYSICAL", "LOGICAL"]).contains(&value.to_uppercase().as_str()) {
                    Ok(())
                } else {
                    Err("Invalid value for `storage_billing_model` option. Supported values: PHYSICAL or LOGICAL".to_string())
                }
                DatasetOption::Tags => Ok(()), //TODO: implement
                DatasetOption::Unknown(_) => Ok(())
            }
        }
    }
}

pub mod copy {
    use tabled::Tabled;

    #[derive(Tabled, Debug)]
    pub struct CopyMetadata {
        #[tabled(rename = "ID")]
        pub id: i64,

        #[tabled(rename = "Project")]
        pub project: String,

        #[tabled(rename = "Dataset")]
        pub dataset: String,

        #[tabled(rename = "Table")]
        pub table: String,

        #[tabled(rename = "Created At")]
        pub creation_time: chrono::DateTime<chrono::Utc>,

        #[tabled(rename = "Origin")]
        pub origin: String,
    }

    impl CopyMetadata {
        pub fn new(
            id: i64,
            table_catalog: &str,
            table_schema: &str,
            table_name: &str,
            creation_time: f64,
            origin: &str,
        ) -> Self {
            CopyMetadata {
                id,
                project: table_catalog.to_string(),
                dataset: table_schema.to_string(),
                table: table_name.to_string(),
                creation_time: chrono::DateTime::from_timestamp_millis(creation_time as i64)
                    .unwrap(),
                origin: origin.to_string(),
            }
        }
    }
}

pub mod queries {
    use tabled::Tabled;

    pub fn format_bytes(bytes: i64) -> String {
        const KB: f64 = 1024.0;
        const MB: f64 = KB * 1024.0;
        const GB: f64 = MB * 1024.0;
        const TB: f64 = GB * 1024.0;
        let f = bytes as f64;
        if f >= TB {
            format!("{:.2} Tb", f / TB)
        } else if f >= GB {
            format!("{:.2} Gb", f / GB)
        } else if f >= MB {
            format!("{:.2} Mb", f / MB)
        } else if f >= KB {
            format!("{:.2} Kb", f / KB)
        } else {
            format!("{} B", bytes)
        }
    }

    #[derive(Tabled, Debug)]
    pub struct QueryJobMetadata {
        #[tabled(rename = "Job ID")]
        pub job_id: String,

        #[tabled(rename = "Created At")]
        pub creation_time: chrono::DateTime<chrono::Utc>,

        #[tabled(rename = "User")]
        pub user_email: String,

        #[tabled(rename = "Statement")]
        pub statement_type: String,

        #[tabled(rename = "State")]
        pub state: String,

        #[tabled(rename = "Data Billed")]
        pub data_billed: String,

        #[tabled(rename = "Query")]
        pub query: String,
    }
}

pub mod stats {
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum BillingMode {
        Logical,
        Physical,
    }

    impl BillingMode {
        pub fn parse(s: &str) -> Self {
            match s.trim_matches('"').to_uppercase().as_str() {
                "PHYSICAL" => BillingMode::Physical,
                _ => BillingMode::Logical,
            }
        }
    }

    #[derive(Debug)]
    pub struct ExternalInfo {
        pub file_format: Option<String>,
        pub uris: Vec<String>,
    }

    #[derive(Debug)]
    pub struct BasicInfo {
        pub fqn: String,
        pub table_type: String,
        pub created: Option<chrono::DateTime<chrono::Utc>>,
        pub updated: Option<chrono::DateTime<chrono::Utc>>,
        pub origin: Option<String>,
        pub external: Option<ExternalInfo>,
        pub snapshots: Vec<String>,
        pub clones: Vec<String>,
    }

    #[derive(Debug)]
    pub struct SizeInfo {
        pub total_rows: i64,
        pub active_logical: i64,
        pub long_term_logical: i64,
        pub total_logical: i64,
        pub active_physical: i64,
        pub long_term_physical: i64,
        pub total_physical: i64,
        pub time_travel: i64,
        pub billing_mode: BillingMode,
    }

    #[derive(Debug)]
    pub struct PartitioningInfo {
        pub column: Option<String>,
        pub clause: Option<String>,
        pub partitions_count: Option<u64>,
        pub avg_partition_bytes: Option<i64>,
        pub require_partition_filter: Option<bool>,
    }

    #[derive(Debug)]
    pub struct ClusteringInfo {
        pub fields: Vec<String>,
    }

    #[derive(Debug, Clone)]
    pub struct OtherOption {
        pub name: String,
        pub value: String,
    }

    pub struct TableStatsData {
        pub basic: BasicInfo,
        pub size: Option<SizeInfo>,
        pub partitioning: Option<PartitioningInfo>,
        pub clustering: Option<ClusteringInfo>,
        pub other_options: Vec<OtherOption>,
        pub ddl: Option<String>,
    }

    pub struct ColumnMetaInfo {
        pub data_type: String,
        pub is_nullable: bool,
        pub clustering_position: Option<i64>,
        pub is_partitioned: bool,
        pub partition_clause: Option<String>,
    }

    pub struct BinCount {
        pub lower: f64,
        pub upper: f64,
        pub count: i64,
    }

    pub enum DeepStats {
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

    pub struct CategoryStats {
        pub distinct_count: i64,
        pub frequency: Option<Vec<(String, i64)>>,
    }
}

pub mod references {
    use crate::errors::ArgumentsParsingError;
    use md5;
    use std::{fmt::Display, str::FromStr};

    #[derive(Debug, Clone)]
    pub struct TableRef {
        pub project: Option<String>,
        pub dataset: String,
        pub table: String,
    }

    impl FromStr for TableRef {
        type Err = ArgumentsParsingError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let parts: Vec<&str> = s.split('.').collect();

            match parts.as_slice() {
                [dataset, table] => Ok(TableRef {
                    project: None,
                    dataset: dataset.to_string(),
                    table: table.to_string(),
                }),
                [project, dataset, table] => Ok(TableRef {
                    project: Some(project.to_string()),
                    dataset: dataset.to_string(),
                    table: table.to_string(),
                }),
                _ => Err(ArgumentsParsingError::InvalidTableRefFormat),
            }
        }
    }

    impl Display for TableRef {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "`{:?}.{}.{}`", self.project, self.dataset, self.table)
        }
    }

    impl TableRef {
        pub fn hex_digest(&self, project: Option<&str>) -> String {
            if self.project.is_none() && project.is_none() {
                panic!("Can't generate table reference digest without project specified");
            }
            let input = format!(
                "{}.{}.{}",
                if let Some(ref ref_project) = self.project {
                    ref_project.to_lowercase()
                } else {
                    project.unwrap().to_lowercase()
                },
                self.dataset.to_lowercase(),
                self.table.to_lowercase()
            );

            format!("{:x}", md5::compute(input))
        }
    }

    #[derive(Clone, Debug)]
    pub struct DatasetRef {
        pub project: Option<String>,
        pub dataset: String,
    }

    impl FromStr for DatasetRef {
        type Err = ArgumentsParsingError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let parts: Vec<&str> = s.split('.').collect();

            match parts.as_slice() {
                [dataset] => Ok(DatasetRef {
                    project: None,
                    dataset: dataset.to_string(),
                }),
                [project, dataset] => Ok(DatasetRef {
                    project: Some(project.to_string()),
                    dataset: dataset.to_string(),
                }),
                _ => Err(ArgumentsParsingError::InvalidDatasetRefFormat),
            }
        }
    }

    impl Display for DatasetRef {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "`{:?}.{}`", self.project, self.dataset)
        }
    }
}

pub mod snapshot {
    use tabled::Tabled;

    #[derive(Tabled, Debug)]
    pub struct SnapshotMetadata {
        #[tabled(rename = "ID")]
        pub id: i64,

        #[tabled(rename = "Project")]
        pub project: String,

        #[tabled(rename = "Dataset")]
        pub dataset: String,

        #[tabled(rename = "Table")]
        pub table: String,

        #[tabled(rename = "Created At")]
        pub creation_time: chrono::DateTime<chrono::Utc>,

        #[tabled(rename = "Origin")]
        pub origin: String,
    }

    impl SnapshotMetadata {
        pub fn new(
            id: i64,
            table_catalog: &str,
            table_schema: &str,
            table_name: &str,
            creation_time: f64,
            origin: &str,
        ) -> Self {
            SnapshotMetadata {
                id,
                project: table_catalog.to_string(),
                dataset: table_schema.to_string(),
                table: table_name.to_string(),
                creation_time: chrono::DateTime::from_timestamp_millis(creation_time as i64)
                    .unwrap(),
                origin: origin.to_string(),
            }
        }
    }
}
