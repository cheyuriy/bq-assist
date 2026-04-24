use std::time::Duration;

use crate::models::bigquery::{self, columns::Type};
use minijinja::{Environment, context};
use regex::Regex;

fn setup() -> Environment<'static> {
    let mut env = Environment::new();
    minijinja_embed::load_templates!(&mut env);
    env
}

pub struct ClusteringQueries;

impl ClusteringQueries {
    pub fn list_clustering_fields(project: &str, dataset: &str, table: &str) -> String {
        let env = setup();
        let template = env.get_template("table_clustering_list.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
        };
        
        template.render(context).unwrap()
    }

    pub fn add_or_remove_clustering(
        ddl: &str,
        project: &str,
        temp_dataset: &str,
        original_dataset: &str,
        table: &str,
        fields: Vec<String>,
    ) -> String {
        let re = Regex::new(r"(?i)(PARTITION\s+BY\s+[^\n;]+)").unwrap();

        let partitioning_clause = re.captures(ddl).map(|caps| caps[1].trim().to_string());

        let clustering_clause = if fields.is_empty() {
            None
        } else {
            let clustering_fields = fields.join(", ");
            Some(format!("CLUSTER BY {clustering_fields}"))
        };

        let env = setup();
        let template = env
            .get_template("table_partitioningclustering_addremove.sql")
            .unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => original_dataset,
            bigquery_temp_dataset => temp_dataset,
            bigquery_table => table,
            partitioning_clause => partitioning_clause,
            clustering_clause => clustering_clause
        };
        
        template.render(context).unwrap()
    }
}

pub struct PartitioningQueries;

impl PartitioningQueries {
    pub fn add_or_remove_partitioning(
        ddl: &str,
        project: &str,
        temp_dataset: &str,
        original_dataset: &str,
        table: &str,
        partitioning: Option<&bigquery::partitioning::Partitioning>,
    ) -> String {
        let re = Regex::new(r"(?i)(CLUSTER\s+BY\s+[^\n;]+)").unwrap();

        let clustering_clause = re.captures(ddl).map(|caps| caps[1].trim().to_string());

        let partitioning_clause = match partitioning {
            Some(bigquery::partitioning::Partitioning::Ingestion(
                bigquery::partitioning::IngestionTimePartitioning { granularity },
            )) => {
                let schema_re = Regex::new(r"(?s)(\([\s\S]*?\n\))").unwrap();
                let schema_clause = schema_re
                    .find(ddl)
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default();
                // BigQuery top-level column names are indented with exactly 2 spaces in DDL.
                // Matching exactly 2 spaces avoids picking up nested STRUCT fields.
                let col_re = Regex::new(r"(?m)^  (\w+) ").unwrap();
                let columns_clause = col_re
                    .captures_iter(&schema_clause)
                    .map(|caps| caps[1].to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let partitioning_clause =
                    format!("PARTITION BY TIMESTAMP_TRUNC(_PARTITIONTIME, {granularity})");
                let env = setup();
                let template = env
                    .get_template("table_partitioning_ingestion_addremove.sql")
                    .unwrap();
                let context = context! {
                    bigquery_project => project,
                    bigquery_dataset => original_dataset,
                    bigquery_temp_dataset => temp_dataset,
                    bigquery_table => table,
                    schema_clause => schema_clause,
                    partitioning_clause => partitioning_clause,
                    clustering_clause => clustering_clause,
                    columns_clause => columns_clause
                };
                return template.render(context).unwrap();
            }
            Some(bigquery::partitioning::Partitioning::Range(
                bigquery::partitioning::IntegerRangePartitioning {
                    column,
                    from,
                    to,
                    interval,
                },
            )) => Some(format!(
                "PARTITION BY RANGE_BUCKET({column}, GENERATE_ARRAY({from}, {to}, {interval}))"
            )),
            Some(bigquery::partitioning::Partitioning::Time(
                bigquery::partitioning::TimeUnitColumnPartitioning {
                    column,
                    column_type,
                    granularity,
                },
            )) => Some(match column_type {
                bigquery::partitioning::ColumnType::Date => {
                    format!("PARTITION BY DATE_TRUNC({column}, {granularity})")
                }
                bigquery::partitioning::ColumnType::Timestamp => {
                    format!("PARTITION BY TIMESTAMP_TRUNC({column}, {granularity})")
                }
                bigquery::partitioning::ColumnType::DateTime => {
                    format!("PARTITION BY DATE_TRUNC({column}, {granularity})")
                }
            }),
            None => None,
        };

        let env = setup();
        let template = env
            .get_template("table_partitioningclustering_addremove.sql")
            .unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => original_dataset,
            bigquery_temp_dataset => temp_dataset,
            bigquery_table => table,
            partitioning_clause => partitioning_clause,
            clustering_clause => clustering_clause
        };
        
        template.render(context).unwrap()
    }
}

pub struct ColumnsQueries;

impl ColumnsQueries {
    pub fn add_column(
        project: &str,
        dataset: &str,
        table: &str,
        column_name: &str,
        column_type: &bigquery::columns::Type,
        default_clause: Option<String>,
    ) -> String {
        let env = setup();
        let template = env.get_template("table_columns_add.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            column_name => column_name,
            column_type => column_type.to_string(),
            default_clause => default_clause
        };
        template.render(context).unwrap()
    }

    pub fn remove_column(project: &str, dataset: &str, table: &str, column_name: &str) -> String {
        let env = setup();
        let template = env.get_template("table_columns_remove.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            column_name => column_name
        };
        
        template.render(context).unwrap()
    }

    pub fn rename_column(
        project: &str,
        dataset: &str,
        table: &str,
        column_name: &str,
        column_new_name: &str,
        column_type: &bigquery::columns::Type,
        default_clause: Option<String>,
    ) -> String {
        let env = setup();
        let template = env.get_template("table_columns_rename.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            column_name => column_name,
            column_new_name => column_new_name,
            column_type => column_type.to_string(),
            default_clause => default_clause
        };
        
        template.render(context).unwrap()
    }

    pub fn cast_column(
        project: &str,
        dataset: &str,
        table: &str,
        column_name: &str,
        column_type: &bigquery::columns::Type,
        column_new_type: &bigquery::columns::Type,
    ) -> (String, Option<String>) {
        let env = setup();

        if (*column_type == Type::Integer
            && ([Type::Numeric, Type::BigNumeric, Type::Float]).contains(column_new_type))
            || (*column_type == Type::Numeric
                && ([Type::BigNumeric, Type::Float]).contains(column_new_type))
        {
            let cast_template = env.get_template("table_columns_cast_fast.sql").unwrap();
            let cast_context = context! {
                bigquery_project => project,
                bigquery_dataset => dataset,
                bigquery_table => table,
                column_name => column_name,
                column_new_type => column_new_type.to_string()
            };
            let cast_rendered = cast_template.render(cast_context).unwrap();
            (cast_rendered, None)
        } else {
            let column_temp_name = format!("{column_name}_temp");
            let rename_template = env.get_template("table_columns_rename.sql").unwrap();
            let rename_context = context! {
                bigquery_project => project,
                bigquery_dataset => dataset,
                bigquery_table => table,
                column_name => column_name,
                column_new_name => column_temp_name,
                column_type => column_type.to_string(),
                default_clause => None::<Option<String>>
            };
            let rename_rendered = rename_template.render(rename_context).unwrap();

            let cast_clause = match (column_type, column_new_type) {
                (Type::Integer, to) => match to {
                    Type::Numeric | Type::BigNumeric | Type::Float => unreachable!(),
                    Type::Boolean | Type::String => format!("CAST({column_temp_name} AS {to})"),
                    Type::Bytes => format!("FROM_HEX(FORMAT('%016x', {column_temp_name}))"),
                    Type::Date | Type::DateTime | Type::Time => {
                        format!("{to}(TIMESTAMP_SECONDS({column_temp_name}))")
                    }
                    Type::Timestamp => format!("TIMESTAMP_SECONDS({column_temp_name})"),
                    Type::JSON => format!("PARSE_JSON(CAST({column_temp_name} AS STRING))"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::Numeric, to) => match to {
                    Type::Integer | Type::String => format!("CAST({column_temp_name} AS {to})"),
                    Type::BigNumeric | Type::Float => unreachable!(),
                    Type::Boolean => format!("CAST(CAST({column_temp_name} AS INT64) AS BOOLEAN)"),
                    Type::JSON => format!("PARSE_JSON(CAST({column_temp_name} AS STRING))"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::BigNumeric, to) => match to {
                    Type::Integer | Type::Numeric | Type::Float | Type::String => {
                        format!("CAST({column_temp_name} AS {to})")
                    }
                    Type::Boolean => format!("CAST(CAST({column_temp_name} AS INT64) AS BOOLEAN)"),
                    Type::JSON => format!("PARSE_JSON(CAST({column_temp_name} AS STRING))"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::Float, to) => match to {
                    Type::Integer | Type::Numeric | Type::BigNumeric | Type::String => {
                        format!("CAST({column_temp_name} AS {to})")
                    }
                    Type::Boolean => format!("CAST(CAST({column_temp_name} AS INT64) AS BOOLEAN)"),
                    Type::JSON => format!("PARSE_JSON(CAST({column_temp_name} AS STRING))"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::Boolean, to) => match to {
                    Type::Integer | Type::String => format!("CAST({column_temp_name} AS {to})"),
                    Type::Numeric | Type::BigNumeric | Type::Float => {
                        format!("CAST(CAST({column_temp_name} AS int64) AS {to})")
                    }
                    Type::Bytes => {
                        format!("FROM_HEX(FORMAT('%016x', CAST({column_temp_name} AS INT64)))")
                    }
                    Type::JSON => format!("PARSE_JSON(CAST({column_temp_name} AS STRING))"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::String, to) => match to {
                    Type::Integer
                    | Type::Float
                    | Type::Boolean
                    | Type::Bytes
                    | Type::Date
                    | Type::DateTime
                    | Type::Time
                    | Type::Timestamp => format!("CAST({column_temp_name} AS {to})"),
                    Type::Numeric | Type::BigNumeric => format!("PARSE_{to}({column_temp_name})"),
                    Type::JSON => format!("PARSE_JSON({column_temp_name})"),
                    Type::Geography => format!("ST_GEOGFROM({column_temp_name})"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::Bytes, to) => match to {
                    Type::Integer => format!(
                        "CAST(CONCAT('0x', TO_HEX(FROM_HEX(FORMAT('%016x', {column_temp_name})) )) AS INT64)"
                    ),
                    Type::Boolean => format!(
                        "CAST(CAST(CONCAT('0x', TO_HEX(FROM_HEX(FORMAT('%016x', {column_temp_name})) )) AS INT64) AS BOOLEAN)"
                    ),
                    Type::String => format!("SAFE_CONVERT_BYTES_TO_STRING({column_temp_name})"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::Date, to) => match to {
                    Type::String | Type::DateTime | Type::Timestamp => {
                        format!("CAST({column_temp_name} AS {to})")
                    }
                    Type::Integer => format!("UNIX_DATE({column_temp_name})"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::DateTime, to) => match to {
                    Type::String | Type::Date | Type::Time | Type::Timestamp => {
                        format!("CAST({column_temp_name} AS {to})")
                    }
                    Type::Integer => format!(
                        "UNIX_SECONDS(CAST(CAST({column_temp_name} AS DATETIME) AS TIMESTAMP))"
                    ),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::Time, to) => match to {
                    Type::String => format!("CAST({column_temp_name} AS {to})"),
                    Type::Integer => format!("EXTRACT(SECOND FROM {column_temp_name})"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::Timestamp, to) => match to {
                    Type::String | Type::Date | Type::DateTime | Type::Time => {
                        format!("CAST({column_temp_name} AS {to})")
                    }
                    Type::Integer => format!("UNIX_SECONDS({column_temp_name})"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::Struct, _) => panic!("Casting from {column_type} is not supported"),
                (Type::Range, to) => match to {
                    Type::String => format!("CAST({column_temp_name} AS {to})"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::JSON, to) => match to {
                    Type::Integer => format!("LAX_INT64({column_temp_name})"),
                    Type::Numeric | Type::BigNumeric => {
                        format!("CAST(LAX_FLOAT64({column_temp_name}) AS {to}")
                    }
                    Type::Float => format!("LAX_FLOAT64({column_temp_name})"),
                    Type::Boolean => format!("LAX_BOOL({column_temp_name})"),
                    Type::String => format!("TO_JSON_STRING({column_temp_name})"),
                    Type::JSON => format!("ST_GEOGFROM(TO_JSON_STRING({column_temp_name}))"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
                (Type::Geography, to) => match to {
                    Type::String => format!("ST_ASTEXT({column_temp_name})"),
                    Type::Bytes => format!("ST_ASBINARY({column_temp_name})"),
                    Type::JSON => format!("PARSE_JSON(ST_ASGEOJSON({column_temp_name}))"),
                    _ => panic!("Casting from {column_type} to {to} is not supported"),
                },
            };

            let cast_template = env.get_template("table_columns_cast.sql").unwrap();
            let cast_context = context! {
                bigquery_project => project,
                bigquery_dataset => dataset,
                bigquery_table => table,
                column_name => column_temp_name,
                column_new_name => column_name,
                column_new_type => column_new_type.to_string(),
                cast_clause => Some(cast_clause)
            };
            let cast_rendered = cast_template.render(cast_context).unwrap();

            (rename_rendered, Some(cast_rendered))
        }
    }
}

pub struct TableQueries;

impl TableQueries {
    pub fn rename(project: &str, dataset: &str, table: &str, new_name: &str) -> String {
        let env = setup();
        let template = env.get_template("table_rename.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            new_name => new_name
        };
        
        template.render(context).unwrap()
    }

    pub fn set_option(
        project: &str,
        dataset: &str,
        table: &str,
        option: &bigquery::options::TableOption,
        value: &str,
    ) -> String {
        let env = setup();

        let value_clause = match option {
            bigquery::options::TableOption::ExpirationTimestamp => format!("TIMESTAMP \"{value}\""),
            bigquery::options::TableOption::Labels | bigquery::options::TableOption::Tags => {
                let pairs: Vec<String> = value
                    .split(',')
                    .filter_map(|pair| {
                        let (k, v) = pair.split_once(':')?;
                        Some(format!("(\"{}\",\"{}\")", k, v))
                    })
                    .collect();

                format!("[{}]", pairs.join(","))
            }
            bigquery::options::TableOption::Description => format!("\"{value}\""),
            bigquery::options::TableOption::Unknown(s) => s.clone(),
            _ => value.to_string(),
        };

        let template = env.get_template("table_option.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            option_name => option.to_string(),
            option_value => value_clause
        };
        
        template.render(context).unwrap()
    }

    pub fn rewind(project: &str, dataset: &str, table: &str, duration: &Duration) -> String {
        let duration_seconds = duration.as_secs();

        let env = setup();
        let template = env.get_template("table_restore_rewind.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            interval => duration_seconds
        };
        
        template.render(context).unwrap()
    }
}

pub struct DatasetQueries;

impl DatasetQueries {
    pub fn set_option(
        project: &str,
        dataset: &str,
        option: &bigquery::options::DatasetOption,
        value: &str,
    ) -> String {
        let env = setup();

        let value_clause = match option {
            bigquery::options::DatasetOption::Labels | bigquery::options::DatasetOption::Tags => {
                let pairs: Vec<String> = value
                    .split(',')
                    .filter_map(|pair| {
                        let (k, v) = pair.split_once(':')?;
                        Some(format!("(\"{}\",\"{}\")", k, v))
                    })
                    .collect();

                format!("[{}]", pairs.join(","))
            }
            bigquery::options::DatasetOption::Description => format!("\"{value}\""),
            bigquery::options::DatasetOption::Unknown(s) => s.clone(),
            _ => value.to_string(),
        };

        let template = env.get_template("dataset_option.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            option_name => option.to_string(),
            option_value => value_clause
        };
        
        template.render(context).unwrap()
    }
}

pub struct CopyQueries;

impl CopyQueries {
    pub fn list(region: &str, table_digest: &str) -> String {
        let env = setup();
        let template = env.get_template("table_copy_list.sql").unwrap();
        let context = context! {
            region => region,
            table_ref_digest => table_digest,
        };
        
        template.render(context).unwrap()
    }

    pub fn add(
        project: &str,
        dataset: &str,
        table: &str,
        copy_name: &str,
        copy_dataset: &str,
        table_digest: Option<String>,
        copy_id: i64,
    ) -> String {
        let env = setup();
        let template = env.get_template("table_copy_add.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_copy_dataset => copy_dataset,
            bigquery_copy => copy_name,
            bigquery_dataset => dataset,
            bigquery_table => table,
            table_ref_digest => table_digest,
            copy_id => copy_id
        };
        
        template.render(context).unwrap()
    }

    pub fn remove(project: &str, dataset: &str, table: &str) -> String {
        let env = setup();
        let template = env.get_template("table_copy_remove.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
        };
        
        template.render(context).unwrap()
    }
}

pub struct SnapshotsQueries;

impl SnapshotsQueries {
    pub fn list(region: &str, table_digest: &str) -> String {
        let env = setup();
        let template = env.get_template("table_snapshots_list.sql").unwrap();
        let context = context! {
            region => region,
            table_ref_digest => table_digest,
        };
        
        template.render(context).unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add(
        project: &str,
        dataset: &str,
        table: &str,
        snapshot_name: &str,
        snapshot_dataset: &str,
        snapshot_ts: Option<String>,
        table_digest: Option<String>,
        snapshot_id: i64,
    ) -> String {
        let env = setup();
        let template = env.get_template("table_snapshots_add.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_snapshot_dataset => snapshot_dataset,
            bigquery_snapshot => snapshot_name,
            bigquery_dataset => dataset,
            bigquery_table => table,
            table_ref_digest => table_digest,
            snapshot_ts => snapshot_ts,
            snapshot_id => snapshot_id
        };
        
        template.render(context).unwrap()
    }

    pub fn remove(project: &str, dataset: &str, table: &str) -> String {
        let env = setup();
        let template = env.get_template("table_snapshots_remove.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
        };
        
        template.render(context).unwrap()
    }
}

pub struct QueriesQueries;

impl QueriesQueries {
    #[allow(clippy::too_many_arguments)]
    pub fn modify(
        project: &str,
        dataset: &str,
        table: &str,
        region: &str,
        from_ts: &str,
        to_ts: Option<&str>,
        user: Option<&str>,
        query_type: Option<&str>,
        related: bool,
        limit: u64,
    ) -> String {
        let env = setup();
        let template = env.get_template("table_queries_modify.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            region => region,
            from_ts => from_ts,
            to_ts => to_ts,
            user => user,
            query_type => query_type,
            related => related,
            limit => limit,
        };
        template.render(context).unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read(
        project: &str,
        dataset: &str,
        table: &str,
        region: &str,
        from_ts: &str,
        to_ts: Option<&str>,
        user: Option<&str>,
        single: bool,
        limit: u64,
    ) -> String {
        let env = setup();
        let template = env.get_template("table_queries_read.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            region => region,
            from_ts => from_ts,
            to_ts => to_ts,
            user => user,
            single => single,
            limit => limit,
        };
        template.render(context).unwrap()
    }
}

pub struct CommonQueries;

impl CommonQueries {
    pub fn ddl(project: &str, dataset: &str, table: &str) -> String {
        let env = setup();
        let template = env.get_template("table_ddl.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
        };
        
        template.render(context).unwrap()
    }

    pub fn columns(
        project: &str,
        dataset: &str,
        table: &str,
        specific_column: Option<String>,
    ) -> String {
        let env = setup();
        let template = env.get_template("table_columns_list.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            specific_column => specific_column
        };
        
        template.render(context).unwrap()
    }
}

pub struct StatsQueries;

impl StatsQueries {
    pub fn table_info(project: &str, dataset: &str, table: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_table_info.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
        };
        template.render(context).unwrap()
    }

    pub fn table_options(project: &str, dataset: &str, table: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_table_options.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
        };
        template.render(context).unwrap()
    }

    pub fn table_storage(region: &str, project: &str, dataset: &str, table: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_table_storage.sql").unwrap();
        let context = context! {
            region => region,
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
        };
        template.render(context).unwrap()
    }

    pub fn dataset_billing_mode(region: &str, project: &str, dataset: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_dataset_billing.sql").unwrap();
        let context = context! {
            region => region,
            bigquery_project => project,
            bigquery_dataset => dataset,
        };
        template.render(context).unwrap()
    }

    pub fn partitions(project: &str, dataset: &str, table: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_partitions.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
        };
        template.render(context).unwrap()
    }

    pub fn child_snapshots(region: &str, project: &str, dataset: &str, table: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_child_snapshots.sql").unwrap();
        let context = context! {
            region => region,
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
        };
        template.render(context).unwrap()
    }

    pub fn child_clones(project: &str, dataset: &str, table: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_child_clones.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
        };
        template.render(context).unwrap()
    }

    pub fn column_info(project: &str, dataset: &str, table: &str, column: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_column_info.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            bigquery_column => column,
        };
        template.render(context).unwrap()
    }

    pub fn column_numeric(
        project: &str,
        dataset: &str,
        table: &str,
        column: &str,
        bins_number: u32,
    ) -> String {
        let env = setup();
        let template = env.get_template("stats_column_numeric.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            bigquery_column => column,
            bins_number => bins_number,
        };
        template.render(context).unwrap()
    }

    pub fn column_string(project: &str, dataset: &str, table: &str, column: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_column_string.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            bigquery_column => column,
        };
        template.render(context).unwrap()
    }

    pub fn column_datetime(
        project: &str,
        dataset: &str,
        table: &str,
        column: &str,
        time_bins: &str,
        is_time: bool,
        trunc_fn: &str,
    ) -> String {
        let env = setup();
        let template = env.get_template("stats_column_datetime.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            bigquery_column => column,
            time_bins => time_bins,
            is_time => is_time,
            trunc_fn => trunc_fn,
        };
        template.render(context).unwrap()
    }

    pub fn column_boolean(project: &str, dataset: &str, table: &str, column: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_column_boolean.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            bigquery_column => column,
        };
        template.render(context).unwrap()
    }

    pub fn column_generic(project: &str, dataset: &str, table: &str, column: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_column_generic.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            bigquery_column => column,
        };
        template.render(context).unwrap()
    }

    pub fn column_frequency(project: &str, dataset: &str, table: &str, column: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_column_frequency.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
            bigquery_table => table,
            bigquery_column => column,
        };
        template.render(context).unwrap()
    }

    pub fn dataset_info(region: &str, project: &str, dataset: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_dataset_info.sql").unwrap();
        let context = context! {
            region => region,
            bigquery_project => project,
            bigquery_dataset => dataset,
        };
        template.render(context).unwrap()
    }

    pub fn dataset_options(region: &str, project: &str, dataset: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_dataset_options.sql").unwrap();
        let context = context! {
            region => region,
            bigquery_project => project,
            bigquery_dataset => dataset,
        };
        template.render(context).unwrap()
    }

    pub fn dataset_tables(project: &str, dataset: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_dataset_tables.sql").unwrap();
        let context = context! {
            bigquery_project => project,
            bigquery_dataset => dataset,
        };
        template.render(context).unwrap()
    }

    pub fn dataset_storage_aggregate(region: &str, project: &str, dataset: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_dataset_storage.sql").unwrap();
        let context = context! {
            region => region,
            bigquery_project => project,
            bigquery_dataset => dataset,
        };
        template.render(context).unwrap()
    }

    pub fn dataset_table_sizes(region: &str, project: &str, dataset: &str) -> String {
        let env = setup();
        let template = env.get_template("stats_dataset_table_sizes.sql").unwrap();
        let context = context! {
            region => region,
            bigquery_project => project,
            bigquery_dataset => dataset,
        };
        template.render(context).unwrap()
    }
}
