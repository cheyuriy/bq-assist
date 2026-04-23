use crate::bigquery::client;
use crate::bigquery::executor;
use crate::bigquery::queries;
use crate::bigquery::validators;
use crate::errors::ValidationError;
use crate::models::bigquery::columns::{ColumnMetadata, Type};
use crate::models::config::AppConfig;
use crate::models::bigquery::references::TableRef;
use tokio::time::{Duration, sleep};

fn map_column_row(row: google_cloud_bigquery::query::row::Row) -> ColumnMetadata {
    ColumnMetadata::new(
        row.column::<String>(3).unwrap().as_str(),
        row.column::<String>(4).unwrap().parse::<u8>().unwrap(),
        row.column::<String>(5).unwrap().as_str(),
        row.column::<String>(6).unwrap().as_str(),
        row.column::<String>(10).unwrap().as_str(),
        row.column::<String>(13).unwrap().as_str(),
        match row.column::<String>(14) {
            Ok(v) => match v.parse::<u8>() {
                Ok(r) => Some(r),
                Err(_) => None,
            },
            Err(_) => None,
        },
        match row.column::<String>(16) {
            Ok(v) => {
                if v != "NULL" {
                    Some(v)
                } else {
                    None
                }
            }
            Err(_) => None,
        },
    )
}

pub async fn list(config: AppConfig, table_ref: &TableRef) -> Result<Vec<ColumnMetadata>, Box<dyn std::error::Error>> {
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;

    let query = queries::CommonQueries::columns(
        table_ref
            .project
            .as_deref()
            .unwrap_or(project_id.clone().as_str()),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        None,
    );

    let columns = executor::query_collect(&bq_client, &project_id, query, map_column_row).await?;

    Ok(columns)
}

pub async fn add(
    config: AppConfig,
    table_ref: &TableRef,
    name: &str,
    field_type: &Type,
    default_value: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if *field_type == Type::Range || *field_type == Type::Struct {
        return Err(ValidationError(format!("Adding column with type {field_type:?} is not implemented")).into());
    }

    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;

    let query = queries::ColumnsQueries::add_column(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        name,
        &field_type,
        default_value,
    );

    executor::execute(&bq_client, &project_id, query).await?;

    Ok(())
}

pub async fn remove(config: AppConfig, table_ref: &TableRef, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;

    let query = queries::ColumnsQueries::remove_column(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        name,
    );

    executor::execute(&bq_client, &project_id, query).await?;

    Ok(())
}

pub async fn rename(config: AppConfig, table_ref: &TableRef, name: &str, new_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;

    let column_query = queries::CommonQueries::columns(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        Some(name.to_string()),
    );

    let column = executor::query_first(&bq_client, &project_id, column_query, map_column_row)
        .await?
        .ok_or("Can't find metadata about that column")?;

    let rename_query = queries::ColumnsQueries::rename_column(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        name,
        new_name,
        &column.data_type,
        column.column_default,
    );

    executor::execute(&bq_client, &project_id, rename_query).await?;

    Ok(())
}

pub async fn cast(config: AppConfig, table_ref: &TableRef, name: &str, field_type: &Type) -> Result<(), Box<dyn std::error::Error>> {
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;

    let column_query = queries::CommonQueries::columns(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        Some(name.to_string()),
    );

    let column = executor::query_first(&bq_client, &project_id, column_query, map_column_row)
        .await?
        .ok_or("Can't find metadata about that column")?;

    // BigQuery limits number of DDL and DML jobs per table (5 per 10 seconds per table by default)
    // However, casting column to another type needs more than 5 operations
    // So we divide it in two steps with a 10 seconds pause between them
    let (first_query, second_query) = queries::ColumnsQueries::cast_column(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        name,
        &column.data_type,
        &field_type,
    );

    executor::execute(&bq_client, &project_id, first_query).await?;

    if let Some(second_query) = second_query {
        crate::output::print_cast_waiting();
        sleep(Duration::from_secs(10)).await;
        crate::output::print_cast_done();

        executor::execute(&bq_client, &project_id, second_query).await?;
    }

    Ok(())
}
