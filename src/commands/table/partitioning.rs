use crate::bigquery::client;
use crate::bigquery::executor;
use crate::bigquery::queries;
use crate::bigquery::validators;
use crate::models::bigquery::partitioning::Partitioning;
use crate::models::config::AppConfig;
use crate::models::bigquery::references::TableRef;
use regex::Regex;

pub async fn list(config: AppConfig, table_ref: &TableRef) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;

    let ddl_query = queries::CommonQueries::ddl(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
    );

    let ddl = executor::query_first(&bq_client, &project_id, ddl_query, |row| {
        row.column::<String>(0).unwrap()
    })
    .await?
    .ok_or("Can't find DDL for the table!")?;

    let re = Regex::new(r"(?i)(PARTITION\s+BY\s+[^\n;]+)").unwrap();
    let partitioning_clause = re.captures(&ddl).map(|caps| caps[1].trim().to_string());

    Ok(partitioning_clause)
}

pub async fn add(config: AppConfig, table_ref: &TableRef, partitioning: Option<&Partitioning>) -> Result<(), Box<dyn std::error::Error>> {
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;

    let ddl_query = queries::CommonQueries::ddl(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
    );

    let original_ddl = executor::query_first(&bq_client, &project_id, ddl_query, |row| {
        row.column::<String>(0).unwrap()
    })
    .await?
    .ok_or("Can't find DDL for the table!")?;

    let query = queries::PartitioningQueries::add_or_remove_partitioning(
        &original_ddl,
        table_ref.project.as_deref().unwrap_or(&project_id),
        config
            .temp_dataset
            .as_deref()
            .unwrap_or(table_ref.dataset.as_str()),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        partitioning,
    );

    executor::execute(&bq_client, &project_id, query).await?;

    Ok(())
}

pub async fn remove(config: AppConfig, table_ref: &TableRef) -> Result<(), Box<dyn std::error::Error>> {
    add(config, table_ref, None).await
}
