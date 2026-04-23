use crate::bigquery::client;
use crate::bigquery::executor;
use crate::bigquery::queries;
use crate::bigquery::validators;
use crate::models::bigquery::copy::CopyMetadata;
use crate::models::config::AppConfig;
use crate::models::bigquery::references::{DatasetRef, TableRef};
use rand;

async fn get_tracked_copies(config: &AppConfig, table_ref: &TableRef) -> Result<Vec<CopyMetadata>, Box<dyn std::error::Error>> {
    let (bq_client, project_id) = client::get_client(config).await?;

    let query = queries::CopyQueries::list(
        &config.region,
        table_ref.hex_digest(Some(&project_id)).as_str(),
    );

    let copies = executor::query_collect(&bq_client, &project_id, query, |row| {
        CopyMetadata::new(
            row.column::<i64>(0).unwrap(),
            row.column::<String>(1).unwrap().as_str(),
            row.column::<String>(2).unwrap().as_str(),
            row.column::<String>(3).unwrap().as_str(),
            row.column::<f64>(4).unwrap(),
            row.column::<String>(5).unwrap().as_str(),
        )
    })
    .await?;

    Ok(copies)
}

pub async fn list(config: AppConfig, table_ref: &TableRef) -> Result<Vec<CopyMetadata>, Box<dyn std::error::Error>> {
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;

    let copies = get_tracked_copies(&config, table_ref).await?;

    Ok(copies)
}

pub async fn add(
    config: AppConfig,
    table_ref: &TableRef,
    name: Option<String>,
    dataset: Option<DatasetRef>,
    no_track: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let copy_name = if let Some(name) = name {
        name
    } else {
        let table_name = table_ref.table.as_str();
        let ts = chrono::Utc::now().format("%Y_%d_%mT%H_%M_%S").to_string();
        format!("{table_name}_{ts}")
    };

    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;

    let query = queries::CopyQueries::add(
        table_ref.project.as_deref().unwrap_or(&project_id),
        &table_ref.dataset,
        &table_ref.table,
        &copy_name,
        if let Some(ref dataset_ref) = dataset {
            &dataset_ref.dataset
        } else {
            &table_ref.dataset
        },
        if no_track {
            None
        } else {
            Some(table_ref.hex_digest(Some(&project_id)))
        },
        rand::random_range(1..=1_000_000),
    );

    executor::execute(&bq_client, &project_id, query).await?;

    Ok(())
}

pub async fn remove(config: AppConfig, table_ref: &TableRef, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;

    let copies = get_tracked_copies(&config, table_ref).await?;

    let selected_copies: Vec<&CopyMetadata> = copies
        .iter()
        .filter(|x| x.id.to_string().as_str() == name || x.table == name)
        .collect();
    if let Some(copy) = selected_copies.first() {
        let (bq_client, project_id) = client::get_client(&config).await?;

        let query = queries::CopyQueries::remove(&copy.project, &copy.dataset, &copy.table);

        executor::execute(&bq_client, &project_id, query).await?;
    } else {
        return Err("Copy with provided name or ID not found or not tracked".into());
    }

    Ok(())
}
