use crate::bigquery::client;
use crate::bigquery::executor;
use crate::bigquery::queries;
use crate::bigquery::validators;
use crate::models::bigquery::snapshot::SnapshotMetadata;
use crate::models::config::AppConfig;
use crate::models::bigquery::references::{DatasetRef, TableRef};
use chrono::DateTime;
use chrono::Utc;
use rand;
use std::time::Duration;

async fn get_tracked_snapshots(config: &AppConfig, table_ref: &TableRef) -> Result<Vec<SnapshotMetadata>, Box<dyn std::error::Error>> {
    let (bq_client, project_id) = client::get_client(&config).await?;

    let query = queries::SnapshotsQueries::list(
        &config.region,
        table_ref.hex_digest(Some(&project_id)).as_str(),
    );

    let snapshots = executor::query_collect(&bq_client, &project_id, query, |row| {
        SnapshotMetadata::new(
            row.column::<i64>(0).unwrap(),
            row.column::<String>(1).unwrap().as_str(),
            row.column::<String>(2).unwrap().as_str(),
            row.column::<String>(3).unwrap().as_str(),
            row.column::<f64>(4).unwrap(),
            row.column::<String>(5).unwrap().as_str(),
        )
    })
    .await?;

    Ok(snapshots)
}

pub async fn list(config: AppConfig, table_ref: &TableRef) -> Result<Vec<SnapshotMetadata>, Box<dyn std::error::Error>> {
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;

    let snapshots = get_tracked_snapshots(&config, table_ref).await?;

    Ok(snapshots)
}

pub async fn add(
    config: AppConfig,
    table_ref: &TableRef,
    name: Option<String>,
    dataset: Option<DatasetRef>,
    rewind: Option<Duration>,
    timestamp: Option<DateTime<Utc>>,
    no_track: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = Utc::now();
    let target_timestamp = if rewind.is_some() {
        Some(now.timestamp() - rewind.map(|d| d.as_secs() as i64).unwrap())
    } else if timestamp.is_some() {
        Some(now.timestamp() - timestamp.map(|t| t.timestamp()).unwrap())
    } else {
        None
    };

    let snapshot_ts = target_timestamp
        .map(|x| {
            chrono::DateTime::from_timestamp(x, 0)
                .unwrap()
                .format("%Y_%d_%mT%H_%M_%S")
                .to_string()
        })
        .unwrap_or(now.format("%Y_%d_%mT%H_%M_%S").to_string());
    let snapshot_name = if let Some(name) = name {
        name
    } else {
        let table_name = table_ref.table.as_str();
        format!("{table_name}_{snapshot_ts}")
    };

    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;

    let query = queries::SnapshotsQueries::add(
        table_ref.project.as_deref().unwrap_or(&project_id),
        &table_ref.dataset,
        &table_ref.table,
        &snapshot_name,
        if let Some(ref dataset_ref) = dataset {
            &dataset_ref.dataset
        } else {
            &table_ref.dataset
        },
        if target_timestamp.is_some() {
            Some(snapshot_ts)
        } else {
            None
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

    let snapshots = get_tracked_snapshots(&config, table_ref).await?;

    let selected_snapshots: Vec<&SnapshotMetadata> = snapshots
        .iter()
        .filter(|x| x.id.to_string().as_str() == name || x.table == name)
        .collect();
    if let Some(snapshot) = selected_snapshots.first() {
        let (bq_client, project_id) = client::get_client(&config).await?;

        let query = queries::SnapshotsQueries::remove(
            &snapshot.project,
            &snapshot.dataset,
            &snapshot.table,
        );

        executor::execute(&bq_client, &project_id, query).await?;
    } else {
        return Err("Snapshot with provided name or ID not found or not tracked".into());
    }

    Ok(())
}
