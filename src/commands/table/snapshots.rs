use crate::bigquery::client;
use crate::bigquery::queries;
use crate::models::bigquery::snapshot::SnapshotMetadata;
use crate::models::config::AppConfig;
use crate::models::schema::DatasetRef;
use crate::models::schema::TableRef;
use chrono::DateTime;
use chrono::Utc;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;
use rand;
use std::time::Duration;
use tabled::Table;

async fn get_tracked_snapshots(config: &AppConfig, table_ref: &TableRef) -> Vec<SnapshotMetadata> {
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let query = queries::SnapshotsQueries::list(
        &config.region,
        table_ref.hex_digest(Some(&project_id)).as_str(),
    );

    let request = QueryRequest {
        query: query,
        ..Default::default()
    };

    let mut iter = bq_client
        .query::<Row>(project_id.as_str(), request)
        .await
        .unwrap();

    let mut snapshots: Vec<SnapshotMetadata> = Vec::new();
    while let Some(row) = iter.next().await.unwrap() {
        let column = SnapshotMetadata::new(
            row.column::<i64>(0).unwrap(),
            row.column::<String>(1).unwrap().as_str(),
            row.column::<String>(2).unwrap().as_str(),
            row.column::<String>(3).unwrap().as_str(),
            row.column::<f64>(4).unwrap(),
            row.column::<String>(5).unwrap().as_str(),
        );

        snapshots.push(column);
    }

    snapshots
}

pub async fn list(config: AppConfig, table_ref: &TableRef) {
    let snapshots = get_tracked_snapshots(&config, table_ref).await;

    let table = Table::new(snapshots);
    println!("{}", table);
}

pub async fn add(
    config: AppConfig,
    table_ref: &TableRef,
    name: Option<String>,
    dataset: Option<DatasetRef>,
    rewind: Option<Duration>,
    timestamp: Option<DateTime<Utc>>,
    no_track: bool,
) {
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

    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

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

    let request = QueryRequest {
        query: query,
        ..Default::default()
    };

    let mut iter = bq_client
        .query::<Row>(project_id.as_str(), request)
        .await
        .unwrap();

    while let Some(row) = iter.next().await.unwrap() {
        let data = row.column::<String>(0);
        println!("{data:?}");
    }
}

pub async fn remove(config: AppConfig, table_ref: &TableRef, name: &str) {
    let snapshots = get_tracked_snapshots(&config, table_ref).await;

    let selected_snapshots: Vec<&SnapshotMetadata> = snapshots
        .iter()
        .filter(|x| x.id.to_string().as_str() == name || x.table == name)
        .collect();
    if let Some(snapshot) = selected_snapshots.first() {
        let (bq_client, project_id) = match client::get_client(&config).await {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        };

        let query = queries::SnapshotsQueries::remove(
            &snapshot.project,
            &snapshot.dataset,
            &snapshot.table,
        );

        let request = QueryRequest {
            query: query,
            ..Default::default()
        };

        let mut iter = bq_client
            .query::<Row>(project_id.as_str(), request)
            .await
            .unwrap();

        while let Some(row) = iter.next().await.unwrap() {
            let data = row.column::<String>(0);
            println!("{data:?}");
        }
    } else {
        panic!("Copy with provided name or ID not found or not tracked");
    }
}
