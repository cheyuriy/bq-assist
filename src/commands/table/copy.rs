use crate::bigquery::client;
use crate::bigquery::queries;
use crate::models::bigquery::copy::CopyMetadata;
use crate::models::config::AppConfig;
use crate::models::schema::DatasetRef;
use crate::models::schema::TableRef;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;
use rand;
use tabled::Table;

async fn get_tracked_copies(config: &AppConfig, table_ref: &TableRef) -> Vec<CopyMetadata> {
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let query = queries::CopyQueries::list(
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

    let mut copies: Vec<CopyMetadata> = Vec::new();
    while let Some(row) = iter.next().await.unwrap() {
        let column = CopyMetadata::new(
            row.column::<i64>(0).unwrap(),
            row.column::<String>(1).unwrap().as_str(),
            row.column::<String>(2).unwrap().as_str(),
            row.column::<String>(3).unwrap().as_str(),
            row.column::<f64>(4).unwrap(),
            row.column::<String>(5).unwrap().as_str(),
        );

        copies.push(column);
    }

    copies
}

pub async fn list(config: AppConfig, table_ref: &TableRef) {
    let copies = get_tracked_copies(&config, table_ref).await;

    let table = Table::new(copies);
    println!("{}", table);
}

pub async fn add(
    config: AppConfig,
    table_ref: &TableRef,
    name: Option<String>,
    dataset: Option<DatasetRef>,
    no_track: bool,
) {
    let copy_name = if let Some(name) = name {
        name
    } else {
        let table_name = table_ref.table.as_str();
        let ts = chrono::Utc::now().format("%Y_%d_%mT%H_%M_%S").to_string();
        format!("{table_name}_{ts}")
    };

    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

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

    println!("{query}");

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
    let copies = get_tracked_copies(&config, table_ref).await;

    let selected_copies: Vec<&CopyMetadata> = copies
        .iter()
        .filter(|x| x.id.to_string().as_str() == name || x.table == name)
        .collect();
    if let Some(copy) = selected_copies.first() {
        let (bq_client, project_id) = match client::get_client(&config).await {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        };

        let query = queries::CopyQueries::remove(&copy.project, &copy.dataset, &copy.table);

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
