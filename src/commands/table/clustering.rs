use crate::bigquery::client;
use crate::bigquery::queries;
use crate::models::config::AppConfig;
use crate::models::schema::TableRef;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;

pub async fn list(config: AppConfig, table_ref: &TableRef) {
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let query = queries::ClusteringQueries::list_clustering_fields(
        table_ref
            .project
            .as_deref()
            .unwrap_or(project_id.clone().as_str()),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
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

pub async fn add(config: AppConfig, table_ref: &TableRef, fields: Vec<String>) {
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let ddl_query = queries::CommonQueries::ddl(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
    );

    let request = QueryRequest {
        query: ddl_query,
        ..Default::default()
    };

    let mut iter = bq_client
        .query::<Row>(project_id.as_str(), request)
        .await
        .unwrap();

    let original_ddl = if let Some(row) = iter.next().await.unwrap() {
        row.column::<String>(0).unwrap()
    } else {
        panic!("Can't find DDL for the table!");
    };

    let query = queries::ClusteringQueries::add_or_remove_clustering(
        &original_ddl,
        table_ref.project.as_deref().unwrap_or(&project_id),
        config
            .temp_dataset
            .as_deref()
            .unwrap_or(table_ref.dataset.as_str()),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        fields,
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

pub async fn remove(config: AppConfig, table_ref: &TableRef) {
    add(config, table_ref, Vec::new()).await;
}
