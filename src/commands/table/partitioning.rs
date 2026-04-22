use crate::bigquery::client;
use crate::bigquery::executor;
use crate::bigquery::queries;
use crate::models::bigquery::partitioning::Partitioning;
use crate::models::config::AppConfig;
use crate::models::schema::TableRef;
use regex::Regex;

pub async fn list(config: AppConfig, table_ref: &TableRef) {
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let ddl_query = queries::CommonQueries::ddl(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
    );

    let ddl = executor::query_first(&bq_client, &project_id, ddl_query, |row| {
        row.column::<String>(0).unwrap()
    })
    .await
    .unwrap_or_else(|| panic!("Can't find DDL for the table!"));

    let re = Regex::new(r"(?i)(PARTITION\s+BY\s+[^\n;]+)").unwrap();
    let partitioning_clause = if let Some(caps) = re.captures(&ddl) {
        Some(caps[1].trim().to_string())
    } else {
        None
    };
    println!("{partitioning_clause:?}");
}

pub async fn add(config: AppConfig, table_ref: &TableRef, partitioning: Option<&Partitioning>) {
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let ddl_query = queries::CommonQueries::ddl(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
    );

    let original_ddl = executor::query_first(&bq_client, &project_id, ddl_query, |row| {
        row.column::<String>(0).unwrap()
    })
    .await
    .unwrap_or_else(|| panic!("Can't find DDL for the table!"));

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

    executor::execute(&bq_client, &project_id, query).await;
}

pub async fn remove(config: AppConfig, table_ref: &TableRef) {
    add(config, table_ref, None).await;
}
