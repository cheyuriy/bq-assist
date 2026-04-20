use crate::bigquery::client;
use crate::bigquery::queries;
use crate::models::bigquery::options::DatasetOption;
use crate::models::config::AppConfig;
use crate::models::schema::DatasetRef;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;

pub async fn set_option(
    config: AppConfig,
    dataset_ref: &DatasetRef,
    option_name: &DatasetOption,
    option_value: &str,
) {
    match option_name.validate_value(option_value) {
        Err(e) => panic!("{e}"),
        _ => (),
    }

    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let option_query = queries::DatasetQueries::set_option(
        dataset_ref.project.as_deref().unwrap_or(&project_id),
        dataset_ref.dataset.as_str(),
        option_name,
        option_value,
    );

    println!("{option_query}");

    let request = QueryRequest {
        query: option_query,
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
