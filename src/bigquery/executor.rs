use crate::errors::BigQueryError;
use google_cloud_bigquery::client::Client;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;

pub async fn query_collect<T, F>(
    client: &Client,
    project_id: &str,
    sql: String,
    map_row: F,
) -> Result<Vec<T>, BigQueryError>
where
    F: Fn(Row) -> T,
{
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await?;
    let mut out = Vec::new();
    while let Some(row) = iter.next().await? {
        out.push(map_row(row));
    }
    Ok(out)
}

pub async fn query_first<T, F>(
    client: &Client,
    project_id: &str,
    sql: String,
    map_row: F,
) -> Result<Option<T>, BigQueryError>
where
    F: Fn(Row) -> T,
{
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await?;
    Ok(iter.next().await?.map(map_row))
}

pub async fn execute(client: &Client, project_id: &str, sql: String) -> Result<(), BigQueryError> {
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await?;
    while iter.next().await?.is_some() {}
    Ok(())
}
