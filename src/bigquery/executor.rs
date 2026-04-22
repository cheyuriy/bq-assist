use google_cloud_bigquery::client::Client;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;

pub async fn query_collect<T, F>(client: &Client, project_id: &str, sql: String, map_row: F) -> Vec<T>
where
    F: Fn(Row) -> T,
{
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await.unwrap();
    let mut out = Vec::new();
    while let Some(row) = iter.next().await.unwrap() {
        out.push(map_row(row));
    }
    out
}

pub async fn query_first<T, F>(client: &Client, project_id: &str, sql: String, map_row: F) -> Option<T>
where
    F: Fn(Row) -> T,
{
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await.unwrap();
    iter.next().await.unwrap().map(map_row)
}

pub async fn execute(client: &Client, project_id: &str, sql: String) {
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };
    let mut iter = client.query::<Row>(project_id, request).await.unwrap();
    while iter.next().await.unwrap().is_some() {}
}
