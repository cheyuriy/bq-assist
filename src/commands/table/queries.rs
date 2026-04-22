use crate::bigquery::client;
use crate::bigquery::queries;
use crate::bigquery::validators;
use crate::models::bigquery::queries::{QueryJobMetadata, format_bytes};
use crate::models::config::AppConfig;
use crate::models::schema::TableRef;
use chrono::{DateTime, Utc};
use google_cloud_bigquery::client::Client;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;
use tabled::Table;

fn resolve_time_window(
    period: Option<std::time::Duration>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
) -> (String, Option<String>) {
    let effective_from = from.unwrap_or_else(|| {
        let p = period.unwrap();
        Utc::now() - chrono::Duration::from_std(p).unwrap()
    });
    let from_ts = effective_from.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let to_ts = to.map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string());
    (from_ts, to_ts)
}

async fn run_jobs_query(
    bq_client: &Client,
    project_id: &str,
    sql: String,
) -> Result<Vec<QueryJobMetadata>, Box<dyn std::error::Error>> {
    let request = QueryRequest {
        query: sql,
        ..Default::default()
    };

    let mut iter = bq_client.query::<Row>(project_id, request).await?;

    let mut jobs: Vec<QueryJobMetadata> = Vec::new();
    while let Some(row) = iter.next().await? {
        let bytes_billed = row.column::<Option<i64>>(6).unwrap();
        jobs.push(QueryJobMetadata {
            job_id: row.column::<String>(0).unwrap(),
            creation_time: DateTime::from_timestamp_millis(row.column::<i64>(1).unwrap()).unwrap(),
            user_email: row.column::<String>(2).unwrap(),
            query: row.column::<String>(3).unwrap(),
            statement_type: row.column::<String>(4).unwrap(),
            state: row.column::<String>(5).unwrap(),
            data_billed: bytes_billed.map(format_bytes).unwrap_or_default(),
        });
    }

    Ok(jobs)
}

pub async fn read(
    config: AppConfig,
    table_ref: &TableRef,
    single: bool,
    user: Option<String>,
    period: Option<std::time::Duration>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    limit: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let region = config.region.clone();
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;
    let (from_ts, to_ts) = resolve_time_window(period, from, to);
    let sql = queries::QueriesQueries::read(
        &project_id,
        &table_ref.dataset,
        &table_ref.table,
        &region,
        &from_ts,
        to_ts.as_deref(),
        user.as_deref(),
        single,
        limit,
    );
    println!("{sql}");
    let jobs = run_jobs_query(&bq_client, &project_id, sql).await?;
    println!("{}", Table::new(jobs));

    Ok(())
}

pub async fn modify(
    config: AppConfig,
    table_ref: &TableRef,
    query_type: Option<String>,
    user: Option<String>,
    period: Option<std::time::Duration>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    limit: u64,
    related: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let region = config.region.clone();
    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = table_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_table_exists(&bq_client, project, &table_ref.dataset, &table_ref.table).await?;
    let (from_ts, to_ts) = resolve_time_window(period, from, to);
    let sql = queries::QueriesQueries::modify(
        &project_id,
        &table_ref.dataset,
        &table_ref.table,
        &region,
        &from_ts,
        to_ts.as_deref(),
        user.as_deref(),
        query_type.as_deref(),
        related,
        limit,
    );
    println!("{sql}");
    let jobs = run_jobs_query(&bq_client, &project_id, sql).await?;
    println!("{}", Table::new(jobs));

    Ok(())
}
