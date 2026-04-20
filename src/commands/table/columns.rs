use crate::bigquery::client;
use crate::bigquery::queries;
use crate::models::bigquery::columns::{ColumnMetadata, Type};
use crate::models::config::AppConfig;
use crate::models::schema::TableRef;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;
use tabled::Table;
use tokio::time::{Duration, sleep};

pub async fn list(config: AppConfig, table_ref: &TableRef) {
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let query = queries::CommonQueries::columns(
        table_ref
            .project
            .as_deref()
            .unwrap_or(project_id.clone().as_str()),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        None,
    );

    let request = QueryRequest {
        query: query,
        ..Default::default()
    };

    let mut iter = bq_client
        .query::<Row>(project_id.as_str(), request)
        .await
        .unwrap();

    let mut columns: Vec<ColumnMetadata> = Vec::new();
    while let Some(row) = iter.next().await.unwrap() {
        let column = ColumnMetadata::new(
            row.column::<String>(3).unwrap().as_str(),
            row.column::<String>(4).unwrap().parse::<u8>().unwrap(),
            row.column::<String>(5).unwrap().as_str(),
            row.column::<String>(6).unwrap().as_str(),
            row.column::<String>(10).unwrap().as_str(),
            row.column::<String>(13).unwrap().as_str(),
            match row.column::<String>(14) {
                Ok(v) => match v.parse::<u8>() {
                    Ok(r) => Some(r),
                    Err(_) => None,
                },
                Err(_) => None,
            },
            match row.column::<String>(16) {
                Ok(v) => {
                    if v != "NULL" {
                        Some(v)
                    } else {
                        None
                    }
                }
                Err(_) => None,
            },
        );

        columns.push(column);
    }

    let table = Table::new(columns);
    println!("{}", table);
}

pub async fn add(
    config: AppConfig,
    table_ref: &TableRef,
    name: &str,
    field_type: &Type,
    default_value: Option<String>,
) {
    if *field_type == Type::Range || *field_type == Type::Struct {
        panic!("Adding column with type {field_type:?} is not implemented");
    }

    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let query = queries::ColumnsQueries::add_column(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        name,
        &field_type,
        default_value,
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
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let query = queries::ColumnsQueries::remove_column(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        name,
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

pub async fn rename(config: AppConfig, table_ref: &TableRef, name: &str, new_name: &str) {
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let column_query = queries::CommonQueries::columns(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        Some(name.to_string()),
    );

    let request = QueryRequest {
        query: column_query,
        ..Default::default()
    };

    let mut iter = bq_client
        .query::<Row>(project_id.as_str(), request)
        .await
        .unwrap();

    let column = if let Some(row) = iter.next().await.unwrap() {
        ColumnMetadata::new(
            row.column::<String>(3).unwrap().as_str(),
            row.column::<String>(4).unwrap().parse::<u8>().unwrap(),
            row.column::<String>(5).unwrap().as_str(),
            row.column::<String>(6).unwrap().as_str(),
            row.column::<String>(10).unwrap().as_str(),
            row.column::<String>(13).unwrap().as_str(),
            match row.column::<String>(14) {
                Ok(v) => match v.parse::<u8>() {
                    Ok(r) => Some(r),
                    Err(_) => None,
                },
                Err(_) => None,
            },
            match row.column::<String>(16) {
                Ok(v) => {
                    if v != "NULL" {
                        Some(v)
                    } else {
                        None
                    }
                }
                Err(_) => None,
            },
        )
    } else {
        panic!("Can't find metadata about that column");
    };

    println!("{column:?}");

    let rename_query = queries::ColumnsQueries::rename_column(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        name,
        new_name,
        &column.data_type,
        column.column_default,
    );

    let request = QueryRequest {
        query: rename_query,
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

pub async fn cast(config: AppConfig, table_ref: &TableRef, name: &str, field_type: &Type) {
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let column_query = queries::CommonQueries::columns(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        Some(name.to_string()),
    );

    let request = QueryRequest {
        query: column_query,
        ..Default::default()
    };

    let mut iter = bq_client
        .query::<Row>(project_id.as_str(), request)
        .await
        .unwrap();

    let column = if let Some(row) = iter.next().await.unwrap() {
        ColumnMetadata::new(
            row.column::<String>(3).unwrap().as_str(),
            row.column::<String>(4).unwrap().parse::<u8>().unwrap(),
            row.column::<String>(5).unwrap().as_str(),
            row.column::<String>(6).unwrap().as_str(),
            row.column::<String>(10).unwrap().as_str(),
            row.column::<String>(13).unwrap().as_str(),
            match row.column::<String>(14) {
                Ok(v) => match v.parse::<u8>() {
                    Ok(r) => Some(r),
                    Err(_) => None,
                },
                Err(_) => None,
            },
            match row.column::<String>(16) {
                Ok(v) => {
                    if v != "NULL" {
                        Some(v)
                    } else {
                        None
                    }
                }
                Err(_) => None,
            },
        )
    } else {
        panic!("Can't find metadata about that column");
    };

    // BigQuery limits number of DDL and DML jobs per table (5 per 10 seconds per table by default)
    // However, casting column to another type needs more than 5 operations
    // So we divide it in two steps with a 10 seconds pause between them
    let (first_query, second_query) = queries::ColumnsQueries::cast_column(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        name,
        &column.data_type,
        &field_type,
    );

    let request = QueryRequest {
        query: first_query,
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

    if let Some(second_query) = second_query {
        print!(
            "Sleeping for 10 seconds due to BigQuery limitations on the rate of updates per table..."
        );
        sleep(Duration::from_secs(10)).await;
        println!(" Done!");

        let request = QueryRequest {
            query: second_query,
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
}
