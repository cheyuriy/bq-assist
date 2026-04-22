use crate::errors::BigQueryError;
use google_cloud_bigquery::client::Client;
use google_cloud_bigquery::http::error;

pub async fn ensure_table_exists(
    client: &Client,
    project: &str,
    dataset: &str,
    table: &str,
) -> Result<(), BigQueryError> {
    match client.table().get(project, dataset, table).await {
        Ok(_) => Ok(()),
        Err(error::Error::Response(data)) if data.code == 404 => Err(
            BigQueryError::TableNotExists(format!("{project}.{dataset}.{table}")),
        ),
        Err(e) => Err(BigQueryError::ApiError(format!("{e:?}"))),
    }
}

pub async fn ensure_dataset_exists(
    client: &Client,
    project: &str,
    dataset: &str,
) -> Result<(), BigQueryError> {
    match client.dataset().get(project, dataset).await {
        Ok(_) => Ok(()),
        Err(error::Error::Response(data)) if data.code == 404 => {
            Err(BigQueryError::DatasetNotExists(format!("{project}.{dataset}")))
        }
        Err(e) => Err(BigQueryError::ApiError(format!("{e:?}"))),
    }
}
