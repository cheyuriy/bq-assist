use crate::bigquery::client;
use crate::bigquery::executor;
use crate::bigquery::queries;
use crate::bigquery::validators;
use crate::errors::ValidationError;
use crate::models::bigquery::options::DatasetOption;
use crate::models::config::AppConfig;
use crate::models::bigquery::references::DatasetRef;

pub async fn set_option(
    config: AppConfig,
    dataset_ref: &DatasetRef,
    option_name: &DatasetOption,
    option_value: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    option_name
        .validate_value(option_value)
        .map_err(ValidationError)?;

    let (bq_client, project_id) = client::get_client(&config).await?;
    let project = dataset_ref.project.as_deref().unwrap_or(&project_id);
    validators::ensure_dataset_exists(&bq_client, project, &dataset_ref.dataset).await?;

    let option_query = queries::DatasetQueries::set_option(
        dataset_ref.project.as_deref().unwrap_or(&project_id),
        dataset_ref.dataset.as_str(),
        option_name,
        option_value,
    );

    println!("{option_query}");

    executor::execute(&bq_client, &project_id, option_query).await?;

    Ok(())
}
