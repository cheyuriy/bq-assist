use crate::bigquery::client;
use crate::bigquery::executor;
use crate::bigquery::queries;
use crate::models::bigquery::options::TableOption;
use crate::models::config::AppConfig;
use crate::models::schema::TableRef;
use google_cloud_bigquery::http::error;
use google_cloud_bigquery::http::job::{
    CreateDisposition, Job, JobConfiguration, JobConfigurationSourceTable,
    JobConfigurationTableCopy, JobReference, JobType, OperationType, WriteDisposition,
};
use google_cloud_bigquery::http::table::TableReference;
use std::time::Duration;

pub async fn rename(config: AppConfig, table_ref: &TableRef, new_name: &str) {
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let ddl_query = queries::TableQueries::rename(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        new_name,
    );

    executor::execute(&bq_client, &project_id, ddl_query).await;
}

pub async fn set_option(
    config: AppConfig,
    table_ref: &TableRef,
    option_name: &TableOption,
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

    let option_query = queries::TableQueries::set_option(
        table_ref.project.as_deref().unwrap_or(&project_id),
        table_ref.dataset.as_str(),
        table_ref.table.as_str(),
        option_name,
        option_value,
    );

    executor::execute(&bq_client, &project_id, option_query).await;
}

pub async fn restore(
    config: AppConfig,
    table_ref: &TableRef,
    rewind_period: &Option<Duration>,
    _copy_id: &Option<String>,
    _snapshot_id: &Option<String>,
    _archive: &Option<bool>,
) {
    let (bq_client, project_id) = match client::get_client(&config).await {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    };

    let table_exists = match bq_client
        .table()
        .get(
            table_ref.project.as_deref().unwrap_or(&project_id),
            table_ref.dataset.as_str(),
            table_ref.table.as_str(),
        )
        .await
    {
        Ok(_) => true,
        Err(ref e) => match e {
            error::Error::Response(data) => {
                if data.code == 404 {
                    false
                } else {
                    panic!("{e:?}")
                }
            }
            //TODO: table().get() method returns error for tables with columns of GEOGRAPHY and RANGE types. Seems to be a bug.
            _ => panic!("{e:?}"),
        },
    };

    if table_exists {
        if let Some(duration) = rewind_period {
            let rewind_query = queries::TableQueries::rewind(
                table_ref.project.as_deref().unwrap_or(&project_id),
                table_ref.dataset.as_str(),
                table_ref.table.as_str(),
                duration,
            );

            executor::execute(&bq_client, &project_id, rewind_query).await;
        }
    } else {
        println!("Table doesn't exist");
        if let Some(duration) = rewind_period {
            let duration_in_secs = duration.as_secs();

            let job = Job {
                job_reference: JobReference {
                    project_id: table_ref
                        .project
                        .as_deref()
                        .unwrap_or(&project_id)
                        .to_string(),
                    job_id: "rewind_job".to_string(),
                    location: None,
                },
                configuration: JobConfiguration {
                    job: JobType::Copy(JobConfigurationTableCopy {
                        source_table: JobConfigurationSourceTable::SourceTable(TableReference {
                            project_id: table_ref
                                .project
                                .as_deref()
                                .unwrap_or(&project_id)
                                .to_string(),
                            dataset_id: table_ref.dataset.to_string(),
                            table_id: {
                                let table = &table_ref.table;
                                format!("{table}@-{duration_in_secs}").to_string()
                            },
                        }),
                        destination_table: TableReference {
                            project_id: table_ref
                                .project
                                .as_deref()
                                .unwrap_or(&project_id)
                                .to_string(),
                            dataset_id: table_ref.dataset.to_string(),
                            table_id: table_ref.table.to_string(),
                        },
                        create_disposition: Some(CreateDisposition::CreateIfNeeded),
                        write_disposition: Some(WriteDisposition::WriteTruncate),
                        operation_type: Some(OperationType::Copy),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            };
            match bq_client.job().create(&job).await {
                Ok(_) => (), //TODO: add notification about lost partitioning and clustering
                Err(e) => panic!("{e:?}"),
            }
        }
    }
}
