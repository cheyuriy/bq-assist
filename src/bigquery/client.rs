use crate::errors::ConfigurationError;
use crate::models::config::AppConfig;
use google_cloud_bigquery::client::{
    Client, ClientConfig, google_cloud_auth::credentials::CredentialsFile,
};
use std::path::{PathBuf, Path};
use std::{env, ops::Deref};

pub async fn get_client(config: &AppConfig) -> Result<(Client, String), ConfigurationError> {
    let service_account_path = get_service_account_path(config)?;

    let (client_config, project_id) = load_service_account(&service_account_path).await?;

    let project_id = if config.project.is_some() {
        config.project.as_ref().unwrap().deref().to_string()
    } else if project_id.is_some() {
        project_id.unwrap()
    } else {
        return Err(ConfigurationError::ProjectNotDetermined);
    };

    let bq_client = Client::new(client_config).await.unwrap();

    Ok((bq_client, project_id))
}

fn get_service_account_path(config: &AppConfig) -> Result<PathBuf, ConfigurationError> {
    if env::var("GOOGLE_APPLICATION_CREDENTIALS").is_ok() {
        Ok(PathBuf::from(
            env::var("GOOGLE_APPLICATION_CREDENTIALS").unwrap(),
        ))
    } else if let Some(service_account_path) = &config.service_account_path {
        let res = PathBuf::from(service_account_path);
        if res.exists() {
            Ok(res)
        } else {
            Err(ConfigurationError::ServiceAccountNotFound)
        }
    } else {
        Err(ConfigurationError::ServiceAccountNotFound)
    }
}

async fn load_service_account(
    credentials_path: &Path,
) -> Result<(ClientConfig, Option<String>), ConfigurationError> {
    let credentials_file = match CredentialsFile::new_from_file(
        credentials_path.to_str().unwrap().to_string(),
    )
    .await
    {
        Ok(cred) => cred,
        Err(_) => return Err(ConfigurationError::ServiceAccountNotFound),
    };
    let (config, project_id) = ClientConfig::new_with_credentials(credentials_file)
        .await
        .unwrap();
    Ok((config, project_id))
}
