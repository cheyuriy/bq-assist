use crate::errors::ConfigurationError;
use config::{Config, Environment, File};
use directories;
use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;

fn default_region() -> String {
    "region-eu".to_string()
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppConfig {
    pub service_account_path: Option<String>,
    pub project: Option<String>,
    pub temp_dataset: Option<String>,
    #[serde(default = "default_region")]
    pub region: String,
}

fn default_config_dir() -> Result<PathBuf, ConfigurationError> {
    if let Some(proj_dirs) =
        directories::ProjectDirs::from("com", "example", env!("CARGO_PKG_NAME"))
    {
        Ok(proj_dirs.config_dir().to_path_buf())
    } else {
        Err(ConfigurationError::ConfigDirNotFound)
    }
}

fn config_dir() -> Result<PathBuf, ConfigurationError> {
    if let Ok(dir) = env::var(format!("{}_CONFIG_DIR", env!("CARGO_PKG_NAME").replace("-", "_").to_uppercase())) {
        let dir = PathBuf::from(dir);
        if dir.exists() {
            Ok(dir)
        } else {
            Err(ConfigurationError::ConfigDirNotFound)
        }
    } else {
        default_config_dir()
    }
}

pub fn load_config() -> Result<AppConfig, ConfigurationError> {
    let dir = config_dir()?;
    let path = dir.join("config.yaml");
    let pkg_name = env!("CARGO_PKG_NAME").replace("-", "_").to_uppercase();

    let builder = Config::builder()
        .add_source(File::from(path.clone()).required(false))
        .add_source(Environment::with_prefix(pkg_name.as_str()).separator("__"));

    let config = builder.build()?;

    let config: AppConfig = config.try_deserialize()?;

    Ok(config)
}
