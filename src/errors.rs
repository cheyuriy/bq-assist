use chrono::format::ParseError;
use config;
use humantime::DurationError;

#[derive(Debug)]
pub enum ArgumentsParsingError {
    InvalidTableRefFormat,
    InvalidDatasetRefFormat,
    InvalidDurationFormat(DurationError),
    InvalidDateTimeFormat(ParseError),
}

impl std::fmt::Display for ArgumentsParsingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArgumentsParsingError::InvalidTableRefFormat => {
                write!(f, "Expected `dataset.table` or `project.dataset.table`")
            }
            ArgumentsParsingError::InvalidDatasetRefFormat => {
                write!(f, "Expected `dataset` or `project.dataset`")
            }
            ArgumentsParsingError::InvalidDurationFormat(e) => {
                write!(f, "Incorrect duration format: {e}")
            }
            ArgumentsParsingError::InvalidDateTimeFormat(e) => {
                write!(f, "Incorrect datetime format: {e}")
            }
        }
    }
}

impl std::error::Error for ArgumentsParsingError {}

impl From<humantime::DurationError> for ArgumentsParsingError {
    fn from(err: humantime::DurationError) -> Self {
        ArgumentsParsingError::InvalidDurationFormat(err)
    }
}

impl From<chrono::format::ParseError> for ArgumentsParsingError {
    fn from(err: chrono::format::ParseError) -> Self {
        ArgumentsParsingError::InvalidDateTimeFormat(err)
    }
}

#[derive(Debug)]
pub enum ConfigurationError {
    ConfigDirNotFound,
    ConfigParsingError(config::ConfigError),
    ServiceAccountNotFound,
    ProjectNotDetermined,
}

impl std::fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigurationError::ConfigDirNotFound => {
                write!(
                    f,
                    "Can't find config directory path. Provide environment variable `BQ_ASSIST_CONFIG_PATH` or make sure that the default path `~/.bq-assist` is present."
                )
            }
            ConfigurationError::ConfigParsingError(e) => {
                write!(f, "Unable to parse config file. Error: {e}")
            }
            ConfigurationError::ServiceAccountNotFound => {
                write!(
                    f,
                    "Can't find available service account. Provide environment variable `GOOGLE_APPLICATION_CREDENTIALS` or make sure to set path to the JSON file in `config.yml` in your config path."
                )
            }
            ConfigurationError::ProjectNotDetermined => {
                write!(
                    f,
                    "Can't determine BigQuery project. Make sure that it is set in config or can be determined from your service account."
                )
            }
        }
    }
}

impl std::error::Error for ConfigurationError {}

impl From<config::ConfigError> for ConfigurationError {
    fn from(err: config::ConfigError) -> Self {
        ConfigurationError::ConfigParsingError(err)
    }
}

#[derive(Debug)]
pub enum BigQueryError {
    QueryNotExecuted(google_cloud_bigquery::client::QueryError),
    QueryResultsError(google_cloud_bigquery::query::Error),
    TableNotExists(String),
    DatasetNotExists(String),
    ApiError(String),
}

impl std::fmt::Display for BigQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BigQueryError::QueryNotExecuted(e) => write!(f, "Failed to start BigQuery query: {e}"),
            BigQueryError::QueryResultsError(e) => write!(f, "Failed to read BigQuery results: {e}"),
            BigQueryError::TableNotExists(name) => write!(f, "Table `{name}` does not exist"),
            BigQueryError::DatasetNotExists(name) => write!(f, "Dataset `{name}` does not exist"),
            BigQueryError::ApiError(e) => write!(f, "BigQuery API error: {e}"),
        }
    }
}

impl std::error::Error for BigQueryError {}

impl From<google_cloud_bigquery::client::QueryError> for BigQueryError {
    fn from(err: google_cloud_bigquery::client::QueryError) -> Self {
        BigQueryError::QueryNotExecuted(err)
    }
}

impl From<google_cloud_bigquery::query::Error> for BigQueryError {
    fn from(err: google_cloud_bigquery::query::Error) -> Self {
        BigQueryError::QueryResultsError(err)
    }
}

#[derive(Debug)]
pub struct ValidationError(pub String);

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ValidationError {}
