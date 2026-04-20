use crate::errors::ArgumentsParsingError;
use md5;
use std::{fmt::Display, str::FromStr};

#[derive(Debug, Clone)]
pub struct TableRef {
    pub project: Option<String>,
    pub dataset: String,
    pub table: String,
}

impl FromStr for TableRef {
    type Err = ArgumentsParsingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();

        match parts.as_slice() {
            [dataset, table] => Ok(TableRef {
                project: None,
                dataset: dataset.to_string(),
                table: table.to_string(),
            }),
            [project, dataset, table] => Ok(TableRef {
                project: Some(project.to_string()),
                dataset: dataset.to_string(),
                table: table.to_string(),
            }),
            _ => Err(ArgumentsParsingError::InvalidTableRefFormat),
        }
    }
}

impl Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "`{:?}.{}.{}`", self.project, self.dataset, self.table)
    }
}

impl TableRef {
    pub fn hex_digest(&self, project: Option<&str>) -> String {
        if self.project.is_none() && project.is_none() {
            panic!("Can't generate table reference digest without project specified");
        }
        let input = format!(
            "{}.{}.{}",
            if let Some(ref ref_project) = self.project {
                ref_project.to_lowercase()
            } else {
                project.as_deref().unwrap().to_lowercase()
            },
            self.dataset.to_lowercase(),
            self.table.to_lowercase()
        );

        format!("{:x}", md5::compute(input))
    }
}

#[derive(Clone, Debug)]
pub struct DatasetRef {
    pub project: Option<String>,
    pub dataset: String,
}

impl FromStr for DatasetRef {
    type Err = ArgumentsParsingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();

        match parts.as_slice() {
            [dataset] => Ok(DatasetRef {
                project: None,
                dataset: dataset.to_string(),
            }),
            [project, dataset] => Ok(DatasetRef {
                project: Some(project.to_string()),
                dataset: dataset.to_string(),
            }),
            _ => Err(ArgumentsParsingError::InvalidDatasetRefFormat),
        }
    }
}

impl Display for DatasetRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "`{:?}.{}`", self.project, self.dataset)
    }
}
