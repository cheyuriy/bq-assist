use google_cloud_bigquery::client::{
    Client, ClientConfig, google_cloud_auth::credentials::CredentialsFile,
};
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;
use tempfile::TempDir;
use tokio::sync::OnceCell;

pub struct TestEnvironment {
    pub client: Client,
    pub project: String,
    pub dataset: String,
    _config_dir: TempDir,
}

static TEST_ENV: OnceCell<Option<TestEnvironment>> = OnceCell::const_new();

/// Returns the shared `TestEnvironment`, or `None` if `BQ_TEST_PROJECT` is unset.
/// When `None`, the calling test should print a skip message and return early.
pub async fn get_test_env() -> Option<&'static TestEnvironment> {
    TEST_ENV
        .get_or_init(|| async {
            dotenvy::from_filename(".env.test").ok();

            let project = match std::env::var("BQ_TEST_PROJECT") {
                Ok(v) => v,
                Err(_) => return None,
            };
            let dataset = std::env::var("BQ_TEST_DATASET")
                .expect("BQ_TEST_DATASET must be set when BQ_TEST_PROJECT is set");
            let region = std::env::var("BQ_TEST_REGION")
                .unwrap_or_else(|_| "region-eu".to_string());

            let sa_path = std::env::var("BQ_TEST_SERVICE_ACCOUNT_PATH")
                .or_else(|_| std::env::var("GOOGLE_APPLICATION_CREDENTIALS"))
                .expect("set BQ_TEST_SERVICE_ACCOUNT_PATH or GOOGLE_APPLICATION_CREDENTIALS");

            // Write a config.yaml the CLI subprocess will read.
            // BQ_ASSIST_CONFIG_DIR is inherited by the subprocess automatically.
            let config_dir = TempDir::new().expect("failed to create temp config dir");
            let config_yaml = format!(
                "service_account_path: {sa_path}\nproject: {project}\ntemp_dataset: {dataset}\nregion: {region}\n"
            );
            std::fs::write(config_dir.path().join("config.yaml"), config_yaml)
                .expect("failed to write test config.yaml");
            unsafe { std::env::set_var("BQ_ASSIST_CONFIG_DIR", config_dir.path()) };

            let creds = CredentialsFile::new_from_file(sa_path)
                .await
                .expect("failed to load service account credentials");
            let (client_config, _) = ClientConfig::new_with_credentials(creds)
                .await
                .expect("failed to build BQ client config");
            let client = Client::new(client_config)
                .await
                .expect("failed to create BQ client");

            let env = TestEnvironment { client, project, dataset, _config_dir: config_dir };
            env.reset_dataset().await;
            Some(env)
        })
        .await
        .as_ref()
}

// Fixtures are embedded at compile time; placeholders {project} and {dataset}
// are substituted at runtime.
const FIXTURE_FILES: &[(&str, &str)] = &[
    ("columns_remove",           include_str!("../fixtures/columns_remove.sql")),
    ("columns_lifecycle",        include_str!("../fixtures/columns_lifecycle.sql")),
    ("columns_add_remove",       include_str!("../fixtures/columns_add_remove.sql")),
    ("columns_rename_lifecycle", include_str!("../fixtures/columns_rename.sql")),
    ("columns_cast",             include_str!("../fixtures/columns_cast.sql")),
    ("clustering",         include_str!("../fixtures/clustering.sql")),
    ("partitioning",   include_str!("../fixtures/partitioning.sql")),
    ("rename",         include_str!("../fixtures/table_rename.sql")),
    ("options",        include_str!("../fixtures/table_options.sql")),
    ("rewind",         include_str!("../fixtures/table_rewind.sql")),
    ("copy",           include_str!("../fixtures/table_copy.sql")),
    ("snapshot",       include_str!("../fixtures/table_snapshot.sql")),
];

impl TestEnvironment {
    async fn reset_dataset(&self) {
        self.drop_all_tables().await;
        self.load_fixtures().await;
    }

    async fn drop_all_tables(&self) {
        let list_sql = format!(
            "SELECT table_name FROM `{}.{}.INFORMATION_SCHEMA.TABLES`",
            self.project, self.dataset
        );
        let names = self.run_string_col_query(list_sql).await;
        for name in names {
            let drop_sql = format!(
                "DROP TABLE IF EXISTS `{}.{}.{}`",
                self.project, self.dataset, name
            );
            self.run_ddl(drop_sql).await;
        }
    }

    async fn load_fixtures(&self) {
        for (_name, sql_template) in FIXTURE_FILES {
            self.run_fixture_sql(sql_template).await;
        }
    }

    /// Recreate a single fixture table by name. Useful for resetting state
    /// between tests that modify the same table within one test file.
    #[allow(unused)]
    pub async fn recreate_table(&self, fixture_name: &str) {
        let sql_template = FIXTURE_FILES
            .iter()
            .find(|(name, _)| *name == fixture_name)
            .map(|(_, sql)| sql)
            .unwrap_or_else(|| panic!("unknown fixture: {fixture_name}"));
        self.run_fixture_sql(sql_template).await;
    }

    async fn run_fixture_sql(&self, sql_template: &str) {
        let sql = sql_template
            .replace("{project}", &self.project)
            .replace("{dataset}", &self.dataset);
        self.run_ddl(sql).await;
    }

    pub async fn run_ddl(&self, sql: String) {
        let req = QueryRequest { query: sql, ..Default::default() };
        let mut iter = self
            .client
            .query::<Row>(&self.project, req)
            .await
            .unwrap();
        while iter.next().await.unwrap().is_some() {}
    }

    pub async fn run_string_col_query(&self, sql: String) -> Vec<String> {
        let req = QueryRequest { query: sql, ..Default::default() };
        let mut iter = self
            .client
            .query::<Row>(&self.project, req)
            .await
            .unwrap();
        let mut out = Vec::new();
        while let Some(row) = iter.next().await.unwrap() {
            out.push(row.column::<String>(0).unwrap());
        }
        out
    }
}
