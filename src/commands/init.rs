use dialoguer::{Confirm, Input, theme::ColorfulTheme};
use directories::ProjectDirs;
use std::env;
use std::path::{Path, PathBuf};

use crate::models::config::AppConfig;

fn resolve_config_path() -> PathBuf {
    if let Ok(dir) = env::var("BQ_ASSIST_CONFIG_DIR") {
        PathBuf::from(dir).join("config.yaml")
    } else {
        ProjectDirs::from("com", "example", env!("CARGO_PKG_NAME"))
            .expect("Could not determine platform config directory")
            .config_dir()
            .join("config.yaml")
    }
}

pub async fn init() -> Result<(), Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();

    let config_path = resolve_config_path();

    if config_path.exists() {
        println!();
        println!("A config file already exists at: {}", config_path.display());
        println!();
        let overwrite = Confirm::with_theme(&theme)
            .with_prompt("Overwrite it and run setup again?")
            .default(false)
            .interact()?;

        if !overwrite {
            println!();
            println!(
                "Setup is already complete. Run `bq-assist --help` to see all available commands."
            );
            println!();
            return Ok(());
        }
    }

    println!();
    println!("Welcome to bq-assist setup!");
    println!();
    println!("This wizard will create a config file for you. Alternatively, you can skip");
    println!("this and configure everything via environment variables:");
    println!("  GOOGLE_APPLICATION_CREDENTIALS, BQ_ASSIST__PROJECT, BQ_ASSIST__REGION, etc.");
    println!();

    let create = Confirm::with_theme(&theme)
        .with_prompt("Create a config file now?")
        .default(false)
        .interact()?;

    if !create {
        println!();
        println!(
            "No config file created. You can configure bq-assist using environment variables:"
        );
        println!("  GOOGLE_APPLICATION_CREDENTIALS  — path to service account JSON");
        println!("  BQ_ASSIST__PROJECT              — default BigQuery project");
        println!("  BQ_ASSIST__REGION               — default region (e.g. region-eu)");
        println!("  BQ_ASSIST_CONFIG_DIR            — custom config directory");
        println!();
        println!("Run `bq-assist --help` to see all available commands.");
        return Ok(());
    }

    println!();
    println!("Config will be written to: {}", config_path.display());
    println!("  Tip: set BQ_ASSIST_CONFIG_DIR to use a custom directory.");
    println!();

    // --- Service account ---
    let mut service_account_path: Option<String> = None;

    if let Ok(gac) = env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        println!("GOOGLE_APPLICATION_CREDENTIALS is already set:");
        println!("  {gac}");
        println!();
        let use_gac = Confirm::with_theme(&theme)
            .with_prompt("Use this service account?")
            .default(true)
            .interact()?;

        if !use_gac {
            service_account_path = Some(prompt_service_account_path(&theme)?);
        }
    } else {
        service_account_path = Some(prompt_service_account_path(&theme)?);
    }

    // --- Default project ---
    println!();
    println!("The default project can also be set via BQ_ASSIST__PROJECT, inferred from");
    println!(
        "fully-qualified table names (project.dataset.table), or read from the service account."
    );

    let project_input: String = Input::with_theme(&theme)
        .with_prompt("Default BigQuery project ID (leave blank to skip)")
        .allow_empty(true)
        .interact_text()?;

    let project = if project_input.is_empty() {
        None
    } else {
        Some(project_input)
    };

    // --- Default region ---
    println!();
    println!("The region is used for snapshot and copy metadata lookups.");

    let region: String = Input::with_theme(&theme)
        .with_prompt("Default region")
        .default("region-eu".into())
        .interact_text()?;

    // --- Write config ---
    let config = AppConfig {
        service_account_path,
        project,
        temp_dataset: None,
        region,
    };

    let config_dir = config_path.parent().ok_or("Config path has no parent")?;
    std::fs::create_dir_all(config_dir)?;

    let yaml = serde_yml::to_string(&config)?;

    std::fs::write(&config_path, yaml)?;

    println!();
    println!("Config written to: {}", config_path.display());
    println!();
    println!("Setup complete! Run `bq-assist --help` to see all available commands.");
    println!();

    Ok(())
}

fn prompt_service_account_path(theme: &ColorfulTheme) -> Result<String, Box<dyn std::error::Error>> {
    loop {
        let path: String = Input::with_theme(theme)
            .with_prompt("Absolute path to service account JSON file")
            .interact_text()?;

        if Path::new(&path).is_absolute() && Path::new(&path).exists() {
            return Ok(path);
        }

        if !Path::new(&path).is_absolute() {
            println!("  Please provide an absolute path.");
        } else {
            println!("  File not found: {path}");
        }
    }
}
