mod bigquery;
mod cli;
mod commands;
mod errors;
mod models;

use clap::Parser;
use cli::CLI;
use minijinja::Environment;

#[tokio::main]
async fn main() {
    let cli = CLI::parse();
    let mut env = Environment::new();
    minijinja_embed::load_templates!(&mut env);

    let config = models::config::load_config().unwrap();

    match cli.commands {
        cli::Commands::Table {
            table_ref,
            table_subcommands,
        } => match table_subcommands {
            cli::TableSubcommands::Clustering { command } => match command {
                Some(cli::ClusteringSubcommands::Add { fields }) => {
                    commands::table::clustering::add(config, &table_ref, fields).await;
                }
                Some(cli::ClusteringSubcommands::Remove) => {
                    commands::table::clustering::remove(config, &table_ref).await;
                }
                None => {
                    commands::table::clustering::list(config, &table_ref).await;
                }
            },

            cli::TableSubcommands::Partitioning { command } => match command {
                Some(cli::PartitioningSubcommands::Partitioning(partition)) => {
                    commands::table::partitioning::add(config, &table_ref, Some(&partition)).await;
                }
                Some(cli::PartitioningSubcommands::Remove) => {
                    commands::table::partitioning::remove(config, &table_ref).await;
                }
                None => {
                    commands::table::partitioning::list(config, &table_ref).await;
                }
            },

            cli::TableSubcommands::Columns { command } => match command {
                Some(cli::ColumnsSubcommands::Add {
                    name,
                    field_type,
                    default_value,
                }) => {
                    commands::table::columns::add(
                        config,
                        &table_ref,
                        &name,
                        &field_type,
                        default_value,
                    )
                    .await;
                }
                Some(cli::ColumnsSubcommands::Rename { name, new_name }) => {
                    commands::table::columns::rename(config, &table_ref, &name, &new_name).await;
                }
                Some(cli::ColumnsSubcommands::Remove { name }) => {
                    commands::table::columns::remove(config, &table_ref, &name).await;
                }
                Some(cli::ColumnsSubcommands::Cast { name, field_type }) => {
                    commands::table::columns::cast(config, &table_ref, &name, &field_type).await;
                }
                None => {
                    commands::table::columns::list(config, &table_ref).await;
                }
            },

            cli::TableSubcommands::Restore {
                rewind,
                copy,
                snapshot,
                archive,
            } => {
                commands::table::restore(config, &table_ref, &rewind, &copy, &snapshot, &archive)
                    .await;
            }

            cli::TableSubcommands::Snapshots { command } => match command {
                Some(cli::SnapshotsSubcommands::Add {
                    name,
                    dataset,
                    rewind,
                    timestamp,
                    no_track,
                }) => {
                    println!(
                        "table {table_ref} snapshots add {name:?} {dataset:?} {rewind:?} {timestamp:?} {no_track:?}"
                    );
                    commands::table::snapshots::add(
                        config, &table_ref, name, dataset, rewind, timestamp, no_track,
                    )
                    .await;
                }
                Some(cli::SnapshotsSubcommands::Remove { name }) => {
                    commands::table::snapshots::remove(config, &table_ref, &name).await;
                }
                None => {
                    commands::table::snapshots::list(config, &table_ref).await;
                }
            },

            cli::TableSubcommands::Copy { command } => match command {
                Some(cli::CopySubcommands::Add {
                    name,
                    dataset,
                    no_track,
                }) => {
                    commands::table::copy::add(config, &table_ref, name, dataset, no_track).await;
                }
                Some(cli::CopySubcommands::Remove { name }) => {
                    commands::table::copy::remove(config, &table_ref, &name).await;
                }
                None => {
                    commands::table::copy::list(config, &table_ref).await;
                }
            },

            cli::TableSubcommands::Options { option, value } => {
                commands::table::set_option(config, &table_ref, &option, &value).await;
            }

            cli::TableSubcommands::Queries { command } => match command {
                cli::QueriesSubcommand::Read {
                    single,
                    user,
                    period,
                    from,
                    to,
                    limit,
                } => {
                    commands::table::queries::read(
                        config, &table_ref, single, user, period, from, to, limit,
                    )
                    .await;
                }
                cli::QueriesSubcommand::Modify {
                    query_type,
                    user,
                    period,
                    from,
                    to,
                    limit,
                    related,
                } => {
                    commands::table::queries::modify(
                        config, &table_ref, query_type, user, period, from, to, limit, related,
                    )
                    .await;
                }
            },

            cli::TableSubcommands::Stats { with_ddl, command } => match command {
                Some(cli::StatsSubcommands::Columns { name }) => {
                    println!("table {table_ref} stats columns {name}");
                    // TODO: implement stats columns command
                }
                None => {
                    commands::table::stats::report(config, &table_ref, with_ddl).await;
                }
            },

            cli::TableSubcommands::Archive { command } => match command {
                Some(cli::ArchiveSubcommands::Add {
                    archive_type,
                    frequency,
                    start_time,
                    delete_after,
                }) => {
                    println!(
                        "table {table_ref} archive add {archive_type:?} {frequency:?} {start_time:?} {delete_after:?}"
                    );
                    // TODO: implement archive add command
                }
                None => {
                    println!("table {table_ref} archive");
                    // TODO: implement archive command
                }
            },

            cli::TableSubcommands::Rename { new_name } => {
                commands::table::rename(config, &table_ref, &new_name).await;
            }
        },

        cli::Commands::Dataset {
            dataset_ref,
            dataset_subcommands,
        } => match dataset_subcommands {
            cli::DatasetSubcommands::Options { option, value } => {
                println!("dataset {dataset_ref} options {option} {value}");
                commands::dataset::set_option(config, &dataset_ref, &option, &value).await;
            }
            cli::DatasetSubcommands::Stats {} => {
                println!("dataset {dataset_ref} stats");
                // TODO: implement dataset stats command
            }
        },

        cli::Commands::Merge {
            left_ref,
            right_ref,
            destination_ref,
            merge_subcommands,
        } => match merge_subcommands {
            cli::MergeSubcommands::Diff {
                key,
                left_key,
                right_key,
                left_filter,
                right_filter,
            } => {
                println!(
                    "merge {left_ref} {right_ref} {destination_ref:?} diff {key:?} {left_key:?} {right_key:?} {left_filter:?} {right_filter:?}"
                );
                // TODO: implement merge diff command
            }
            cli::MergeSubcommands::DiffLeft {
                key,
                left_key,
                right_key,
                left_filter,
                right_filter,
            } => {
                println!(
                    "merge {left_ref} {right_ref} {destination_ref:?} diff_left {key:?} {left_key:?} {right_key:?} {left_filter:?} {right_filter:?}"
                );
                // TODO: implement merge diff left command
            }
            cli::MergeSubcommands::DiffRight {
                key,
                left_key,
                right_key,
                left_filter,
                right_filter,
            } => {
                println!(
                    "merge {left_ref} {right_ref} {destination_ref:?} diff_right {key:?} {left_key:?} {right_key:?} {left_filter:?} {right_filter:?}"
                );
                // TODO: implement merge diff right command
            }
            cli::MergeSubcommands::InnerLeft {
                key,
                left_key,
                right_key,
                left_filter,
                right_filter,
            } => {
                println!(
                    "merge {left_ref} {right_ref} {destination_ref:?} inner_left {key:?} {left_key:?} {right_key:?} {left_filter:?} {right_filter:?}"
                );
                // TODO: implement merge inner left command
            }
            cli::MergeSubcommands::InnerRight {
                key,
                left_key,
                right_key,
                left_filter,
                right_filter,
            } => {
                println!(
                    "merge {left_ref} {right_ref} {destination_ref:?} inner_right {key:?} {left_key:?} {right_key:?} {left_filter:?} {right_filter:?}"
                );
                // TODO: implement merge inner right command
            }
            cli::MergeSubcommands::Insert {
                key,
                left_key,
                right_key,
                left_filter,
                right_filter,
            } => {
                println!(
                    "merge {left_ref} {right_ref} {destination_ref:?}insert {key:?} {left_key:?} {right_key:?} {left_filter:?} {right_filter:?}"
                );
                // TODO: implement merge insert command
            }
            cli::MergeSubcommands::Union {
                key,
                left_key,
                right_key,
                left_filter,
                right_filter,
            } => {
                println!(
                    "merge {left_ref} {right_ref} {destination_ref:?} union {key:?} {left_key:?} {right_key:?} {left_filter:?} {right_filter:?}"
                );
                // TODO: implement merge union command
            }
            cli::MergeSubcommands::Update {
                key,
                left_key,
                right_key,
                left_filter,
                right_filter,
            } => {
                println!(
                    "merge {left_ref} {right_ref} {destination_ref:?} update {key:?} {left_key:?} {right_key:?} {left_filter:?} {right_filter:?}"
                );
                // TODO: implement merge update command
            }
            cli::MergeSubcommands::Upsert {
                key,
                left_key,
                right_key,
                left_filter,
                right_filter,
            } => {
                println!(
                    "merge {left_ref} {right_ref} {destination_ref:?} upsert {key:?} {left_key:?} {right_key:?} {left_filter:?} {right_filter:?}"
                );
                // TODO: implement merge upsert command
            }
        },

        cli::Commands::Compare {
            left_ref,
            left_copy,
            left_snapshot,
            right_ref,
            right_copy,
            right_snapshot,
        } => {
            println!(
                "compare {left_ref} {left_copy:?} {left_snapshot:?} {right_ref} {right_copy:?} {right_snapshot:?}"
            );
            // TODO: implement compare command
        }

        cli::Commands::Checks {} => {
            println!("checks");
            // TODO: implement checks command
        }

        cli::Commands::Init => {
            commands::init().await;
        }
    }
}
