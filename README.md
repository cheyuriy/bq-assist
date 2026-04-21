# bq-assist

A CLI tool for complex BigQuery operations that are either unavailable in the BigQuery console, require raw SQL, or demand many manual steps to accomplish. It is not a full BigQuery client — it focuses specifically on the operations that are difficult or tedious to perform otherwise.

## Why bq-assist?

The BigQuery console covers the basics well, but many real-world tasks are still painful:

- **Partitioning and clustering** can only be set at table creation time in the console — changing them on an existing table requires recreating it manually.
- **Merging tables** (upsert, diff, union, etc.) requires writing non-trivial MERGE SQL each time.
- **Column operations** like renaming or casting a type require DDL that is easy to get wrong and tedious to repeat.
- **Snapshots and copies** are hard to track and restore from without scripting.
- **Table options and metadata** (labels, expiration, KMS, change history, etc.) can only be set via `ALTER TABLE` SQL.
- **Query history** for a specific table is buried in INFORMATION_SCHEMA views.

`bq-assist` wraps all of these into a single CLI with a consistent, discoverable interface.

## Commands

Table references accept `[project.]dataset.table`. Dataset references accept `[project.]dataset`. If a project is omitted, the one from your config is used.

### `table <TABLE> ...`

Operations on a specific table.

| Subcommand | Description |
|---|---|
| `clustering` | Show current clustering settings |
| `clustering add <fields...>` | Add or replace clustering fields (up to 4) |
| `clustering remove` | Remove clustering |
| `partitioning` | Show current partitioning settings |
| `partitioning add range <col> <from> <to> <interval>` | Integer range partitioning |
| `partitioning add time <col> [type] [granularity]` | Time-unit column partitioning |
| `partitioning add ingestion [granularity]` | Ingestion-time partitioning |
| `partitioning remove` | Remove partitioning |
| `columns` | Show current schema |
| `columns add <name> <type> [default]` | Add a column |
| `columns rename <name> <new-name>` | Rename a column |
| `columns cast <name> <type>` | Change a column's type (data is cast in place) |
| `columns remove <name>` | Delete a column |
| `restore` | Restore table from time-travel, tracked copy, snapshot, or archive |
| `snapshots` | List tracked snapshots |
| `snapshots add [name]` | Create (and track) a snapshot |
| `snapshots remove <name\|id\|*>` | Delete a tracked snapshot |
| `copy` | List tracked copies |
| `copy add [name]` | Create (and track) a copy |
| `copy remove <name\|id\|*>` | Delete a tracked copy |
| `options <option> <value>` | Set a table option (use `NULL` to remove) |
| `queries read` | List queries that read from this table |
| `queries modify` | List queries that modified this table |
| `stats [--with-ddl]` | Show a full report: type, size, partitioning, clustering, options. Add `--with-ddl` to include the table DDL. |
| `stats columns <name>` | Detailed statistics for a specific column |
| `archive add` | Archive the table (one-time or periodic) |
| `rename <new-name>` | Rename the table |

**Known table options:** `expiration_timestamp`, `partition_expiration_days`, `require_partition_filter`, `kms_key_name`, `friendly_name`, `description`, `labels`, `default_rounding_mode`, `enable_change_history`, `max_staleness`, `enable_fine_grained_mutations`, `storage_uri`, `file_format`, `table_format`, `tags`

### `dataset <DATASET> ...`

| Subcommand | Description |
|---|---|
| `options <option> <value>` | Set a dataset option (use `NULL` to remove) |
| `stats` | Show dataset statistics |

**Known dataset options:** `default_kms_key_name`, `default_partition_expiration_days`, `default_rounding_mode`, `default_table_expiration_days`, `description`, `failover_reservation`, `friendly_name`, `is_case_insensitive`, `is_primary`, `labels`, `max_time_travel_hours`, `primary_replica`, `storage_billing_model`, `tags`

### `merge <LEFT> <RIGHT> [DESTINATION] ...`

Merge two tables using common patterns. `LEFT` is the destination by default; use `DESTINATION` to write results elsewhere.

| Subcommand | Description |
|---|---|
| `insert` | Append only new rows from RIGHT (no updates) |
| `upsert` | Insert new rows and update existing ones from RIGHT |
| `update` | Update existing rows from RIGHT, no new inserts |
| `inner-left` | Keep only LEFT rows whose key exists in RIGHT |
| `inner-right` | Keep only RIGHT rows whose key exists in LEFT |
| `diff` | Keep rows whose key appears in only one table (symmetric difference) |
| `diff-left` | Keep LEFT rows whose key does not exist in RIGHT (LEFT ANTI JOIN) |
| `diff-right` | Keep RIGHT rows whose key does not exist in LEFT (RIGHT ANTI JOIN) |
| `union` | Combine all rows from both tables (UNION ALL) |

All merge subcommands accept: `key`, `--left-key`, `--right-key`, `left_filter`, `right_filter`.

### `compare <LEFT> <RIGHT>`

Compare two tables and show a diff report. Supports `--left-copy`, `--left-snapshot`, `--right-copy`, `--right-snapshot` to compare against tracked copies or snapshots.

### `init`

Interactive setup wizard — creates the config file by prompting for required values.

### `checks`

Run sanity checks on tables.

## Installation

### Pre-built binaries

Download the latest release for your platform from the [Releases](../../releases) page:

| Platform | File |
|---|---|
| Linux (x86_64) | `bq-assist-linux-x86_64` |
| macOS (x86_64) | `bq-assist-macos-x86_64` |
| macOS (Apple Silicon) | `bq-assist-macos-aarch64` |
| Windows | `bq-assist-windows-x86_64.exe` |

Make the binary executable (Linux/macOS) and move it somewhere on your `PATH`:

```sh
chmod +x bq-assist-linux-x86_64
mv bq-assist-linux-x86_64 /usr/local/bin/bq-assist
```

### From source

Requires [Rust](https://rustup.rs/) (edition 2024, stable toolchain).

```sh
git clone <repo-url>
cd bq-assist
cargo build --release
# Binary is at ./target/release/bq-assist
```

### Via cargo

```sh
cargo install --git <repo-url> bq-assist
```

## Configuration

### Setup wizard

Run `bq-assist init` for an interactive prompt that creates the config file for you.

### Config file

The config file lives at the platform default config directory under `bq-assist/config.yaml`:

- **Linux:** `~/.config/bq-assist/config.yaml`
- **macOS:** `~/Library/Application Support/com.example.bq-assist/config.yaml`
- **Windows:** `%APPDATA%\example\bq-assist\config\config.yaml`

Override the directory with the `BQ_ASSIST_CONFIG_DIR` environment variable.

```yaml
service_account_path: /path/to/service-account.json
project: my-gcp-project
temp_dataset: my_temp_dataset
region: region-eu
```

| Field | Required | Description |
|---|---|---|
| `service_account_path` | No* | Path to a GCP service account JSON key file |
| `project` | No* | Default GCP project ID |
| `temp_dataset` | No | Dataset used for intermediate tables in merge/compare operations |
| `region` | No | BigQuery region (default: `region-eu`) |

*Required unless supplied via environment variables.

### Environment variables

| Variable | Description |
|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON (takes priority over `service_account_path`) |
| `BQ_ASSIST_CONFIG_DIR` | Override config directory path |
| `BQ_ASSIST__PROJECT` | Override `project` from config |
| `BQ_ASSIST__TEMP_DATASET` | Override `temp_dataset` from config |
| `BQ_ASSIST__REGION` | Override `region` from config |

Environment variables use the prefix `BQ_ASSIST__` with double underscore as separator.

## BigQuery Permissions

`bq-assist` executes DDL and DML on your behalf using the configured service account. The service account needs sufficient permissions for the operations you intend to run. At minimum:

- **`bigquery.tables.create`** — create snapshots, copies, restore operations
- **`bigquery.tables.delete`** — remove snapshots, copies
- **`bigquery.tables.update`** — alter table options, schema, clustering, partitioning
- **`bigquery.tables.getData`** / **`bigquery.tables.get`** — read table metadata and schema
- **`bigquery.jobs.create`** — execute queries
- **`bigquery.datasets.update`** — alter dataset options

The predefined IAM roles that cover these are:

| Role | Use case |
|---|---|
| `roles/bigquery.dataEditor` | Read, create, update, and delete tables within datasets |
| `roles/bigquery.jobUser` | Run jobs (required alongside data roles) |
| `roles/bigquery.admin` | Full access — simplest for development |

For production, grant the minimum set of permissions required for your workflows.

## Examples

```sh
# Run the setup wizard
bq-assist init

# View the current schema of a table
bq-assist table my_dataset.my_table columns

# Add a new column
bq-assist table my_dataset.my_table columns add created_at TIMESTAMP

# Rename a column
bq-assist table my_dataset.my_table columns rename old_name new_name

# Change a column's type
bq-assist table my_dataset.my_table columns cast amount NUMERIC

# Add day-level partitioning on an existing table
bq-assist table my_dataset.my_table partitioning add time event_time timestamp day

# Add clustering on two fields
bq-assist table my_dataset.my_table clustering add user_id country

# Remove clustering
bq-assist table my_dataset.my_table clustering remove

# Set table description
bq-assist table my_dataset.my_table options description "My important table"

# Set table expiration
bq-assist table my_dataset.my_table options expiration_timestamp 2027-01-01T00:00:00Z

# Remove table expiration
bq-assist table my_dataset.my_table options expiration_timestamp NULL

# Create a tracked snapshot
bq-assist table my_dataset.my_table snapshots add before_migration

# Create a snapshot rewinding 2 hours
bq-assist table my_dataset.my_table snapshots add --rewind 2h

# List snapshots
bq-assist table my_dataset.my_table snapshots

# Restore from a named snapshot
bq-assist table my_dataset.my_table restore --snapshot before_migration

# Restore using time-travel (rewind 30 minutes)
bq-assist table my_dataset.my_table restore --rewind 30m

# Upsert from a staging table into production
bq-assist merge my_dataset.prod_table my_dataset.staging_table upsert id

# Append only new rows (insert missing)
bq-assist merge my_dataset.target my_dataset.source insert id

# Show a full stats report for a table
bq-assist table my_dataset.my_table stats

# Show stats report including DDL
bq-assist table my_dataset.my_table stats --with-ddl

# Show queries that read from a table in the last 6 hours
bq-assist table my_dataset.my_table queries read --period 6h

# Show queries made by a specific user
bq-assist table my_dataset.my_table queries read --user analyst@example.com

# Compare two tables
bq-assist compare my_dataset.table_v1 my_dataset.table_v2

# Set dataset time-travel window to 48 hours
bq-assist dataset my_dataset options max_time_travel_hours 48

# Rename a table
bq-assist table my_dataset.old_name rename new_name
```
