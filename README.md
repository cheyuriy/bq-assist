# bq-assist

A Rust CLI for BigQuery operations that are unavailable in the console, require raw SQL, or demand many manual steps. Not a full BigQuery client — focused on the tasks that are otherwise tedious or hard to get right.

## Why bq-assist?

The BigQuery console covers the basics, but real-world DDL/DML workflows are still painful:

- **Partitioning and clustering** can only be set at table creation time in the console — modifying them on an existing table requires recreating it manually.
- **Merging tables** (upsert, diff, union, etc.) requires writing non-trivial MERGE SQL each time.
- **Column operations** — renames, type casts, removals — involve DDL that is easy to get wrong and tedious to repeat.
- **Snapshots, copies, and restores** are hard to track and operate on without scripting.
- **Table and dataset metadata** (labels, expiration, KMS, change history, etc.) can only be set via `ALTER TABLE` SQL.
- **Query history** for a specific table is buried in `INFORMATION_SCHEMA` views.

`bq-assist` wraps all of these into a consistent, discoverable CLI.

## Commands

Table references accept `[project.]dataset.table`. Dataset references accept `[project.]dataset`. If `project` is omitted, the one from your config is used.

Run any command with `--help` for the full flag reference.

---

### `table <TABLE_REF>`

Operations on a specific table. All subcommands follow the pattern `bq-assist table <TABLE_REF> <subcommand>`.

#### `clustering`

Show, add, or remove clustering fields (up to 4).

> **Cost warning:** Changing clustering requires several full table copies internally. For large tables or on-demand billing this can be expensive — check your table size before running.

```sh
# Show current clustering fields
bq-assist table myds.events clustering
# Cluster by user_id and country
bq-assist table myds.events clustering add user_id country
# Remove all clustering
bq-assist table myds.events clustering remove
```

#### `partitioning`

Show, add, or remove partitioning. Supports time-unit column (`time`), integer range (`range`), and ingestion-time (`ingestion`) strategies. Partitioning can be added to an existing table — no manual recreation needed.

> **Cost warning:** Changing partitioning requires several full table copies internally. For large tables or on-demand billing this can be expensive — check your table size before running.

```sh
# Show current partitioning
bq-assist table myds.events partitioning
# Partition by a TIMESTAMP column at day granularity
bq-assist table myds.events partitioning add time event_ts timestamp day
# Partition by integer range: bucket_id in [0, 1000) with interval 10
bq-assist table myds.events partitioning add range bucket_id 0 1000 10
# Partition by ingestion time at day granularity
bq-assist table myds.events partitioning add ingestion day
# Remove partitioning
bq-assist table myds.events partitioning remove
```

#### `columns`

Show the schema or modify it: add, rename, cast, or remove columns.

```sh
# Show current schema
bq-assist table myds.events columns
# Add a new TIMESTAMP column
bq-assist table myds.events columns add created_at TIMESTAMP
# Rename a column (requires a full column scan — see note below)
bq-assist table myds.events columns rename old_name new_name
# Cast a column's type in place (see note below)
bq-assist table myds.events columns cast amount NUMERIC
# Drop a column
bq-assist table myds.events columns remove legacy_col
```

> **`rename` cost warning:** Renaming a column requires a full column scan. For large tables or on-demand billing this can be expensive.
>
> **`cast` type support:** When BigQuery natively supports the conversion, the fast built-in path is used. For other pairs, the most logical available cast is applied. Not all type pairs are supported — the command will error if the conversion cannot be performed.

#### `snapshots`

List tracked snapshots, create new ones (with optional time-travel via `--rewind` or `--timestamp`), or delete them. Tracking is implemented by setting two special labels on the created snapshot — this is how `bq-assist` finds and lists them later.

```sh
# List tracked snapshots
bq-assist table myds.events snapshots
# Create a named snapshot of the current state
bq-assist table myds.events snapshots add before_migration
# Snapshot the state from 2 hours ago (time-travel)
bq-assist table myds.events snapshots add --rewind 2h
# Delete a snapshot by name
bq-assist table myds.events snapshots remove before_migration
```

#### `copy`

List tracked copies, create new ones, or delete them. Similar to snapshots but produces a full independent table copy. Tracking is implemented by setting two special labels on the created copy.

```sh
# List tracked copies
bq-assist table myds.events copy
# Create a named full copy of the table
bq-assist table myds.events copy add pre_deploy
# Delete a tracked copy
bq-assist table myds.events copy remove pre_deploy
```

#### `restore`

Restore the table in-place from time-travel (`--rewind`), a tracked copy (`--copy`), a tracked snapshot (`--snapshot`), or an archive (`--archive`).

> **Note:** Only time-travel restore (`--rewind`) is currently implemented. Restoring from a copy, snapshot, or archive is not yet supported.

```sh
# Restore to the state 30 minutes ago (time-travel)
bq-assist table myds.events restore --rewind 30m
# Restore from a named snapshot
bq-assist table myds.events restore --snapshot before_migration
# Restore from a named copy
bq-assist table myds.events restore --copy pre_deploy
```

#### `options`

Set any table metadata option. Pass `none` or `null` to unset an option. For the known options listed below, the CLI performs basic value validation, but correctness is not guaranteed — BigQuery is the final authority. Options not in the known list are also accepted and the value is passed through as-is, so future BigQuery options will work without a tool update.

```sh
# Set a description
bq-assist table myds.events options description "Click-stream events"
# Set table expiration date
bq-assist table myds.events options expiration_timestamp 2027-01-01T00:00:00Z
# Remove table expiration
bq-assist table myds.events options expiration_timestamp none
# Apply multiple labels at once
bq-assist table myds.events options labels env:prod,team:data
```

Known options: `expiration_timestamp`, `partition_expiration_days`, `require_partition_filter`, `kms_key_name`, `friendly_name`, `description`, `labels`, `default_rounding_mode`, `enable_change_history`, `max_staleness`, `enable_fine_grained_mutations`, `storage_uri`, `file_format`, `table_format`, `tags`.

#### `queries`

Inspect BigQuery job history for a table. `read` lists queries that read from it; `modify` lists queries that wrote to it. Filter by time window, user, and more.

```sh
# Queries that read from this table in the last 6 hours
bq-assist table myds.events queries read --period 6h
# Queries by a specific user, capped at 20 results
bq-assist table myds.events queries read --user analyst@example.com --limit 20
# Queries that modified this table in the last 24 hours
bq-assist table myds.events queries modify --period 24h
```

#### `stats`

Show a full report (type, size, partitioning, clustering, options). Add `--with-ddl` to include the table's DDL. The `column` sub-subcommand shows per-column metadata and, with `--deep`, runs a full table scan to produce distributions and histograms.

> **`stats column --deep` cost warning:** A deep scan reads the entire table. For large tables or on-demand billing this can be expensive.

```sh
# Full table report (type, size, partitioning, clustering, options)
bq-assist table myds.events stats
# Same report, also printing the table DDL
bq-assist table myds.events stats --with-ddl
# Column metadata without a table scan
bq-assist table myds.events stats column event_ts
# Deep column scan with a 20-bucket numeric histogram
bq-assist table myds.events stats column amount --deep --bins-number 20
```

#### `archive`

Archive the table once or on a recurring schedule.

> **Not yet implemented.**

```sh
# Archive the table once
bq-assist table myds.events archive add
# Archive every 7 days and delete the source table after each run
bq-assist table myds.events archive add --frequency 7d --delete-after
```

#### `rename`

Rename the table.

```sh
# Rename the table in place
bq-assist table myds.old_name rename new_name
```

---

### `dataset <DATASET_REF>`

Dataset-level operations.

```sh
# Extend the time-travel window to 48 hours
bq-assist dataset myds options max_time_travel_hours 48
# Set a dataset description
bq-assist dataset myds options description "Core analytics dataset"
# Show dataset statistics
bq-assist dataset myds stats
```

Known dataset options: `default_kms_key_name`, `default_partition_expiration_days`, `default_rounding_mode`, `default_table_expiration_days`, `description`, `failover_reservation`, `friendly_name`, `is_case_insensitive`, `labels`, `max_time_travel_hours`, `storage_billing_model`, `tags`.

---

### `merge <LEFT> <RIGHT> [DESTINATION]`

> **Not yet implemented.**

Merge two tables using one of eight patterns. `LEFT` is the destination by default; pass a `DESTINATION` to write results to a different table. All patterns accept `--key` (join column name), `--left-key` / `--right-key` (per-table overrides), and `--left-filter` / `--right-filter` (WHERE-clause predicates).

| Pattern | What it does |
|---|---|
| `insert` | Append rows from RIGHT that don't exist in LEFT (no updates) |
| `upsert` | Insert new rows and update existing ones from RIGHT |
| `update` | Update existing rows from RIGHT, no new inserts |
| `inner-left` | Keep only LEFT rows whose key exists in RIGHT |
| `inner-right` | Keep only RIGHT rows whose key exists in LEFT |
| `diff` | Keep rows whose key appears in exactly one table (symmetric difference) |
| `diff-left` | Keep LEFT rows whose key does not exist in RIGHT (LEFT ANTI JOIN) |
| `diff-right` | Keep RIGHT rows whose key does not exist in LEFT (RIGHT ANTI JOIN) |
| `union` | Combine all rows from both tables (UNION ALL) |

```sh
# Upsert staging into prod, keyed on id
bq-assist merge myds.prod myds.staging upsert --key id
# Append only completed orders not already in target
bq-assist merge myds.target myds.source insert --key order_id --right-filter "status = 'complete'"
# Keep rows whose key appears in only one table (symmetric difference)
bq-assist merge myds.a myds.b diff --key event_id
```

---

### `compare <LEFT> <RIGHT>`

> **Not yet implemented.**

Compare two tables and show a diff report. Pass `--left-snapshot`, `--left-copy`, `--right-snapshot`, or `--right-copy` to compare against a tracked snapshot or copy.

```sh
# Compare two tables directly
bq-assist compare myds.table_v1 myds.table_v2
# Compare the current state against a pre-migration snapshot
bq-assist compare myds.events myds.events --left-snapshot before_migration
```

---

### `init`

Interactive setup wizard that creates the config file by prompting for required values.

```sh
# Launch the interactive setup wizard
bq-assist init
```

---

### `checks`

> **Not yet implemented.**

Run sanity checks on tables.

```sh
# Run sanity checks on tables
bq-assist checks
```

---

## Configuration

### Config file

The config file is read from `config.yaml` in the platform default config directory:

- **Linux:** `~/.config/bq-assist/config.yaml`
- **macOS:** `~/Library/Application Support/com.cheyuriydev.bq-assist/config.yaml`
- **Windows:** `%APPDATA%\cheyuriydev\bq-assist\config\config.yaml`

Override the directory with the `BQ_ASSIST_CONFIG_DIR` environment variable.

```yaml
service_account_path: /path/to/service-account.json
project: my-gcp-project
temp_dataset: my_temp_dataset
region: region-eu
```

| Field | Default | Description |
|---|---|---|
| `service_account_path` | — | Path to a GCP service account JSON key file |
| `project` | — | Default GCP project ID |
| `temp_dataset` | — | Dataset used for intermediate tables in `merge` and `compare` |
| `region` | `region-eu` | BigQuery region |

All fields are optional in the config file; they can instead be supplied via environment variables.

### Environment variables

All config fields can be overridden with environment variables using the `BQ_ASSIST__` prefix (double underscore):

| Variable | Description |
|---|---|
| `BQ_ASSIST_CONFIG_DIR` | Override the config directory path |
| `BQ_ASSIST__PROJECT` | Override `project` |
| `BQ_ASSIST__TEMP_DATASET` | Override `temp_dataset` |
| `BQ_ASSIST__REGION` | Override `region` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON — takes priority over `service_account_path` |

---

## Installation

### Install with Cargo

```sh
cargo install --git https://github.com/cheyuriy/bq-assist bq-assist
```

Requires the [Rust toolchain](https://rustup.rs/).

### Build from source

```sh
git clone https://github.com/cheyuriy/bq-assist
cd bq-assist
cargo build --release
# Binary at ./target/release/bq-assist
```

### Upgrade

Re-run the same `cargo install` command — it replaces the existing binary in place:

```sh
cargo install --git https://github.com/cheyuriy/bq-assist bq-assist
```

---

## Integration Testing

Integration tests run against real BigQuery — they are not mocked. To run them:

1. Copy `.env.test.example` to `.env.test` in the repo root and fill in the values:

```sh
cp .env.test.example .env.test
```

| Variable | Required | Description |
|---|---|---|
| `BQ_TEST_PROJECT` | Yes | GCP project to run tests against |
| `BQ_TEST_DATASET` | Yes | Dataset where test tables are created and destroyed |
| `BQ_TEST_SERVICE_ACCOUNT_PATH` | No* | Path to service account JSON |
| `BQ_TEST_REGION` | No | BigQuery region (default: `region-eu`) |

*`GOOGLE_APPLICATION_CREDENTIALS` can be used instead.

2. Run tests:

```sh
cargo test
```

Tests skip gracefully when `BQ_TEST_PROJECT` is not set, so they are safe to run in environments without BigQuery access.

> **Warning:** All tables in `BQ_TEST_DATASET` are dropped before each test run. Use a dedicated test dataset, never a production one.

---

## BigQuery Permissions

`bq-assist` executes DDL and DML using your configured credentials. The service account (or user credentials) needs sufficient permissions for the operations you intend to use.

The predefined IAM roles that cover all operations:

| Role | Purpose |
|---|---|
| `roles/bigquery.dataEditor` | Create, read, update, and delete tables within datasets |
| `roles/bigquery.jobUser` | Run jobs (required alongside data roles) |

`roles/bigquery.admin` covers everything and is the simplest option for development. For production, grant only the permissions required for your workflows.

Specific permissions used: `bigquery.tables.create`, `bigquery.tables.delete`, `bigquery.tables.update`, `bigquery.tables.get`, `bigquery.jobs.create`, `bigquery.datasets.update`.
