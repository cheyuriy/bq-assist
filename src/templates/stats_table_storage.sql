SELECT
  IFNULL(total_rows, 0) AS total_rows,
  IFNULL(active_logical_bytes, 0) AS active_logical_bytes,
  IFNULL(long_term_logical_bytes, 0) AS long_term_logical_bytes,
  IFNULL(total_logical_bytes, 0) AS total_logical_bytes,
  IFNULL(active_physical_bytes, 0) AS active_physical_bytes,
  IFNULL(long_term_physical_bytes, 0) AS long_term_physical_bytes,
  IFNULL(total_physical_bytes, 0) AS total_physical_bytes,
  IFNULL(time_travel_physical_bytes, 0) AS time_travel_physical_bytes,
  IFNULL(UNIX_MILLIS(storage_last_modified_time), 0) AS storage_last_modified_time_ms
FROM
  `{{region}}.INFORMATION_SCHEMA.TABLE_STORAGE`
WHERE
  project_id = '{{bigquery_project}}'
  AND table_schema = '{{bigquery_dataset}}'
  AND table_name = '{{bigquery_table}}'
LIMIT 1
