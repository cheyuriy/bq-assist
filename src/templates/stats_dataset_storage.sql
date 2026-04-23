SELECT
  IFNULL(SUM(active_logical_bytes), 0)     AS active_logical_bytes,
  IFNULL(SUM(long_term_logical_bytes), 0)  AS long_term_logical_bytes,
  IFNULL(SUM(total_logical_bytes), 0)      AS total_logical_bytes,
  IFNULL(SUM(active_physical_bytes), 0)    AS active_physical_bytes,
  IFNULL(SUM(long_term_physical_bytes), 0) AS long_term_physical_bytes,
  IFNULL(SUM(total_physical_bytes), 0)     AS total_physical_bytes,
  MAX(UNIX_MILLIS(storage_last_modified_time)) AS last_modified_ms
FROM
  `{{region}}.INFORMATION_SCHEMA.TABLE_STORAGE`
WHERE
  project_id = '{{bigquery_project}}'
  AND table_schema = '{{bigquery_dataset}}'
