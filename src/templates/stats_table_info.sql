SELECT
  table_type,
  UNIX_MILLIS(creation_time) AS creation_time_ms,
  IFNULL(ddl, '') AS ddl
FROM
  `{{bigquery_project}}.{{bigquery_dataset}}.INFORMATION_SCHEMA.TABLES`
WHERE
  table_name = '{{bigquery_table}}'
LIMIT 1
