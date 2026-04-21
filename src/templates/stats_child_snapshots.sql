SELECT
  table_catalog,
  table_schema,
  table_name
FROM
  `{{region}}.INFORMATION_SCHEMA.TABLE_SNAPSHOTS`
WHERE
  base_table_catalog = '{{bigquery_project}}'
  AND base_table_schema = '{{bigquery_dataset}}'
  AND base_table_name = '{{bigquery_table}}'
ORDER BY snapshot_time DESC
