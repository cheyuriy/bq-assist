SELECT
  data_type,
  is_nullable,
  clustering_ordinal_position
FROM `{{bigquery_project}}.{{bigquery_dataset}}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{{bigquery_table}}'
  AND column_name = '{{bigquery_column}}'
LIMIT 1
