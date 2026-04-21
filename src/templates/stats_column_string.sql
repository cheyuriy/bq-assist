SELECT
  COUNT(*) AS total_count,
  COUNTIF(`{{bigquery_column}}` IS NULL) AS null_count,
  MIN(LENGTH(`{{bigquery_column}}`)) AS min_len,
  MAX(LENGTH(`{{bigquery_column}}`)) AS max_len,
  AVG(LENGTH(`{{bigquery_column}}`)) AS avg_len
FROM `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
