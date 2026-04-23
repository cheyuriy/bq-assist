SELECT
  table_type,
  COUNT(*) AS cnt
FROM
  `{{bigquery_project}}.{{bigquery_dataset}}.INFORMATION_SCHEMA.TABLES`
GROUP BY
  table_type
