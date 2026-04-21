SELECT
  COUNT(*) AS total_count,
  COUNTIF(`{{bigquery_column}}` IS NULL) AS null_count,
  COUNTIF(`{{bigquery_column}}` IS TRUE) AS true_count,
  COUNTIF(`{{bigquery_column}}` IS NOT NULL) AS non_null_count
FROM `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
