WITH freq AS (
  SELECT
    CAST(`{{bigquery_column}}` AS STRING) AS value,
    COUNT(*) AS cnt
  FROM `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
  WHERE `{{bigquery_column}}` IS NOT NULL
  GROUP BY 1
)
SELECT
  value,
  cnt,
  COUNT(*) OVER () AS total_distinct
FROM freq
ORDER BY cnt DESC
