SELECT
  COUNT(*) AS partitions_count,
  IFNULL(SUM(total_rows), 0) AS total_rows,
  IFNULL(SUM(total_logical_bytes), 0) AS total_logical_bytes
FROM
  `{{bigquery_project}}.{{bigquery_dataset}}.INFORMATION_SCHEMA.PARTITIONS`
WHERE
  table_name = '{{bigquery_table}}'
