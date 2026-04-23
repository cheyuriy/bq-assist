SELECT
  table_name,
  IFNULL(total_logical_bytes, 0)  AS total_logical_bytes,
  IFNULL(total_physical_bytes, 0) AS total_physical_bytes
FROM
  `{{region}}.INFORMATION_SCHEMA.TABLE_STORAGE`
WHERE
  project_id = '{{bigquery_project}}'
  AND table_schema = '{{bigquery_dataset}}'
ORDER BY
  GREATEST(total_logical_bytes, total_physical_bytes) DESC
LIMIT 20
