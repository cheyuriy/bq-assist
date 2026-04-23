SELECT
  UNIX_MILLIS(creation_time) AS creation_time_ms,
  UNIX_MILLIS(last_modified_time)   AS last_modified_ms,
  location
FROM
  `{{region}}.INFORMATION_SCHEMA.SCHEMATA`
WHERE
  catalog_name = '{{bigquery_project}}'
  AND schema_name = '{{bigquery_dataset}}'
LIMIT 1
