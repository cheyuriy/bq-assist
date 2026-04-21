SELECT
  table_catalog,
  table_schema,
  table_name
FROM
  `{{bigquery_project}}.{{bigquery_dataset}}.INFORMATION_SCHEMA.TABLES`
WHERE
  table_type = 'CLONE'
  AND CONTAINS_SUBSTR(
    IFNULL(ddl, ''),
    '`{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`'
  )
ORDER BY table_name
