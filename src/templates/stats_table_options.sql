SELECT
  option_name,
  option_value
FROM
  `{{bigquery_project}}.{{bigquery_dataset}}.INFORMATION_SCHEMA.TABLE_OPTIONS`
WHERE
  table_name = '{{bigquery_table}}'
ORDER BY option_name
