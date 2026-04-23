SELECT
  option_name,
  option_value
FROM
  `{{region}}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS`
WHERE
  catalog_name = '{{bigquery_project}}'
  AND schema_name = '{{bigquery_dataset}}'
ORDER BY option_name
