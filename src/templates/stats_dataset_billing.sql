SELECT
  option_value
FROM
  `{{region}}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS`
WHERE
  catalog_name = '{{bigquery_project}}'
  AND schema_name = '{{bigquery_dataset}}'
  AND option_name = 'storage_billing_model'
LIMIT 1
