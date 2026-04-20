SELECT
  *
FROM
  `{{bigquery_project}}.{{bigquery_dataset}}.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = '{{bigquery_table}}'
{% if specific_column -%}
  AND
  column_name = '{{specific_column}}'
{% endif %}
ORDER BY
  ordinal_position;