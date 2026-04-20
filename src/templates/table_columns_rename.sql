ALTER TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
ADD COLUMN {{column_new_name}} {{column_type}};

{% if default_clause -%}
ALTER TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
ALTER COLUMN {{column_new_name}}
SET DEFAULT {{default_clause}};
{% endif %}

UPDATE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
SET {{column_new_name}} = {{column_name}}
WHERE TRUE;

ALTER TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
DROP COLUMN {{column_name}};