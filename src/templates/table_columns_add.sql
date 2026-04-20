ALTER TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
ADD COLUMN {{column_name}} {{column_type}};

{% if default_clause -%}
ALTER TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
ALTER COLUMN {{column_name}}
SET DEFAULT {{default_clause}}
{% endif %}