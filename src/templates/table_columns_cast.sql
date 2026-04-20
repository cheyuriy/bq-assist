ALTER TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
ADD COLUMN {{column_new_name}} {{column_new_type}};

UPDATE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
SET {{column_new_name}} = {{cast_clause}}
WHERE TRUE;

ALTER TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
DROP COLUMN {{column_name}};