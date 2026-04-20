SELECT
    ddl
FROM
    `{{bigquery_project}}.{{bigquery_dataset}}.INFORMATION_SCHEMA.TABLES`
WHERE
    table_name="{{bigquery_table}}"