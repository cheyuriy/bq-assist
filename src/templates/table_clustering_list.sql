SELECT
    column_name,
    clustering_ordinal_position
FROM
    `{{bigquery_project}}.{{bigquery_dataset}}.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = '{{bigquery_table}}'
    AND clustering_ordinal_position IS NOT NULL
ORDER BY
    clustering_ordinal_position;