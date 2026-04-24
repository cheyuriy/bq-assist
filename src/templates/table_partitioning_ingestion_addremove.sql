BEGIN
    {% if bigquery_temp_dataset == bigquery_dataset -%}
    CREATE TABLE `{{bigquery_project}}.{{bigquery_temp_dataset}}.{{bigquery_table}}_tmp`
    {{schema_clause}}
    {{partitioning_clause}}
    {% if clustering_clause -%}{{clustering_clause}}{% endif %};

    INSERT INTO `{{bigquery_project}}.{{bigquery_temp_dataset}}.{{bigquery_table}}_tmp` ({{columns_clause}})
    SELECT * FROM `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`;

    DROP TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`;

    ALTER TABLE `{{bigquery_project}}.{{bigquery_temp_dataset}}.{{bigquery_table}}_tmp`
    RENAME TO `{{bigquery_table}}`;
    {%- else -%}
    CREATE TABLE `{{bigquery_project}}.{{bigquery_temp_dataset}}.{{bigquery_table}}_tmp`
    {{schema_clause}}
    {{partitioning_clause}}
    {% if clustering_clause -%}{{clustering_clause}}{% endif %};

    INSERT INTO `{{bigquery_project}}.{{bigquery_temp_dataset}}.{{bigquery_table}}_tmp` ({{columns_clause}})
    SELECT * FROM `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`;

    DROP TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`;

    CREATE TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
    COPY `{{bigquery_project}}.{{bigquery_temp_dataset}}.{{bigquery_table}}_tmp`;

    DROP TABLE IF EXISTS `{{bigquery_project}}.{{bigquery_temp_dataset}}.{{bigquery_table}}_tmp`;
    {%- endif %}
EXCEPTION WHEN ERROR THEN
    RAISE;
END;
