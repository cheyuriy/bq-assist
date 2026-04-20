BEGIN
    {% if bigquery_temp_dataset == bigquery_dataset -%}
    CREATE TABLE `{{bigquery_project}}.{{bigquery_temp_dataset}}.{{bigquery_table}}_tmp`
    {% else %}
    CREATE TEMP TABLE `{{bigquery_table}}_tmp`
    {% endif %}
    {% if partitioning_clause -%}{{partitioning_clause}}{% endif %}
    {% if clustering_clause -%}{{clustering_clause}}{% endif %}
    AS
    SELECT * FROM `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`;

    DROP TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`;

    {% if bigquery_temp_dataset == bigquery_dataset -%}
    ALTER TABLE `{{bigquery_project}}.{{bigquery_temp_dataset}}.{{bigquery_table}}_tmp`
    RENAME TO `{{bigquery_table}}`;
    {%- else -%}
    CREATE TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
    COPY `{{bigquery_table}}_tmp`;

    DROP TABLE IF EXISTS `{{bigquery_table}}_tmp`;
    {%- endif %}
EXCEPTION WHEN ERROR THEN
    RAISE;
END;