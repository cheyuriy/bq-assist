CREATE TABLE `{{bigquery_project}}.{{bigquery_copy_dataset}}.{{bigquery_copy}}`
COPY `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
{%- if table_ref_digest %}
OPTIONS (labels=[("bq_assist_origin", "{{table_ref_digest}}"), ("bq_assist_copy_id", "{{copy_id}}")]);
{%- else -%}
;
{%- endif %}