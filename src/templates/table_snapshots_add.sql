CREATE SNAPSHOT TABLE `{{bigquery_project}}.{{bigquery_snapshot_dataset}}.{{bigquery_snapshot}}`
CLONE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
{%- if snapshot_ts %}
FOR SYSTEM_TIME AS OF TIMESTAMP("{{snapshot_ts}}")
{%- endif %}
{%- if table_ref_digest %}
OPTIONS (labels=[("bq_assist_origin", "{{table_ref_digest}}"), ("bq_assist_snapshot_id", "{{snapshot_id}}")]);
{%- else -%}
;
{%- endif %}