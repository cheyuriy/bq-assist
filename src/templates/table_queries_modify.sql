SELECT
  job_id,
  UNIX_MILLIS(creation_time) AS creation_time,
  user_email,
  query,
  statement_type,
  state,
  total_bytes_billed
FROM
  `{{region}}`.INFORMATION_SCHEMA.JOBS
WHERE
  project_id = '{{bigquery_project}}'
  AND creation_time >= TIMESTAMP('{{from_ts}}')
  {%- if to_ts %}
  AND creation_time <= TIMESTAMP('{{to_ts}}')
  {%- endif %}
  {%- if query_type %}
  AND statement_type = '{{query_type}}'
  {%- else %}
  AND statement_type != 'SELECT'
  {%- endif %}
  {%- if related %}
  AND (
    (destination_table.project_id = '{{bigquery_project}}'
     AND destination_table.dataset_id = '{{bigquery_dataset}}'
     AND destination_table.table_id = '{{bigquery_table}}')
    OR EXISTS (
      SELECT 1 FROM UNNEST(referenced_tables) AS t
      WHERE t.project_id = '{{bigquery_project}}'
        AND t.dataset_id = '{{bigquery_dataset}}'
        AND t.table_id = '{{bigquery_table}}'
    )
  )
  {%- else %}
  AND destination_table.project_id = '{{bigquery_project}}'
  AND destination_table.dataset_id = '{{bigquery_dataset}}'
  AND destination_table.table_id = '{{bigquery_table}}'
  {%- endif %}
  {%- if user %}
  AND user_email = '{{user}}'
  {%- endif %}
ORDER BY
  creation_time DESC
LIMIT {{limit}};
