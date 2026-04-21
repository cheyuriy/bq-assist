WITH stats AS (
  SELECT
    COUNT(*) AS total_count,
    COUNTIF(`{{bigquery_column}}` IS NULL) AS null_count,
    CAST(MIN(`{{bigquery_column}}`) AS STRING) AS earliest,
    CAST(MAX(`{{bigquery_column}}`) AS STRING) AS latest
  FROM `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
),
bucketed AS (
  SELECT
    {% if is_time %}
    CAST(TIME_TRUNC(`{{bigquery_column}}`, HOUR) AS STRING)
    {% else %}
    CAST({{trunc_fn}}(`{{bigquery_column}}`, {{time_bins}}) AS STRING)
    {% endif %}
    AS bucket
  FROM `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
  WHERE `{{bigquery_column}}` IS NOT NULL
)
SELECT
  (SELECT total_count FROM stats) AS total_count,
  (SELECT null_count FROM stats) AS null_count,
  (SELECT earliest FROM stats) AS earliest,
  (SELECT latest FROM stats) AS latest,
  bucket,
  COUNT(*) AS bucket_count
FROM bucketed
GROUP BY bucket
ORDER BY bucket
