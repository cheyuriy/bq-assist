WITH stats AS (
  SELECT
    COUNT(*) AS total_count,
    COUNTIF(`{{bigquery_column}}` IS NULL) AS null_count,
    MIN(CAST(`{{bigquery_column}}` AS BIGNUMERIC)) AS min_val,
    MAX(CAST(`{{bigquery_column}}` AS BIGNUMERIC)) AS max_val,
    AVG(CAST(`{{bigquery_column}}` AS FLOAT64)) AS avg_val
  FROM `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
),
bucketed AS (
  SELECT
    CASE
      WHEN (SELECT max_val = min_val FROM stats) THEN 0
      ELSE LEAST(
        CAST(FLOOR(
          (CAST(`{{bigquery_column}}` AS FLOAT64) - CAST((SELECT min_val FROM stats) AS FLOAT64)) /
          CAST((SELECT max_val - min_val FROM stats) AS FLOAT64) * {{bins_number}}
        ) AS INT64),
        {{bins_number}} - 1
      )
    END AS bucket
  FROM `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
  WHERE `{{bigquery_column}}` IS NOT NULL
),
bucket_counts AS (
  SELECT bucket, COUNT(*) AS bin_count
  FROM bucketed
  GROUP BY bucket
),
all_buckets AS (
  SELECT b AS bucket
  FROM UNNEST(GENERATE_ARRAY(0, {{bins_number}} - 1)) AS b
)
SELECT
  (SELECT total_count FROM stats) AS total_count,
  (SELECT null_count FROM stats) AS null_count,
  (SELECT CAST(min_val AS FLOAT64) FROM stats) AS min_val,
  (SELECT CAST(max_val AS FLOAT64) FROM stats) AS max_val,
  (SELECT avg_val FROM stats) AS avg_val,
  ab.bucket,
  IFNULL(bc.bin_count, 0) AS bin_count
FROM all_buckets ab
LEFT JOIN bucket_counts bc USING (bucket)
ORDER BY ab.bucket
