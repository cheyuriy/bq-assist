CREATE OR REPLACE TABLE `{project}.{dataset}.test_partitioning` (
  id       INT64,
  event_ts TIMESTAMP,
  value    INT64,
  label    STRING
);

INSERT INTO `{project}.{dataset}.test_partitioning` (id, event_ts, value, label)
VALUES
  (1, TIMESTAMP '2024-01-15 10:00:00 UTC', 100, 'alpha'),
  (2, TIMESTAMP '2024-03-22 14:30:00 UTC', 500, 'beta'),
  (3, TIMESTAMP '2024-07-04 09:15:00 UTC', 900, 'gamma');
