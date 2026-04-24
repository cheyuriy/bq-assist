CREATE OR REPLACE TABLE `{project}.{dataset}.test_clustering` (
  id       INT64,
  name     STRING,
  active   BOOL,
  score    INT64,
  category STRING
);

INSERT INTO `{project}.{dataset}.test_clustering` (id, name, active, score, category)
VALUES
  (1, 'Alice', TRUE,  95, 'premium'),
  (2, 'Bob',   FALSE, 42, 'basic'),
  (3, 'Carol', TRUE,  78, 'premium');
