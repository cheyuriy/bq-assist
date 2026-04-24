CREATE OR REPLACE TABLE `{project}.{dataset}.test_rewind` (
  id    INT64,
  label STRING
);

INSERT INTO `{project}.{dataset}.test_rewind` (id, label)
VALUES (1, 'initial_a'), (2, 'initial_b'), (3, 'initial_c');
