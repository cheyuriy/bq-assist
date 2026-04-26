CREATE OR REPLACE TABLE `{project}.{dataset}.test_columns_cast` (
  id         INT64     NOT NULL,
  label      STRING    NOT NULL,
  value      FLOAT64,
  amount     NUMERIC,
  metadata   JSON,
  is_active  BOOL,
  created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
);

INSERT INTO `{project}.{dataset}.test_columns_cast`
  (id, label, value, amount, is_active)
VALUES
  (1, 'first_row',  1.5,  10.0, TRUE),
  (2, 'second_row', 2.75, 20.0, FALSE);
