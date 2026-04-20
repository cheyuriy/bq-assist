CREATE OR REPLACE TABLE `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}` AS
SELECT *
FROM `{{bigquery_project}}.{{bigquery_dataset}}.{{bigquery_table}}`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{interval}} SECOND);