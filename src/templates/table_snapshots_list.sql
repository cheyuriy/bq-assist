SELECT REGEXP_EXTRACT(option_value, '\\("bq_assist_snapshot_id", "(.+?)"\\)') as id, table_catalog, table_schema, table_name, creation_time, REGEXP_EXTRACT(option_value, '\\("bq_assist_origin", "(.+?)"\\)') as hex_digest
FROM 
	{{region}}.INFORMATION_SCHEMA.TABLES 
	INNER JOIN 
	{{region}}.INFORMATION_SCHEMA.TABLE_OPTIONS 
	USING(table_catalog, table_schema, table_name) 
WHERE 
  table_type="SNAPSHOT" and 
  option_name="labels" AND 
  CONTAINS_SUBSTR(option_value, 'STRUCT("bq_assist_origin", "{{table_ref_digest}}"')