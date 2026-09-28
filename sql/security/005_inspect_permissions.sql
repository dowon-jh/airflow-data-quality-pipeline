SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
FROM pg_roles WHERE rolname IN ('etl_user', 'etl_app') ORDER BY rolname;
SELECT has_table_privilege('etl_app', 'raw.ai_product_output', 'SELECT') AS raw_select,
       has_table_privilege('etl_app', 'raw.ai_product_output', 'INSERT') AS raw_insert,
       has_table_privilege('etl_app', 'raw.ai_product_output', 'UPDATE') AS raw_update,
       has_table_privilege('etl_app', 'raw.ai_product_output', 'DELETE') AS raw_delete,
       has_schema_privilege('etl_app', 'raw', 'CREATE') AS schema_create,
       has_database_privilege('etl_app', 'etl_project', 'CREATE') AS database_create,
       has_database_privilege('etl_app', 'etl_project', 'TEMP') AS temporary_tables;
SELECT batch_id, row_count, data_sha256 FROM raw.ingest_batch ORDER BY batch_id;
SELECT COUNT(*) AS raw_rows FROM raw.ai_product_output;
