-- Admin migration only. Read passwords inside PostgreSQL instead of SQL literals.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolcanlogin AND rolname NOT IN ('etl_user', 'etl_app')) THEN
        RAISE EXCEPTION 'Additional login roles exist; review database access before applying local policy';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_app') THEN
        CREATE ROLE etl_app LOGIN;
    END IF;
    ALTER ROLE etl_app NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    EXECUTE format('ALTER ROLE etl_app PASSWORD %L',
        btrim(pg_read_file('/run/secrets/project_app_password'), E' \t\r\n'));
    EXECUTE format('ALTER ROLE %I PASSWORD %L', current_user,
        btrim(pg_read_file('/run/secrets/project_admin_password'), E' \t\r\n'));
END $$;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE CONNECT, CREATE, TEMPORARY ON DATABASE etl_project FROM PUBLIC;
GRANT CONNECT ON DATABASE etl_project TO etl_app;
GRANT CONNECT ON DATABASE etl_project TO etl_user;
REVOKE CONNECT, TEMPORARY ON DATABASE postgres, template1 FROM PUBLIC;
REVOKE ALL ON SCHEMA raw, clean, quarantine, mart FROM etl_app;
GRANT USAGE ON SCHEMA raw, clean, quarantine, mart TO etl_app;
REVOKE ALL ON ALL TABLES IN SCHEMA raw FROM etl_app;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA raw TO etl_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA clean, quarantine, mart TO etl_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA clean, quarantine, mart TO etl_app;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA raw
    GRANT SELECT, INSERT ON TABLES TO etl_app;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA clean, quarantine, mart
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_app;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA clean, quarantine, mart
    GRANT USAGE, SELECT ON SEQUENCES TO etl_app;
