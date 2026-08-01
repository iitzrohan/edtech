CREATE SCHEMA cell_control AUTHORIZATION edtech_cell_migrator;
CREATE SCHEMA edtech_internal AUTHORIZATION edtech_cell_migrator;
CREATE SCHEMA tenant_data AUTHORIZATION edtech_cell_migrator;
REVOKE ALL ON SCHEMA cell_control, edtech_internal, tenant_data FROM PUBLIC;

CREATE DOMAIN cell_control.assignment_epoch AS NUMERIC(20, 0)
    CHECK (VALUE >= 1 AND VALUE <= 18446744073709551615);

CREATE TABLE cell_control.schema_contract (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    contract_name TEXT NOT NULL CHECK (contract_name = 'cell'),
    contract_version INTEGER NOT NULL CHECK (contract_version >= 1),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now()
);
ALTER TABLE cell_control.schema_contract OWNER TO edtech_cell_migrator;
INSERT INTO cell_control.schema_contract (contract_name, contract_version)
VALUES ('cell', 1);

CREATE TABLE cell_control.tenant_authority (
    tenant_id UUID PRIMARY KEY CHECK (pg_catalog.uuid_extract_version(tenant_id) = 7),
    assignment_epoch cell_control.assignment_epoch NOT NULL,
    serving_enabled BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now()
);
ALTER TABLE cell_control.tenant_authority OWNER TO edtech_cell_migrator;

CREATE FUNCTION edtech_internal.current_tenant_id()
RETURNS UUID
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
    SELECT NULLIF(pg_catalog.current_setting('edtech.tenant_id', true), '')::uuid
$function$;
ALTER FUNCTION edtech_internal.current_tenant_id() OWNER TO edtech_cell_migrator;

CREATE FUNCTION edtech_internal.current_assignment_epoch()
RETURNS cell_control.assignment_epoch
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
    SELECT NULLIF(
        pg_catalog.current_setting('edtech.assignment_epoch', true), ''
    )::cell_control.assignment_epoch
$function$;
ALTER FUNCTION edtech_internal.current_assignment_epoch() OWNER TO edtech_cell_migrator;

CREATE FUNCTION edtech_internal.tenant_scope_status()
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM cell_control.tenant_authority AS authority
            WHERE authority.tenant_id = edtech_internal.current_tenant_id()
        ) THEN 'absent'
        WHEN EXISTS (
            SELECT 1
            FROM cell_control.tenant_authority AS authority
            WHERE authority.tenant_id = edtech_internal.current_tenant_id()
              AND NOT authority.serving_enabled
        ) THEN 'disabled'
        WHEN NOT EXISTS (
            SELECT 1
            FROM cell_control.tenant_authority AS authority
            WHERE authority.tenant_id = edtech_internal.current_tenant_id()
              AND authority.assignment_epoch = edtech_internal.current_assignment_epoch()
        ) THEN 'stale'
        ELSE 'active'
    END
$function$;
ALTER FUNCTION edtech_internal.tenant_scope_status() OWNER TO edtech_cell_migrator;

CREATE FUNCTION edtech_internal.tenant_is_authorized(row_tenant_id UUID)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
    SELECT row_tenant_id = edtech_internal.current_tenant_id()
       AND EXISTS (
           SELECT 1
           FROM cell_control.tenant_authority AS authority
           WHERE authority.tenant_id = row_tenant_id
             AND authority.serving_enabled
             AND authority.assignment_epoch = edtech_internal.current_assignment_epoch()
       )
$function$;
ALTER FUNCTION edtech_internal.tenant_is_authorized(UUID) OWNER TO edtech_cell_migrator;

REVOKE ALL ON FUNCTION edtech_internal.current_tenant_id() FROM PUBLIC;
REVOKE ALL ON FUNCTION edtech_internal.current_assignment_epoch() FROM PUBLIC;
REVOKE ALL ON FUNCTION edtech_internal.tenant_scope_status() FROM PUBLIC;
REVOKE ALL ON FUNCTION edtech_internal.tenant_is_authorized(UUID) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION edtech_internal.current_tenant_id() TO edtech_cell_api,
    edtech_cell_worker;
GRANT EXECUTE ON FUNCTION edtech_internal.current_assignment_epoch() TO edtech_cell_api,
    edtech_cell_worker;
GRANT EXECUTE ON FUNCTION edtech_internal.tenant_scope_status() TO edtech_cell_api,
    edtech_cell_worker;
GRANT EXECUTE ON FUNCTION edtech_internal.tenant_is_authorized(UUID) TO edtech_cell_api,
    edtech_cell_worker;

CREATE TABLE tenant_data.isolation_canary (
    tenant_id UUID NOT NULL CHECK (pg_catalog.uuid_extract_version(tenant_id) = 7),
    canary_id UUID NOT NULL CHECK (pg_catalog.uuid_extract_version(canary_id) = 7),
    payload TEXT NOT NULL CHECK (pg_catalog.char_length(payload) BETWEEN 1 AND 4096),
    created_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now(),
    PRIMARY KEY (tenant_id, canary_id)
);
ALTER TABLE tenant_data.isolation_canary OWNER TO edtech_cell_migrator;
ALTER TABLE tenant_data.isolation_canary ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_data.isolation_canary FORCE ROW LEVEL SECURITY;
CREATE POLICY isolation_canary_tenant_policy ON tenant_data.isolation_canary
    FOR ALL
    TO edtech_cell_api, edtech_cell_worker
    USING (edtech_internal.tenant_is_authorized(tenant_id))
    WITH CHECK (edtech_internal.tenant_is_authorized(tenant_id));

GRANT USAGE ON SCHEMA cell_control, edtech_internal, tenant_data TO edtech_cell_api,
    edtech_cell_worker;
GRANT SELECT ON cell_control.schema_contract TO edtech_cell_api, edtech_cell_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON tenant_data.isolation_canary TO edtech_cell_api,
    edtech_cell_worker;
REVOKE ALL ON cell_control.schema_contract, cell_control.tenant_authority,
    tenant_data.isolation_canary FROM PUBLIC;

ALTER DEFAULT PRIVILEGES FOR ROLE edtech_cell_migrator IN SCHEMA cell_control
    REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE edtech_cell_migrator IN SCHEMA edtech_internal
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE edtech_cell_migrator IN SCHEMA tenant_data
    REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE edtech_cell_migrator IN SCHEMA tenant_data
    REVOKE ALL ON SEQUENCES FROM PUBLIC;
