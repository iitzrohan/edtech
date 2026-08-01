CREATE SCHEMA platform_control AUTHORIZATION edtech_platform_migrator;
REVOKE ALL ON SCHEMA platform_control FROM PUBLIC;

CREATE TABLE platform_control.schema_contract (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    contract_name TEXT NOT NULL CHECK (contract_name = 'platform'),
    contract_version INTEGER NOT NULL CHECK (contract_version >= 1),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now()
);
ALTER TABLE platform_control.schema_contract OWNER TO edtech_platform_migrator;
INSERT INTO platform_control.schema_contract (contract_name, contract_version)
VALUES ('platform', 1);

GRANT USAGE ON SCHEMA platform_control TO edtech_platform_api, edtech_platform_worker;
GRANT SELECT ON platform_control.schema_contract TO edtech_platform_api, edtech_platform_worker;
REVOKE ALL ON platform_control.schema_contract FROM PUBLIC;

ALTER DEFAULT PRIVILEGES FOR ROLE edtech_platform_migrator IN SCHEMA platform_control
    REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE edtech_platform_migrator IN SCHEMA platform_control
    REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE edtech_platform_migrator IN SCHEMA platform_control
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
