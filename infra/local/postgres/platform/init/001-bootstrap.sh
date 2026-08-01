#!/usr/bin/env bash
set -Eeuo pipefail
set +x

platform_migrator_password="$(</run/edtech-secrets/migrator-password)"
platform_api_password="$(</run/edtech-secrets/api-password)"
platform_worker_password="$(</run/edtech-secrets/worker-password)"

psql --set=ON_ERROR_STOP=1 \
  --set=migrator_password="${platform_migrator_password}" \
  --set=api_password="${platform_api_password}" \
  --set=worker_password="${platform_worker_password}" \
  --username "${POSTGRES_USER}" \
  --dbname "${POSTGRES_DB}" <<'SQL'
SELECT format(
    'CREATE ROLE edtech_platform_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    :'migrator_password'
) \gexec
SELECT format(
    'CREATE ROLE edtech_platform_api LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    :'api_password'
) \gexec
SELECT format(
    'CREATE ROLE edtech_platform_worker LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    :'worker_password'
) \gexec

REVOKE ALL ON DATABASE edtech_platform FROM PUBLIC;
GRANT CONNECT, CREATE ON DATABASE edtech_platform TO edtech_platform_migrator;
GRANT CONNECT ON DATABASE edtech_platform TO edtech_platform_api, edtech_platform_worker;
REVOKE TEMP ON DATABASE edtech_platform FROM edtech_platform_migrator,
    edtech_platform_api, edtech_platform_worker;
REVOKE ALL ON SCHEMA public FROM PUBLIC;

ALTER ROLE edtech_platform_migrator IN DATABASE edtech_platform SET search_path = pg_catalog;
ALTER ROLE edtech_platform_api IN DATABASE edtech_platform SET search_path = pg_catalog;
ALTER ROLE edtech_platform_worker IN DATABASE edtech_platform SET search_path = pg_catalog;
ALTER ROLE edtech_platform_api IN DATABASE edtech_platform SET row_security = on;
ALTER ROLE edtech_platform_worker IN DATABASE edtech_platform SET row_security = on;

CREATE SCHEMA edtech_bootstrap AUTHORIZATION edtech_platform_bootstrap;
REVOKE ALL ON SCHEMA edtech_bootstrap FROM PUBLIC;

CREATE TABLE edtech_bootstrap.authority_identity (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    authority_kind TEXT NOT NULL CHECK (authority_kind IN ('platform', 'cell')),
    cell_id TEXT,
    initialized_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now(),
    CONSTRAINT authority_identity_kind_cell CHECK (
        (authority_kind = 'platform' AND cell_id IS NULL)
        OR (authority_kind = 'cell' AND cell_id IS NOT NULL)
    ),
    CONSTRAINT authority_identity_platform_only CHECK (
        authority_kind = 'platform' AND cell_id IS NULL
    )
);
ALTER TABLE edtech_bootstrap.authority_identity OWNER TO edtech_platform_bootstrap;
INSERT INTO edtech_bootstrap.authority_identity (authority_kind, cell_id)
VALUES ('platform', NULL);

GRANT USAGE ON SCHEMA edtech_bootstrap TO edtech_platform_migrator,
    edtech_platform_api, edtech_platform_worker;
GRANT SELECT ON edtech_bootstrap.authority_identity TO edtech_platform_migrator,
    edtech_platform_api, edtech_platform_worker;
SQL

unset platform_migrator_password platform_api_password platform_worker_password
