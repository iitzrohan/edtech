#!/usr/bin/env bash
set -Eeuo pipefail
set +x

cell_migrator_password="$(</run/edtech-secrets/migrator-password)"
cell_api_password="$(</run/edtech-secrets/api-password)"
cell_worker_password="$(</run/edtech-secrets/worker-password)"

psql --set=ON_ERROR_STOP=1 \
  --set=migrator_password="${cell_migrator_password}" \
  --set=api_password="${cell_api_password}" \
  --set=worker_password="${cell_worker_password}" \
  --username "${POSTGRES_USER}" \
  --dbname "${POSTGRES_DB}" <<'SQL'
SELECT format(
    'CREATE ROLE edtech_cell_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    :'migrator_password'
) \gexec
SELECT format(
    'CREATE ROLE edtech_cell_api LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    :'api_password'
) \gexec
SELECT format(
    'CREATE ROLE edtech_cell_worker LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    :'worker_password'
) \gexec

REVOKE ALL ON DATABASE edtech_cell FROM PUBLIC;
GRANT CONNECT, CREATE ON DATABASE edtech_cell TO edtech_cell_migrator;
GRANT CONNECT ON DATABASE edtech_cell TO edtech_cell_api, edtech_cell_worker;
REVOKE TEMP ON DATABASE edtech_cell FROM edtech_cell_migrator,
    edtech_cell_api, edtech_cell_worker;
REVOKE ALL ON SCHEMA public FROM PUBLIC;

ALTER ROLE edtech_cell_migrator IN DATABASE edtech_cell SET search_path = pg_catalog;
ALTER ROLE edtech_cell_api IN DATABASE edtech_cell SET search_path = pg_catalog;
ALTER ROLE edtech_cell_worker IN DATABASE edtech_cell SET search_path = pg_catalog;
ALTER ROLE edtech_cell_api IN DATABASE edtech_cell SET row_security = on;
ALTER ROLE edtech_cell_worker IN DATABASE edtech_cell SET row_security = on;

CREATE SCHEMA edtech_bootstrap AUTHORIZATION edtech_cell_bootstrap;
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
    CONSTRAINT authority_identity_cell_id_grammar CHECK (
        cell_id IS NULL OR (
            pg_catalog.octet_length(cell_id) BETWEEN 3 AND 63
            AND cell_id ~ '^[a-z0-9][a-z0-9-]*[a-z0-9]$'
            AND cell_id !~ '--'
        )
    ),
    CONSTRAINT authority_identity_cell_only CHECK (
        authority_kind = 'cell' AND cell_id IS NOT NULL
    )
);
ALTER TABLE edtech_bootstrap.authority_identity OWNER TO edtech_cell_bootstrap;
INSERT INTO edtech_bootstrap.authority_identity (authority_kind, cell_id)
VALUES ('cell', 'cell-001');

GRANT USAGE ON SCHEMA edtech_bootstrap TO edtech_cell_migrator,
    edtech_cell_api, edtech_cell_worker;
GRANT SELECT ON edtech_bootstrap.authority_identity TO edtech_cell_migrator,
    edtech_cell_api, edtech_cell_worker;
SQL

unset cell_migrator_password cell_api_password cell_worker_password
