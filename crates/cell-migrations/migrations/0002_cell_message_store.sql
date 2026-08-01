DO $block$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM cell_control.schema_contract
        WHERE singleton AND contract_name = 'cell' AND contract_version = 1
    ) THEN
        RAISE EXCEPTION 'cell schema contract must be version 1 before migration 0002';
    END IF;
END
$block$;

CREATE SCHEMA cell_messaging AUTHORIZATION edtech_cell_migrator;
REVOKE ALL ON SCHEMA cell_messaging FROM PUBLIC;

CREATE TABLE cell_messaging.outbox_message (
    message_id UUID PRIMARY KEY CHECK (pg_catalog.uuid_extract_version(message_id) = 7),
    envelope_version SMALLINT NOT NULL CHECK (envelope_version = 1),
    message_kind TEXT NOT NULL CHECK (message_kind IN ('command', 'event')),
    message_name TEXT NOT NULL CHECK (
        pg_catalog.octet_length(message_name) BETWEEN 10 AND 160
        AND message_name ~ '^edtech(?:\.[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?){3,}$'
        AND message_name !~ '--'
        AND message_name !~ '\.(v[0-9]+|version-[0-9]+)$'
    ),
    message_schema_version INTEGER NOT NULL CHECK (message_schema_version BETWEEN 1 AND 65535),
    emitted_at TIMESTAMPTZ NOT NULL CHECK (emitted_at >= TIMESTAMPTZ '1970-01-01 00:00:00+00'),
    source_kind TEXT NOT NULL CHECK (source_kind = 'cell'),
    source_cell_id TEXT NOT NULL,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('cell', 'tenant')),
    scope_cell_id TEXT NOT NULL,
    tenant_id UUID CHECK (tenant_id IS NULL OR pg_catalog.uuid_extract_version(tenant_id) = 7),
    assignment_epoch cell_control.assignment_epoch,
    target_kind TEXT CHECK (target_kind IN ('platform', 'cell')),
    target_cell_id TEXT,
    correlation_id UUID NOT NULL CHECK (pg_catalog.uuid_extract_version(correlation_id) = 7),
    causation_id UUID CHECK (
        causation_id IS NULL OR (
            pg_catalog.uuid_extract_version(causation_id) = 7 AND causation_id <> message_id
        )
    ),
    content_type TEXT NOT NULL CHECK (
        content_type = 'application/vnd.edtech.message+json;version=1'
    ),
    envelope BYTEA NOT NULL CHECK (pg_catalog.octet_length(envelope) BETWEEN 2 AND 262144),
    created_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now(),
    CHECK (
        (scope_kind = 'cell' AND tenant_id IS NULL AND assignment_epoch IS NULL)
        OR (scope_kind = 'tenant' AND tenant_id IS NOT NULL AND assignment_epoch IS NOT NULL)
    ),
    CHECK (source_cell_id = scope_cell_id),
    CHECK (
        (message_kind = 'command' AND target_kind IS NOT NULL)
        OR (message_kind = 'event' AND target_kind IS NULL AND target_cell_id IS NULL)
    ),
    CHECK (
        target_kind IS NULL
        OR (target_kind = 'platform' AND target_cell_id IS NULL)
        OR (target_kind = 'cell' AND target_cell_id = scope_cell_id)
    ),
    CHECK (
        pg_catalog.octet_length(source_cell_id) BETWEEN 3 AND 63
        AND source_cell_id ~ '^[a-z0-9]+(?:-[a-z0-9]+)*$'
    ),
    CHECK (
        pg_catalog.octet_length(scope_cell_id) BETWEEN 3 AND 63
        AND scope_cell_id ~ '^[a-z0-9]+(?:-[a-z0-9]+)*$'
    ),
    CHECK (target_cell_id IS NULL OR (
        pg_catalog.octet_length(target_cell_id) BETWEEN 3 AND 63
        AND target_cell_id ~ '^[a-z0-9]+(?:-[a-z0-9]+)*$'
    ))
);
ALTER TABLE cell_messaging.outbox_message OWNER TO edtech_cell_migrator;

CREATE TABLE cell_messaging.outbox_delivery (
    message_id UUID PRIMARY KEY REFERENCES cell_messaging.outbox_message(message_id)
        ON DELETE NO ACTION,
    available_at TIMESTAMPTZ NOT NULL,
    attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    publisher_instance_id UUID,
    lease_id UUID,
    leased_until TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    last_failure_category TEXT CHECK (
        last_failure_category IS NULL OR (
            pg_catalog.octet_length(last_failure_category) BETWEEN 3 AND 96
            AND last_failure_category ~ '^[a-z0-9]+(?:[-.][a-z0-9]+)*$'
            AND last_failure_category !~ '--'
        )
    ),
    CHECK (
        (publisher_instance_id IS NULL AND lease_id IS NULL AND leased_until IS NULL)
        OR (publisher_instance_id IS NOT NULL AND lease_id IS NOT NULL AND leased_until IS NOT NULL
            AND pg_catalog.uuid_extract_version(publisher_instance_id) = 7
            AND pg_catalog.uuid_extract_version(lease_id) = 7)
    ),
    CHECK (published_at IS NULL OR (
        publisher_instance_id IS NULL AND lease_id IS NULL AND leased_until IS NULL
    ))
);
ALTER TABLE cell_messaging.outbox_delivery OWNER TO edtech_cell_migrator;
CREATE INDEX cell_outbox_eligible_idx
    ON cell_messaging.outbox_delivery (available_at, message_id)
    WHERE published_at IS NULL;

CREATE TABLE cell_messaging.inbox_receipt (
    consumer_name TEXT NOT NULL CHECK (
        pg_catalog.octet_length(consumer_name) BETWEEN 3 AND 96
        AND consumer_name ~ '^[a-z0-9]+(?:[-.][a-z0-9]+)*$'
        AND consumer_name !~ '--'
        AND consumer_name !~ '\.\.'
    ),
    message_id UUID NOT NULL CHECK (pg_catalog.uuid_extract_version(message_id) = 7),
    message_name TEXT NOT NULL CHECK (pg_catalog.octet_length(message_name) BETWEEN 10 AND 160),
    message_schema_version INTEGER NOT NULL CHECK (message_schema_version BETWEEN 1 AND 65535),
    message_kind TEXT NOT NULL CHECK (message_kind IN ('command', 'event')),
    envelope BYTEA NOT NULL CHECK (pg_catalog.octet_length(envelope) BETWEEN 2 AND 262144),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now(),
    PRIMARY KEY (consumer_name, message_id)
);
ALTER TABLE cell_messaging.inbox_receipt OWNER TO edtech_cell_migrator;
CREATE INDEX cell_inbox_processed_idx ON cell_messaging.inbox_receipt (processed_at);

GRANT USAGE ON SCHEMA cell_messaging TO edtech_cell_api, edtech_cell_worker;
GRANT INSERT, SELECT ON cell_messaging.outbox_message TO edtech_cell_api, edtech_cell_worker;
GRANT INSERT, SELECT ON cell_messaging.outbox_delivery TO edtech_cell_api;
GRANT INSERT, SELECT, UPDATE ON cell_messaging.outbox_delivery TO edtech_cell_worker;
GRANT INSERT, SELECT ON cell_messaging.inbox_receipt TO edtech_cell_worker;
REVOKE ALL ON ALL TABLES IN SCHEMA cell_messaging FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA cell_messaging FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA cell_messaging FROM PUBLIC;

ALTER DEFAULT PRIVILEGES FOR ROLE edtech_cell_migrator IN SCHEMA cell_messaging
    REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE edtech_cell_migrator IN SCHEMA cell_messaging
    REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE edtech_cell_migrator IN SCHEMA cell_messaging
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

UPDATE cell_control.schema_contract
SET contract_version = 2, updated_at = pg_catalog.now()
WHERE singleton AND contract_name = 'cell' AND contract_version = 1;
