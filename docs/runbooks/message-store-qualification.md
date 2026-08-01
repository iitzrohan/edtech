# Message-store qualification runbook

## Prerequisites

Use Rust 1.97.1 and a working Docker/Compose engine with the pinned PostgreSQL 18.4 image. The
commands generate credential files under `target/local-postgres`, pass only file references to
tools, and clean containers, networks, volumes, and credentials on success or failure.

## Service-free contracts

```console
cargo xtask verify-contracts
```

This checks domain/codec tests, the JSON Schema document, exact fixture round trips and file LF,
descriptor uniqueness, timestamp/epoch representation, size bounds, redaction, credential markers,
and architecture documentation wording.

## Disposable PostgreSQL profiles

```console
cargo xtask verify-message-store --profile ci
cargo xtask verify-postgres --profile ci
```

The first qualifies message-store behavior plus prerequisite authority/RLS behavior. The second is
the canonical combined CI database command. Neither retries failures to green.

Regenerate committed full evidence intentionally:

```console
cargo xtask qualify-message-store \
  --profile full \
  --output docs/evidence/checkpoint-03 \
  --replace
```

Without `--replace`, existing evidence is preserved.

## Safe troubleshooting and inspection

Errors and evidence expose categories and aggregate counts only. Never print `BYTEA` envelope
columns, payload JSON, credentials, URLs, or per-message/per-tenant identifiers. In a disposable or
approved operational session, inspect only aggregate delivery state:

```sql
SELECT
  count(*) FILTER (WHERE published_at IS NULL AND lease_id IS NULL) AS pending,
  count(*) FILTER (WHERE published_at IS NULL AND lease_id IS NOT NULL) AS leased,
  count(*) FILTER (WHERE published_at IS NOT NULL) AS published
FROM platform_messaging.outbox_delivery;
```

Use the equivalent fixed `cell_messaging` query for a Cell authority. Do not manually update or
delete immutable outbox messages or inbox receipts. Do not disable `row_security`.

If a command fails, confirm `docker compose ls` has no `edtech-pg-*` project and inspect
`target/local-postgres` for an unexpected leftover directory without opening credential files.
Use `cargo xtask postgres-down --project <name>` only for a deliberately persistent manual project.
