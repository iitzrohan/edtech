# Checkpoint 2: PostgreSQL authority and tenancy

## Objective and included scope

Establish the PostgreSQL foundation without adding product or distributed-system behavior:

- two physically separate local PostgreSQL 18.4 authorities;
- separate bootstrap, migrator, API, and worker roles per authority;
- opaque file secret resolution and safe SQLx 0.9.0 provider mechanics;
- independently embedded Platform and Cell migrations;
- immutable authority markers and compatible schema contracts;
- lossless full-range non-zero `u64` assignment epochs;
- Cell-local authority fencing and a forced-RLS isolation canary;
- qualification-only schema-per-tenant comparison;
- catalog-driven tenant-table inspection;
- process database checks, architecture enforcement, CI, runbook, ADR, and evidence.

No HTTP/API contract, broker, outbox/inbox, identity integration, CRUD, provisioning, placement,
routing projection, cache, cloud infrastructure, high availability, backup/recovery, or EdTech
product entity is included.

## Deliverables and decision

The new packages are `secret-resolution`, `postgres-runtime`, `platform-postgres`, `cell-postgres`,
`platform-migrations`, `cell-migrations`, and the non-deployable `postgres-qualification` tool.
Compose/bootstrap infrastructure lives under `infra/local/postgres`; migration SQL is owned only by
the two migration crates.

The accepted production baseline is shared tenant tables with UUIDv7 `tenant_id`, forced RLS,
transaction-local context, exact assignment-epoch fencing, fully qualified SQL, and mandatory
schema inspection. Schema-per-tenant remains qualification-only.

## Verification and evidence

```console
cargo xtask doctor
cargo xtask verify
cargo xtask doctor-postgres
cargo xtask verify-postgres --profile ci
cargo xtask qualify-tenancy --profile full --output docs/evidence/checkpoint-02 --replace
cargo xtask verify-all
```

`cargo test` remains Docker-independent. PostgreSQL verification creates unique disposable
authorities, performs migrations and all real-database checks, runs every database-enabled binary
in check mode, proves router rejection, and removes containers, volumes, and credentials on success
or failure.

Generated full-profile measurements and correctness results are in
`docs/evidence/checkpoint-02/`. They contain no credential material, database URL, username, host
port, or container ID.

## Claims supported

For the pinned versions and local environment, this checkpoint supports claims about PostgreSQL
18.4 and SQLx 0.9.0 compatibility; physical Platform/Cell separation; authority-marker and
runtime/migration credential enforcement; separately owned idempotent/concurrent migrations;
forced-RLS behavior; transaction-local context; full-range assignment fencing; tested pooled and
prepared-query reuse; current tenant-table inspection; and transactional migration failure.

It does not support claims about cloud network isolation, Kubernetes secret delivery, production
hardening or availability, backup/restore, disaster recovery, protection after complete Cell
runtime compromise, event delivery, outbox/inbox, tenant provisioning, Cell registration, routing,
serving through a Platform outage, identity/authorization policy, product behavior, portability, or
production readiness.

## Checkpoint 3 prerequisites

Checkpoint 3 may rely on the two-authority contract, exact tenant scope/fence, selected storage
rules, one-shot migration process, secret/provider boundaries, safe readiness checks, and canonical
PostgreSQL CI profile. It must preserve these boundaries while adding only its explicitly owned
capabilities and evidence.
