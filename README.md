# EdTech message and PostgreSQL foundation

This repository contains the Checkpoint 3 Rust/PostgreSQL message foundation for a future
multi-tenant, event-driven EdTech platform. It preserves Checkpoints 1 and 2 while adding a
transport-neutral contract, transactional outboxes, and database-local inbox receipts.

## Platform and Cell authority

Each `dev`, `npr`, or `prd` environment has one Platform control plane and one or more logical
Cells. The Platform owns future organization, tenant-lifecycle, placement, routing-intent, and
provisioning authority. A Cell owns its local tenant projection, serving authorization, tenant
application data, jobs, readiness, and availability.

Platform and Cell use physically separate PostgreSQL clusters locally. Platform processes receive
only Platform credentials; Cell processes receive only credentials for their configured Cell.
`tenant-router` receives no database credential. Migrations run only through the one-shot
`db-migrator` with a distinct authority-specific credential.

The selected tenant-storage baseline is shared tables with a `tenant_id` and forced PostgreSQL row
level security. Tenant identity and the complete non-zero `u64` assignment epoch are applied with
transaction-local settings, then checked against Cell-local tenant authority. Every production
tenant table belongs in `tenant_data` and is inspected by the real-PostgreSQL schema linter.

RLS protects against missing predicates and many accidental access errors. It does not protect
against a completely compromised Cell runtime, because that runtime necessarily has the capability
to serve multiple tenants. A future dedicated-Cell tier can provide a stronger isolation boundary.

## Dependency direction

Dependencies point inward:

```text
composition roots -> secret/database adapters -> application boundaries -> domain
```

Runtime binaries do not import SQLx or migration crates. Platform binaries cannot import Cell
adapters, Cell binaries cannot import Platform adapters, and migration SQL exists only in the two
migration crates. The permitted edges and source constraints are machine-enforced by
`cargo xtask verify-architecture`.

Message dependencies also point inward:

```text
application/domain -> message-domain -> message-codec-json -> exact bytes -> authority outbox
```

Commands request intent and target exactly one authority. Events record immutable facts committed
by their source and have no target. Canonical envelope version 1 carries UUIDv7 identity,
descriptor, microsecond UTC time, source, scope, target, correlation, causation, and a typed object
payload. Tenant scope includes `TenantId`, `CellId`, and decimal-text assignment epoch.

Each authority stores exact immutable bytes beside mutable delivery state. Worker claims use
database time, `SKIP LOCKED`, and lease fencing. A published marker means only future transport
acceptance, not consumption. A named inbox receipt suppresses exact duplicate delivery inside one
database handler transaction; it does not prove exactly-once business processing.

No broker, network delivery, publisher loop, or consumer loop exists yet.

## Local commands

The repository pins Rust 1.97.1, SQLx 0.9.0, and PostgreSQL 18.4.

```console
cargo xtask doctor
cargo xtask verify
cargo xtask verify-contracts
cargo xtask doctor-postgres
cargo xtask postgres-up
cargo xtask migrate-local
cargo xtask verify-postgres --profile ci
cargo xtask verify-message-store --profile ci
cargo xtask qualify-message-store --profile full --output docs/evidence/checkpoint-03 --replace
cargo xtask qualify-tenancy --profile full --output docs/evidence/checkpoint-02 --replace
cargo xtask postgres-down
cargo xtask verify-all
```

`postgres-up` generates disposable passwords and URL secret files beneath
`target/local-postgres/edtech-local/`, binds both services only to loopback, and prints only safe
ports and file references. `postgres-down` removes the containers, volumes, and generated files.
Automated verification uses unique project names, available loopback ports, and unconditional
cleanup.

## Runtime configuration

Database-enabled processes require an opaque absolute file reference; ordinary configuration never
accepts a plaintext password or URL. For example:

```console
EDTECH__ENVIRONMENT=dev \
EDTECH__DATABASE__TLS_MODE=disable \
EDTECH__DATABASE__CREDENTIAL_REF=file:/absolute/path/to/platform-api-url \
cargo run --locked -p platform-api -- --check-database
```

Cell processes additionally require `EDTECH__CELL_ID=cell-001`. `npr` and `prd` require
`verify_full` TLS. Unknown, raw-password, unexpected migration, and router database fields fail
closed. `--check-config` does not resolve a secret or connect; `--check-database` resolves once and
performs bounded server, role, authority-marker, and schema-contract checks.

## Current claims and limitations

Checkpoint 3 proves only the tested behavior recorded in
`docs/checkpoints/03-message-contract-and-transactional-store.md` and its qualification evidence.
That includes canonical typed envelopes, exact-byte persistence, transactional outbox/inbox
behavior, lease fencing, assignment checks, canary atomicity, and direct broker-free transfer under
the tested profiles. Existing physical authority, role, migration, RLS, and schema-inspection
proofs remain green.

It does not establish cloud network isolation, Kubernetes secret delivery, production hardening,
availability, backup/restore, disaster recovery, network or broker delivery, publisher/consumer
behavior, global ordering, business-operation idempotency, retention/replay, provisioning, routing,
identity, product correctness, provider portability, production capacity, or production readiness.
