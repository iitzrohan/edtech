# EdTech PostgreSQL and JetStream foundation

This repository contains the Checkpoint 4 Rust/PostgreSQL message foundation for a future
multi-tenant, event-driven EdTech platform. It preserves the earlier authority, tenancy, contract,
outbox, and inbox invariants and adds NATS JetStream as the selected inter-authority transport.

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

Message and transport dependencies also point inward:

```text
application/domain -> message-domain -> message-codec-json -> exact bytes -> authority outbox
composition root -> authority message runtime -> opaque nats-jetstream provider
nats-provisioner -> nats-jetstream-admin -> pre-created topology
```

Commands request intent and target exactly one authority. Events record immutable facts committed
by their source and have no target. Canonical envelope version 1 carries UUIDv7 identity,
descriptor, microsecond UTC time, source, scope, target, correlation, causation, and a typed object
payload. Tenant scope includes `TenantId`, `CellId`, and decimal-text assignment epoch.

Each authority stores exact immutable bytes beside mutable delivery state. Worker claims use
database time, `SKIP LOCKED`, and lease fencing. A published marker means only future transport
acceptance, not consumption. A named inbox receipt suppresses exact duplicate delivery inside one
database handler transaction; it does not prove exactly-once business processing.

The local transport is a three-node, TLS-authenticated JetStream cluster. Commands use the
`EDTECH_COMMANDS_V1` WorkQueue stream; events use the `EDTECH_EVENTS_V1` Limits stream. Subjects are
fixed by direction and kind:

```text
edtech.v1.command.platform-to-cell.<cell-id>.<message-name-suffix>
edtech.v1.command.cell-to-platform.<cell-id>.<message-name-suffix>
edtech.v1.event.platform-to-cell.<cell-id>.<message-name-suffix>
edtech.v1.event.cell-to-platform.<cell-id>.<message-name-suffix>
```

Tenant identity is deliberately absent from subjects. It remains inside the canonical envelope,
together with `CellId` and `AssignmentEpoch`, and is checked by the destination database authority.
Checkpoint 4 provisions only `cell-001` consumers.

`platform-worker` publishes Platform outbox rows and consumes Platform command/event durables.
`cell-worker` does the equivalent for its configured Cell. A publisher marks a row only after a
validated JetStream acknowledgment. A consumer commits its inbox receipt and any derived message
in one database transaction before requesting a JetStream double acknowledgment. Crashes can
therefore cause republishing or redelivery. JetStream's two-minute duplicate window reduces one
class of duplicate publication; the database inbox is the durable duplicate fence after that
window. Published does not mean consumed, and no global exactly-once claim is made.

## Local commands

The repository pins Rust 1.97.1, SQLx 0.9.0, and PostgreSQL 18.4.

```console
cargo xtask doctor
cargo xtask verify
cargo xtask verify-contracts
cargo xtask doctor-postgres
cargo xtask doctor-nats
cargo xtask postgres-up
cargo xtask migrate-local
cargo xtask nats-up
cargo xtask provision-nats-local
cargo xtask verify-postgres --profile ci
cargo xtask verify-message-store --profile ci
cargo xtask verify-nats --profile ci
cargo xtask verify-integration --profile ci
cargo xtask qualify-nats --profile full --output docs/evidence/checkpoint-04 --replace
cargo xtask qualify-message-store --profile full --output docs/evidence/checkpoint-03 --replace
cargo xtask qualify-tenancy --profile full --output docs/evidence/checkpoint-02 --replace
cargo xtask nats-down
cargo xtask postgres-down
cargo xtask verify-all
```

`postgres-up` generates disposable passwords and URL secret files beneath
`target/local-postgres/edtech-local/`, binds both services only to loopback, and prints only safe
ports and file references. `postgres-down` removes the containers, volumes, and generated files.
Automated verification uses unique project names, available loopback ports, and unconditional
cleanup.

`nats-up` generates a local CA, per-node certificates, six least-privilege credential files, and
rendered server configurations beneath `target/local-nats/edtech-nats-local/`. Client and monitor
ports are loopback-only; route ports are not published. `provision-nats-local` applies only approved
non-destructive topology changes, and `nats-down` removes containers, volumes, credentials,
certificates, and rendered configuration.

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

Workers additionally require `transport.servers`, an opaque `transport.credential_ref`, TLS mode,
and a CA file for `verify_full`. They support `--check-transport` and `--check-runtime` for bounded
transport-only and combined readiness checks. APIs, `tenant-router`, and `db-migrator` reject all
transport fields and receive no NATS credential. `nats-provisioner` receives no database field or
credential.

## Current claims and limitations

Checkpoint 4 proves only the bounded behavior in
`docs/checkpoints/04-nats-jetstream-transport.md` and the committed full-profile evidence. The real
qualification uses one Docker host, two PostgreSQL authorities, three NATS nodes, actual worker
processes, R3 streams/consumers, ACL negatives, deliberate crash windows, leader/quorum faults,
restarts, and final database/broker reconciliation. Existing physical authority, role, migration,
RLS, contract, and message-store proofs remain green; Checkpoint 4 adds no migration.

It does not establish multi-host or multi-zone availability, production sizing or retention,
backup/restore, disaster recovery, dynamic Cell registration, tenant provisioning or movement,
dead-letter handling, global or per-tenant ordering, business-operation idempotency, identity,
routing, product correctness, provider portability, or production readiness. All local NATS nodes
still share one Docker host, and only static `cell-001` topology is supported.
