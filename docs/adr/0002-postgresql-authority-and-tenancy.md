# ADR 0002: PostgreSQL authority and tenant storage

- Status: Accepted
- Date: 2026-08-01
- Checkpoint: 2

## Context

Checkpoint 1 established Platform/Cell authority boundaries but deliberately deferred database and
tenant-storage decisions. Checkpoint 2 needed to prove PostgreSQL/SQLx compatibility, separate
runtime and migration authority, lossless assignment fencing, and tenant isolation under pooled
connection and prepared-statement reuse.

The threat model includes missing tenant predicates, missing/malformed/stale context, ordinary
runtime-role SQL mistakes, connection-state leakage, unsafe schema evolution, and use of a wrong
authority credential. It does not claim protection after complete compromise of a multi-tenant Cell
runtime.

## Candidates

The selected candidate uses shared tenant-owned tables, UUIDv7 `tenant_id`, forced RLS, and
transaction-local tenant/epoch context fenced by `cell_control.tenant_authority`.

The comparison candidate uses one opaque schema per tenant named from the 32 lowercase hexadecimal
UUID digits. It sets a transaction-local search path beginning with `pg_catalog`, excludes `public`
and `$user` from the effective path, uses generated/quoted identifiers only, and records per-tenant
migration state. It exists only in `postgres-qualification`.

PostgreSQL allows an ordinary login role to set its own future session defaults. Qualification
therefore contains its role-default probe in a rolled-back transaction and proves that the explicit
tenant transaction path remains `pg_catalog` plus the validated tenant schema. This PostgreSQL
behavior is additional operational complexity and is one reason the candidate is not a runtime
selection.

## Evidence

Both profiles use PostgreSQL 18.4 and SQLx 0.9.0. The CI profile uses 32 tenants, 6 logical tables,
2 secondary indexes per table, 10 primary-table rows per tenant, 500 measured switches, and
concurrency 8. The full checkpoint profile uses exactly 500 tenants, 20 logical tables, 2 secondary
indexes per table, 50 rows per tenant, 10,000 switches, and concurrency 32.

The qualification evidence is generated at:

- `docs/evidence/checkpoint-02/postgres-qualification.json`
- `docs/evidence/checkpoint-02/postgres-qualification.md`

Correctness is pass/fail; wall-clock timings are machine-dependent evidence and are not CI
thresholds. The suite covers all 35 forced-RLS cases, authority/privilege and migration cases, the
catalog inspector with unsafe fixtures, and schema-candidate behavior. It measures creation,
migration fan-out, provisioning, catalogs, database size, throughput, switching percentiles,
concurrency, probe movement, and cleanup.

The full profile passed all 82 correctness checks. Representative results were:

| Measurement | Shared forced RLS | Schema per tenant |
|---|---:|---:|
| Initial schema migration | 11 ms | 5,637 ms |
| Incremental migration | 9 ms | 1,150 ms |
| Schemas | 1 | 500 |
| Tables | 20 | 10,000 |
| Indexes | 61 | 30,500 |
| Relevant `pg_class` rows | 81 | 40,500 |
| Relevant `pg_attribute` rows | 243 | 80,500 |
| Database size | 15,316,671 bytes | 486,815,423 bytes |
| Tenant-switch p95 | 58,042 us | 1,649 us |

The timing values describe this one local machine and are not capacity claims. Catalog and
migration fan-out, correctness, ownership, and recovery complexity—not a single latency result—drive
the decision.

## Decision

Adopt shared tenant tables using `tenant_id` and forced PostgreSQL RLS as the production baseline.
Every tenant table must follow the rules in `docs/architecture/tenant-storage-rules.md`; fully
qualified production SQL, transaction-local context, assignment-epoch fencing, and the CI schema
inspector are mandatory.

The selected tenant-table contract requires:

- `tenant_id UUIDv7 NOT NULL`;
- every primary and unique key to include `tenant_id`;
- RLS enabled and forced;
- an applicable policy with both `USING` and `WITH CHECK` expressions;
- a non-runtime table owner and runtime roles without `BYPASSRLS`;
- no `PUBLIC` grants and no runtime schema-creation privilege;
- fully qualified application SQL;
- tenant context applied transaction-locally with `SET LOCAL` semantics;
- assignment-epoch authorization without conversion through signed 64-bit storage; and
- the catalog inspector in PostgreSQL CI for every current and future `tenant_data` table.

## Rejected initial alternative

Do not select schema-per-tenant initially. It creates table/index/catalog and migration fan-out,
increases per-tenant drift and interrupted-migration recovery work, complicates SQLx prepared-query
and `search_path` qualification, and adds operational work for hundreds of tenants. It also provides
no stronger protection from a fully compromised shared Cell runtime that can choose all tenant
schemas.

Reconsider the decision for a contractual schema-level restore requirement; regulation not met by
dedicated Cells; measured RLS planning or policy cost; tenant/table scale that invalidates the
model; a dedicated-database product tier; or evidence-backed changes in PostgreSQL or SQLx
behavior.

## Consequences and limits

Platform and Cell remain physically separate authorities with separate bootstrap, migrator, API,
and worker roles. Runtime roles own no application object and cannot migrate. Migration history is
separately owned and hidden. `tenant-router` has no database dependency or credential.

This decision proves only the local tested behavior. It does not prove cloud network isolation,
production hardening/availability/backup/recovery, event or provisioning behavior, routing,
identity policy, product correctness, provider portability, or production readiness.
