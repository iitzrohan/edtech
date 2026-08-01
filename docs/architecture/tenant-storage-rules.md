# Tenant storage rules

The production baseline is shared Cell tables with a `tenant_id` column and forced PostgreSQL row
level security. The schema-per-tenant implementation is qualification-only and must not appear in
runtime configuration, Cell adapters, or deployable binaries.

## Required table contract

Every future tenant-owned table belongs in `tenant_data` and is rejected unless all of these hold:

1. `tenant_id` exists, has PostgreSQL type UUID, is `NOT NULL`, and contains UUIDv7 values.
2. RLS is enabled and forced.
3. At least one policy applies to the runtime roles and contains both `USING` and `WITH CHECK`.
4. The policy authorizes only the transaction tenant whose enabled Cell-local authority record has
   the exact current assignment epoch.
5. The owner is the Cell migrator, never a runtime role, and runtime roles cannot reach the owner
   through `SET ROLE`.
6. PUBLIC has no table privilege and no policy.
7. Every primary/unique index includes `tenant_id`.
8. Every foreign key from a tenant table includes `tenant_id` in its referencing columns; every
   foreign key targeting a tenant table includes it in the referenced columns.
9. Runtime roles have no CREATE on `tenant_data` and no capability to ALTER or DROP its tables.

`cargo xtask verify-postgres` inspects the real PostgreSQL catalogs for these conditions, reports
the exact unsafe schema/table/index/constraint, proves synthetic violations are rejected, and then
checks the selected schema.

## Tenant context and fencing

`TenantExecutionScope` is complete only with a `TenantId`, logical `CellId`, and non-zero
`AssignmentEpoch`. Cell assignment epochs are stored as constrained `NUMERIC(20,0)`, covering the
complete range 1 through 18446744073709551615 without a signed conversion.

Each Cell tenant operation:

1. rejects a scope for another logical Cell before data access;
2. begins a transaction;
3. applies tenant ID, assignment epoch, and `row_security=on` with transaction-local
   `set_config` calls;
4. checks `cell_control.tenant_authority` for presence, enabled serving, and exact epoch;
5. performs only a narrowly named adapter operation;
6. explicitly commits success or rolls back failure.

Missing, malformed, disabled, absent, stale, or newer-unregistered context fails closed. Context is
tested across commit, rollback, a one-connection pool, 1,000 alternations, prepared-query reuse, and
at least 32 concurrent tenants. The retained `tenant_data.isolation_canary` is an operational
isolation surface, not product data.

## Threat model and future tier

Forced RLS protects against omitted predicates, absent context, connection-state leakage, and many
accidental cross-tenant queries. It does not stop a fully compromised Cell runtime from choosing
another active tenant context, because that runtime legitimately serves many tenants.

A future dedicated-Cell isolation tier may place one customer in its own Cell authority when risk,
regulation, scale, or operations justify a stronger boundary. That decision does not change
`TenantId`, `CellId`, or assignment-epoch semantics.
