# Contributing

Use the pinned toolchain from `rust-toolchain.toml`. Before editing, run:

```console
cargo xtask doctor
```

Before handing off a change, run the canonical full check:

```console
cargo xtask verify
```

Database, migration, role, policy, or tenant-table changes also require:

```console
cargo xtask verify-postgres --profile ci
```

## Where code belongs

- Domain invariants and provider-neutral value types belong in the narrowly named domain crate that
  owns them.
- Use cases and their provider-neutral ports belong in the matching Platform, Cell, or routing
  application crate.
- Runtime implementation belongs in a concrete adapter or composition-support crate introduced by
  the checkpoint that needs it.
- Process wiring belongs in its binary composition root.
- Deterministic fixtures belong in `test-support` and may be consumed only as dev-dependencies.
- Repository automation belongs in `tools/xtask`, not a shell script.

Never take a cross-layer shortcut. Domain/application public APIs cannot expose framework, provider,
configuration, telemetry, or `anyhow` types, and application crates cannot depend on one another.

## Introducing a crate

1. Choose a bounded responsibility and an ownership-revealing name. Generic names such as
   `common`, `shared`, `core`, `utils`, `helpers`, and `misc` are forbidden.
2. Add the package as a workspace member and root path dependency with an exact workspace version.
3. Inherit all workspace package metadata, dependencies, and lints; set no independent version.
4. Add crate-level documentation stating both responsibility and forbidden responsibilities.
5. Add only dependencies used by the checkpoint and inherit every one from the root table.
6. Add each permitted workspace/external edge and kind to
   `architecture/dependency-rules.json`.
7. Run `cargo xtask verify`.

## Dependency and provider changes

An external dependency needs an exact `=version` root pin, minimum required features with defaults
disabled where feasible, a member-level `workspace = true` declaration, and an explicit rule entry.
Git and out-of-workspace path dependencies are prohibited.

Any new provider dependency must be documented with its owning checkpoint, concrete boundary,
failure semantics, and verification. Provider types must stay behind application-owned ports and
must not leak into domain or application public APIs. Update the dependency rules in the same change;
do not weaken a layer rule merely to make a new edge pass.

Tests should protect durable behavior or material failure risk at the lowest useful level. Extend the
closest existing test when it already owns the contract, and do not add tests whose only purpose is
to prove removed implementation remains absent.

## PostgreSQL migrations and tenant tables

- Platform SQL belongs only in `crates/platform-migrations/migrations`; Cell SQL belongs only in
  `crates/cell-migrations/migrations`.
- Add a monotonically numbered forward `.sql` file. Never edit a migration after it has been
  applied or merged, and do not add a down migration in this checkpoint.
- Runtime adapters and API/worker binaries must contain no DDL or migration invocation. Only
  `db-migrator` and the non-deployable qualification tool may import migration crates.
- Fully qualify every application object. Do not create application objects in `public`.
- Update a schema-contract version only as an intentional compatibility decision, with matching
  adapter support and qualification evidence.
- Every tenant-owned product table belongs in `tenant_data` and must have a non-null UUIDv7
  `tenant_id`, forced RLS, an applicable `USING` and `WITH CHECK` policy, and no PUBLIC privilege.
  Every primary/unique key and every inbound/outbound tenant foreign key must include `tenant_id`.
  A runtime role must not own the table or have schema CREATE/DDL capability.
- Run `cargo xtask verify-postgres --profile ci` after every database change; the catalog inspector
  rejects unsafe tables, indexes, constraints, policies, owners, and privileges.
