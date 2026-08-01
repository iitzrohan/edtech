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

Message contract changes require `cargo xtask verify-contracts`. Message-store changes require
both that command and `cargo xtask verify-postgres --profile ci`.
Transport contracts, subjects, providers, topology, worker message loops, or ACL changes also
require `cargo xtask verify-nats --profile ci`. Regenerate committed Checkpoint 4 evidence with the
full profile when a change alters qualified transport behavior or recorded metrics.

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

## Message contracts and stores

- Add a narrowly owned typed payload struct with unknown fields denied by default. Do not expose
  provider SDK types or `serde_json::Value` in a domain/application API.
- Use a validated lowercase `edtech.` name with at least four dot segments. Do not put a version in
  the name; use `MessageSchemaVersion`.
- An incompatible semantic change gets a new schema version and new fixture. Never edit a released
  fixture, message name/version meaning, or envelope version in place.
- Keep secrets, credentials, tokens, authorization headers, database URLs, large binaries, and
  private object contents out of metadata and payloads. Never log payload or envelope bytes.
- Outbox message bytes and inbox receipts are immutable. Runtime roles do not update or delete them.
- Application code must not publish directly to a broker. Runtime publication belongs only behind
  the qualified `nats-jetstream` provider.
- Documentation must not claim exactly-once delivery or processing. Describe at-least-once behavior,
  duplicate windows, database inbox suppression, and business idempotency separately.
- Run `cargo xtask verify-contracts` after any contract, fixture, codec, or contract-documentation
  change.

## JetStream transport and topology

- Application subjects are derived only by `nats-jetstream` from a validated envelope route. Do
  not accept arbitrary subjects in configuration, interpolate a tenant or assignment epoch into a
  subject, or publish through Core NATS.
- The four route families are fixed by command/event kind and Platform-to-Cell/Cell-to-Platform
  direction. The subject Cell token is topology scope; tenant authority remains in the envelope.
- Workers bind only the pre-created durable consumers they own. They must not call
  `get_or_create_stream`, `get_or_create_consumer`, or any stream/consumer mutation API.
- Only `nats-provisioner` imports `nats-jetstream-admin`. It may create missing declared assets and
  apply an explicitly safe monotonic capacity increase. It must refuse destructive drift and must
  report unknown `EDTECH_` assets without deleting them.
- To declare another static Cell, add its validated ID to
  `infra/local/nats/templates/topology.toml`, review the derived non-overlapping command/event
  filters and least-privilege ACLs, update qualification expectations, then run the CI transport
  profile. Dynamic Cell registration is not present.
- Never log an envelope, payload, individual tenant/message identifier, credential, private-key
  path, or raw provider error. Persist and report stable content-free categories.
- Documentation must describe transport acceptance, database publication marking, delivery,
  inbox commit, and business completion as distinct states. It must not claim exactly-once behavior
  or imply that broker acceptance proves consumption.
- New or changed operational contracts require a strict typed payload, unique descriptor, canonical
  LF-terminated fixture, byte-for-byte round-trip coverage, and `cargo xtask verify-contracts`.
- Regenerate stable evidence only with
  `cargo xtask qualify-nats --profile full --output docs/evidence/checkpoint-04 --replace`. Inspect
  the result for aggregate-only content, confirm all generated resources were removed, and commit
  both JSON and Markdown evidence with the behavior change.
