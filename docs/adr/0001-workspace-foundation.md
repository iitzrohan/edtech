# ADR 0001: Workspace foundation

- Status: Accepted
- Date: 2026-08-01
- Checkpoint: 1

## Context

The platform needs durable authority boundaries before provider adapters or product capabilities
exist. An early monolith or generic shared crate would make Platform and Cell dependencies easy to
mix and difficult to remove later.

## Decision

Adopt one Rust workspace containing six binary composition roots, four focused domain crates,
three separate application boundaries, two composition-support crates, deterministic test support,
and `xtask`. The accepted directories and packages are the ones listed in the Checkpoint 1 record.

Pin Rust 1.97.1, edition 2024, resolver 3, workspace MSRV 1.97, rustfmt, Clippy, Cargo.lock, and exact
root workspace dependency requirements. Members inherit package metadata, dependencies, and lints.
Unsafe code is forbidden. Normal verification uses `--locked`.

Dependencies point inward from composition roots through runtime/adapters and application-owned
ports to domains. Application crates do not depend on each other. Explicit permitted edges and
direct external dependencies live in `architecture/dependency-rules.json`; `xtask` checks Cargo
metadata, manifests, and relevant source boundaries.

Environment selection is required runtime configuration because one compiled artifact must move
unchanged through `dev`, `npr`, and `prd`. Cargo features describe compilation capabilities, not a
deployment environment, and would create environment-specific artifacts.

Generic crates named `common`, `shared`, `core`, `utils`, `helpers`, or `misc` are prohibited. Such
names hide ownership and encourage cross-authority coupling; new code must have a bounded domain,
application, adapter, or composition responsibility.

Provider integrations are deferred until their owning checkpoints can define concrete ports,
failure semantics, and verification. Empty PostgreSQL, messaging, HTTP, cloud, identity, and cache
adapter crates would falsely imply decisions and create unused boundaries.

This checkpoint does not choose schema-per-tenant or shared tables plus `tenant_id`/RLS. That choice
requires the PostgreSQL isolation spike in Checkpoint 2 and evidence about operational isolation,
pooling, migrations, and failure modes.

## Consequences

- Authority and dependency direction are visible before infrastructure code exists.
- All processes share one typed configuration schema and supervised shutdown mechanism.
- Direct dependency changes require an exact root pin and an explicit architecture-rule update.
- Some application crates are intentionally documentation-only until a real use case owns a port.
- Process binaries are intentionally repetitive composition roots; no generic runtime dumping
  ground is introduced to remove a small amount of wiring.
- CI and local verification need no external service.

Known limitations include the absence of database, transport, identity, routing, provisioning,
product, infrastructure, availability, recovery, and security evidence. The foundation is not a
production system.
