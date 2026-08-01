# Checkpoint 1: Workspace foundation

## Objective

Create a reproducible, compiling, tested Rust monorepo foundation with explicit authority-oriented
crate boundaries, typed startup configuration, supervised process shutdown, machine-enforced
dependency rules, canonical commands, and CI that needs no external service.

## Included scope

- Rust 1.97.1/edition 2024/resolver 3 pinning, exact direct dependencies, Cargo.lock, and central
  lints.
- Six composition roots: `platform-api`, `platform-worker`, `tenant-router`, `cell-api`,
  `cell-worker`, and `db-migrator`.
- `tenancy-domain`, `provisioning-domain`, `auth-context`, and `audit-domain` foundational
  primitives.
- Separate Platform, Cell, and routing application crate boundaries.
- Typed `dev`/`npr`/`prd` runtime configuration and migration-scope validation.
- Root cancellation, named task supervision, failure propagation, Unix SIGINT/SIGTERM handling,
  and bounded draining.
- Deterministic fixture support, architecture rules/checks, doctor/smoke/verify workflows, pinned
  CI, and foundation documentation.

## Explicit non-goals

There is no PostgreSQL or SQLx access, schema, migration, tenancy-isolation choice, outbox/inbox,
broker, HTTP/OpenAPI/AsyncAPI contract, JWT/OIDC provider, cache, Terraform, Kubernetes, Docker,
cloud SDK, tenant provisioning, Cell registration, routing snapshot, CRUD, or EdTech product
behavior. No fake implementation stands in for any of these later concerns.

## Deliverables

The repository structure, package manifests, source, rules, tests, documentation, and CI workflow
are the deliverables. `architecture/dependency-rules.json` is authoritative for permitted workspace
and direct external dependency edges. `tools/xtask` is authoritative for repository workflows.

## Verification

Run each command from the repository root:

```console
cargo xtask doctor
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo xtask verify-architecture
cargo xtask smoke
cargo xtask verify
```

## Evidence and claim limitations

Passing this checkpoint proves only repository/toolchain reproducibility, static dependency
boundaries, typed startup configuration, process cancellation and supervision, and compile/test/CI
readiness without external services.

It does not prove tenant database isolation, Platform/Cell credential isolation at runtime, event
delivery, outbox/inbox correctness, provisioning recovery, routing correctness, tenant serving
during Platform outage, provider portability, high availability, disaster recovery, security
certification, or production readiness.

## Prerequisites for Checkpoint 2

Checkpoint 2 can rely on the pinned toolchain, inward dependency rules, topology-neutral identifiers,
typed process/environment selection, migration authority scope, and canonical verification flow. It
must perform the PostgreSQL tenancy-isolation spike before choosing schema-per-tenant versus shared
tables with `tenant_id`/RLS, and must preserve the Platform/Cell credential and query boundaries.
