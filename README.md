# EdTech platform foundation

This repository is the Checkpoint 1 Rust foundation for a future multi-tenant, event-driven EdTech
platform. It compiles and tests without a database, broker, identity provider, cache, container
runtime, or cloud credentials.

## Platform and Cell model

Each isolated `dev`, `npr`, or `prd` environment will contain one Platform control plane and one or
more logical Cells. The Platform owns organizations, tenant lifecycle, the logical Cell registry,
tenant placement, provisioning operations, routing intent, and provider administration. A Cell
owns its tenant projection, tenant-serving authorization and data, local jobs, readiness, and
serving availability. A Cell contains many tenants, while a tenant has exactly one active logical
Cell assignment at a time.

Platform and Cell are separate database authorities. Platform processes must never receive Cell
database credentials or query Cell databases; Cell processes must never query the Platform
database. Active tenant requests therefore cannot synchronously call the Platform. A Cell must
eventually be able to serve already-active tenants during a Platform outage, using Cell-authority
state delivered asynchronously in later checkpoints.

`cell_id` is a stable logical name such as `cell-001`. It is deliberately topology-neutral: it
must not encode a cluster, namespace, region, host, address, or provider coordinate. Deployment
topology can change without changing identity or invalidating tenant assignment history.

## Dependency direction

Dependencies point inward:

```text
composition roots -> runtime/adapters -> application use cases and owned ports -> domain
```

Domain and application crates contain no runtime framework or provider types. Application crates
never depend on one another. The permitted workspace edges and direct external dependencies are
machine-readable in `architecture/dependency-rules.json` and enforced by
`cargo xtask verify-architecture`.

## Local commands

The repository pins Rust 1.97.1, rustfmt, and Clippy through `rust-toolchain.toml`.

```console
cargo xtask doctor
cargo xtask verify-architecture
cargo xtask smoke
cargo xtask verify
```

The full `verify` command runs doctor, formatting, locked workspace check, Clippy with all warnings
denied, locked tests, architecture verification, and configuration smoke checks in deterministic
order.

## Startup configuration

Environment selection is required runtime data; it is not a Cargo feature and has no implicit
default:

```console
EDTECH__ENVIRONMENT=dev cargo run --locked -p platform-api -- --check-config
EDTECH__ENVIRONMENT=npr EDTECH__CELL_ID=cell-001 \
  cargo run --locked -p cell-api -- --check-config
EDTECH__ENVIRONMENT=prd EDTECH__MIGRATION_SCOPE=platform \
  cargo run --locked -p db-migrator -- --check-config
EDTECH__ENVIRONMENT=prd EDTECH__MIGRATION_SCOPE=cell EDTECH__CELL_ID=cell-001 \
  cargo run --locked -p db-migrator -- --check-config
```

An optional TOML file can provide lower-precedence non-secret settings:

```toml
log_filter = "info"
shutdown_grace_ms = 30000
```

Select it with `EDTECH_CONFIG_FILE=/path/to/runtime.toml`. Variables beginning with `EDTECH__`
override the file and use double underscores as the nesting separator. Unknown keys fail. Ordinary
configuration does not accept plaintext secrets; future integrations must use validated,
debug-redacted secret references.

## Current limitations

Checkpoint 1 proves repository/toolchain reproducibility, static dependency boundaries, typed
startup configuration, process cancellation and supervision, and compile/test/CI readiness without
external services. It does not prove tenant database isolation, runtime Platform/Cell credential
isolation, event delivery, outbox/inbox correctness, provisioning recovery, routing correctness,
tenant serving during a Platform outage, provider portability, high availability, disaster
recovery, security certification, or production readiness.

There is no PostgreSQL, SQLx, migration, HTTP API, broker adapter, identity integration, cache,
Terraform, Kubernetes, container definition, or EdTech product feature here. In particular, this
checkpoint does not choose schema-per-tenant versus shared tables with `tenant_id` and RLS.
