# Local PostgreSQL runbook

## Prerequisites

Use the pinned Rust toolchain and a reachable Docker daemon with Compose. Verify them without
printing Docker environment variables:

```console
cargo xtask doctor-postgres
```

The command checks the repository infrastructure and exact pinned PostgreSQL image. Local bootstrap
credentials are disposable qualification authority; they are not production credentials and must
never be copied into deployment configuration.

## Manual Platform and Cell pair

```console
cargo xtask postgres-up
cargo xtask migrate-local
```

The first command starts `edtech-local`, binds Platform and Cell only to loopback, waits for bounded
health, and prints safe ports plus `file:` reference paths. It does not print passwords or URLs.
The second command requires both services healthy, then invokes `db-migrator` once for Platform and
once for `cell-001` without bootstrap credentials.

Use the printed role-specific file reference with `--check-database`; never copy the resolved file
contents into a command, log, issue, or document.

Stop and remove all manual data and generated credentials:

```console
cargo xtask postgres-down
```

The command is idempotent and removes both named volumes. Local data is not recoverable after this
cleanup.

## Automated verification

```console
cargo xtask verify-postgres --profile ci
cargo xtask verify-postgres --profile full
```

Each invocation uses a unique Compose project, available loopback ports, fresh random credentials,
and unconditional teardown. CI uses only the 32-tenant profile.

Generate the explicit full checkpoint evidence with:

```console
cargo xtask qualify-tenancy --profile full --output docs/evidence/checkpoint-02 --replace
```

Omit `--replace` unless replacing evidence is intentional. The full profile uses exactly 500
tenants and may take materially longer than CI.

## Safe troubleshooting

- A doctor failure should be resolved by starting Docker or restoring the pinned Compose/bootstrap
  files; PostgreSQL checks are never silently skipped.
- Startup health is bounded. Inspect only the named service state and recent database logs; do not
  print generated files or Docker environment variables.
- A migration/provider failure reports a sanitized category. Use the failing stage, role purpose,
  authority, and container logs—not the connection file contents—to diagnose it.
- Run `cargo xtask postgres-down` before changing manual ports or restarting with fresh credentials.
- Automated failures execute teardown before returning non-zero. A remaining directory under
  `target/local-postgres` should be treated as sensitive and removed through the matching lifecycle
  command, not copied or committed.
