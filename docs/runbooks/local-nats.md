# Local NATS JetStream runbook

## Prerequisites and startup

Install the pinned Rust toolchain, Docker with Compose, and OpenSSL. Diagnose without printing
Docker environment variables, secret contents, rendered configuration, or private-key paths:

```console
cargo xtask doctor
cargo xtask doctor-nats
cargo xtask nats-up
cargo xtask provision-nats-local
```

`nats-up` verifies the exact `nats:2.14.3-alpine3.22` multi-platform index lock, generates a local
CA/per-node certificates and purpose-specific credentials, renders three configs with restrictive
permissions, allocates loopback ports, starts the cluster, and waits for bounded health. Generated
state lives under `target/local-nats/edtech-nats-local`; do not copy or commit it.

Provisioning is safe to repeat. Missing declared assets and approved capacity increases are
applied; refused drift exits non-zero. Unknown `EDTECH_` assets are reported but retained.

## Readiness and qualification

With matching PostgreSQL authorities and generated worker credential references, workers expose:

```console
cargo run --locked -p platform-worker -- --check-transport
cargo run --locked -p platform-worker -- --check-runtime
cargo run --locked -p cell-worker -- --check-transport
cargo run --locked -p cell-worker -- --check-runtime
```

Use the canonical disposable workflows for automated checks:

```console
cargo xtask verify-nats --profile ci
cargo xtask verify-integration --profile ci
cargo xtask qualify-nats --profile full --output docs/evidence/checkpoint-04 --replace
```

The qualification workflows allocate unique projects/ports, generate their own secrets and TLS,
run actual workers and faults, then remove all generated resources even on failure. Full evidence
may be replaced only with `--replace`; inspect it for aggregate-only content before committing.

## Safe inspection

Prefer the provisioner inspection mode with its generated environment:

```console
cargo run --locked -p nats-provisioner -- --check-transport
```

Topology inspection is limited to stream/consumer names, fixed configuration, leaders, replica
currency, and aggregate pending counts. Pending counts must be read from all four durable consumer
infos; stream message count alone is not a substitute for per-consumer progress. Never print
credentials, payloads, envelopes, individual TenantIds/MessageIds, rendered server configuration,
or certificate private-key paths.

Do not manually purge streams, delete individual messages, reset/delete consumers, recreate a
durable, or edit generated configuration. Those operations can erase unprocessed work or progress
and are deliberately absent from the provisioner.

## Restarts and recovery

For a one-node maintenance simulation, identify a non-leader through safe aggregate inspection,
stop only that Compose service, restart it, then wait for both stream leaders and every replica to
report current. Restart nodes one at a time. A full all-container restart must retain named volumes;
afterward verify stream state and durable progress before resuming workers. Do not remove volumes as
a restart technique.

If quorum is lost, publisher acknowledgments should fail or time out and outbox rows must remain
unpublished/rescheduled. Restore one node, wait for JetStream usability, then the final node and full
R3 currency. Investigate authentication, authorization, version, TLS, invariant, or integrity
failures instead of retrying them to green.

## Cleanup

```console
cargo xtask nats-down
```

This removes the Compose containers, network, named volumes, credentials, certificates, and
rendered configs. Confirm no `edtech-nats-local` containers/volumes remain and the generated state
directory is absent or empty. Cleanup destroys local broker data and is appropriate only for the
disposable local project; it cannot be recovered after volume removal.
