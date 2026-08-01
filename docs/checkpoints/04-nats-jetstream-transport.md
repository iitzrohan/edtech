# Checkpoint 4: NATS JetStream transport

## Objective and included scope

Checkpoint 4 turns the Checkpoint 3 transactional stores into a real cross-authority message path.
It includes a pinned three-node TLS JetStream cluster, least-privilege credentials and ACLs, strict
four-route subject derivation, exact topology manifest/provisioner, Platform and Cell publisher and
consumer runtimes, worker composition and readiness checks, operational probe contracts/fixtures,
schema-v4 architecture enforcement, Docker-independent unit/contract/smoke checks, real-cluster CI
and full profiles, fault injection, aggregate evidence, runbooks, and CI integration.

No database migration was added. The four immutable Checkpoint 2/3 migrations and every prior RLS,
authority, contract, outbox, inbox, and qualification invariant remain required.

## Packages added

- `runtime-identity`: fallible system and deterministic UUIDv7/time/entropy sources.
- `transport-probe-contracts`: four strict operational probe payloads/descriptors.
- `nats-jetstream`: opaque runtime transport, routes, headers, acknowledgments, durable binding.
- `nats-jetstream-admin`: exact topology parsing, planning, non-destructive application/readiness.
- `platform-message-runtime` and `cell-message-runtime`: authority-owned outbox and durable loops.
- `nats-provisioner`: one-shot administrative composition root with no database.
- `nats-qualification`: real worker/cluster/security/fault/reconciliation and evidence runner.

## Topology and runtime

`EDTECH_COMMANDS_V1` uses WorkQueue retention; `EDTECH_EVENTS_V1` uses Limits retention. Both are
file-backed R3 streams with DiscardNew and fixed limits. Two Platform and two static `cell-001`
durable pull consumers use explicit acknowledgment and R3 file state.

Each worker owns exactly three supervised tasks: one outbox publisher, one command consumer, and one
event consumer. Publisher acceptance precedes the database marker. Inbox/handler commit precedes
double acknowledgment. Cancellation, retry, delayed NAK, lease, fetch, handler, shutdown, and drain
work are bounded. Runtime workers bind existing topology and have no administrative capability.

## Verification and evidence

Service-free `cargo xtask verify` runs doctor, six canonical fixtures, formatting, check, strict
Clippy, all workspace tests, architecture verification, and eight configuration smoke cases. It
does not require Docker.

`cargo xtask verify-integration --profile ci` uses one disposable PostgreSQL pair and one NATS
cluster. It runs inherited tenancy/message-store qualification, process database/transport/runtime
checks, provisioning, real worker workflows, security and ACL negatives, crash redelivery,
duplicate publication, follower/leader/quorum faults, restarts, reconciliation, and unconditional
cleanup. The full profile increases the exact workload/fault bounds and records stable aggregate
JSON/Markdown in `docs/evidence/checkpoint-04`.

Supported claims are limited to the recorded versions, one-host three-node topology, static
`cell-001`, exact probe contracts, tested profiles, actual worker binaries, and the observed fault
windows. The profile supports at-least-once transport with bounded broker duplicate suppression and
database-local inbox suppression. It makes no global exactly-once, production readiness,
multi-zone, infinite-outage, ordering, or business-idempotency claim.

## Explicit non-goals and Checkpoint 5 prerequisites

This checkpoint does not implement organization/tenant Platform tables, a Cell registry, dynamic
Cell registration/consumer creation, tenant provisioning or movement, placement reconciliation,
route snapshots, router subscriptions, HTTP APIs, identity/OIDC, Redis, Kubernetes, managed NATS,
schema registry, poison quarantine, dead-letter handling, replay tooling, production observability,
or deployment.

Checkpoint 5 must preserve the fixed route and authority boundaries while adding an authoritative
Cell/tenant lifecycle. Before dynamic consumer provisioning, it needs a reviewed Cell registry,
idempotent topology-change workflow, rollback/reconciliation policy, and qualification for Cell
addition/removal without granting workers topology mutation.
