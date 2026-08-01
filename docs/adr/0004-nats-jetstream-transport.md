# ADR 0004: NATS JetStream transport

- Status: accepted
- Date: 2026-08-01

## Context

Checkpoint 3 established typed canonical messages and physically separate transactional outbox and
inbox stores, but intentionally had no network transport. Checkpoint 4 needs bounded asynchronous
delivery between Platform and Cell authorities while retaining PostgreSQL as business authority,
keeping provider SDKs out of domain/application APIs, and making local failure behavior executable.

## Decision

Use NATS JetStream 2.14.3 through `async-nats` 0.50.0. Local and CI qualification use three
TLS/authenticated nodes, R3 file-backed streams, R3 durable pull consumers, unique placement tags,
subject ACLs, and separately generated purpose-specific credentials.

Commands use one WorkQueue stream and non-overlapping authority filters. Events use one Limits
stream. Four fixed subject families carry kind, direction, Cell scope, and contract suffix. TenantId
is absent from subjects; tenant/Cell/assignment authority remains in the canonical envelope and
destination database transaction.

Workers claim database outboxes, publish the exact stored bytes, require `Nats-Msg-Id`, expected
stream and exact content type, validate the publish acknowledgment, then mark the leased row. They
bind pre-created durable pull consumers, validate subject/headers/envelope, commit the inbox receipt
and derived row, and only then request a double acknowledgment. Crashes leave either an expired
outbox lease or a redeliverable broker message. The broker duplicate window and database inbox have
different scopes; no global exactly-once claim is supported.

Only `nats-provisioner` imports the administrative provider. Its manifest declares two streams,
Platform consumers, and static `cell-001` consumers. It creates missing assets and applies approved
monotonic capacity increases, refuses destructive drift, and preserves unknown assets. Runtime
workers cannot mutate topology. There is no maximum-delivery or dead-letter policy yet.

## Alternatives considered

- Kafka offers a mature partitioned log and broad ecosystem, but its operational and partition
  model is more machinery than this architecture's current fixed cross-authority routes need.
- RabbitMQ provides strong queue/routing primitives, but selecting it now would require a different
  operational and client qualification surface without a present requirement that outweighs the
  JetStream fit.
- Cloud Pub/Sub products can reduce broker operation, but would couple the checkpoint to a cloud
  control plane, credentials, emulator differences, and provider-specific delivery semantics.
- Core NATS without JetStream lacks the durable replicated acceptance and durable pull progress
  required by the outbox/inbox failure model.
- One-node JetStream is simpler but cannot exercise leader election, quorum loss, replicated state,
  or R3 readiness.
- Push consumers move flow control and delivery endpoints into workers; bounded pull consumers
  align better with explicit concurrency, cancellation, and local database handler capacity.
- Ephemeral consumers cannot preserve progress across worker restarts.
- Broker-only deduplication expires and cannot protect database-derived effects indefinitely.
- Direct database-to-database calls would couple authorities and bypass the committed local outbox
  boundary.
- HTTP callbacks require destination availability at send time and introduce endpoint retry and
  authentication semantics not needed for this checkpoint.
- Combining transport with dynamic Cell/tenant provisioning would mix topology authority and
  lifecycle work before a Cell registry exists.

These alternatives are not universally inferior; JetStream is selected for this architecture's
current scale, fixed routes, Rust client support, pull consumption, and executable R3 fault model.

## Consequences and limitations

The repository gains a broker, TLS/certificate generation, credential/ACL management, a
non-destructive provisioner, three supervised tasks per worker, and Docker-based integration cost.
It also gains real replicated acceptance, durable pull progress, explicit commit-before-ack
semantics, and bounded fault evidence.

The local cluster remains one-host evidence, not multi-zone availability. Static `cell-001`
topology, fixed production bounds, username/password credentials, no poison-message quarantine, no
global/per-tenant ordering claim, and no production sizing or operations claim remain limitations.

Revisit this decision when dynamic Cell topology exists; multi-region or much higher throughput is
required; ordering/partition semantics materially change; managed-cloud constraints dominate;
JWT/NKEY operation becomes necessary; or measured production scale exceeds qualified JetStream
bounds.
