# Checkpoint 3: Message contract and transactional store

## Objective and included scope

Checkpoint 3 establishes provider-neutral message identities, canonical typed JSON envelope
version 1, immutable fixtures, fixed Platform/Cell PostgreSQL outboxes and inbox receipts, fenced
claim/reschedule/publication mechanics, Cell tenant validation, direct broker-free transfer
simulation, static architecture enforcement, and deterministic qualification evidence.

It adds `message-domain`, `message-codec-json`, `postgres-message-store`, and the non-deployable
`message-store-qualification` tool. Migrations `0002_platform_message_store.sql` and
`0002_cell_message_store.sql` advance their authority contract to version 2 only after successful
DDL and grants.

## Explicit non-goals

There is no broker, transport adapter, subject/topic configuration, publisher loop, consumer loop,
transport acknowledgment, transport retry, dead-letter queue, retention/pruning, schema registry,
HTTP API, identity integration, Platform control-plane CRUD, provisioning, routing projection, or
product feature.

## Verification and evidence

```console
cargo xtask verify-contracts
cargo xtask verify-message-store --profile ci
cargo xtask verify-postgres --profile ci
cargo xtask qualify-message-store --profile full --output docs/evidence/checkpoint-03 --replace
cargo xtask verify-all
```

Committed full-profile evidence lives in `docs/evidence/checkpoint-03`. CI uses the exact smaller
profile and the existing two PostgreSQL authorities; it starts no broker.

## Claims supported

Checkpoint 3 supports only tested claims for canonical envelope version 1; typed JSON encoding and
decoding; exact byte persistence; transactional Platform/Cell outboxes; fenced concurrent claims;
lease expiry and stale-fence protection; database-local inbox duplicate suppression; rollback;
Cell assignment-epoch validation; canary-state-plus-outbox atomicity under forced RLS; direct
broker-free transfer; tested PostgreSQL 18.4 and SQLx 0.9.0 profiles; and schema ownership/grants.

## Claims not supported

It does not prove network delivery, broker availability/durability/acknowledgments, subject/topic
correctness, publisher or consumer runtime behavior, cross-region delivery, global ordering,
business-operation idempotency, retention/replay, production backlog capacity, Platform placement
validation, provisioning, Cell registration, routing correctness, Platform-outage serving,
identity/authorization policy, product correctness, cloud isolation, or production readiness. It
does not prove exactly-once processing.

Checkpoint 4 prerequisites are the immutable v1 contract, contract-2 stores, clean qualification
evidence, and an explicit transport ADR and qualification plan. This checkpoint does not begin it.
