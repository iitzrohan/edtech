# ADR 0003: Message contract and transactional store

## Context

Future Platform/Cell workflows cross physically separate PostgreSQL authorities, where no local
transaction can include both databases or a transport. Messages must tolerate duplicate, delayed,
and unordered handling while preserving Cell assignment fencing and payload privacy. Checkpoint 3
must establish durable mechanics without selecting a broker.

## Decision

Use provider-neutral `message-domain` metadata, canonical typed JSON envelope version 1, and exact
immutable envelope bytes. Store messages in fixed `platform_messaging` or `cell_messaging` schemas
owned by the matching migrator. A local state change and outbox insert share one transaction. A
named inbox receipt, handler effects, and derived outbox insert share one consumer transaction.

Outbox delivery state is mutable separately from immutable message bytes. Workers claim with
database time, `SKIP LOCKED`, bounded leases, and UUIDv7 lease IDs. Publication and rescheduling
require the active unexpired lease. An inbox identity conflict is not a duplicate.

Assignment epoch remains a PostgreSQL `NUMERIC(20,0)` and JSON decimal string, preserving the full
non-zero `u64` range without signed conversion. Message schemas remain cross-tenant operational
state outside `tenant_data`; they neither use nor disable RLS.

Schema contract 2 adds message-store capability. New providers accept contract 1 or 2 but expose
the capability only at 2. Old Checkpoint 2 binaries accept only contract 1 and must leave service
before a database moves to 2. The expand-first order is new binary, migration 0002, restart/readiness
observation, then later transport activation.

## Alternatives considered

- Publishing after commit without an outbox loses messages during the crash window.
- Publishing before commit can expose a fact that later rolls back.
- Broker transactions as authority cannot atomically prove the authority database state.
- Broker-only deduplication cannot prove a named handler's local commit.
- Event sourcing would change the source-of-truth model beyond this checkpoint.
- Storing only decoded JSONB would lose the exact immutable bytes accepted by future transports.
- No exactly-once global claim is supported because independent authority transactions leave
  expected duplicate windows.
- Selecting and operating a broker in this checkpoint would combine contract/storage proof with an
  unqualified transport decision.

## Consequences and limitations

Exact bytes consume bounded storage and remain immutable. Delivery and receipt retention is not
implemented. Attempt counts have no maximum and there is no dead-letter state. A published marker
means future transport acceptance only. Platform tenant-placement validation awaits Platform
control-plane tables. No exactly-once delivery claim is supported.

Revisit the JSON envelope only for demonstrated interoperability or performance constraints;
revisit fixed schemas if authority storage changes; revisit lease bounds with measured production
workloads. Transport selection, publisher/consumer loops, acknowledgments, retries, and broker
qualification belong to Checkpoint 4.
