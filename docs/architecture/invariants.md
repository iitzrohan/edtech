# Architecture invariants

These rules apply to this foundation and every later checkpoint.

1. Platform processes must never receive Cell database credentials.
2. Platform processes must never connect to or query Cell databases.
3. Cell processes must never connect to or query the Platform database.
4. Active tenant requests must not synchronously call the Platform or query the Platform database.
5. A Cell must eventually be able to serve active tenants while the Platform is unavailable.
6. A tenant is assigned to exactly one active logical Cell at a time.
7. Tenant placement is versioned by a monotonically increasing assignment epoch.
8. `cell_id` is a stable logical identifier. It must not encode a Kubernetes cluster, namespace,
   region, hostname, IP address, database address, or any other deployment coordinate.
9. Environment and provider selection are runtime configuration. Cargo features and separate
   source branches must not select them.
10. The same compiled artifact must be promotable through `dev`, `npr`, and `prd`.
11. Domain and application code must be independent of Axum, Tower, SQLx, NATS, Kafka, Pub/Sub,
    Redis, Valkey, Kubernetes, cloud SDKs, identity-provider SDKs, and telemetry SDKs.
12. Dependencies point inward: composition roots to runtime/adapters to application use cases and
    application-owned ports to domain.
13. An application crate must not depend on another application crate.
14. Runtime application processes must not perform DDL. The separately privileged, one-shot
    `db-migrator` is the only deployable migration caller.
15. Cross-authority workflows use commands, committed events, transactional outbox/inbox,
    idempotency, fencing, and reconciliation. They never use a distributed database transaction.
16. Event delivery must be treated as at-least-once, unordered, and potentially duplicated.
17. Broker ordering, deduplication, dead-letter queues, and transactions must never become business
    authority.
18. Cache data is disposable derived data and must never become correctness or authorization
    authority.
19. Every production tenant table belongs in `tenant_data` and must pass the forced-RLS schema
    inspector.
20. No EdTech product feature is part of Checkpoint 2.
21. Checkpoint 2 must not be represented as proof of production readiness, cloud network
    isolation, messaging correctness, provider portability, high availability, disaster recovery,
    or protection against a completely compromised Cell runtime.

Dependency and source boundaries are enforced by `cargo xtask verify-architecture`. Local database
authority, migration, privilege, and tenant-isolation behavior is exercised by
`cargo xtask verify-postgres`; distributed-systems and production behavior require later evidence.
