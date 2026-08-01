# Checkpoint 4 NATS qualification

- Result: passed
- Profile: full
- NATS: 2.14.3 (`sha256:c11af972c99ae542de8925e6a7d9c533aa1eb039660420d2074beed6089b3bf0`)
- Cluster: three local TLS nodes, two R3 streams, four R3 durable pull consumers
- Platform outbox: 20000 expected / 20000 actual / 20000 published
- Cell outbox: 20000 expected / 20000 actual / 20000 published
- Inbox receipts: 40000 expected / 40000 actual
- Lost expected effects: 0
- Derived duplicate effects: 0
- Active database lease overlap: 0

This evidence covers only the bounded local profile recorded in the adjacent JSON file. It does not prove exactly-once behavior globally or production readiness.
