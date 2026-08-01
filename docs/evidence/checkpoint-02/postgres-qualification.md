# Checkpoint 02 PostgreSQL qualification

Profile: `full`. All 82 correctness checks passed.

The selected baseline is shared tenant tables with `tenant_id` and forced RLS. The schema-per-tenant candidate remains qualification-only.

## Qualified versions

- PostgreSQL server version number: `180004`
- PostgreSQL image: `postgres:18.4-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296`
- Rust: `rustc 1.97.1 (8bab26f4f 2026-07-14)`
- SQLx: `0.9.0`
- Host OS/architecture: `macos/aarch64`
- Available parallelism: `10`

## Profile parameters

- Tenants: 500
- Logical tables: 20
- Secondary indexes per table: 2
- Rows per tenant: 50
- Alternating switches: 10000
- Concurrency: 32

## Shared forced RLS

| Measurement | Value |
|---|---:|
| Clean candidate creation | 2 ms |
| Initial schema migration | 11 ms |
| Incremental migration | 9 ms |
| Tenant provisioning | 270 ms |
| Schemas | 1 |
| Tables | 20 |
| Indexes | 61 |
| Relevant pg_class rows | 81 |
| Relevant pg_attribute rows | 243 |
| Database size | 15316671 bytes |
| Insert throughput | 27827 rows/s |
| Read throughput | 934 rows/s |
| Tenant switch p50 | 53404 us |
| Tenant switch p95 | 58042 us |
| Tenant switch p99 | 67128 us |
| Prepared-query alternation | true |
| Concurrent isolation | true |
| Probe export | 61303 us |
| Probe import | 18473 us |
| Cleanup | 7 ms |

## Schema per tenant (qualification only)

| Measurement | Value |
|---|---:|
| Clean candidate creation | 1 ms |
| Initial schema migration | 5637 ms |
| Incremental migration | 1150 ms |
| Tenant provisioning | 761 ms |
| Schemas | 500 |
| Tables | 10000 |
| Indexes | 30500 |
| Relevant pg_class rows | 40500 |
| Relevant pg_attribute rows | 80500 |
| Database size | 486815423 bytes |
| Insert throughput | 27186 rows/s |
| Read throughput | 43784 rows/s |
| Tenant switch p50 | 1143 us |
| Tenant switch p95 | 1649 us |
| Tenant switch p99 | 2728 us |
| Prepared-query alternation | true |
| Concurrent isolation | true |
| Probe export | 2317 us |
| Probe import | 15846 us |
| Cleanup | 2968 ms |

## Limitations

Wall-clock measurements are machine-dependent evidence and are not pass thresholds. These local measurements do not establish production capacity, availability, hardening, backup, recovery, network isolation, or protection against a completely compromised Cell runtime.
