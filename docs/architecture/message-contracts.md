# Message contracts

Checkpoint 3 defines a transport-neutral message contract. `message-domain` owns identities and
validated metadata without Serde; `message-codec-json` alone owns canonical JSON envelope version
1. Application and domain crates do not depend on the codec or the PostgreSQL message store.

## Envelope metadata

Every envelope carries these fields in canonical order: envelope version, message ID, message
kind, message name, payload schema version, emitted timestamp, source, scope, target, correlation
ID, causation ID, and typed payload. Envelope version describes the outer wire grammar. Message
schema version describes the named payload contract. They evolve independently.

Message and correlation identities are caller-supplied UUIDv7 values. `MessageId` is transport
deduplication identity, not a business-operation idempotency key. A correlation ID groups a
workflow. A causation ID names only the direct predecessor and cannot equal the current message ID.

Names use `edtech.<bounded-context>.<aggregate-or-purpose>.<fact-or-intent>` with at least four
lowercase dot-separated segments. Hyphens are allowed within segments; underscores, slashes,
uppercase, empty segments, consecutive hyphens, and final version suffixes are forbidden. The
schema version is the separate integer from 1 through 65,535.

## Source, scope, and target

A source is Platform or one logical Cell. Platform may emit Platform-, Cell-, or tenant-scoped
messages. A Cell source must use its matching Cell or tenant scope. Tenant scope always includes
UUIDv7 `TenantId`, logical `CellId`, and the complete non-zero `AssignmentEpoch` fence.

A command requests intent and targets exactly one Platform or Cell authority. An event records an
immutable fact committed by its source and has a null target. Receiving a command does not prove
that its requested result occurred. An event must not be created before its source fact commits.

Assignment epoch is decimal JSON text so values above `i64::MAX`, including `u64::MAX`, remain
exact. No message path converts it through `i64`.

## Canonical representation and privacy

`emitted_at` is caller-supplied, UTC-normalized, truncated to microseconds, formatted with exactly
six fractional digits, and terminated by `Z`. Pre-Unix-epoch and years above 9999 fail closed.
The root payload is a typed JSON object. The maximum encoded envelope is 262,144 bytes.

Metadata contains only bounded identity, routing, version, time, causation, correlation, and scope
information. Secrets, credentials, tokens, authorization headers, database URLs, large binaries,
and object contents do not belong in metadata. Payload and complete envelope bytes are never
logged, included in errors, or emitted in qualification evidence. Debug output reports byte length
and `[REDACTED]`.

## Evolution and fixtures

Released message names, versions, and fixtures are immutable. An incompatible semantic change gets
a new payload schema version; an incompatible outer shape gets a new envelope version. Typed
consumers declare one exact descriptor and fail closed on another name, kind, or version. There is
no automatic upcast, downcast, reflection registry, or untyped public forwarding API.

The canonical command and event fixtures in `docs/contracts/fixtures` are qualification-only
contracts. Future bounded contexts own their payload structs and fixtures in narrowly named
contract packages. The final file LF is packaging; bytes before it are the exact canonical
envelope.
