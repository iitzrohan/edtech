# Message envelope version 1

This document is the normative human-readable contract for content type
`application/vnd.edtech.message+json;version=1`. An encoded envelope is 2 through 262,144 bytes.
Errors expose categories and field names only.

## Exact logical structure

```json
{
  "envelope_version": 1,
  "message_id": "UUIDv7",
  "message_kind": "command",
  "message_name": "edtech.qualification.probe.requested",
  "message_schema_version": 1,
  "emitted_at": "2023-11-14T22:13:20.123456Z",
  "source": { "kind": "platform" },
  "scope": {
    "kind": "tenant",
    "cell_id": "cell-001",
    "tenant_id": "UUIDv7",
    "assignment_epoch": "18446744073709551615"
  },
  "target": { "kind": "cell", "cell_id": "cell-001" },
  "correlation_id": "UUIDv7",
  "causation_id": null,
  "payload": { "typed_field": "typed value" }
}
```

The encoder emits fields in that order with no insignificant whitespace. The timestamp is UTC with
exactly six fractional digits and `Z`. Envelope version and payload schema version are JSON numbers;
assignment epoch is decimal JSON text.

Platform source has only `kind`. Cell source requires `cell_id`. Platform scope has only `kind`;
Cell scope adds only `cell_id`; tenant scope adds `cell_id`, UUIDv7 `tenant_id`, and non-zero
assignment epoch. Cell source and Cell target must match a Cell/tenant scope. Commands require a
Platform or Cell target. Events require `target: null`.

All top-level fields, including nullable target and causation, are present exactly once. Unknown,
missing, or duplicate top-level fields fail closed. Invalid versions, UUID versions, names,
timestamps, Cell IDs, scope combinations, target combinations, and epochs fail closed. Input above
the byte bound fails before full parsing.

Payload is an explicitly typed JSON object. Scalar, string, number, null, and array roots fail.
Typed contracts normally use `deny_unknown_fields`; an unknown descriptor is not interpreted as an
older version. Public codec APIs never expose `serde_json::Value`.

Released fixtures are immutable. A new incompatible payload meaning requires a new message schema
version and fixture. A new incompatible outer representation requires a new envelope version and
content type version. There is no runtime registry or reflection service.
