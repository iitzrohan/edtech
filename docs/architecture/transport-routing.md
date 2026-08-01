# Transport routing

JetStream subjects express only contract kind, authority direction, destination/source Cell scope,
and the suffix of the validated message name. `nats-jetstream` derives the subject from canonical
envelope metadata and accepts no caller-supplied subject.

| Kind and direction | Pattern | Source | Target and scope |
|---|---|---|---|
| Platform-to-Cell command | `edtech.v1.command.platform-to-cell.<cell>.<suffix>` | Platform | target Cell equals tenant/cell scope Cell |
| Cell-to-Platform command | `edtech.v1.command.cell-to-platform.<cell>.<suffix>` | that Cell | Platform target; scope belongs to source Cell |
| Platform-to-Cell event | `edtech.v1.event.platform-to-cell.<cell>.<suffix>` | Platform | no target; scope identifies destination Cell |
| Cell-to-Platform event | `edtech.v1.event.cell-to-platform.<cell>.<suffix>` | that Cell | no target; scope belongs to source Cell |

For example, `edtech.transport.cell-probe.requested` becomes
`edtech.v1.command.platform-to-cell.cell-001.transport.cell-probe.requested`. The command stream
captures `edtech.v1.command.>`; the event stream captures `edtech.v1.event.>`.

TenantId never appears in a subject. Putting it there would multiply topology/ACL cardinality,
expose tenant identity in broker metadata, and risk treating transport routing as tenancy
authority. TenantId, CellId, and AssignmentEpoch stay in the signed-by-storage canonical envelope.
AssignmentEpoch is specifically a database authority fence: it remains full-range decimal text in
the envelope and is not interpolated into transport topology.

Inbound handling independently derives the expected subject from the decoded envelope and compares
it with the broker subject. It also verifies `Nats-Msg-Id`, expected stream, exact content type,
source, target, Cell scope, descriptor, and assignment state before committing. A wrong subject,
unsupported direction, Platform-scoped application message, command without one target, targeted
event, source/scope mismatch, invalid Cell, or oversized derived subject fails closed. Malformed or
unsupported deliveries receive a bounded delayed NAK and no inbox receipt.

Subjects are bounded ASCII strings. Runtime configuration contains server endpoints and operational
bounds only; it has no arbitrary subject, tenant subject, epoch subject, stream, or durable input.
