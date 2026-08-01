# Local NATS JetStream infrastructure

This directory commits only the reproducible Compose definition, the verified image-index lock,
and credential-free templates. `cargo xtask nats-up` generates a CA, per-node TLS identities,
random route/account passwords, strict credential JSON files, and rendered server configuration
beneath `target/local-nats/<project>/`.

The three nodes share one local Docker host, so this environment proves the checked R3 behavior and
fault windows only; it is not evidence of multi-zone availability. Username/password accounts are
the locally qualified authentication mechanism, not a final enterprise secret-distribution design.

No stream is provisioned by `nats-up`. Run `cargo xtask provision-nats-local --project <project>`
after the cluster is healthy. Remove everything with `cargo xtask nats-down --project <project>`.
