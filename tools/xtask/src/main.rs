//! Canonical, cross-platform repository workflows through Checkpoint 3.
//!
//! `xtask` performs toolchain diagnostics, static architecture enforcement, configuration smoke
//! checks, and the deterministic full verification sequence without external services.

mod nats;
mod postgres;

use std::{
    collections::{BTreeSet, HashSet},
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use serde::Deserialize;

use postgres::QualificationProfile;

const REQUIRED_RUST_VERSION: &str = "1.97.1";
const RULES_PATH: &str = "architecture/dependency-rules.json";

#[derive(Debug, Parser)]
#[command(name = "xtask", about = "Canonical repository workflows")]
struct Cli {
    #[command(subcommand)]
    command: RepositoryCommand,
}

#[derive(Debug, Subcommand)]
enum RepositoryCommand {
    /// Report whether the pinned local development prerequisites are present.
    Doctor,
    /// Enforce workspace dependency and source-boundary rules.
    VerifyArchitecture,
    /// Verify canonical message contracts and immutable fixtures without services.
    VerifyContracts,
    /// Validate every process composition root with synthetic configuration.
    Smoke,
    /// Run the complete deterministic repository verification sequence.
    Verify,
    /// Check Docker, Compose, the pinned image, and local `PostgreSQL` infrastructure.
    DoctorPostgres,
    /// Check Docker, Compose, OpenSSL, the locked image index, and local NATS templates.
    DoctorNats,
    /// Start a persistent manual three-node TLS NATS `JetStream` cluster.
    NatsUp {
        /// Safe Compose project name.
        #[arg(long, default_value = "edtech-nats-local")]
        project: String,
        /// Optional nats-1 client loopback port.
        #[arg(long)]
        nats_1_port: Option<u16>,
        /// Optional nats-2 client loopback port.
        #[arg(long)]
        nats_2_port: Option<u16>,
        /// Optional nats-3 client loopback port.
        #[arg(long)]
        nats_3_port: Option<u16>,
        /// Optional nats-1 monitor loopback port.
        #[arg(long)]
        monitor_1_port: Option<u16>,
        /// Optional nats-2 monitor loopback port.
        #[arg(long)]
        monitor_2_port: Option<u16>,
        /// Optional nats-3 monitor loopback port.
        #[arg(long)]
        monitor_3_port: Option<u16>,
    },
    /// Apply and verify topology in a healthy manual NATS cluster.
    ProvisionNatsLocal {
        /// Safe Compose project name.
        #[arg(long, default_value = "edtech-nats-local")]
        project: String,
    },
    /// Remove a manual NATS cluster, volumes, certificates, and credentials.
    NatsDown {
        /// Safe Compose project name.
        #[arg(long, default_value = "edtech-nats-local")]
        project: String,
    },
    /// Start a persistent manual Platform/Cell `PostgreSQL` pair.
    PostgresUp {
        /// Safe Compose project name.
        #[arg(long, default_value = "edtech-local")]
        project: String,
        /// Loopback host port for Platform `PostgreSQL`.
        #[arg(long)]
        platform_port: Option<u16>,
        /// Loopback host port for Cell `PostgreSQL`.
        #[arg(long)]
        cell_port: Option<u16>,
    },
    /// Stop a manual `PostgreSQL` pair and remove its volumes and credentials.
    PostgresDown {
        /// Safe Compose project name.
        #[arg(long, default_value = "edtech-local")]
        project: String,
    },
    /// Run Platform and Cell migrations against the healthy manual pair.
    MigrateLocal {
        /// Safe Compose project name.
        #[arg(long, default_value = "edtech-local")]
        project: String,
    },
    /// Qualify `PostgreSQL` correctness using disposable local authorities.
    VerifyPostgres {
        /// Deterministic qualification workload.
        #[arg(long, value_enum, default_value_t = QualificationProfile::Ci)]
        profile: QualificationProfile,
    },
    /// Qualify Checkpoint 3 message-store behavior in disposable `PostgreSQL` authorities.
    VerifyMessageStore {
        /// Deterministic qualification workload.
        #[arg(long, value_enum, default_value_t = QualificationProfile::Ci)]
        profile: QualificationProfile,
    },
    /// Run tenancy qualification and write stable, credential-free evidence.
    QualifyTenancy {
        /// Deterministic qualification workload.
        #[arg(long, value_enum)]
        profile: QualificationProfile,
        /// Directory for JSON and Markdown evidence.
        #[arg(long)]
        output: PathBuf,
        /// Permit replacement of existing qualification evidence.
        #[arg(long)]
        replace: bool,
    },
    /// Run message-store qualification and write stable, credential-free evidence.
    QualifyMessageStore {
        /// Deterministic qualification workload.
        #[arg(long, value_enum)]
        profile: QualificationProfile,
        /// Directory for JSON and Markdown evidence.
        #[arg(long)]
        output: PathBuf,
        /// Permit replacement of existing qualification evidence.
        #[arg(long)]
        replace: bool,
    },
    /// Qualify the complete Checkpoint 4 transport against disposable authorities.
    VerifyNats {
        /// Exact transport qualification workload.
        #[arg(long, value_enum, default_value_t = QualificationProfile::Ci)]
        profile: QualificationProfile,
    },
    /// Qualify Checkpoint 4 and write stable aggregate evidence.
    QualifyNats {
        /// Exact transport qualification workload.
        #[arg(long, value_enum)]
        profile: QualificationProfile,
        /// Directory for JSON and Markdown evidence.
        #[arg(long)]
        output: PathBuf,
        /// Permit replacement of existing transport evidence.
        #[arg(long)]
        replace: bool,
    },
    /// Run inherited `PostgreSQL` and Checkpoint 4 transport qualification in one environment.
    VerifyIntegration {
        /// Exact integration workload.
        #[arg(long, value_enum, default_value_t = QualificationProfile::Ci)]
        profile: QualificationProfile,
    },
    /// Run workspace verification followed by the combined CI integration profile.
    VerifyAll,
}

fn main() -> Result<()> {
    let root = workspace_root()?;
    match Cli::parse().command {
        RepositoryCommand::Doctor => doctor(&root),
        RepositoryCommand::VerifyArchitecture => verify_architecture(&root),
        RepositoryCommand::VerifyContracts => verify_contracts(&root),
        RepositoryCommand::Smoke => smoke(&root),
        RepositoryCommand::Verify => verify(&root),
        RepositoryCommand::DoctorPostgres => postgres::doctor(&root),
        RepositoryCommand::DoctorNats => nats::doctor(&root),
        RepositoryCommand::NatsUp {
            project,
            nats_1_port,
            nats_2_port,
            nats_3_port,
            monitor_1_port,
            monitor_2_port,
            monitor_3_port,
        } => {
            let client_ports = match (nats_1_port, nats_2_port, nats_3_port) {
                (None, None, None) => None,
                (Some(first), Some(second), Some(third)) => Some([first, second, third]),
                _ => bail!("all three NATS client port overrides must be supplied together"),
            };
            let monitor_ports = match (monitor_1_port, monitor_2_port, monitor_3_port) {
                (None, None, None) => None,
                (Some(first), Some(second), Some(third)) => Some([first, second, third]),
                _ => bail!("all three NATS monitor port overrides must be supplied together"),
            };
            nats::up(&root, &project, client_ports, monitor_ports)
        }
        RepositoryCommand::ProvisionNatsLocal { project } => nats::provision_local(&root, &project),
        RepositoryCommand::NatsDown { project } => nats::down(&root, &project),
        RepositoryCommand::PostgresUp {
            project,
            platform_port,
            cell_port,
        } => postgres::up(&root, &project, platform_port, cell_port),
        RepositoryCommand::PostgresDown { project } => postgres::down(&root, &project),
        RepositoryCommand::MigrateLocal { project } => postgres::migrate_local(&root, &project),
        RepositoryCommand::VerifyPostgres { profile } => postgres::verify(&root, profile),
        RepositoryCommand::VerifyMessageStore { profile } => {
            postgres::verify_message_store(&root, profile)
        }
        RepositoryCommand::QualifyTenancy {
            profile,
            output,
            replace,
        } => postgres::qualify(&root, profile, &output, replace),
        RepositoryCommand::QualifyMessageStore {
            profile,
            output,
            replace,
        } => postgres::qualify_message_store(&root, profile, &output, replace),
        RepositoryCommand::VerifyNats { profile }
        | RepositoryCommand::VerifyIntegration { profile } => {
            verify_integration(&root, profile, None, false)
        }
        RepositoryCommand::QualifyNats {
            profile,
            output,
            replace,
        } => verify_integration(&root, profile, Some(&output), replace),
        RepositoryCommand::VerifyAll => {
            verify(&root)?;
            verify_integration(&root, QualificationProfile::Ci, None, false)
        }
    }
}

fn verify_integration(
    root: &Path,
    profile: QualificationProfile,
    output: Option<&Path>,
    replace: bool,
) -> Result<()> {
    doctor(root)?;
    postgres::doctor(root)?;
    nats::doctor(root)?;
    let mut postgres_project = postgres::disposable(root)?;
    let mut nats_project = nats::disposable(root)?;
    let evidence_output = output.map_or_else(
        || {
            postgres_project
                .temporary_directory()
                .join("nats-qualification-evidence")
        },
        |path| {
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                root.join(path)
            }
        },
    );

    let operation = (|| {
        println!(
            "verify-integration: starting one disposable PostgreSQL/NATS environment (profile={})",
            profile.as_str()
        );
        postgres_project.start()?;
        nats_project.start()?;
        postgres::migrate(&postgres_project)?;
        postgres::verify_existing_prerequisites(&postgres_project, profile)?;
        nats::run_provisioner(&nats_project, None)?;
        nats::run_provisioner(&nats_project, Some("--check-transport"))?;
        run_nats_qualification(
            root,
            profile,
            &evidence_output,
            replace || output.is_none(),
            &postgres_project,
            &nats_project,
        )
    })();
    let nats_cleanup = nats_project.cleanup();
    let postgres_cleanup = postgres_project.cleanup();
    match (operation, nats_cleanup, postgres_cleanup) {
        (Ok(()), Ok(()), Ok(())) => {
            println!(
                "verify-integration: profile={} passed; workers and disposable authorities removed",
                profile.as_str()
            );
            Ok(())
        }
        (Err(error), Ok(()), Ok(()))
        | (Ok(()), Err(error), Ok(()))
        | (Ok(()), Ok(()), Err(error)) => Err(error),
        (operation, nats_cleanup, postgres_cleanup) => Err(anyhow!(
            "integration operation={}; NATS cleanup={}; PostgreSQL cleanup={}",
            result_label(&operation),
            result_label(&nats_cleanup),
            result_label(&postgres_cleanup)
        )),
    }
}

fn result_label<T>(result: &Result<T>) -> &'static str {
    if result.is_ok() { "ok" } else { "failed" }
}

fn run_nats_qualification(
    root: &Path,
    profile: QualificationProfile,
    output: &Path,
    replace: bool,
    postgres_project: &postgres::LocalProject<'_>,
    nats_project: &nats::NatsProject<'_>,
) -> Result<()> {
    run_checked(
        root,
        "cargo build --locked --package platform-worker --package cell-worker --package nats-qualification",
        "cargo",
        &[
            "build",
            "--locked",
            "--package",
            "platform-worker",
            "--package",
            "cell-worker",
            "--package",
            "nats-qualification",
        ],
    )?;
    let executable = root
        .join("target/debug")
        .join(format!("nats-qualification{}", env::consts::EXE_SUFFIX));
    let mut command = Command::new(executable);
    command
        .arg("--profile")
        .arg(profile.as_str())
        .arg("--output")
        .arg(output)
        .current_dir(root)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if replace {
        command.arg("--replace");
    }
    clear_edtech_environment(&mut command);
    for (name, authority, purpose) in [
        ("EDTECH_QUAL_PLATFORM_MIGRATOR_REF", "platform", "migrator"),
        ("EDTECH_QUAL_PLATFORM_API_REF", "platform", "api"),
        ("EDTECH_QUAL_PLATFORM_WORKER_REF", "platform", "worker"),
        ("EDTECH_QUAL_CELL_MIGRATOR_REF", "cell", "migrator"),
        ("EDTECH_QUAL_CELL_API_REF", "cell", "api"),
        ("EDTECH_QUAL_CELL_WORKER_REF", "cell", "worker"),
    ] {
        command.env(name, postgres_project.reference(authority, purpose)?);
    }
    command
        .env("EDTECH_QUAL_NATS_SERVERS", nats_project.server_list())
        .env("EDTECH_QUAL_NATS_CA_FILE", nats_project.ca_path())
        .env(
            "EDTECH_QUAL_NATS_PROVISIONER_REF",
            nats_project.credential_reference("provisioner"),
        )
        .env(
            "EDTECH_QUAL_NATS_PLATFORM_WORKER_REF",
            nats_project.credential_reference("platform-worker"),
        )
        .env(
            "EDTECH_QUAL_NATS_CELL_WORKER_REF",
            nats_project.credential_reference("cell-worker"),
        )
        .env(
            "EDTECH_QUAL_NATS_INJECTOR_REF",
            nats_project.credential_reference("qualification-injector"),
        )
        .env(
            "EDTECH_QUAL_NATS_INSPECTOR_REF",
            nats_project.credential_reference("qualification-inspector"),
        )
        .env(
            "EDTECH_QUAL_NATS_SYSTEM_REF",
            nats_project.credential_reference("system"),
        )
        .env("EDTECH_QUAL_NATS_PROJECT", nats_project.project_name())
        .env("EDTECH_QUAL_NATS_STATE_DIR", nats_project.state_directory())
        .env(
            "EDTECH_QUAL_NATS_MONITOR_PORTS",
            nats_project
                .monitor_ports()
                .iter()
                .map(u16::to_string)
                .collect::<Vec<_>>()
                .join(","),
        )
        .env(
            "EDTECH_QUAL_WORK_DIRECTORY",
            postgres_project.temporary_directory(),
        );
    let status = command
        .status()
        .context("could not start NATS qualification")?;
    if status.success() {
        Ok(())
    } else {
        bail!("NATS qualification failed with exit status {status}")
    }
}

fn clear_edtech_environment(command: &mut Command) {
    for (key, _) in env::vars_os()
        .filter(|(key, _)| key.to_str().is_some_and(|key| key.starts_with("EDTECH__")))
    {
        command.env_remove(key);
    }
    command.env_remove("EDTECH_CONFIG_FILE");
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_directory = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_directory
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("xtask is not located under tools/xtask"))
}

#[allow(clippy::too_many_lines)]
fn doctor(root: &Path) -> Result<()> {
    let rustc_version = capture(root, "rustc", &["--version"])?;
    if !rustc_version.starts_with(&format!("rustc {REQUIRED_RUST_VERSION} ")) {
        bail!("selected compiler is `{rustc_version}`; expected rustc {REQUIRED_RUST_VERSION}");
    }

    let cargo_version = capture(root, "cargo", &["--version"])?;
    let verbose_rustc = capture(root, "rustc", &["-vV"])?;
    let host = verbose_rustc
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or_else(|| anyhow!("rustc -vV did not report a host target"))?;
    let rustfmt_version = capture(root, "rustfmt", &["--version"])?;
    let clippy_version = capture(root, "cargo", &["clippy", "--version"])?;

    let required_paths = [
        ".cargo/config.toml",
        ".github/workflows/ci.yml",
        "architecture/dependency-rules.json",
        "apps/platform-api",
        "apps/platform-worker",
        "apps/tenant-router",
        "apps/cell-api",
        "apps/cell-worker",
        "apps/db-migrator",
        "apps/nats-provisioner",
        "crates/tenancy-domain",
        "crates/provisioning-domain",
        "crates/auth-context",
        "crates/audit-domain",
        "crates/platform-application",
        "crates/cell-application",
        "crates/routing-application",
        "crates/runtime-config",
        "crates/process-lifecycle",
        "crates/secret-resolution",
        "crates/postgres-runtime",
        "crates/platform-postgres",
        "crates/cell-postgres",
        "crates/platform-migrations",
        "crates/cell-migrations",
        "crates/test-support",
        "crates/message-domain",
        "crates/message-codec-json",
        "crates/postgres-message-store",
        "crates/runtime-identity",
        "crates/transport-probe-contracts",
        "crates/nats-jetstream",
        "crates/nats-jetstream-admin",
        "crates/platform-message-runtime",
        "crates/cell-message-runtime",
        "infra/local/postgres/compose.yml",
        "infra/local/postgres/platform/init/001-bootstrap.sh",
        "infra/local/postgres/cell/init/001-bootstrap.sh",
        "infra/local/nats/compose.yml",
        "infra/local/nats/nats-image.lock.toml",
        "infra/local/nats/templates/nats-server.conf.tmpl",
        "infra/local/nats/templates/topology.toml",
        "docs/adr/0001-workspace-foundation.md",
        "docs/adr/0002-postgresql-authority-and-tenancy.md",
        "docs/adr/0003-message-contract-and-transactional-store.md",
        "docs/adr/0004-nats-jetstream-transport.md",
        "docs/architecture/invariants.md",
        "docs/architecture/database-authorities.md",
        "docs/architecture/tenant-storage-rules.md",
        "docs/architecture/message-contracts.md",
        "docs/architecture/message-delivery-semantics.md",
        "docs/architecture/nats-topology.md",
        "docs/architecture/transport-routing.md",
        "docs/architecture/runtime-message-delivery.md",
        "docs/checkpoints/01-workspace-foundation.md",
        "docs/checkpoints/02-postgresql-authority-and-tenancy.md",
        "docs/checkpoints/03-message-contract-and-transactional-store.md",
        "docs/checkpoints/04-nats-jetstream-transport.md",
        "docs/contracts/message-envelope-v1.md",
        "docs/contracts/message-envelope-v1.schema.json",
        "docs/contracts/fixtures/qualification-command-v1.json",
        "docs/contracts/fixtures/qualification-event-v1.json",
        "docs/contracts/fixtures/transport-cell-probe-requested-v1.json",
        "docs/contracts/fixtures/transport-cell-probe-observed-v1.json",
        "docs/contracts/fixtures/transport-platform-probe-requested-v1.json",
        "docs/contracts/fixtures/transport-platform-probe-observed-v1.json",
        "docs/evidence/checkpoint-02/postgres-qualification.json",
        "docs/evidence/checkpoint-02/postgres-qualification.md",
        "docs/runbooks/local-postgres.md",
        "docs/runbooks/message-store-qualification.md",
        "docs/runbooks/local-nats.md",
        "tools/xtask",
        "tools/postgres-qualification",
        "tools/message-store-qualification",
        "tools/nats-qualification",
        ".editorconfig",
        ".gitignore",
        "Cargo.lock",
        "Cargo.toml",
        "CONTRIBUTING.md",
        "README.md",
        "rust-toolchain.toml",
        "rustfmt.toml",
    ];
    let missing = required_paths
        .iter()
        .filter(|path| !root.join(path).exists())
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "repository structure is incomplete; missing: {}",
            missing.join(", ")
        );
    }

    capture(
        root,
        "cargo",
        &["metadata", "--format-version", "1", "--locked", "--no-deps"],
    )?;

    println!("doctor: rustc={rustc_version}");
    println!("doctor: cargo={cargo_version}");
    println!("doctor: host={host}");
    println!("doctor: rustfmt={rustfmt_version}");
    println!("doctor: clippy={clippy_version}");
    println!("doctor: Cargo.lock present");
    println!("doctor: cargo metadata loaded");
    println!("doctor: repository structure complete");
    Ok(())
}

fn verify(root: &Path) -> Result<()> {
    doctor(root)?;
    verify_contracts(root)?;
    run_checked(
        root,
        "cargo fmt --all -- --check",
        "cargo",
        &["fmt", "--all", "--", "--check"],
    )?;
    run_checked(
        root,
        "cargo check --workspace --all-targets --locked",
        "cargo",
        &["check", "--workspace", "--all-targets", "--locked"],
    )?;
    run_checked(
        root,
        "cargo clippy --workspace --all-targets --locked -- -D warnings",
        "cargo",
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ],
    )?;
    run_checked(
        root,
        "cargo test --workspace --all-targets --locked",
        "cargo",
        &["test", "--workspace", "--all-targets", "--locked"],
    )?;
    verify_architecture(root)?;
    smoke(root)
}

#[allow(clippy::too_many_lines)]
fn verify_contracts(root: &Path) -> Result<()> {
    run_checked(
        root,
        "cargo test --package message-domain --package message-codec-json --package transport-probe-contracts --locked",
        "cargo",
        &[
            "test",
            "--package",
            "message-domain",
            "--package",
            "message-codec-json",
            "--package",
            "transport-probe-contracts",
            "--locked",
        ],
    )?;
    let schema_path = root.join("docs/contracts/message-envelope-v1.schema.json");
    let schema = fs::read_to_string(&schema_path)
        .with_context(|| format!("could not read {}", schema_path.display()))?;
    let _: serde_json::Value = serde_json::from_str(&schema)
        .with_context(|| format!("could not parse {}", schema_path.display()))?;

    let fixture_directory = root.join("docs/contracts/fixtures");
    let fixtures = [
        (
            "qualification-command-v1.json",
            "\"message_kind\":\"command\"",
        ),
        ("qualification-event-v1.json", "\"message_kind\":\"event\""),
        (
            "transport-cell-probe-requested-v1.json",
            "\"message_kind\":\"command\"",
        ),
        (
            "transport-cell-probe-observed-v1.json",
            "\"message_kind\":\"event\"",
        ),
        (
            "transport-platform-probe-requested-v1.json",
            "\"message_kind\":\"command\"",
        ),
        (
            "transport-platform-probe-observed-v1.json",
            "\"message_kind\":\"event\"",
        ),
    ];
    let mut descriptors = BTreeSet::new();
    for (file_name, kind_marker) in fixtures {
        let path = fixture_directory.join(file_name);
        let bytes =
            fs::read(&path).with_context(|| format!("could not read {}", path.display()))?;
        if !bytes.ends_with(b"\n") || bytes[..bytes.len().saturating_sub(1)].ends_with(b"\n") {
            bail!("contract fixture `{file_name}` must end with exactly one LF");
        }
        if bytes.len().saturating_sub(1) > message_fixture_maximum_bytes() {
            bail!("contract fixture `{file_name}` exceeds the envelope bound");
        }
        let text = std::str::from_utf8(&bytes)
            .with_context(|| format!("contract fixture `{file_name}` is not UTF-8"))?;
        let lowercase = text.to_ascii_lowercase();
        for marker in [
            "postgres://",
            "postgresql://",
            "password",
            "bearer",
            "authorization",
            "private_key",
            "secret-sentinel",
        ] {
            if lowercase.contains(marker) {
                bail!("contract fixture `{file_name}` contains a forbidden credential marker");
            }
        }
        if !text.contains(kind_marker)
            || !text.contains("\"message_schema_version\":1")
            || !text.contains("\"assignment_epoch\":\"")
            || !file_name.ends_with("-v1.json")
        {
            bail!("contract fixture `{file_name}` does not match its descriptor filename");
        }
        let parsed: serde_json::Value = serde_json::from_slice(&bytes[..bytes.len() - 1])
            .with_context(|| format!("contract fixture `{file_name}` is invalid JSON"))?;
        let name = parsed
            .get("message_name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow!("contract fixture `{file_name}` lacks message_name"))?;
        let version = parsed
            .get("message_schema_version")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| anyhow!("contract fixture `{file_name}` lacks schema version"))?;
        if !descriptors.insert((name.to_owned(), version)) {
            bail!("contract fixture descriptors must be unique");
        }
    }
    let documentation = [
        "README.md",
        "CONTRIBUTING.md",
        "docs/architecture",
        "docs/adr",
        "docs/checkpoints",
        "docs/contracts/message-envelope-v1.md",
    ];
    for relative in documentation {
        let path = root.join(relative);
        let mut files = Vec::new();
        if path.is_dir() {
            collect_source_files(&path, &mut files)?;
        } else if path.exists() {
            files.push(path);
        }
        for file in files {
            let contents = fs::read_to_string(&file)
                .with_context(|| format!("could not read {}", file.display()))?;
            for (line_number, line) in contents.lines().enumerate() {
                if exactly_once_claim(line) {
                    bail!(
                        "documentation `{}` line {} makes a forbidden exactly-once claim",
                        relative_display(root, &file),
                        line_number + 1
                    );
                }
            }
        }
    }
    println!("verify-contracts: 6 canonical fixtures and envelope schema passed");
    Ok(())
}

const fn message_fixture_maximum_bytes() -> usize {
    262_144
}

fn exactly_once_claim(line: &str) -> bool {
    let normalized = line.to_ascii_lowercase();
    if !normalized.contains("exactly-once") && !normalized.contains("exactly once") {
        return false;
    }
    ![
        "no exactly",
        "no global exactly",
        "no system-wide exactly",
        "not exactly",
        "never exactly",
        "does not prove exactly",
        "without exactly",
        "prohibition on exactly",
        "forbid exactly",
        "forbidden exactly",
        "cannot claim exactly",
        "must not claim exactly",
        "present exactly once",
    ]
    .iter()
    .any(|denial| normalized.contains(denial))
}

fn capture(root: &Path, program: &str, arguments: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(arguments)
        .current_dir(root)
        .output()
        .with_context(|| format!("could not start `{program} {}`", arguments.join(" ")))?;
    checked_output(program, arguments, output)
}

fn checked_output(program: &str, arguments: &[&str], output: Output) -> Result<String> {
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "`{program} {}` failed with {}: {}",
            arguments.join(" "),
            output.status,
            stderr.trim()
        );
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .context("command output was not UTF-8")
}

fn run_checked(root: &Path, label: &str, program: &str, arguments: &[&str]) -> Result<()> {
    println!("verify: {label}");
    let status = Command::new(program)
        .args(arguments)
        .current_dir(root)
        .status()
        .with_context(|| format!("could not start `{label}`"))?;
    if status.success() {
        Ok(())
    } else {
        bail!("`{label}` failed with exit status {status}")
    }
}

#[derive(Debug, Deserialize)]
struct DependencyRules {
    schema_version: u32,
    classes: RuleClasses,
    forbidden_package_names: BTreeSet<String>,
    allowed_workspace_edges: Vec<AllowedWorkspaceEdge>,
    allowed_external_dependencies: Vec<AllowedExternalDependency>,
}

#[derive(Debug, Default, Deserialize)]
struct RuleClasses {
    domain: BTreeSet<String>,
    application: BTreeSet<String>,
    platform_binary: BTreeSet<String>,
    cell_binary: BTreeSet<String>,
    deployable_binary: BTreeSet<String>,
    composition_support: BTreeSet<String>,
    secret_adapter: BTreeSet<String>,
    postgres_provider: BTreeSet<String>,
    migration_adapter: BTreeSet<String>,
    qualification_tool: BTreeSet<String>,
    test_support: BTreeSet<String>,
    tool: BTreeSet<String>,
    message_domain: BTreeSet<String>,
    contract_codec: BTreeSet<String>,
    message_store_provider: BTreeSet<String>,
    message_qualification_tool: BTreeSet<String>,
    runtime_identity: BTreeSet<String>,
    transport_contract: BTreeSet<String>,
    nats_runtime_provider: BTreeSet<String>,
    nats_admin_provider: BTreeSet<String>,
    authority_message_runtime: BTreeSet<String>,
    transport_provisioner: BTreeSet<String>,
    transport_qualification_tool: BTreeSet<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct AllowedWorkspaceEdge {
    from: String,
    to: String,
    kinds: BTreeSet<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct AllowedExternalDependency {
    from: String,
    dependency: String,
    kinds: BTreeSet<String>,
}

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
    workspace_members: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CargoPackage {
    id: String,
    name: String,
    manifest_path: PathBuf,
    dependencies: Vec<CargoDependency>,
}

#[derive(Debug, Deserialize)]
struct CargoDependency {
    name: String,
    source: Option<String>,
    req: String,
    kind: Option<String>,
    path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct GraphPackage {
    name: String,
    dependencies: Vec<GraphDependency>,
}

#[derive(Clone, Debug)]
struct GraphDependency {
    name: String,
    kind: String,
    requirement: String,
    workspace: bool,
    git: bool,
    outside_workspace_path: bool,
}

fn verify_architecture(root: &Path) -> Result<()> {
    let rules = read_rules(root)?;
    let metadata = read_metadata(root)?;
    let graph = graph_from_metadata(root, &metadata);
    let mut violations = validate_graph(&graph, &rules);
    violations.extend(validate_member_manifests(&metadata, root)?);
    violations.extend(validate_root_dependencies(root)?);
    violations.extend(validate_source_boundaries(root, &rules)?);
    violations.sort();
    violations.dedup();

    if violations.is_empty() {
        println!(
            "verify-architecture: {} workspace packages satisfy the declared dependency and source rules",
            graph.len()
        );
        Ok(())
    } else {
        let rendered = violations
            .iter()
            .map(|violation| format!("  - {violation}"))
            .collect::<Vec<_>>()
            .join("\n");
        bail!("architecture verification failed:\n{rendered}")
    }
}

fn read_rules(root: &Path) -> Result<DependencyRules> {
    let path = root.join(RULES_PATH);
    let contents =
        fs::read_to_string(&path).with_context(|| format!("could not read {}", path.display()))?;
    let rules: DependencyRules = serde_json::from_str(&contents)
        .with_context(|| format!("could not parse {}", path.display()))?;
    if rules.schema_version != 4 {
        bail!(
            "unsupported dependency-rule schema {}",
            rules.schema_version
        );
    }
    Ok(rules)
}

fn read_metadata(root: &Path) -> Result<CargoMetadata> {
    let json = capture(
        root,
        "cargo",
        &["metadata", "--format-version", "1", "--locked", "--no-deps"],
    )?;
    serde_json::from_str(&json).context("cargo metadata output could not be decoded")
}

fn graph_from_metadata(root: &Path, metadata: &CargoMetadata) -> Vec<GraphPackage> {
    let workspace_ids = metadata.workspace_members.iter().collect::<HashSet<_>>();
    let workspace_names = metadata
        .packages
        .iter()
        .filter(|package| workspace_ids.contains(&package.id))
        .map(|package| package.name.as_str())
        .collect::<HashSet<_>>();

    metadata
        .packages
        .iter()
        .filter(|package| workspace_ids.contains(&package.id))
        .map(|package| GraphPackage {
            name: package.name.clone(),
            dependencies: package
                .dependencies
                .iter()
                .map(|dependency| {
                    let path_is_inside = dependency
                        .path
                        .as_deref()
                        .and_then(|path| fs::canonicalize(path).ok())
                        .is_none_or(|path| path.starts_with(root));
                    let workspace = dependency.source.is_none()
                        && dependency.path.is_some()
                        && workspace_names.contains(dependency.name.as_str())
                        && path_is_inside;
                    GraphDependency {
                        name: dependency.name.clone(),
                        kind: dependency
                            .kind
                            .clone()
                            .unwrap_or_else(|| String::from("normal")),
                        requirement: dependency.req.clone(),
                        workspace,
                        git: dependency
                            .source
                            .as_deref()
                            .is_some_and(|source| source.starts_with("git+")),
                        outside_workspace_path: dependency.path.is_some() && !path_is_inside,
                    }
                })
                .collect(),
        })
        .collect()
}

fn validate_graph(graph: &[GraphPackage], rules: &DependencyRules) -> Vec<String> {
    let mut violations = Vec::new();

    for package in graph {
        if rules.forbidden_package_names.contains(&package.name) {
            violations.push(format!(
                "package `{}` uses a forbidden generic crate name",
                package.name
            ));
        }

        for dependency in &package.dependencies {
            if !is_exact_requirement(&dependency.requirement) {
                violations.push(format!(
                    "`{}` dependency `{}` has non-exact requirement `{}`",
                    package.name, dependency.name, dependency.requirement
                ));
            }
            if dependency.git {
                violations.push(format!(
                    "`{}` dependency `{}` is a forbidden Git dependency",
                    package.name, dependency.name
                ));
            }
            if dependency.outside_workspace_path {
                violations.push(format!(
                    "`{}` dependency `{}` points outside the workspace",
                    package.name, dependency.name
                ));
            }

            if dependency.workspace {
                validate_workspace_edge(package, dependency, rules, &mut violations);
            } else {
                validate_external_edge(package, dependency, rules, &mut violations);
            }
        }

        let runtime_binary = rules.classes.deployable_binary.contains(&package.name)
            && package.name != "db-migrator";
        if runtime_binary {
            let has_platform_adapter = package.dependencies.iter().any(|dependency| {
                matches!(
                    dependency.name.as_str(),
                    "platform-postgres" | "platform-migrations"
                )
            });
            let has_cell_adapter = package.dependencies.iter().any(|dependency| {
                matches!(
                    dependency.name.as_str(),
                    "cell-postgres" | "cell-migrations"
                )
            });
            if has_platform_adapter && has_cell_adapter {
                violations.push(format!(
                    "runtime binary `{}` depends on both Platform and Cell database adapters",
                    package.name
                ));
            }
        }
    }

    violations
}

#[allow(clippy::too_many_lines)]
fn validate_workspace_edge(
    package: &GraphPackage,
    dependency: &GraphDependency,
    rules: &DependencyRules,
    violations: &mut Vec<String>,
) {
    let explicitly_allowed = rules.allowed_workspace_edges.iter().any(|edge| {
        edge.from == package.name
            && edge.to == dependency.name
            && edge.kinds.contains(&dependency.kind)
    });
    if !explicitly_allowed {
        violations.push(format!(
            "workspace edge `{} -[{}]-> {}` is not explicitly allowed in {RULES_PATH}",
            package.name, dependency.kind, dependency.name
        ));
    }

    if rules.classes.domain.contains(&package.name)
        && (rules.classes.application.contains(&dependency.name)
            || rules.classes.composition_support.contains(&dependency.name)
            || rules.classes.secret_adapter.contains(&dependency.name)
            || rules.classes.postgres_provider.contains(&dependency.name)
            || rules.classes.migration_adapter.contains(&dependency.name)
            || rules.classes.qualification_tool.contains(&dependency.name)
            || rules.classes.deployable_binary.contains(&dependency.name)
            || rules.classes.tool.contains(&dependency.name))
    {
        violations.push(format!(
            "domain crate `{}` depends outward on `{}`",
            package.name, dependency.name
        ));
    }
    if rules.classes.application.contains(&package.name)
        && rules.classes.application.contains(&dependency.name)
    {
        violations.push(format!(
            "application crate `{}` depends on application crate `{}`",
            package.name, dependency.name
        ));
    }
    if rules.classes.transport_contract.contains(&package.name)
        && (rules
            .classes
            .nats_runtime_provider
            .contains(&dependency.name)
            || rules.classes.nats_admin_provider.contains(&dependency.name)
            || rules
                .classes
                .authority_message_runtime
                .contains(&dependency.name))
    {
        violations.push(format!(
            "transport contract crate `{}` imports forbidden runtime provider `{}`",
            package.name, dependency.name
        ));
    }
    if rules.classes.platform_binary.contains(&package.name)
        && matches!(
            dependency.name.as_str(),
            "cell-application" | "cell-postgres" | "cell-migrations" | "cell-message-runtime"
        )
    {
        violations.push(format!(
            "Platform binary `{}` imports forbidden Cell package `{}`",
            package.name, dependency.name
        ));
    }
    if rules.classes.cell_binary.contains(&package.name)
        && matches!(
            dependency.name.as_str(),
            "platform-application"
                | "platform-postgres"
                | "platform-migrations"
                | "platform-message-runtime"
        )
    {
        violations.push(format!(
            "Cell binary `{}` imports forbidden Platform package `{}`",
            package.name, dependency.name
        ));
    }
    if package.name == "tenant-router"
        && (rules.classes.secret_adapter.contains(&dependency.name)
            || rules.classes.postgres_provider.contains(&dependency.name)
            || rules.classes.migration_adapter.contains(&dependency.name)
            || rules.classes.message_domain.contains(&dependency.name)
            || rules.classes.contract_codec.contains(&dependency.name)
            || rules
                .classes
                .message_store_provider
                .contains(&dependency.name)
            || rules
                .classes
                .nats_runtime_provider
                .contains(&dependency.name)
            || rules.classes.nats_admin_provider.contains(&dependency.name))
    {
        violations.push(format!(
            "tenant-router imports forbidden database package `{}`",
            dependency.name
        ));
    }
    if package.name == "platform-postgres"
        && (dependency.name.starts_with("cell-") || dependency.name == "cell-application")
    {
        violations.push(format!(
            "platform-postgres imports forbidden Cell package `{}`",
            dependency.name
        ));
    }
    if package.name == "cell-postgres" && dependency.name.starts_with("platform-") {
        violations.push(format!(
            "cell-postgres imports forbidden Platform package `{}`",
            dependency.name
        ));
    }
    if package.name == "postgres-message-store"
        && matches!(
            dependency.name.as_str(),
            "platform-postgres" | "cell-postgres"
        )
    {
        violations.push(format!(
            "postgres-message-store imports authority-specific provider `{}`",
            dependency.name
        ));
    }
    if rules.classes.migration_adapter.contains(&dependency.name)
        && !matches!(
            package.name.as_str(),
            "db-migrator" | "postgres-qualification" | "message-store-qualification"
        )
    {
        violations.push(format!(
            "migration crate `{}` is imported by forbidden package `{}`",
            dependency.name, package.name
        ));
    }
    if rules.classes.qualification_tool.contains(&dependency.name) {
        violations.push(format!(
            "qualification tool `{}` is imported by production package `{}`",
            dependency.name, package.name
        ));
    }
    if rules.classes.nats_admin_provider.contains(&dependency.name)
        && !(rules.classes.transport_provisioner.contains(&package.name)
            || rules
                .classes
                .transport_qualification_tool
                .contains(&package.name))
    {
        violations.push(format!(
            "NATS administration provider `{}` is imported by forbidden runtime package `{}`",
            dependency.name, package.name
        ));
    }
    if rules
        .classes
        .authority_message_runtime
        .contains(&package.name)
        && package.name == "platform-message-runtime"
        && matches!(
            dependency.name.as_str(),
            "cell-postgres" | "cell-message-runtime"
        )
    {
        violations.push(format!(
            "Platform authority runtime imports forbidden Cell provider `{}`",
            dependency.name
        ));
    }
    if rules
        .classes
        .authority_message_runtime
        .contains(&package.name)
        && package.name == "cell-message-runtime"
        && matches!(
            dependency.name.as_str(),
            "platform-postgres" | "platform-message-runtime"
        )
    {
        violations.push(format!(
            "Cell authority runtime imports forbidden Platform provider `{}`",
            dependency.name
        ));
    }
    if (rules.classes.domain.contains(&package.name)
        || rules.classes.application.contains(&package.name))
        && (rules.classes.contract_codec.contains(&dependency.name)
            || rules
                .classes
                .message_store_provider
                .contains(&dependency.name))
    {
        violations.push(format!(
            "domain/application crate `{}` imports forbidden message provider `{}`",
            package.name, dependency.name
        ));
    }
    if rules.classes.deployable_binary.contains(&package.name)
        && (rules
            .classes
            .message_store_provider
            .contains(&dependency.name)
            || rules
                .classes
                .message_qualification_tool
                .contains(&dependency.name))
    {
        violations.push(format!(
            "deployable binary `{}` imports forbidden message-store package `{}`",
            package.name, dependency.name
        ));
    }
    if (rules.classes.domain.contains(&package.name)
        || rules.classes.application.contains(&package.name))
        && rules.classes.secret_adapter.contains(&dependency.name)
    {
        violations.push(format!(
            "domain/application crate `{}` imports secret adapter `{}`",
            package.name, dependency.name
        ));
    }
    if rules.classes.deployable_binary.contains(&package.name)
        && package.name != "db-migrator"
        && rules.classes.migration_adapter.contains(&dependency.name)
    {
        violations.push(format!(
            "runtime binary `{}` imports migration crate `{}`",
            package.name, dependency.name
        ));
    }
    if rules.classes.deployable_binary.contains(&package.name)
        && rules.classes.test_support.contains(&dependency.name)
        && dependency.kind == "normal"
    {
        violations.push(format!(
            "deployable binary `{}` has test-support as a normal dependency",
            package.name
        ));
    }
}

#[allow(clippy::too_many_lines)]
fn validate_external_edge(
    package: &GraphPackage,
    dependency: &GraphDependency,
    rules: &DependencyRules,
    violations: &mut Vec<String>,
) {
    let explicitly_allowed = rules.allowed_external_dependencies.iter().any(|allowed| {
        allowed.from == package.name
            && allowed.dependency == dependency.name
            && allowed.kinds.contains(&dependency.kind)
    });
    if !explicitly_allowed {
        violations.push(format!(
            "external dependency `{} -[{}]-> {}` is not explicitly allowed in {RULES_PATH}",
            package.name, dependency.kind, dependency.name
        ));
    }

    if rules.classes.domain.contains(&package.name) && dependency.name == "serde" {
        violations.push(format!(
            "domain crate `{}` must not depend on serde",
            package.name
        ));
    }
    if rules.classes.message_domain.contains(&package.name)
        && matches!(
            dependency.name.as_str(),
            "serde" | "serde_json" | "sqlx" | "tokio" | "tracing"
        )
    {
        violations.push(format!(
            "message-domain imports forbidden external dependency `{}`",
            dependency.name
        ));
    }
    if rules.classes.contract_codec.contains(&package.name)
        && matches!(dependency.name.as_str(), "sqlx" | "tokio" | "tracing")
    {
        violations.push(format!(
            "message-codec-json imports forbidden external dependency `{}`",
            dependency.name
        ));
    }
    if dependency.name == "async-nats"
        && !(rules.classes.nats_runtime_provider.contains(&package.name)
            || rules.classes.nats_admin_provider.contains(&package.name)
            || rules
                .classes
                .transport_qualification_tool
                .contains(&package.name))
    {
        violations.push(format!(
            "package `{}` imports async-nats outside the approved NATS provider or qualification boundary",
            package.name
        ));
    }
    if matches!(dependency.name.as_str(), "rdkafka" | "lapin" | "pulsar") {
        violations.push(format!(
            "package `{}` introduces forbidden broker dependency `{}`",
            package.name, dependency.name
        ));
    }
    if (rules.classes.domain.contains(&package.name)
        || rules.classes.application.contains(&package.name))
        && dependency.name == "anyhow"
        && dependency.kind == "normal"
    {
        violations.push(format!(
            "`{}` must not use anyhow as a normal dependency",
            package.name
        ));
    }

    let domain_or_application = rules.classes.domain.contains(&package.name)
        || rules.classes.application.contains(&package.name);
    if domain_or_application && matches!(dependency.name.as_str(), "sqlx" | "secrecy") {
        violations.push(format!(
            "domain/application crate `{}` imports forbidden `{}`",
            package.name, dependency.name
        ));
    }

    if dependency.name == "sqlx"
        && !(rules.classes.postgres_provider.contains(&package.name)
            || rules.classes.migration_adapter.contains(&package.name)
            || rules.classes.qualification_tool.contains(&package.name))
    {
        violations.push(format!(
            "package `{}` imports SQLx outside the PostgreSQL provider, migration, or qualification boundary",
            package.name
        ));
    }

    if dependency.name == "secrecy"
        && !(rules.classes.composition_support.contains(&package.name)
            || rules.classes.secret_adapter.contains(&package.name)
            || rules.classes.postgres_provider.contains(&package.name)
            || rules.classes.migration_adapter.contains(&package.name)
            || rules.classes.qualification_tool.contains(&package.name)
            || rules.classes.nats_runtime_provider.contains(&package.name)
            || rules.classes.nats_admin_provider.contains(&package.name))
    {
        violations.push(format!(
            "package `{}` imports secrecy outside a secret, provider, composition, migration, or qualification boundary",
            package.name
        ));
    }

    if dependency.name == "getrandom"
        && package.name != "xtask"
        && !rules.classes.runtime_identity.contains(&package.name)
    {
        violations.push(format!(
            "package `{}` imports getrandom; disposable credential generation belongs to xtask",
            package.name
        ));
    }

    if dependency.name == "tempfile" && dependency.kind == "normal" {
        violations.push(format!(
            "package `{}` uses tempfile as a normal dependency",
            package.name
        ));
    }
}

fn is_exact_requirement(requirement: &str) -> bool {
    requirement.strip_prefix('=').is_some_and(|version| {
        !version.is_empty()
            && !version
                .chars()
                .any(|character| "*^~<>,| ".contains(character))
    })
}

fn validate_member_manifests(metadata: &CargoMetadata, root: &Path) -> Result<Vec<String>> {
    let workspace_ids = metadata.workspace_members.iter().collect::<HashSet<_>>();
    let mut violations = Vec::new();

    for package in metadata
        .packages
        .iter()
        .filter(|package| workspace_ids.contains(&package.id))
    {
        let contents = fs::read_to_string(&package.manifest_path)
            .with_context(|| format!("could not read {}", package.manifest_path.display()))?;
        for marker in [
            "version.workspace = true",
            "edition.workspace = true",
            "rust-version.workspace = true",
            "publish.workspace = true",
            "[lints]",
            "workspace = true",
        ] {
            if !contents.contains(marker) {
                violations.push(format!(
                    "`{}` manifest does not inherit required workspace metadata marker `{marker}`",
                    package.name
                ));
            }
        }

        let mut dependency_section = false;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                dependency_section = is_dependency_section(trimmed);
                continue;
            }
            if dependency_section
                && !trimmed.is_empty()
                && !trimmed.starts_with('#')
                && trimmed.contains('=')
                && !trimmed.contains(".workspace = true")
                && !trimmed.contains("workspace = true")
            {
                violations.push(format!(
                    "`{}` declares a dependency outside the root workspace table: `{trimmed}`",
                    package.name
                ));
            }
        }

        let package_directory = package.manifest_path.parent().unwrap_or(root);
        if package_directory.join("build.rs").exists() {
            violations.push(format!(
                "`{}` introduces a forbidden build.rs",
                package.name
            ));
        }
    }

    Ok(violations)
}

fn is_dependency_section(section: &str) -> bool {
    matches!(
        section,
        "[dependencies]" | "[dev-dependencies]" | "[build-dependencies]"
    ) || (section.starts_with("[target.")
        && (section.ends_with(".dependencies]")
            || section.ends_with(".dev-dependencies]")
            || section.ends_with(".build-dependencies]")))
}

fn validate_root_dependencies(root: &Path) -> Result<Vec<String>> {
    let manifest_path = root.join("Cargo.toml");
    let contents = fs::read_to_string(&manifest_path)
        .with_context(|| format!("could not read {}", manifest_path.display()))?;
    let mut in_workspace_dependencies = false;
    let mut violations = Vec::new();

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_workspace_dependencies = trimmed == "[workspace.dependencies]";
            continue;
        }
        if !in_workspace_dependencies
            || trimmed.is_empty()
            || trimmed.starts_with('#')
            || !trimmed.contains('=')
        {
            continue;
        }

        let dependency_name = trimmed
            .split_once('=')
            .map_or("unknown", |(name, _)| name.trim());
        if trimmed.contains("git =") {
            violations.push(format!(
                "root workspace dependency `{dependency_name}` uses a forbidden Git source"
            ));
        }

        match quoted_attribute(trimmed, "version")
            .or_else(|| shorthand_requirement(trimmed))
        {
            Some(requirement) if is_exact_requirement(requirement) => {}
            Some(requirement) => violations.push(format!(
                "root workspace dependency `{dependency_name}` has non-exact requirement `{requirement}`"
            )),
            None => violations.push(format!(
                "root workspace dependency `{dependency_name}` has no exact version requirement"
            )),
        }

        if let Some(path) = quoted_attribute(trimmed, "path") {
            let resolved = root.join(path);
            match fs::canonicalize(&resolved) {
                Ok(canonical) if canonical.starts_with(root) => {}
                Ok(_) => violations.push(format!(
                    "root workspace dependency `{dependency_name}` points outside the workspace"
                )),
                Err(_) => violations.push(format!(
                    "root workspace dependency `{dependency_name}` has an unresolved path `{path}`"
                )),
            }
        }
    }

    Ok(violations)
}

fn quoted_attribute<'a>(line: &'a str, attribute: &str) -> Option<&'a str> {
    let marker = format!("{attribute} = \"");
    let remainder = line.split_once(&marker)?.1;
    remainder.split_once('"').map(|(value, _)| value)
}

fn shorthand_requirement(line: &str) -> Option<&str> {
    let value = line.split_once('=')?.1.trim();
    value
        .strip_prefix('"')?
        .split_once('"')
        .map(|(requirement, _)| requirement)
}

fn validate_source_boundaries(root: &Path, rules: &DependencyRules) -> Result<Vec<String>> {
    let mut source_files = Vec::new();
    collect_source_files(root, &mut source_files)?;
    let mut violations = Vec::new();

    for path in source_files {
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("could not read {}", path.display()))?;
        violations.extend(source_violations(root, &path, &contents, rules));
    }

    for relative in [
        "crates/platform-migrations/migrations/0001_platform_foundation.sql",
        "crates/platform-migrations/migrations/0002_platform_message_store.sql",
        "crates/cell-migrations/migrations/0001_cell_foundation.sql",
        "crates/cell-migrations/migrations/0002_cell_message_store.sql",
    ] {
        if !root.join(relative).is_file() {
            violations.push(format!(
                "immutable pre-Checkpoint-4 migration `{relative}` is missing"
            ));
        }
    }
    for directory in [
        root.join("crates/platform-migrations/migrations"),
        root.join("crates/cell-migrations/migrations"),
    ] {
        for entry in fs::read_dir(&directory)
            .with_context(|| format!("could not inspect {}", directory.display()))?
        {
            let path = entry?.path();
            let relative = relative_display(root, &path);
            if path.is_file()
                && !matches!(
                    relative.as_str(),
                    "crates/platform-migrations/migrations/0001_platform_foundation.sql"
                        | "crates/platform-migrations/migrations/0002_platform_message_store.sql"
                        | "crates/cell-migrations/migrations/0001_cell_foundation.sql"
                        | "crates/cell-migrations/migrations/0002_cell_message_store.sql"
                )
            {
                violations.push(format!(
                    "Checkpoint 4 introduces forbidden database migration `{relative}`"
                ));
            }
        }
    }

    Ok(violations)
}

fn collect_source_files(directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("could not inspect {}", directory.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = entry.file_name();
            if name != "target" && name != ".git" {
                collect_source_files(&path, files)?;
            }
        } else if path.extension().is_some_and(|extension| {
            matches!(
                extension.to_str(),
                Some("rs" | "sql" | "toml" | "json" | "md" | "yml" | "yaml" | "sh")
            )
        }) {
            files.push(path);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn source_violations(
    root: &Path,
    path: &Path,
    contents: &str,
    rules: &DependencyRules,
) -> Vec<String> {
    let mut violations = Vec::new();
    let relative = relative_display(root, path);
    let is_rust = path.extension().is_some_and(|extension| extension == "rs");
    let is_sql = path.extension().is_some_and(|extension| extension == "sql");
    let is_test_file = is_approved_test_path(path);
    let production = if is_rust && !is_test_file {
        contents
            .split_once("#[cfg(test)]")
            .map_or(contents, |(before_tests, _)| before_tests)
    } else if is_test_file {
        ""
    } else {
        contents
    };
    let package = package_name_from_path(root, path);
    let approved_nats_provider = package.is_some_and(|name| {
        rules.classes.nats_runtime_provider.contains(name)
            || rules.classes.nats_admin_provider.contains(name)
    });
    let approved_nats_source = approved_nats_provider
        || rules
            .classes
            .transport_qualification_tool
            .contains(package.unwrap_or_default())
        || path.starts_with(root.join("infra/local/nats"))
        || path.starts_with(root.join("tools/xtask"));

    if is_rust && !is_test_file {
        for pattern in [
            ["todo", "!"].concat(),
            ["unimplemented", "!"].concat(),
            ["panic", "!"].concat(),
            String::from(".unwrap("),
            String::from(".expect("),
        ] {
            if production.contains(&pattern) {
                violations.push(format!(
                    "non-test source `{relative}` contains forbidden construct `{pattern}`"
                ));
            }
        }
        if production.contains("unsafe {")
            || production.contains("unsafe fn")
            || production.contains("unsafe impl")
            || production.contains("unsafe trait")
        {
            violations.push(format!(
                "non-test source `{relative}` contains forbidden unsafe code"
            ));
        }
    }

    if is_rust
        && !matches!(
            package,
            Some(
                "process-lifecycle"
                    | "postgres-qualification"
                    | "message-store-qualification"
                    | "nats-qualification"
            )
        )
        && production.contains("tokio::spawn")
    {
        violations.push(format!(
            "production source `{relative}` starts a Tokio task directly outside process-lifecycle"
        ));
    }

    if is_rust
        && package.is_some_and(|name| {
            rules.classes.domain.contains(name) || rules.classes.application.contains(name)
        })
        && (production.contains("Uuid::now_v7")
            || production.contains("Uuid::new_v4")
            || production.contains("uuid::Uuid::now_v7")
            || production.contains("uuid::Uuid::new_v4"))
    {
        violations.push(format!(
            "domain/application source `{relative}` uses ambient UUID generation"
        ));
    }

    let approved_url_location = path.starts_with(root.join("infra/local/postgres"))
        || path.starts_with(root.join("tools/postgres-qualification"))
        || path.starts_with(root.join("tools/message-store-qualification"))
        || path.starts_with(root.join("tools/nats-qualification"))
        || path.starts_with(root.join("tools/xtask"))
        || is_test_file;
    if !approved_url_location
        && (production.contains("postgres://") || production.contains("postgresql://"))
    {
        violations.push(format!(
            "production source `{relative}` contains a forbidden PostgreSQL URL literal"
        ));
    }

    if is_sql {
        let approved_migration_location = path
            .starts_with(root.join("crates/platform-migrations/migrations"))
            || path.starts_with(root.join("crates/cell-migrations/migrations"))
            || path.starts_with(root.join("tools/postgres-qualification"));
        if !approved_migration_location {
            violations.push(format!(
                "SQL file `{relative}` is outside an owned migration or qualification directory"
            ));
        }
    }

    if matches!(package, Some("platform-postgres" | "cell-postgres"))
        && path
            .components()
            .any(|component| component.as_os_str() == "migrations")
    {
        violations.push(format!(
            "runtime adapter source `{relative}` contains a migration file"
        ));
    }

    if is_rust {
        for (line_number, line) in production.lines().enumerate() {
            let normalized = line.to_ascii_lowercase();
            if normalized.contains("expose_secret")
                && [
                    "println!",
                    "eprintln!",
                    "format!",
                    "tracing::",
                    "debug!",
                    "info!",
                    "warn!",
                    "error!",
                ]
                .iter()
                .any(|marker| normalized.contains(marker))
            {
                violations.push(format!(
                    "non-test source `{relative}` line {} formats or logs an exposed secret",
                    line_number + 1
                ));
            }
            if normalized.contains("as i64")
                && (normalized.contains("assignmentepoch")
                    || normalized.contains("assignment_epoch")
                    || normalized.contains("u64")
                    || production.contains("AssignmentEpoch"))
            {
                violations.push(format!(
                    "non-test source `{relative}` line {} contains a lossy assignment-epoch/u64 cast",
                    line_number + 1
                ));
            }
            let logging_line = [
                "println!",
                "eprintln!",
                "tracing::",
                "debug!",
                "info!",
                "warn!",
                "error!",
            ]
            .iter()
            .any(|marker| normalized.contains(marker));
            if logging_line
                && (normalized.contains("encodedmessage")
                    || normalized.contains("encoded_message")
                    || normalized.contains("payload_bytes")
                    || normalized.contains("envelope_bytes"))
            {
                violations.push(format!(
                    "non-test source `{relative}` line {} logs message or payload bytes",
                    line_number + 1
                ));
            }
            if !approved_nats_source
                && normalized.contains("edtech.v1.")
                && !path.starts_with(root.join("docs"))
            {
                violations.push(format!(
                    "source `{relative}` line {} contains a raw application subject outside an approved transport boundary",
                    line_number + 1
                ));
            }
            if normalized.contains("format!(")
                && normalized.contains("tenant")
                && (normalized.contains("subject") || normalized.contains("edtech.v1."))
            {
                violations.push(format!(
                    "source `{relative}` line {} interpolates a tenant identifier into a subject",
                    line_number + 1
                ));
            }
            if normalized.contains("format!(")
                && (normalized.contains("assignment_epoch")
                    || normalized.contains("assignmentepoch"))
                && (normalized.contains("subject") || normalized.contains("edtech.v1."))
            {
                violations.push(format!(
                    "source `{relative}` line {} interpolates an assignment epoch into a subject",
                    line_number + 1
                ));
            }
        }
    }

    if is_rust
        && (path.starts_with(root.join("apps")) || path.starts_with(root.join("crates")))
        && !approved_nats_provider
        && ["async_nats", "rdkafka", "lapin", "pulsar"]
            .iter()
            .any(|name| production.contains(name))
    {
        violations.push(format!(
            "runtime source `{relative}` contains a forbidden broker SDK or provider name"
        ));
    }

    if is_rust && !approved_nats_provider && public_api_contains_async_nats(production) {
        violations.push(format!(
            "source `{relative}` exposes an async-nats type outside a NATS provider package"
        ));
    }

    let runtime_source =
        path.starts_with(root.join("apps")) || path.starts_with(root.join("crates"));
    if is_rust && runtime_source && !approved_nats_provider {
        for token in ["get_or_create_stream", "get_or_create_consumer"] {
            if production.contains(token) {
                violations.push(format!(
                    "runtime source `{relative}` invokes forbidden topology mutation `{token}`"
                ));
            }
        }
    }
    if is_rust
        && matches!(package, Some("platform-worker" | "cell-worker"))
        && (production.contains(".publish(")
            || production.contains("publish_with_headers(")
            || production.contains("jetstream::new("))
    {
        violations.push(format!(
            "worker source `{relative}` performs forbidden direct Core NATS publication"
        ));
    }
    if is_rust && matches!(package, Some("platform-worker" | "cell-worker")) {
        for token in [
            "create_stream",
            "update_stream",
            "delete_stream",
            "create_consumer",
            "update_consumer",
            "delete_consumer",
        ] {
            if production.contains(token) {
                violations.push(format!(
                    "worker source `{relative}` performs forbidden topology mutation `{token}`"
                ));
            }
        }
    }

    if is_rust
        && package == Some("runtime-config")
        && production.lines().any(|line| {
            let normalized = line.trim().to_ascii_lowercase();
            normalized.starts_with("pub subject:")
                || normalized.starts_with("subject:")
                || normalized.starts_with("pub subjects:")
                || normalized.starts_with("subjects:")
        })
    {
        violations.push(format!(
            "runtime configuration source `{relative}` accepts arbitrary subject input"
        ));
    }

    if production.lines().any(|line| {
        let normalized = line.to_ascii_lowercase();
        normalized.contains("nats://") && normalized.contains('@')
    }) && !is_test_file
    {
        violations.push(format!(
            "production source `{relative}` embeds broker credentials in a NATS URL literal"
        ));
    }

    if matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("yml" | "yaml")
    ) {
        for (line_number, line) in contents.lines().enumerate() {
            let normalized = line.trim().to_ascii_lowercase();
            if normalized.starts_with("image:")
                && normalized.contains("nats")
                && (!normalized.contains("@sha256:")
                    || normalized.contains(":latest")
                    || normalized.contains(":alpine@"))
            {
                violations.push(format!(
                    "source `{relative}` line {} uses an unpinned or floating NATS image",
                    line_number + 1
                ));
            }
        }
    }

    if path.starts_with(root.join("docs")) {
        for (line_number, line) in contents.lines().enumerate() {
            if exactly_once_claim(line) {
                violations.push(format!(
                    "documentation `{relative}` line {} makes a forbidden exactly-once claim",
                    line_number + 1
                ));
            }
            if published_means_consumed_claim(line) {
                violations.push(format!(
                    "documentation `{relative}` line {} claims that published means consumed",
                    line_number + 1
                ));
            }
        }
    }

    if path.starts_with(root.join("apps")) && is_rust {
        for token in [
            "sqlx::query(",
            "sqlx::query_as",
            "PgPool",
            "PgConnection",
            "sqlx::migrate::Migrator",
            "postgres_message_store::",
        ] {
            if production.contains(token) {
                violations.push(format!(
                    "application binary source `{relative}` invokes SQLx directly via `{token}`"
                ));
            }
        }

        if package != Some("db-migrator") {
            for token in [
                "CREATE TABLE",
                "CREATE SCHEMA",
                "ALTER TABLE",
                "DROP TABLE",
                "sqlx::migrate",
                "platform_migrations",
                "cell_migrations",
            ] {
                if production.contains(token) {
                    violations.push(format!(
                        "runtime binary source `{relative}` contains forbidden DDL or migration invocation `{token}`"
                    ));
                }
            }
        }
    }

    if is_rust
        && package.is_some_and(|name| !rules.classes.postgres_provider.contains(name))
        && public_api_contains_sqlx(production)
    {
        violations.push(format!(
            "source `{relative}` exposes a SQLx type outside the PostgreSQL provider layer"
        ));
    }
    if is_rust
        && package.is_some_and(|name| {
            rules.classes.domain.contains(name) || rules.classes.application.contains(name)
        })
        && public_api_contains_json_value(production)
    {
        violations.push(format!(
            "source `{relative}` exposes serde_json::Value from a domain/application API"
        ));
    }

    violations
}

fn is_approved_test_path(path: &Path) -> bool {
    path.components().any(|component| {
        matches!(
            component.as_os_str().to_str(),
            Some("tests" | "test-fixtures" | "fixtures")
        )
    }) || path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .is_some_and(|stem| stem.ends_with("_test"))
}

fn package_name_from_path<'a>(root: &Path, path: &'a Path) -> Option<&'a str> {
    let relative = path.strip_prefix(root).ok()?;
    let mut components = relative.components();
    let top_level = components.next()?.as_os_str().to_str()?;
    if !matches!(top_level, "apps" | "crates" | "tools") {
        return None;
    }
    components.next()?.as_os_str().to_str()
}

fn public_api_contains_sqlx(contents: &str) -> bool {
    let sqlx_tokens = [
        "sqlx::",
        "PgPool",
        "PgConnection",
        "Transaction<'",
        "Transaction<",
        "sqlx::migrate::Migrator",
        "PgRow",
    ];
    let lines = contents.lines().collect::<Vec<_>>();
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        let starts_public_api = trimmed.starts_with("pub fn ")
            || trimmed.starts_with("pub async fn ")
            || trimmed.starts_with("pub type ")
            || trimmed.starts_with("pub trait ")
            || trimmed.starts_with("pub struct ")
            || trimmed.starts_with("pub enum ")
            || trimmed.starts_with("pub use ")
            || trimmed.starts_with("pub static ")
            || trimmed.starts_with("pub const ")
            || trimmed.starts_with("pub ");
        if !starts_public_api {
            continue;
        }

        let end = usize::min(index + 6, lines.len());
        let signature = lines[index..end]
            .iter()
            .take_while(|candidate| !candidate.contains('{') && !candidate.contains(';'))
            .copied()
            .chain(std::iter::once(*line))
            .collect::<Vec<_>>()
            .join(" ");
        if sqlx_tokens.iter().any(|token| signature.contains(token)) {
            return true;
        }
    }
    false
}

fn public_api_contains_json_value(contents: &str) -> bool {
    let lines = contents.lines().collect::<Vec<_>>();
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        if !(trimmed.starts_with("pub fn ")
            || trimmed.starts_with("pub async fn ")
            || trimmed.starts_with("pub type ")
            || trimmed.starts_with("pub trait ")
            || trimmed.starts_with("pub struct ")
            || trimmed.starts_with("pub enum "))
        {
            continue;
        }
        let end = usize::min(index + 6, lines.len());
        let signature = lines[index..end].join(" ");
        if signature.contains("serde_json::Value") {
            return true;
        }
    }
    false
}

fn public_api_contains_async_nats(contents: &str) -> bool {
    let lines = contents.lines().collect::<Vec<_>>();
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("pub ") {
            continue;
        }
        let end = usize::min(index + 6, lines.len());
        if lines[index..end].join(" ").contains("async_nats") {
            return true;
        }
    }
    false
}

fn published_means_consumed_claim(line: &str) -> bool {
    let normalized = line.to_ascii_lowercase();
    let mentions_both = normalized.contains("published")
        && (normalized.contains("consumed") || normalized.contains("processed"));
    mentions_both
        && [" means ", " guarantees ", " is equivalent to "]
            .iter()
            .any(|marker| normalized.contains(marker))
        && !["does not", "not mean", "never", "must not"]
            .iter()
            .any(|denial| normalized.contains(denial))
}

fn relative_display<'a>(root: &'a Path, path: &'a Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}

struct SmokeCase<'a> {
    package: &'a str,
    environment: &'a [(&'a str, &'a str)],
}

#[allow(clippy::too_many_lines)]
fn smoke(root: &Path) -> Result<()> {
    let cases = [
        SmokeCase {
            package: "platform-api",
            environment: &[
                ("EDTECH__ENVIRONMENT", "dev"),
                (
                    "EDTECH__DATABASE__CREDENTIAL_REF",
                    "file:/run/secrets/platform-api-database",
                ),
                ("EDTECH__DATABASE__TLS_MODE", "disable"),
            ],
        },
        SmokeCase {
            package: "platform-worker",
            environment: &[
                ("EDTECH__ENVIRONMENT", "npr"),
                (
                    "EDTECH__DATABASE__CREDENTIAL_REF",
                    "file:/run/secrets/platform-worker-database",
                ),
                ("EDTECH__DATABASE__TLS_MODE", "verify_full"),
                (
                    "EDTECH__TRANSPORT__SERVERS",
                    "tls://nats-1:4222,tls://nats-2:4222,tls://nats-3:4222",
                ),
                (
                    "EDTECH__TRANSPORT__CREDENTIAL_REF",
                    "file:/run/secrets/platform-worker-nats",
                ),
                ("EDTECH__TRANSPORT__TLS_MODE", "verify_full"),
                (
                    "EDTECH__TRANSPORT__CA_CERTIFICATE_FILE",
                    "/run/edtech-nats/ca.pem",
                ),
            ],
        },
        SmokeCase {
            package: "tenant-router",
            environment: &[("EDTECH__ENVIRONMENT", "prd")],
        },
        SmokeCase {
            package: "cell-api",
            environment: &[
                ("EDTECH__ENVIRONMENT", "dev"),
                ("EDTECH__CELL_ID", "cell-001"),
                (
                    "EDTECH__DATABASE__CREDENTIAL_REF",
                    "file:/run/secrets/cell-api-database",
                ),
                ("EDTECH__DATABASE__TLS_MODE", "disable"),
            ],
        },
        SmokeCase {
            package: "cell-worker",
            environment: &[
                ("EDTECH__ENVIRONMENT", "npr"),
                ("EDTECH__CELL_ID", "cell-002"),
                (
                    "EDTECH__DATABASE__CREDENTIAL_REF",
                    "file:/run/secrets/cell-worker-database",
                ),
                ("EDTECH__DATABASE__TLS_MODE", "verify_full"),
                (
                    "EDTECH__TRANSPORT__SERVERS",
                    "tls://nats-1:4222,tls://nats-2:4222,tls://nats-3:4222",
                ),
                (
                    "EDTECH__TRANSPORT__CREDENTIAL_REF",
                    "file:/run/secrets/cell-worker-nats",
                ),
                ("EDTECH__TRANSPORT__TLS_MODE", "verify_full"),
                (
                    "EDTECH__TRANSPORT__CA_CERTIFICATE_FILE",
                    "/run/edtech-nats/ca.pem",
                ),
            ],
        },
        SmokeCase {
            package: "db-migrator",
            environment: &[
                ("EDTECH__ENVIRONMENT", "prd"),
                ("EDTECH__MIGRATION_SCOPE", "platform"),
                (
                    "EDTECH__DATABASE__CREDENTIAL_REF",
                    "file:/run/secrets/platform-migrator-database",
                ),
                ("EDTECH__DATABASE__TLS_MODE", "verify_full"),
            ],
        },
        SmokeCase {
            package: "nats-provisioner",
            environment: &[
                ("EDTECH__ENVIRONMENT", "npr"),
                (
                    "EDTECH__TRANSPORT__SERVERS",
                    "tls://nats-1:4222,tls://nats-2:4222,tls://nats-3:4222",
                ),
                (
                    "EDTECH__TRANSPORT__CREDENTIAL_REF",
                    "file:/run/secrets/nats-provisioner",
                ),
                ("EDTECH__TRANSPORT__TLS_MODE", "verify_full"),
                (
                    "EDTECH__TRANSPORT__CA_CERTIFICATE_FILE",
                    "/run/edtech-nats/ca.pem",
                ),
                (
                    "EDTECH__TOPOLOGY_FILE",
                    "infra/local/nats/templates/topology.toml",
                ),
            ],
        },
        SmokeCase {
            package: "db-migrator",
            environment: &[
                ("EDTECH__ENVIRONMENT", "prd"),
                ("EDTECH__MIGRATION_SCOPE", "cell"),
                ("EDTECH__CELL_ID", "cell-001"),
                (
                    "EDTECH__DATABASE__CREDENTIAL_REF",
                    "file:/run/secrets/cell-migrator-database",
                ),
                ("EDTECH__DATABASE__TLS_MODE", "verify_full"),
            ],
        },
    ];

    for case in cases {
        println!("smoke: {} --check-config", case.package);
        let mut command = Command::new("cargo");
        command
            .args([
                "run",
                "--quiet",
                "--locked",
                "--package",
                case.package,
                "--",
                "--check-config",
            ])
            .current_dir(root)
            .env_remove("EDTECH_CONFIG_FILE");
        for (key, _) in env::vars_os()
            .filter(|(key, _)| key.to_str().is_some_and(|key| key.starts_with("EDTECH__")))
        {
            command.env_remove(key);
        }
        for (key, value) in case.environment {
            command.env(key, value);
        }
        let status = command
            .status()
            .with_context(|| format!("could not start smoke case `{}`", case.package))?;
        if !status.success() {
            bail!(
                "smoke case `{}` failed with exit status {status}",
                case.package
            );
        }
    }

    println!("smoke: all eight configuration cases passed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, path::Path};

    use super::{
        AllowedExternalDependency, AllowedWorkspaceEdge, DependencyRules, GraphDependency,
        GraphPackage, RuleClasses, source_violations, validate_graph,
    };

    fn set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn rules() -> DependencyRules {
        DependencyRules {
            schema_version: 4,
            classes: RuleClasses {
                domain: set(&["tenancy-domain"]),
                application: set(&["platform-application", "cell-application"]),
                platform_binary: set(&["platform-api", "tenant-router"]),
                cell_binary: set(&["cell-api"]),
                deployable_binary: set(&[
                    "platform-api",
                    "cell-api",
                    "tenant-router",
                    "db-migrator",
                ]),
                composition_support: BTreeSet::new(),
                secret_adapter: set(&["secret-resolution"]),
                postgres_provider: set(&["postgres-runtime", "platform-postgres", "cell-postgres"]),
                migration_adapter: set(&["platform-migrations", "cell-migrations"]),
                qualification_tool: set(&["postgres-qualification"]),
                test_support: set(&["test-support"]),
                tool: BTreeSet::new(),
                message_domain: BTreeSet::new(),
                contract_codec: BTreeSet::new(),
                message_store_provider: BTreeSet::new(),
                message_qualification_tool: BTreeSet::new(),
                runtime_identity: set(&["runtime-identity"]),
                transport_contract: set(&["transport-probe-contracts"]),
                nats_runtime_provider: set(&["nats-jetstream"]),
                nats_admin_provider: set(&["nats-jetstream-admin"]),
                authority_message_runtime: set(&[
                    "platform-message-runtime",
                    "cell-message-runtime",
                ]),
                transport_provisioner: set(&["nats-provisioner"]),
                transport_qualification_tool: set(&["nats-qualification"]),
            },
            forbidden_package_names: set(&["common", "shared", "core", "utils", "helpers", "misc"]),
            allowed_workspace_edges: vec![
                AllowedWorkspaceEdge {
                    from: String::from("platform-api"),
                    to: String::from("cell-application"),
                    kinds: set(&["normal"]),
                },
                AllowedWorkspaceEdge {
                    from: String::from("platform-application"),
                    to: String::from("cell-application"),
                    kinds: set(&["normal"]),
                },
                AllowedWorkspaceEdge {
                    from: String::from("platform-api"),
                    to: String::from("test-support"),
                    kinds: set(&["normal"]),
                },
            ],
            allowed_external_dependencies: vec![AllowedExternalDependency {
                from: String::from("tenancy-domain"),
                dependency: String::from("uuid"),
                kinds: set(&["normal"]),
            }],
        }
    }

    fn workspace_dependency(name: &str) -> GraphDependency {
        GraphDependency {
            name: name.to_owned(),
            kind: String::from("normal"),
            requirement: String::from("=0.1.0"),
            workspace: true,
            git: false,
            outside_workspace_path: false,
        }
    }

    fn external_dependency(name: &str, requirement: &str) -> GraphDependency {
        GraphDependency {
            name: name.to_owned(),
            kind: String::from("normal"),
            requirement: requirement.to_owned(),
            workspace: false,
            git: false,
            outside_workspace_path: false,
        }
    }

    fn has_violation(violations: &[String], fragment: &str) -> bool {
        violations
            .iter()
            .any(|violation| violation.contains(fragment))
    }

    fn allow_workspace_edge(rules: &mut DependencyRules, from: &str, to: &str) {
        rules.allowed_workspace_edges.push(AllowedWorkspaceEdge {
            from: from.to_owned(),
            to: to.to_owned(),
            kinds: set(&["normal"]),
        });
    }

    #[test]
    fn valid_domain_external_dependency_is_accepted() {
        let graph = [GraphPackage {
            name: String::from("tenancy-domain"),
            dependencies: vec![external_dependency("uuid", "=1.24.0")],
        }];
        assert!(validate_graph(&graph, &rules()).is_empty());
    }

    #[test]
    fn domain_importing_sqlx_is_rejected() {
        let graph = [GraphPackage {
            name: String::from("tenancy-domain"),
            dependencies: vec![external_dependency("sqlx", "=0.8.0")],
        }];
        let violations = validate_graph(&graph, &rules());
        assert!(has_violation(&violations, "sqlx"));
        assert!(has_violation(&violations, "not explicitly allowed"));
    }

    #[test]
    fn platform_binary_importing_cell_application_is_rejected() {
        let graph = [GraphPackage {
            name: String::from("platform-api"),
            dependencies: vec![workspace_dependency("cell-application")],
        }];
        let violations = validate_graph(&graph, &rules());
        assert!(has_violation(&violations, "Platform binary"));
    }

    #[test]
    fn application_importing_another_application_is_rejected() {
        let graph = [GraphPackage {
            name: String::from("platform-application"),
            dependencies: vec![workspace_dependency("cell-application")],
        }];
        let violations = validate_graph(&graph, &rules());
        assert!(has_violation(&violations, "application crate"));
    }

    #[test]
    fn deployable_binary_normal_dependency_on_test_support_is_rejected() {
        let graph = [GraphPackage {
            name: String::from("platform-api"),
            dependencies: vec![workspace_dependency("test-support")],
        }];
        let violations = validate_graph(&graph, &rules());
        assert!(has_violation(
            &violations,
            "test-support as a normal dependency"
        ));
    }

    #[test]
    fn non_exact_direct_dependency_is_rejected() {
        let graph = [GraphPackage {
            name: String::from("tenancy-domain"),
            dependencies: vec![external_dependency("uuid", "1.24.0")],
        }];
        let violations = validate_graph(&graph, &rules());
        assert!(has_violation(&violations, "non-exact requirement"));
    }

    #[test]
    fn forbidden_generic_package_name_is_rejected() {
        let graph = [GraphPackage {
            name: String::from("common"),
            dependencies: Vec::new(),
        }];
        let violations = validate_graph(&graph, &rules());
        assert!(has_violation(&violations, "forbidden generic crate name"));
    }

    #[test]
    fn platform_binary_importing_cell_postgres_is_rejected() {
        let mut rules = rules();
        allow_workspace_edge(&mut rules, "platform-api", "cell-postgres");
        let graph = [GraphPackage {
            name: String::from("platform-api"),
            dependencies: vec![workspace_dependency("cell-postgres")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "Platform binary"
        ));
    }

    #[test]
    fn cell_binary_importing_platform_postgres_is_rejected() {
        let mut rules = rules();
        allow_workspace_edge(&mut rules, "cell-api", "platform-postgres");
        let graph = [GraphPackage {
            name: String::from("cell-api"),
            dependencies: vec![workspace_dependency("platform-postgres")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "Cell binary"
        ));
    }

    #[test]
    fn runtime_binary_importing_migrations_is_rejected() {
        let mut rules = rules();
        allow_workspace_edge(&mut rules, "platform-api", "platform-migrations");
        let graph = [GraphPackage {
            name: String::from("platform-api"),
            dependencies: vec![workspace_dependency("platform-migrations")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "runtime binary"
        ));
    }

    #[test]
    fn tenant_router_importing_postgres_runtime_is_rejected() {
        let mut rules = rules();
        allow_workspace_edge(&mut rules, "tenant-router", "postgres-runtime");
        let graph = [GraphPackage {
            name: String::from("tenant-router"),
            dependencies: vec![workspace_dependency("postgres-runtime")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "tenant-router"
        ));
    }

    #[test]
    fn deployable_importing_qualification_tool_is_rejected() {
        let mut rules = rules();
        allow_workspace_edge(&mut rules, "platform-api", "postgres-qualification");
        let graph = [GraphPackage {
            name: String::from("platform-api"),
            dependencies: vec![workspace_dependency("postgres-qualification")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "qualification tool"
        ));
    }

    #[test]
    fn application_importing_secret_resolution_is_rejected() {
        let mut rules = rules();
        allow_workspace_edge(&mut rules, "cell-application", "secret-resolution");
        let graph = [GraphPackage {
            name: String::from("cell-application"),
            dependencies: vec![workspace_dependency("secret-resolution")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "secret adapter"
        ));
    }

    #[test]
    fn deployable_binary_direct_sqlx_dependency_is_rejected() {
        let mut rules = rules();
        rules
            .allowed_external_dependencies
            .push(AllowedExternalDependency {
                from: String::from("platform-api"),
                dependency: String::from("sqlx"),
                kinds: set(&["normal"]),
            });
        let graph = [GraphPackage {
            name: String::from("platform-api"),
            dependencies: vec![external_dependency("sqlx", "=0.9.0")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "outside the PostgreSQL provider"
        ));
    }

    #[test]
    fn lossy_assignment_epoch_cast_in_production_source_is_rejected() {
        let root = Path::new("/workspace");
        let path = root.join("crates/cell-postgres/src/lossy.rs");
        let violations = source_violations(
            root,
            &path,
            "pub fn lossy(epoch: AssignmentEpoch) { let value = epoch.get() as i64; }",
            &rules(),
        );
        assert!(has_violation(&violations, "lossy assignment-epoch"));
    }

    #[test]
    fn postgres_url_literal_in_production_source_is_rejected() {
        let root = Path::new("/workspace");
        let path = root.join("apps/platform-api/src/main.rs");
        let violations = source_violations(
            root,
            &path,
            "const CREDENTIAL: &str = \"postgres://example.invalid/database\";",
            &rules(),
        );
        assert!(has_violation(&violations, "PostgreSQL URL literal"));
    }

    #[test]
    fn message_domain_importing_serde_is_rejected() {
        let mut rules = rules();
        rules
            .classes
            .message_domain
            .insert(String::from("message-domain"));
        rules.classes.domain.insert(String::from("message-domain"));
        rules
            .allowed_external_dependencies
            .push(AllowedExternalDependency {
                from: String::from("message-domain"),
                dependency: String::from("serde"),
                kinds: set(&["normal"]),
            });
        let graph = [GraphPackage {
            name: String::from("message-domain"),
            dependencies: vec![external_dependency("serde", "=1.0.229")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "message-domain imports forbidden"
        ));
    }

    #[test]
    fn message_codec_importing_sqlx_is_rejected() {
        let mut rules = rules();
        rules
            .classes
            .contract_codec
            .insert(String::from("message-codec-json"));
        rules
            .allowed_external_dependencies
            .push(AllowedExternalDependency {
                from: String::from("message-codec-json"),
                dependency: String::from("sqlx"),
                kinds: set(&["normal"]),
            });
        let graph = [GraphPackage {
            name: String::from("message-codec-json"),
            dependencies: vec![external_dependency("sqlx", "=0.9.0")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "message-codec-json imports forbidden"
        ));
    }

    #[test]
    fn platform_api_importing_postgres_message_store_is_rejected() {
        let mut rules = rules();
        rules
            .classes
            .message_store_provider
            .insert(String::from("postgres-message-store"));
        allow_workspace_edge(&mut rules, "platform-api", "postgres-message-store");
        let graph = [GraphPackage {
            name: String::from("platform-api"),
            dependencies: vec![workspace_dependency("postgres-message-store")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "deployable binary"
        ));
    }

    #[test]
    fn tenant_router_importing_message_domain_is_rejected() {
        let mut rules = rules();
        rules
            .classes
            .message_domain
            .insert(String::from("message-domain"));
        allow_workspace_edge(&mut rules, "tenant-router", "message-domain");
        let graph = [GraphPackage {
            name: String::from("tenant-router"),
            dependencies: vec![workspace_dependency("message-domain")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "tenant-router"
        ));
    }

    #[test]
    fn cell_application_importing_message_codec_is_rejected() {
        let mut rules = rules();
        rules
            .classes
            .contract_codec
            .insert(String::from("message-codec-json"));
        allow_workspace_edge(&mut rules, "cell-application", "message-codec-json");
        let graph = [GraphPackage {
            name: String::from("cell-application"),
            dependencies: vec![workspace_dependency("message-codec-json")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "domain/application crate"
        ));
    }

    #[test]
    fn deployable_importing_message_qualification_is_rejected() {
        let mut rules = rules();
        rules
            .classes
            .qualification_tool
            .insert(String::from("message-store-qualification"));
        rules
            .classes
            .message_qualification_tool
            .insert(String::from("message-store-qualification"));
        allow_workspace_edge(&mut rules, "platform-api", "message-store-qualification");
        let graph = [GraphPackage {
            name: String::from("platform-api"),
            dependencies: vec![workspace_dependency("message-store-qualification")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "qualification tool"
        ));
    }

    #[test]
    fn broker_dependency_introduction_is_rejected() {
        let mut rules = rules();
        rules
            .allowed_external_dependencies
            .push(AllowedExternalDependency {
                from: String::from("platform-api"),
                dependency: String::from("async-nats"),
                kinds: set(&["normal"]),
            });
        let graph = [GraphPackage {
            name: String::from("platform-api"),
            dependencies: vec![external_dependency("async-nats", "=0.1.0")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "outside the approved NATS provider"
        ));
    }

    #[test]
    fn message_domain_importing_async_nats_is_rejected() {
        let mut rules = rules();
        rules.classes.domain.insert(String::from("message-domain"));
        rules
            .allowed_external_dependencies
            .push(AllowedExternalDependency {
                from: String::from("message-domain"),
                dependency: String::from("async-nats"),
                kinds: set(&["normal"]),
            });
        let graph = [GraphPackage {
            name: String::from("message-domain"),
            dependencies: vec![external_dependency("async-nats", "=0.50.0")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "outside the approved NATS provider"
        ));
    }

    #[test]
    fn platform_worker_importing_nats_admin_is_rejected() {
        let mut rules = rules();
        rules
            .classes
            .platform_binary
            .insert(String::from("platform-worker"));
        allow_workspace_edge(&mut rules, "platform-worker", "nats-jetstream-admin");
        let graph = [GraphPackage {
            name: String::from("platform-worker"),
            dependencies: vec![workspace_dependency("nats-jetstream-admin")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "administration provider"
        ));
    }

    #[test]
    fn tenant_router_importing_nats_runtime_is_rejected() {
        let mut rules = rules();
        allow_workspace_edge(&mut rules, "tenant-router", "nats-jetstream");
        let graph = [GraphPackage {
            name: String::from("tenant-router"),
            dependencies: vec![workspace_dependency("nats-jetstream")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "tenant-router"
        ));
    }

    #[test]
    fn platform_runtime_importing_cell_provider_is_rejected() {
        let mut rules = rules();
        allow_workspace_edge(&mut rules, "platform-message-runtime", "cell-postgres");
        let graph = [GraphPackage {
            name: String::from("platform-message-runtime"),
            dependencies: vec![workspace_dependency("cell-postgres")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "Platform authority runtime"
        ));
    }

    #[test]
    fn cell_runtime_importing_platform_provider_is_rejected() {
        let mut rules = rules();
        allow_workspace_edge(&mut rules, "cell-message-runtime", "platform-postgres");
        let graph = [GraphPackage {
            name: String::from("cell-message-runtime"),
            dependencies: vec![workspace_dependency("platform-postgres")],
        }];
        assert!(has_violation(
            &validate_graph(&graph, &rules),
            "Cell authority runtime"
        ));
    }

    #[test]
    fn worker_get_or_create_consumer_is_rejected() {
        let root = Path::new("/workspace");
        let path = root.join("apps/platform-worker/src/main.rs");
        let violations = source_violations(
            root,
            &path,
            "fn run() { let _ = context.get_or_create_consumer(); }",
            &rules(),
        );
        assert!(has_violation(&violations, "get_or_create_consumer"));
    }

    #[test]
    fn worker_core_nats_publish_is_rejected() {
        let root = Path::new("/workspace");
        let path = root.join("apps/cell-worker/src/main.rs");
        let violations = source_violations(
            root,
            &path,
            "async fn run(client: Client) { client.publish(\"subject\", vec![]).await; }",
            &rules(),
        );
        assert!(has_violation(&violations, "Core NATS publication"));
    }

    #[test]
    fn domain_ambient_uuid_v7_is_rejected() {
        let root = Path::new("/workspace");
        let path = root.join("crates/tenancy-domain/src/lib.rs");
        let violations = source_violations(
            root,
            &path,
            "pub fn generate() { let _ = Uuid::now_v7(); }",
            &rules(),
        );
        assert!(has_violation(&violations, "ambient UUID generation"));
    }

    #[test]
    fn tenant_identifier_subject_interpolation_is_rejected() {
        let root = Path::new("/workspace");
        let path = root.join("crates/nats-jetstream/src/lib.rs");
        let violations = source_violations(
            root,
            &path,
            "fn subject(tenant_id: &str) { let subject = format!(\"edtech.v1.command.{tenant_id}\"); }",
            &rules(),
        );
        assert!(has_violation(&violations, "tenant identifier"));
    }

    #[test]
    fn direct_tokio_spawn_is_rejected() {
        let root = Path::new("/workspace");
        let path = root.join("crates/platform-message-runtime/src/lib.rs");
        let violations = source_violations(
            root,
            &path,
            "pub fn run() { tokio::spawn(async {}); }",
            &rules(),
        );
        assert!(has_violation(&violations, "starts a Tokio task directly"));
    }

    #[test]
    fn floating_nats_image_is_rejected() {
        let root = Path::new("/workspace");
        let path = root.join("infra/local/nats/compose.yml");
        let violations = source_violations(
            root,
            &path,
            "services:\n  nats:\n    image: nats:latest\n",
            &rules(),
        );
        assert!(has_violation(&violations, "floating NATS image"));
    }

    #[test]
    fn exactly_once_documentation_claim_is_rejected_by_architecture() {
        let root = Path::new("/workspace");
        let path = root.join("docs/architecture/transport.md");
        let violations = source_violations(
            root,
            &path,
            "The runtime guarantees exactly-once processing.\n",
            &rules(),
        );
        assert!(has_violation(&violations, "forbidden exactly-once claim"));
    }

    #[test]
    fn public_application_json_value_is_rejected() {
        let root = Path::new("/workspace");
        let path = root.join("crates/cell-application/src/lib.rs");
        let violations = source_violations(
            root,
            &path,
            "pub fn untyped() -> serde_json::Value { unreachable!() }",
            &rules(),
        );
        assert!(has_violation(&violations, "serde_json::Value"));
    }

    #[test]
    fn public_application_sqlx_transaction_is_rejected() {
        let root = Path::new("/workspace");
        let path = root.join("crates/cell-application/src/lib.rs");
        let violations = source_violations(
            root,
            &path,
            "pub fn leak(value: sqlx::Transaction<'_, sqlx::Postgres>) { drop(value); }",
            &rules(),
        );
        assert!(has_violation(&violations, "SQLx type"));
    }

    #[test]
    fn exactly_once_claim_wording_is_rejected_but_denials_are_allowed() {
        assert!(super::exactly_once_claim(
            "The system guarantees exactly-once delivery."
        ));
        assert!(!super::exactly_once_claim(
            "The system does not prove exactly-once delivery."
        ));
        assert!(!super::exactly_once_claim(
            "Documentation must not claim exactly-once processing."
        ));
    }
}
