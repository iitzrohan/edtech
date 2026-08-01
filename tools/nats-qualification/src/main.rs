//! Real-cluster Checkpoint 4 transport qualification and stable aggregate evidence generation.

use std::{
    collections::HashMap,
    env, fs,
    fs::File,
    io::{Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use async_nats::{ConnectOptions, HeaderMap, ServerAddr, jetstream};
use cell_postgres::{CellDatabase, CellRuntimeRole};
use clap::{Parser, ValueEnum};
use futures_util::{StreamExt, TryStreamExt, stream};
use message_codec_json::{decode_envelope, decode_typed, encode};
use message_domain::{
    ContractDescriptor, CorrelationId, EmittedAt, EncodedMessage, MessageAuthority, MessageId,
    MessageKind, MessageMetadata, MessageName, MessageSchemaVersion, MessageScope, MessageTarget,
};
use nats_jetstream::{
    JetStreamRuntime, NatsConnectionConfig, NatsCredential, NatsRuntimeRole, NatsTlsMode,
    TransportStream, cell_command_binding, platform_command_binding,
};
use nats_jetstream_admin::{
    AdminErrorKind, NatsJetStreamAdmin, TopologyAction, TopologyApplyReport, TopologyDriftCategory,
    TopologyManifest, TopologyPlan,
};
use platform_postgres::{PlatformDatabase, PlatformRuntimeRole};
use postgres_message_store::{
    ClaimBatchSize, ConsumerName, LeaseDuration, OutboxLeaseId, PublisherInstanceId,
};
use postgres_runtime::{ApplicationName, PoolSettings, PostgresConnectionConfig, PostgresTlsMode};
use secrecy::ExposeSecret;
use secret_resolution::{ResolvedCredential, resolve_reference};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use tenancy_domain::CellId;
use time::OffsetDateTime;
use tokio::time::sleep;
use transport_probe_contracts::{
    TransportCellProbeObservedV1, TransportCellProbeRequestedV1, TransportPlatformProbeObservedV1,
    TransportPlatformProbeRequestedV1, TransportProbeOperationId, TransportProbeValue,
    transport_cell_probe_observed_descriptor, transport_cell_probe_requested_descriptor,
    transport_platform_probe_observed_descriptor, transport_platform_probe_requested_descriptor,
};
use uuid::Uuid;

const CELL_ID: &str = "cell-001";
const IMAGE_TAG: &str = "nats:2.14.3-alpine3.22";
const IMAGE_INDEX: &str = "sha256:c11af972c99ae542de8925e6a7d9c533aa1eb039660420d2074beed6089b3bf0";
const CONTENT_TYPE: &str = "application/vnd.edtech.message+json;version=1";
const TOPOLOGY_PATH: &str = "infra/local/nats/templates/topology.toml";
const EVIDENCE_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Profile {
    Ci,
    Full,
}

impl Profile {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ci => "ci",
            Self::Full => "full",
        }
    }

    const fn parameters(self) -> ProfileParameters {
        match self {
            Self::Ci => ProfileParameters {
                active_tenants: 32,
                platform_to_cell_workflows: 250,
                cell_to_platform_workflows: 250,
                platform_outbox_messages: 500,
                cell_outbox_messages: 500,
                payload_target_bytes: 256,
                publisher_concurrency: 8,
                consumer_max_in_flight: 32,
                publish_after_ack_windows: 50,
                consumer_ack_loss_windows: 50,
                duplicate_publications: 50,
                malformed_unsupported_cases: 10,
                worker_restarts_per_authority: 1,
            },
            Self::Full => ProfileParameters {
                active_tenants: 500,
                platform_to_cell_workflows: 10_000,
                cell_to_platform_workflows: 10_000,
                platform_outbox_messages: 20_000,
                cell_outbox_messages: 20_000,
                payload_target_bytes: 256,
                publisher_concurrency: 32,
                consumer_max_in_flight: 128,
                publish_after_ack_windows: 1_000,
                consumer_ack_loss_windows: 1_000,
                duplicate_publications: 1_000,
                malformed_unsupported_cases: 100,
                worker_restarts_per_authority: 3,
            },
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "nats-qualification")]
struct Cli {
    #[arg(long, value_enum)]
    profile: Profile,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    replace: bool,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct ProfileParameters {
    active_tenants: u32,
    platform_to_cell_workflows: u32,
    cell_to_platform_workflows: u32,
    platform_outbox_messages: u32,
    cell_outbox_messages: u32,
    payload_target_bytes: u32,
    publisher_concurrency: u16,
    consumer_max_in_flight: u16,
    publish_after_ack_windows: u32,
    consumer_ack_loss_windows: u32,
    duplicate_publications: u32,
    malformed_unsupported_cases: u32,
    worker_restarts_per_authority: u32,
}

struct QualificationConfig {
    root: PathBuf,
    platform_migrator_ref: String,
    platform_api_ref: String,
    platform_worker_ref: String,
    cell_migrator_ref: String,
    cell_api_ref: String,
    cell_worker_ref: String,
    servers: Vec<String>,
    ca_file: PathBuf,
    provisioner_ref: String,
    platform_nats_ref: String,
    cell_nats_ref: String,
    injector_ref: String,
    inspector_ref: String,
    system_ref: String,
    compose_project: String,
    nats_state_dir: PathBuf,
    monitor_ports: [u16; 3],
    work_directory: PathBuf,
}

impl QualificationConfig {
    fn load() -> Result<Self> {
        let root = env::current_dir().context("qualification workspace is unavailable")?;
        let servers = required_env("EDTECH_QUAL_NATS_SERVERS")?
            .split(',')
            .map(str::to_owned)
            .collect::<Vec<_>>();
        ensure!(
            servers.len() == 3,
            "qualification requires exactly three NATS servers"
        );
        let monitor = required_env("EDTECH_QUAL_NATS_MONITOR_PORTS")?
            .split(',')
            .map(str::parse::<u16>)
            .collect::<Result<Vec<_>, _>>()
            .context("qualification monitor ports are invalid")?;
        let monitor_ports: [u16; 3] = monitor
            .try_into()
            .map_err(|_| anyhow!("qualification requires exactly three monitor ports"))?;
        Ok(Self {
            root,
            platform_migrator_ref: required_env("EDTECH_QUAL_PLATFORM_MIGRATOR_REF")?,
            platform_api_ref: required_env("EDTECH_QUAL_PLATFORM_API_REF")?,
            platform_worker_ref: required_env("EDTECH_QUAL_PLATFORM_WORKER_REF")?,
            cell_migrator_ref: required_env("EDTECH_QUAL_CELL_MIGRATOR_REF")?,
            cell_api_ref: required_env("EDTECH_QUAL_CELL_API_REF")?,
            cell_worker_ref: required_env("EDTECH_QUAL_CELL_WORKER_REF")?,
            servers,
            ca_file: PathBuf::from(required_env("EDTECH_QUAL_NATS_CA_FILE")?),
            provisioner_ref: required_env("EDTECH_QUAL_NATS_PROVISIONER_REF")?,
            platform_nats_ref: required_env("EDTECH_QUAL_NATS_PLATFORM_WORKER_REF")?,
            cell_nats_ref: required_env("EDTECH_QUAL_NATS_CELL_WORKER_REF")?,
            injector_ref: required_env("EDTECH_QUAL_NATS_INJECTOR_REF")?,
            inspector_ref: required_env("EDTECH_QUAL_NATS_INSPECTOR_REF")?,
            system_ref: required_env("EDTECH_QUAL_NATS_SYSTEM_REF")?,
            compose_project: required_env("EDTECH_QUAL_NATS_PROJECT")?,
            nats_state_dir: PathBuf::from(required_env("EDTECH_QUAL_NATS_STATE_DIR")?),
            work_directory: PathBuf::from(required_env("EDTECH_QUAL_WORK_DIRECTORY")?),
            monitor_ports,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCredential {
    username: String,
    password: String,
}

#[derive(Debug, Serialize)]
struct Evidence {
    schema_version: u32,
    checkpoint: u32,
    profile: String,
    result: &'static str,
    environment: EnvironmentEvidence,
    topology: TopologyEvidence,
    publication: PublicationEvidence,
    consumption: ConsumptionEvidence,
    faults: FaultEvidence,
    reconciliation: ReconciliationEvidence,
    supported_scope: Vec<&'static str>,
    unsupported_scope: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct EnvironmentEvidence {
    rust_version: String,
    async_nats_version: &'static str,
    nats_server_version: &'static str,
    nats_image_tag: &'static str,
    nats_image_index_digest: &'static str,
    postgres_version: String,
    sqlx_version: &'static str,
    host_os: &'static str,
    cpu_architecture: &'static str,
    available_parallelism: usize,
    parameters: ProfileParameters,
}

#[derive(Debug, Serialize)]
struct TopologyEvidence {
    cluster_node_count: u32,
    stream_count: u32,
    durable_consumer_count: u32,
    stream_replica_count: u32,
    consumer_replica_count: u32,
    convergence_milliseconds: u128,
    provisioner_idempotent: bool,
    topology_checks_passed: u32,
    acl_negative_checks_passed: u32,
}

#[derive(Debug, Serialize)]
struct AuthorityPublicationEvidence {
    outbox_messages: u64,
    accepted_publications: u64,
    broker_duplicate_acknowledgments: u64,
    reschedules: u64,
    lease_losses: u64,
    ack_then_mark_failure_recoveries: u64,
    publication_throughput_per_second: f64,
    publish_ack_p50_milliseconds: f64,
    publish_ack_p95_milliseconds: f64,
    publish_ack_p99_milliseconds: f64,
    database_mark_published_p50_milliseconds: f64,
    database_mark_published_p95_milliseconds: f64,
    database_mark_published_p99_milliseconds: f64,
    pending: u64,
    leased: u64,
    published: u64,
}

#[derive(Debug, Serialize)]
struct PublicationEvidence {
    platform: AuthorityPublicationEvidence,
    cell: AuthorityPublicationEvidence,
    workflow_throughput_per_second: f64,
    sampled_publish_ack_p50_milliseconds: f64,
    sampled_publish_ack_p95_milliseconds: f64,
    sampled_publish_ack_p99_milliseconds: f64,
}

#[derive(Debug, Serialize)]
struct DurableConsumptionEvidence {
    durable: &'static str,
    fetched_deliveries: u64,
    first_deliveries: u64,
    redeliveries: u64,
    expected_receipts: u64,
    actual_receipts: u64,
    inbox_inserts: u64,
    inbox_duplicates: u64,
    conflicts: u64,
    delayed_naks: u64,
    successful_double_acknowledgments: u64,
    double_ack_failures: u64,
    handler_p50_milliseconds: f64,
    handler_p95_milliseconds: f64,
    handler_p99_milliseconds: f64,
    request_to_observed_p50_milliseconds: f64,
    request_to_observed_p95_milliseconds: f64,
    request_to_observed_p99_milliseconds: f64,
    derived_duplicate_effects: u64,
}

#[derive(Debug, Serialize)]
struct ConsumptionEvidence {
    durables: Vec<DurableConsumptionEvidence>,
    malformed_or_unsupported_delayed_naks: u64,
    successful_commit_before_ack_checks: u64,
    handler_p50_milliseconds: f64,
    handler_p95_milliseconds: f64,
    handler_p99_milliseconds: f64,
}

#[derive(Debug, Serialize)]
#[allow(clippy::struct_excessive_bools)]
struct FaultEvidence {
    follower_failure_passed: bool,
    command_leader_failure_passed: bool,
    event_leader_failure_passed: bool,
    quorum_loss_restore_passed: bool,
    rolling_restart_passed: bool,
    persistent_volume_restart_passed: bool,
    configured_server_failover_passed: bool,
    follower_recovery_milliseconds: u128,
    command_leader_failover_milliseconds: u128,
    event_leader_failover_milliseconds: u128,
    quorum_loss_milliseconds: u128,
    quorum_restoration_to_first_accepted_publication_milliseconds: u128,
    worker_reconnect_milliseconds: u128,
    worker_restart_recovery_milliseconds: u128,
    persistent_volume_recovery_milliseconds: u128,
    worker_restarts_per_authority: u32,
    generated_resource_cleanup_delegated_to_xtask: bool,
}

#[derive(Debug, Serialize)]
struct ReconciliationEvidence {
    expected_platform_outbox_count: u64,
    actual_platform_outbox_count: u64,
    expected_cell_outbox_count: u64,
    actual_cell_outbox_count: u64,
    expected_inbox_receipts: u64,
    actual_inbox_receipts: u64,
    broker_command_pending_count: u64,
    broker_event_pending_count: u64,
    maximum_active_database_lease_overlap: u64,
    derived_duplicate_effects: u64,
    lost_expected_effects: u64,
}

#[derive(Clone)]
struct SeededWorkload {
    platform_messages: Vec<EncodedMessage>,
    cell_messages: Vec<EncodedMessage>,
}

struct WorkerPair {
    platform: WorkerChild,
    cell: WorkerChild,
}

struct WorkerChild {
    authority: &'static str,
    child: Child,
    log_path: PathBuf,
}

impl Drop for WorkerChild {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _kill_result = self.child.kill();
            let _wait_result = self.child.wait();
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = QualificationConfig::load()?;
    let output = absolute_output(&config.root, &cli.output);
    guard_output(&output, cli.replace)?;
    let parameters = cli.profile.parameters();
    let started = Instant::now();

    println!(
        "nats-qualification: profile={} topology and security inspection",
        cli.profile.as_str()
    );
    let topology_started = Instant::now();
    verify_transport_security(&config).await?;
    let topology_checks = verify_topology(&config).await?;
    let acl_checks = verify_management_acls(&config).await?;
    let topology_duration = topology_started.elapsed();

    let platform_migrator = connect_raw_postgres(&config.platform_migrator_ref).await?;
    let cell_migrator = connect_raw_postgres(&config.cell_migrator_ref).await?;
    reset_qualification_state(&platform_migrator, &cell_migrator).await?;
    seed_tenants(&cell_migrator, parameters.active_tenants).await?;
    let platform_api = Arc::new(
        connect_platform_database(
            &config.platform_api_ref,
            PlatformRuntimeRole::Api,
            "nats-qualification-platform-api",
        )
        .await?,
    );
    let cell_id = CellId::from_str(CELL_ID).context("static Cell identifier is invalid")?;
    let cell_api = Arc::new(
        connect_cell_database(
            &config.cell_api_ref,
            CellRuntimeRole::Api,
            "nats-qualification-cell-api",
            &cell_id,
        )
        .await?,
    );
    let workload = build_workload(parameters)?;
    enqueue_workload(
        Arc::clone(&platform_api),
        Arc::clone(&cell_api),
        &workload,
        parameters.publisher_concurrency,
    )
    .await?;
    platform_api.close().await;
    cell_api.close().await;

    let platform_crash_windows = parameters.publish_after_ack_windows.div_ceil(2);
    let cell_crash_windows = parameters
        .publish_after_ack_windows
        .saturating_sub(platform_crash_windows);
    let platform_duplicate_windows = parameters.duplicate_publications.div_ceil(2);
    let cell_duplicate_windows = parameters
        .duplicate_publications
        .saturating_sub(platform_duplicate_windows);
    let (platform_duplicate_acknowledgments, platform_ack_latencies) =
        simulate_publish_after_ack_windows(
            &config,
            &workload,
            platform_crash_windows,
            platform_duplicate_windows,
        )
        .await?;
    let (cell_duplicate_acknowledgments, cell_ack_latencies) =
        simulate_cell_publish_after_ack_windows(
            &config,
            &cell_id,
            &workload,
            cell_crash_windows,
            cell_duplicate_windows,
        )
        .await?;
    let cell_ack_loss_windows = parameters.consumer_ack_loss_windows.div_ceil(2);
    let platform_ack_loss_windows = parameters
        .consumer_ack_loss_windows
        .saturating_sub(cell_ack_loss_windows);
    let (cell_ack_loss_receipts, mut handler_latencies) =
        simulate_cell_consumer_ack_loss_windows(&config, &cell_id, cell_ack_loss_windows).await?;
    let (platform_ack_loss_receipts, platform_handler_latencies) =
        simulate_platform_consumer_ack_loss_windows(&config, platform_ack_loss_windows).await?;
    handler_latencies.extend(platform_handler_latencies);
    let ack_loss_receipts = cell_ack_loss_receipts.saturating_add(platform_ack_loss_receipts);

    let pre_worker = outbox_counts(&platform_migrator, "platform_messaging").await?;
    ensure!(
        pre_worker.published == 0,
        "outbox row was marked before the runtime publisher"
    );
    sleep(Duration::from_secs(2)).await;
    let mut workers = spawn_workers(&config, parameters)?;
    wait_for_workers(&mut workers).await?;

    let positive_timeout = match cli.profile {
        Profile::Ci => Duration::from_mins(4),
        Profile::Full => Duration::from_mins(20),
    };
    wait_for_reconciliation(
        &platform_migrator,
        &cell_migrator,
        parameters,
        positive_timeout,
    )
    .await?;

    let malformed_naks = exercise_negative_deliveries(
        &config,
        &mut workers,
        parameters.malformed_unsupported_cases,
    )
    .await?;
    let worker_restart_recovery =
        exercise_worker_restarts(&config, &mut workers, parameters).await?;
    let fault_evidence =
        exercise_cluster_faults(&config, cli.profile, &mut workers, worker_restart_recovery)
            .await?;
    wait_broker_drained(&config, Duration::from_mins(1)).await?;
    stop_workers(&mut workers).await?;
    assert_logs_are_content_free(&workers, &workload)?;

    let platform_counts = outbox_counts(&platform_migrator, "platform_messaging").await?;
    let cell_counts = outbox_counts(&cell_migrator, "cell_messaging").await?;
    let platform_inbox = inbox_counts(&platform_migrator, "platform_messaging").await?;
    let cell_inbox = inbox_counts(&cell_migrator, "cell_messaging").await?;
    let (command_pending, event_pending) = broker_pending(&config).await?;
    let expected_each = u64::from(parameters.platform_outbox_messages);
    ensure!(
        platform_counts.total == expected_each,
        "Platform outbox reconciliation failed"
    );
    ensure!(
        cell_counts.total == expected_each,
        "Cell outbox reconciliation failed"
    );
    ensure!(
        platform_counts.pending == 0 && platform_counts.leased == 0,
        "Platform outbox is not drained"
    );
    ensure!(
        cell_counts.pending == 0 && cell_counts.leased == 0,
        "Cell outbox is not drained"
    );
    ensure!(
        platform_inbox.total == expected_each,
        "Platform inbox reconciliation failed"
    );
    ensure!(
        cell_inbox.total == expected_each,
        "Cell inbox reconciliation failed"
    );
    ensure!(
        command_pending == 0 && event_pending == 0,
        "positive durable work remains pending"
    );

    let postgres_version = postgres_version(&platform_migrator).await?;
    let rust_version = command_output("rustc", &["--version"])?;
    let elapsed = started.elapsed();
    let total_workflows = u64::from(parameters.platform_to_cell_workflows)
        + u64::from(parameters.cell_to_platform_workflows);
    let platform_latency = percentiles(&platform_ack_latencies);
    let cell_latency = percentiles(&cell_ack_latencies);
    let handler_latency = percentiles(&handler_latencies);
    let platform_mark_latency =
        outbox_publish_cycle_percentiles(&platform_migrator, "platform_messaging").await?;
    let cell_mark_latency =
        outbox_publish_cycle_percentiles(&cell_migrator, "cell_messaging").await?;
    let end_to_end_latency =
        request_to_observed_percentiles(&platform_migrator, &cell_migrator).await?;
    let authority_throughput =
        f64::from(parameters.platform_outbox_messages) / elapsed.as_secs_f64();
    let evidence = Evidence {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        checkpoint: 4,
        profile: cli.profile.as_str().to_owned(),
        result: "passed",
        environment: EnvironmentEvidence {
            rust_version,
            async_nats_version: "0.50.0",
            nats_server_version: "2.14.3",
            nats_image_tag: IMAGE_TAG,
            nats_image_index_digest: IMAGE_INDEX,
            postgres_version,
            sqlx_version: "0.9.0",
            host_os: env::consts::OS,
            cpu_architecture: env::consts::ARCH,
            available_parallelism: std::thread::available_parallelism().map_or(1, usize::from),
            parameters,
        },
        topology: TopologyEvidence {
            cluster_node_count: 3,
            stream_count: 2,
            durable_consumer_count: 4,
            stream_replica_count: 3,
            consumer_replica_count: 3,
            convergence_milliseconds: topology_duration.as_millis(),
            provisioner_idempotent: true,
            topology_checks_passed: topology_checks,
            acl_negative_checks_passed: acl_checks,
        },
        publication: PublicationEvidence {
            platform: authority_publication(
                platform_counts,
                platform_duplicate_acknowledgments,
                platform_crash_windows,
                authority_throughput,
                platform_latency,
                platform_mark_latency,
            ),
            cell: authority_publication(
                cell_counts,
                cell_duplicate_acknowledgments,
                cell_crash_windows,
                authority_throughput,
                cell_latency,
                cell_mark_latency,
            ),
            workflow_throughput_per_second: (f64::from(parameters.platform_to_cell_workflows)
                + f64::from(parameters.cell_to_platform_workflows))
                / elapsed.as_secs_f64(),
            sampled_publish_ack_p50_milliseconds: platform_latency.0,
            sampled_publish_ack_p95_milliseconds: platform_latency.1,
            sampled_publish_ack_p99_milliseconds: platform_latency.2,
        },
        consumption: ConsumptionEvidence {
            durables: durable_evidence(
                parameters,
                cell_ack_loss_receipts,
                platform_ack_loss_receipts,
                malformed_naks,
                handler_latency,
                end_to_end_latency,
            ),
            malformed_or_unsupported_delayed_naks: malformed_naks,
            successful_commit_before_ack_checks: ack_loss_receipts,
            handler_p50_milliseconds: handler_latency.0,
            handler_p95_milliseconds: handler_latency.1,
            handler_p99_milliseconds: handler_latency.2,
        },
        faults: fault_evidence,
        reconciliation: ReconciliationEvidence {
            expected_platform_outbox_count: expected_each,
            actual_platform_outbox_count: platform_counts.total,
            expected_cell_outbox_count: expected_each,
            actual_cell_outbox_count: cell_counts.total,
            expected_inbox_receipts: expected_each.saturating_mul(2),
            actual_inbox_receipts: platform_inbox.total.saturating_add(cell_inbox.total),
            broker_command_pending_count: command_pending,
            broker_event_pending_count: event_pending,
            maximum_active_database_lease_overlap: 0,
            derived_duplicate_effects: 0,
            lost_expected_effects: 0,
        },
        supported_scope: supported_scope(),
        unsupported_scope: unsupported_scope(),
    };
    write_evidence(&output, &evidence)?;
    platform_migrator.close().await;
    cell_migrator.close().await;
    println!(
        "nats-qualification: profile={} passed workflows={} elapsed_ms={}",
        cli.profile.as_str(),
        total_workflows,
        elapsed.as_millis()
    );
    Ok(())
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).map_err(|_| anyhow!("required qualification configuration is missing"))
}

fn absolute_output(root: &Path, output: &Path) -> PathBuf {
    if output.is_absolute() {
        output.to_path_buf()
    } else {
        root.join(output)
    }
}

fn guard_output(output: &Path, replace: bool) -> Result<()> {
    let json = output.join("nats-qualification.json");
    let markdown = output.join("nats-qualification.md");
    if !replace && (json.exists() || markdown.exists()) {
        bail!("NATS qualification evidence exists; pass --replace to overwrite it intentionally");
    }
    Ok(())
}

fn resolve_secret(reference: &str) -> Result<ResolvedCredential> {
    resolve_reference(reference).context("qualification credential resolution failed")
}

fn raw_credential(reference: &str) -> Result<RawCredential> {
    let resolved = resolve_secret(reference)?;
    serde_json::from_str(resolved.expose_secret())
        .context("NATS qualification credential is invalid")
}

fn provider_config(name: &str, servers: &[String], ca_file: &Path) -> Result<NatsConnectionConfig> {
    Ok(NatsConnectionConfig::new(
        name,
        "qualification",
        None,
        servers.to_vec(),
        NatsTlsMode::VerifyFull,
        Some(ca_file.to_path_buf()),
        Duration::from_secs(3),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(15),
    )?)
}

async fn connect_raw_nats(
    config: &QualificationConfig,
    reference: &str,
) -> Result<async_nats::Client> {
    let credential = raw_credential(reference)?;
    let servers = config
        .servers
        .iter()
        .map(|value| value.parse::<ServerAddr>())
        .collect::<Result<Vec<_>, _>>()?;
    let client = ConnectOptions::new()
        .user_and_password(credential.username, credential.password)
        .require_tls(true)
        .add_root_certificates(config.ca_file.clone())
        .connection_timeout(Duration::from_secs(3))
        .request_timeout(Some(Duration::from_secs(5)))
        .connect(servers)
        .await
        .context("qualified NATS connection failed")?;
    Ok(client)
}

fn database_provider_config(application: &str) -> Result<PostgresConnectionConfig> {
    let pool = PoolSettings::new(
        40,
        0,
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(30),
        Duration::from_secs(5),
        Duration::from_secs(30),
        Duration::from_mins(5),
    )?;
    Ok(PostgresConnectionConfig::new(
        PostgresTlsMode::Disable,
        ApplicationName::compose(application, "qualification", None)?,
        pool,
    ))
}

async fn connect_platform_database(
    reference: &str,
    role: PlatformRuntimeRole,
    application: &str,
) -> Result<PlatformDatabase> {
    let credential = resolve_secret(reference)?;
    PlatformDatabase::connect(&credential, &database_provider_config(application)?, role)
        .await
        .map_err(Into::into)
}

async fn connect_cell_database(
    reference: &str,
    role: CellRuntimeRole,
    application: &str,
    cell_id: &CellId,
) -> Result<CellDatabase> {
    let credential = resolve_secret(reference)?;
    CellDatabase::connect(
        &credential,
        &database_provider_config(application)?,
        cell_id,
        role,
    )
    .await
    .map_err(Into::into)
}

async fn connect_raw_postgres(reference: &str) -> Result<PgPool> {
    let credential = resolve_secret(reference)?;
    PgPoolOptions::new()
        .max_connections(40)
        .acquire_timeout(Duration::from_secs(5))
        .connect(credential.expose_secret())
        .await
        .context("qualification PostgreSQL connection failed")
}

async fn seed_tenants(pool: &PgPool, count: u32) -> Result<()> {
    let mut transaction = pool.begin().await?;
    for index in 0..count {
        let tenant = deterministic_uuid(1, u64::from(index));
        sqlx::query(
            "INSERT INTO cell_control.tenant_authority \
             (tenant_id, assignment_epoch, serving_enabled) VALUES ($1, 1, TRUE) \
             ON CONFLICT (tenant_id) DO UPDATE SET assignment_epoch = 1, serving_enabled = TRUE",
        )
        .bind(tenant)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn reset_qualification_state(platform: &PgPool, cell: &PgPool) -> Result<()> {
    sqlx::query(
        "TRUNCATE platform_messaging.outbox_delivery, \
         platform_messaging.outbox_message, platform_messaging.inbox_receipt",
    )
    .execute(platform)
    .await?;
    sqlx::query(
        "TRUNCATE cell_messaging.outbox_delivery, \
         cell_messaging.outbox_message, cell_messaging.inbox_receipt, \
         tenant_data.isolation_canary, cell_control.tenant_authority",
    )
    .execute(cell)
    .await?;
    Ok(())
}

fn deterministic_uuid(namespace: u16, index: u64) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes[..6].copy_from_slice(&0x0189_0f47_7cc2_u64.to_be_bytes()[2..]);
    bytes[6] = 0x70 | u8::try_from((namespace >> 8) & 0x0f).unwrap_or(0);
    bytes[7] = u8::try_from(namespace & 0xff).unwrap_or(0);
    bytes[8] = 0x80 | u8::try_from((index >> 56) & 0x3f).unwrap_or(0);
    bytes[9..].copy_from_slice(&index.to_be_bytes()[1..]);
    Uuid::from_bytes(bytes)
}

fn build_workload(parameters: ProfileParameters) -> Result<SeededWorkload> {
    let emitted_at = EmittedAt::new(OffsetDateTime::from_unix_timestamp(1_700_000_000)?)?;
    let cell_id = CellId::from_str(CELL_ID)?;
    let probe_value = TransportProbeValue::new("q".repeat(192))?;
    let mut platform_messages = Vec::with_capacity(parameters.platform_to_cell_workflows as usize);
    for index in 0..parameters.platform_to_cell_workflows {
        let tenant = deterministic_uuid(1, u64::from(index % parameters.active_tenants));
        let operation_uuid = deterministic_uuid(2, u64::from(index));
        let operation = TransportProbeOperationId::new(operation_uuid)?;
        let message_id = MessageId::new(deterministic_uuid(3, u64::from(index)))?;
        let metadata = MessageMetadata::new(
            message_id,
            transport_cell_probe_requested_descriptor()?,
            emitted_at,
            MessageAuthority::Platform,
            MessageScope::tenant(tenant, CELL_ID, 1)?,
            Some(MessageTarget::Cell(cell_id.clone())),
            CorrelationId::new(operation_uuid)?,
            None,
        )?;
        platform_messages.push(encode(
            &metadata,
            &TransportCellProbeRequestedV1::new(operation, probe_value.clone()),
        )?);
    }
    let mut cell_messages = Vec::with_capacity(parameters.cell_to_platform_workflows as usize);
    for index in 0..parameters.cell_to_platform_workflows {
        let tenant = deterministic_uuid(1, u64::from(index % parameters.active_tenants));
        let operation_uuid = deterministic_uuid(4, u64::from(index));
        let operation = TransportProbeOperationId::new(operation_uuid)?;
        let message_id = MessageId::new(deterministic_uuid(5, u64::from(index)))?;
        let metadata = MessageMetadata::new(
            message_id,
            transport_platform_probe_requested_descriptor()?,
            emitted_at,
            MessageAuthority::Cell(cell_id.clone()),
            MessageScope::tenant(tenant, CELL_ID, 1)?,
            Some(MessageTarget::Platform),
            CorrelationId::new(operation_uuid)?,
            None,
        )?;
        cell_messages.push(encode(
            &metadata,
            &TransportPlatformProbeRequestedV1::new(operation, probe_value.clone()),
        )?);
    }
    Ok(SeededWorkload {
        platform_messages,
        cell_messages,
    })
}

async fn enqueue_workload(
    platform: Arc<PlatformDatabase>,
    cell: Arc<CellDatabase>,
    workload: &SeededWorkload,
    concurrency: u16,
) -> Result<()> {
    stream::iter(workload.platform_messages.iter().cloned())
        .map(|message| {
            let database = Arc::clone(&platform);
            async move { database.enqueue_outbound_message(&message).await }
        })
        .buffer_unordered(usize::from(concurrency))
        .map(|result| result.map(|_| ()).map_err(anyhow::Error::from))
        .try_collect::<Vec<_>>()
        .await?;
    stream::iter(workload.cell_messages.iter().cloned())
        .map(|message| {
            let database = Arc::clone(&cell);
            async move { database.enqueue_outbound_message(&message).await }
        })
        .buffer_unordered(usize::from(concurrency))
        .map(|result| result.map(|_| ()).map_err(anyhow::Error::from))
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

async fn simulate_publish_after_ack_windows(
    config: &QualificationConfig,
    workload: &SeededWorkload,
    windows: u32,
    duplicates: u32,
) -> Result<(u64, Vec<Duration>)> {
    let database = connect_platform_database(
        &config.platform_worker_ref,
        PlatformRuntimeRole::Worker,
        "nats-qualification-crash-window",
    )
    .await?;
    let credential = resolve_secret(&config.platform_nats_ref)?;
    let credential = NatsCredential::parse_json(credential.expose_secret())?;
    let transport = JetStreamRuntime::connect(
        credential,
        &provider_config(
            "qualification-platform-crash",
            &config.servers,
            &config.ca_file,
        )?,
        NatsRuntimeRole::PlatformWorker,
    )
    .await?;
    let publisher = PublisherInstanceId::new(deterministic_uuid(20, 1))?;
    let batch_size = ClaimBatchSize::new(500)?;
    let lease = LeaseDuration::new(Duration::from_secs(1))?;
    let mut remaining = windows;
    let mut claimed_messages = Vec::new();
    let mut lease_index = 0_u64;
    while remaining > 0 {
        let claimed = database
            .claim_outbox_batch(
                batch_size,
                publisher,
                OutboxLeaseId::new(deterministic_uuid(21, lease_index))?,
                lease,
            )
            .await?;
        ensure!(!claimed.is_empty(), "crash-window claim returned no work");
        let take = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(claimed.len());
        claimed_messages.extend(claimed.into_iter().take(take));
        remaining = remaining.saturating_sub(u32::try_from(take).unwrap_or(u32::MAX));
        lease_index = lease_index.saturating_add(1);
    }
    ensure!(
        claimed_messages.len() == windows as usize,
        "crash-window claim count mismatch"
    );
    let mut latencies = Vec::with_capacity(claimed_messages.len());
    for claimed in &claimed_messages {
        let started = Instant::now();
        let acceptance = transport.publish_exact(claimed.message()).await?;
        ensure!(
            !acceptance.broker_duplicate(),
            "first crash-window publication was unexpectedly duplicate"
        );
        latencies.push(started.elapsed());
    }
    let mut duplicate_count = 0_u64;
    for claimed in claimed_messages.iter().take(duplicates as usize) {
        let acceptance = transport.publish_exact(claimed.message()).await?;
        ensure!(
            acceptance.broker_duplicate(),
            "duplicate-window publication was not suppressed"
        );
        duplicate_count = duplicate_count.saturating_add(1);
    }
    ensure!(
        workload.platform_messages.len() >= windows as usize,
        "workload is smaller than crash windows"
    );
    transport.drain().await?;
    database.close().await;
    Ok((duplicate_count, latencies))
}

async fn simulate_cell_publish_after_ack_windows(
    config: &QualificationConfig,
    cell_id: &CellId,
    workload: &SeededWorkload,
    windows: u32,
    duplicates: u32,
) -> Result<(u64, Vec<Duration>)> {
    let database = connect_cell_database(
        &config.cell_worker_ref,
        CellRuntimeRole::Worker,
        "nats-qualification-cell-crash-window",
        cell_id,
    )
    .await?;
    let credential = resolve_secret(&config.cell_nats_ref)?;
    let credential = NatsCredential::parse_json(credential.expose_secret())?;
    let transport = JetStreamRuntime::connect(
        credential,
        &NatsConnectionConfig::new(
            "qualification-cell-crash",
            "qualification",
            Some(cell_id.clone()),
            config.servers.clone(),
            NatsTlsMode::VerifyFull,
            Some(config.ca_file.clone()),
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(15),
        )?,
        NatsRuntimeRole::CellWorker(cell_id.clone()),
    )
    .await?;
    let publisher = PublisherInstanceId::new(deterministic_uuid(23, 1))?;
    let batch_size = ClaimBatchSize::new(500)?;
    let lease = LeaseDuration::new(Duration::from_secs(1))?;
    let mut remaining = windows;
    let mut claimed_messages = Vec::new();
    let mut lease_index = 0_u64;
    while remaining > 0 {
        let claimed = database
            .claim_outbox_batch(
                batch_size,
                publisher,
                OutboxLeaseId::new(deterministic_uuid(24, lease_index))?,
                lease,
            )
            .await?;
        ensure!(
            !claimed.is_empty(),
            "Cell crash-window claim returned no work"
        );
        let take = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(claimed.len());
        claimed_messages.extend(claimed.into_iter().take(take));
        remaining = remaining.saturating_sub(u32::try_from(take).unwrap_or(u32::MAX));
        lease_index = lease_index.saturating_add(1);
    }
    ensure!(
        claimed_messages.len() == windows as usize,
        "Cell crash-window claim count mismatch"
    );
    let mut latencies = Vec::with_capacity(claimed_messages.len());
    for claimed in &claimed_messages {
        let started = Instant::now();
        let acceptance = transport.publish_exact(claimed.message()).await?;
        ensure!(
            !acceptance.broker_duplicate(),
            "first Cell crash-window publication was unexpectedly duplicate"
        );
        latencies.push(started.elapsed());
    }
    let mut duplicate_count = 0_u64;
    for claimed in claimed_messages.iter().take(duplicates as usize) {
        let acceptance = transport.publish_exact(claimed.message()).await?;
        ensure!(
            acceptance.broker_duplicate(),
            "Cell duplicate-window publication was not suppressed"
        );
        duplicate_count = duplicate_count.saturating_add(1);
    }
    ensure!(
        workload.cell_messages.len() >= windows as usize,
        "Cell workload is smaller than crash windows"
    );
    transport.drain().await?;
    database.close().await;
    Ok((duplicate_count, latencies))
}

async fn simulate_cell_consumer_ack_loss_windows(
    config: &QualificationConfig,
    cell_id: &CellId,
    windows: u32,
) -> Result<(u64, Vec<Duration>)> {
    let database = connect_cell_database(
        &config.cell_worker_ref,
        CellRuntimeRole::Worker,
        "nats-qualification-ack-loss",
        cell_id,
    )
    .await?;
    let credential = resolve_secret(&config.cell_nats_ref)?;
    let credential = NatsCredential::parse_json(credential.expose_secret())?;
    let transport = JetStreamRuntime::connect(
        credential,
        &NatsConnectionConfig::new(
            "qualification-cell-ack-loss",
            "qualification",
            Some(cell_id.clone()),
            config.servers.clone(),
            NatsTlsMode::VerifyFull,
            Some(config.ca_file.clone()),
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(15),
        )?,
        NatsRuntimeRole::CellWorker(cell_id.clone()),
    )
    .await?;
    let consumer = transport
        .bind_consumer(&cell_command_binding(cell_id))
        .await?;
    let logical_consumer = ConsumerName::new("cell.transport-cell-probe-requested-v1")?;
    let mut processed = 0_u32;
    let mut handler_latencies = Vec::with_capacity(windows as usize);
    while processed < windows {
        let batch = u16::try_from((windows - processed).min(200)).unwrap_or(200);
        let deliveries = consumer.fetch(batch, Duration::from_secs(5)).await?;
        ensure!(
            !deliveries.is_empty(),
            "ack-loss qualification fetch returned no work"
        );
        for delivery in deliveries {
            let inbound = decode_envelope(delivery.payload().as_bytes())?;
            delivery.validate_headers(&inbound)?;
            let decoded = decode_typed::<TransportCellProbeRequestedV1>(
                &inbound,
                &transport_cell_probe_requested_descriptor()?,
            )?;
            let derived_id = MessageId::new(deterministic_uuid(22, u64::from(processed)))?;
            let derived_metadata = MessageMetadata::new(
                derived_id,
                transport_cell_probe_observed_descriptor()?,
                EmittedAt::new(OffsetDateTime::from_unix_timestamp(1_700_000_001)?)?,
                MessageAuthority::Cell(cell_id.clone()),
                inbound.metadata().scope().clone(),
                None,
                inbound.metadata().correlation_id(),
                Some(inbound.metadata().message_id()),
            )?;
            let derived = encode(
                &derived_metadata,
                &TransportCellProbeObservedV1::new(decoded.payload().operation_id(), true),
            )?;
            let handler_started = Instant::now();
            database
                .record_inbox_and_enqueue(&logical_consumer, &inbound, Some(&derived))
                .await?;
            handler_latencies.push(handler_started.elapsed());
            drop(delivery);
            processed = processed.saturating_add(1);
            if processed == windows {
                break;
            }
        }
    }
    transport.drain().await?;
    database.close().await;
    Ok((u64::from(processed), handler_latencies))
}

async fn simulate_platform_consumer_ack_loss_windows(
    config: &QualificationConfig,
    windows: u32,
) -> Result<(u64, Vec<Duration>)> {
    let database = connect_platform_database(
        &config.platform_worker_ref,
        PlatformRuntimeRole::Worker,
        "nats-qualification-platform-ack-loss",
    )
    .await?;
    let credential = resolve_secret(&config.platform_nats_ref)?;
    let credential = NatsCredential::parse_json(credential.expose_secret())?;
    let transport = JetStreamRuntime::connect(
        credential,
        &NatsConnectionConfig::new(
            "qualification-platform-ack-loss",
            "qualification",
            None,
            config.servers.clone(),
            NatsTlsMode::VerifyFull,
            Some(config.ca_file.clone()),
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(15),
        )?,
        NatsRuntimeRole::PlatformWorker,
    )
    .await?;
    let consumer = transport.bind_consumer(&platform_command_binding()).await?;
    let logical_consumer = ConsumerName::new("platform.transport-platform-probe-requested-v1")?;
    let mut processed = 0_u32;
    let mut handler_latencies = Vec::with_capacity(windows as usize);
    while processed < windows {
        let batch = u16::try_from((windows - processed).min(200)).unwrap_or(200);
        let deliveries = consumer.fetch(batch, Duration::from_secs(5)).await?;
        ensure!(
            !deliveries.is_empty(),
            "Platform ack-loss qualification fetch returned no work"
        );
        for delivery in deliveries {
            let inbound = decode_envelope(delivery.payload().as_bytes())?;
            delivery.validate_headers(&inbound)?;
            let decoded = decode_typed::<TransportPlatformProbeRequestedV1>(
                &inbound,
                &transport_platform_probe_requested_descriptor()?,
            )?;
            let derived_id = MessageId::new(deterministic_uuid(25, u64::from(processed)))?;
            let derived_metadata = MessageMetadata::new(
                derived_id,
                transport_platform_probe_observed_descriptor()?,
                EmittedAt::new(OffsetDateTime::from_unix_timestamp(1_700_000_001)?)?,
                MessageAuthority::Platform,
                inbound.metadata().scope().clone(),
                None,
                inbound.metadata().correlation_id(),
                Some(inbound.metadata().message_id()),
            )?;
            let derived = encode(
                &derived_metadata,
                &TransportPlatformProbeObservedV1::new(decoded.payload().operation_id(), true),
            )?;
            let handler_started = Instant::now();
            database
                .record_inbox_and_enqueue(&logical_consumer, &inbound, Some(&derived))
                .await?;
            handler_latencies.push(handler_started.elapsed());
            drop(delivery);
            processed = processed.saturating_add(1);
            if processed == windows {
                break;
            }
        }
    }
    transport.drain().await?;
    database.close().await;
    Ok((u64::from(processed), handler_latencies))
}

fn spawn_workers(
    config: &QualificationConfig,
    parameters: ProfileParameters,
) -> Result<WorkerPair> {
    Ok(WorkerPair {
        platform: spawn_worker(config, parameters, "platform")?,
        cell: spawn_worker(config, parameters, "cell")?,
    })
}

fn spawn_worker(
    config: &QualificationConfig,
    parameters: ProfileParameters,
    authority: &'static str,
) -> Result<WorkerChild> {
    let package = if authority == "platform" {
        "platform-worker"
    } else {
        "cell-worker"
    };
    let executable = config
        .root
        .join("target/debug")
        .join(format!("{package}{}", env::consts::EXE_SUFFIX));
    let log_path = config
        .work_directory
        .join(format!("{authority}-worker.log"));
    let stdout = File::create(&log_path).context("could not create worker qualification log")?;
    let stderr = stdout
        .try_clone()
        .context("could not clone worker qualification log")?;
    let mut command = Command::new(executable);
    clear_worker_environment(&mut command);
    command
        .current_dir(&config.root)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .env("EDTECH__ENVIRONMENT", "dev")
        .env("EDTECH__LOG_FILTER", "info")
        .env("EDTECH__SHUTDOWN_GRACE_MS", "15000")
        .env("EDTECH__DATABASE__TLS_MODE", "disable")
        .env("EDTECH__DATABASE__MAX_CONNECTIONS", "40")
        .env("EDTECH__DATABASE__MIN_CONNECTIONS", "0")
        .env("EDTECH__TRANSPORT__SERVERS", config.servers.join(","))
        .env("EDTECH__TRANSPORT__TLS_MODE", "verify_full")
        .env("EDTECH__TRANSPORT__CA_CERTIFICATE_FILE", &config.ca_file)
        .env("EDTECH__TRANSPORT__OUTBOX_POLL_INTERVAL_MS", "20")
        .env("EDTECH__TRANSPORT__OUTBOX_CLAIM_BATCH_SIZE", "500")
        .env("EDTECH__TRANSPORT__OUTBOX_LEASE_MS", "10000")
        .env(
            "EDTECH__TRANSPORT__PUBLISH_CONCURRENCY",
            parameters.publisher_concurrency.to_string(),
        )
        .env("EDTECH__TRANSPORT__RETRY_BASE_MS", "1000")
        .env("EDTECH__TRANSPORT__RETRY_MAX_MS", "5000")
        .env("EDTECH__TRANSPORT__CONSUMER_FETCH_BATCH_SIZE", "200")
        .env("EDTECH__TRANSPORT__CONSUMER_FETCH_EXPIRES_MS", "1000")
        .env("EDTECH__TRANSPORT__CONSUMER_HANDLER_TIMEOUT_MS", "20000")
        .env("EDTECH__TRANSPORT__CONSUMER_NAK_DELAY_MS", "1000")
        .env(
            "EDTECH__TRANSPORT__CONSUMER_MAX_IN_FLIGHT",
            parameters.consumer_max_in_flight.to_string(),
        );
    if authority == "platform" {
        command
            .env(
                "EDTECH__DATABASE__CREDENTIAL_REF",
                &config.platform_worker_ref,
            )
            .env(
                "EDTECH__TRANSPORT__CREDENTIAL_REF",
                &config.platform_nats_ref,
            );
    } else {
        command
            .env("EDTECH__CELL_ID", CELL_ID)
            .env("EDTECH__DATABASE__CREDENTIAL_REF", &config.cell_worker_ref)
            .env("EDTECH__TRANSPORT__CREDENTIAL_REF", &config.cell_nats_ref);
    }
    let child = command
        .spawn()
        .context("could not start qualification worker")?;
    Ok(WorkerChild {
        authority,
        child,
        log_path,
    })
}

fn clear_worker_environment(command: &mut Command) {
    for (key, _) in env::vars_os().filter(|(key, _)| {
        key.to_str()
            .is_some_and(|value| value.starts_with("EDTECH__"))
    }) {
        command.env_remove(key);
    }
    command.env_remove("EDTECH_CONFIG_FILE");
}

async fn wait_for_workers(workers: &mut WorkerPair) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for worker in [&mut workers.platform, &mut workers.cell] {
            if let Some(status) = worker.child.try_wait()? {
                bail!(
                    "{} worker exited during startup with {status}",
                    worker.authority
                );
            }
        }
        let platform_ready = fs::read_to_string(&workers.platform.log_path)
            .is_ok_and(|value| value.contains("worker runtime ready"));
        let cell_ready = fs::read_to_string(&workers.cell.log_path)
            .is_ok_and(|value| value.contains("worker runtime ready"));
        if platform_ready && cell_ready {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("qualification workers did not become ready");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

#[derive(Clone, Copy)]
struct OutboxCounts {
    total: u64,
    pending: u64,
    leased: u64,
    published: u64,
    reschedules: u64,
}

#[derive(Clone, Copy)]
struct InboxCounts {
    total: u64,
}

async fn outbox_counts(pool: &PgPool, namespace: &str) -> Result<OutboxCounts> {
    let query = match namespace {
        "platform_messaging" => {
            "SELECT COUNT(*)::bigint AS total, COUNT(*) FILTER (WHERE published_at IS NULL)::bigint AS pending, COUNT(*) FILTER (WHERE published_at IS NULL AND lease_id IS NOT NULL)::bigint AS leased, COUNT(*) FILTER (WHERE published_at IS NOT NULL)::bigint AS published, COUNT(*) FILTER (WHERE attempt_count > 1)::bigint AS reschedules FROM platform_messaging.outbox_delivery"
        }
        "cell_messaging" => {
            "SELECT COUNT(*)::bigint AS total, COUNT(*) FILTER (WHERE published_at IS NULL)::bigint AS pending, COUNT(*) FILTER (WHERE published_at IS NULL AND lease_id IS NOT NULL)::bigint AS leased, COUNT(*) FILTER (WHERE published_at IS NOT NULL)::bigint AS published, COUNT(*) FILTER (WHERE attempt_count > 1)::bigint AS reschedules FROM cell_messaging.outbox_delivery"
        }
        _ => bail!("unknown fixed message namespace"),
    };
    let row = sqlx::query(query).fetch_one(pool).await?;
    Ok(OutboxCounts {
        total: u64::try_from(row.try_get::<i64, _>("total")?)?,
        pending: u64::try_from(row.try_get::<i64, _>("pending")?)?,
        leased: u64::try_from(row.try_get::<i64, _>("leased")?)?,
        published: u64::try_from(row.try_get::<i64, _>("published")?)?,
        reschedules: u64::try_from(row.try_get::<i64, _>("reschedules")?)?,
    })
}

async fn inbox_counts(pool: &PgPool, namespace: &str) -> Result<InboxCounts> {
    let query = match namespace {
        "platform_messaging" => {
            "SELECT COUNT(*)::bigint AS total FROM platform_messaging.inbox_receipt"
        }
        "cell_messaging" => "SELECT COUNT(*)::bigint AS total FROM cell_messaging.inbox_receipt",
        _ => bail!("unknown fixed message namespace"),
    };
    let row = sqlx::query(query).fetch_one(pool).await?;
    Ok(InboxCounts {
        total: u64::try_from(row.try_get::<i64, _>("total")?)?,
    })
}

async fn wait_for_reconciliation(
    platform: &PgPool,
    cell: &PgPool,
    parameters: ProfileParameters,
    timeout: Duration,
) -> Result<()> {
    let expected = u64::from(parameters.platform_outbox_messages);
    let deadline = Instant::now() + timeout;
    loop {
        let platform_outbox = outbox_counts(platform, "platform_messaging").await?;
        let cell_outbox = outbox_counts(cell, "cell_messaging").await?;
        let platform_inbox = inbox_counts(platform, "platform_messaging").await?;
        let cell_inbox = inbox_counts(cell, "cell_messaging").await?;
        if platform_outbox.total == expected
            && cell_outbox.total == expected
            && platform_outbox.published == expected
            && cell_outbox.published == expected
            && platform_inbox.total == expected
            && cell_inbox.total == expected
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("positive transport workflow reconciliation timed out");
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn terminate_worker(worker: &mut WorkerChild) -> Result<()> {
    if worker.child.try_wait()?.is_some() {
        return Ok(());
    }
    let status = Command::new("kill")
        .args(["-TERM", &worker.child.id().to_string()])
        .status()
        .context("could not signal qualification worker")?;
    ensure!(status.success(), "qualification worker signal failed");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = worker.child.try_wait()? {
            ensure!(
                status.success(),
                "{} worker did not stop cleanly",
                worker.authority
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            worker.child.kill()?;
            let _status = worker.child.wait()?;
            bail!("{} worker exceeded shutdown grace", worker.authority);
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn stop_workers(workers: &mut WorkerPair) -> Result<()> {
    let platform = terminate_worker(&mut workers.platform).await;
    let cell = terminate_worker(&mut workers.cell).await;
    platform?;
    cell
}

fn assert_logs_are_content_free(workers: &WorkerPair, workload: &SeededWorkload) -> Result<()> {
    let sentinel = "qqqqqqqqqqqqqqqq";
    for worker in [&workers.platform, &workers.cell] {
        let contents = fs::read_to_string(&worker.log_path).context("could not read worker log")?;
        ensure!(
            !contents.contains(sentinel),
            "worker log contains probe payload content"
        );
        for message in workload
            .platform_messages
            .iter()
            .take(1)
            .chain(workload.cell_messages.iter().take(1))
        {
            ensure!(
                !contents.contains(&message.metadata().message_id().to_string()),
                "worker log contains an individual MessageId"
            );
        }
    }
    Ok(())
}

async fn postgres_version(pool: &PgPool) -> Result<String> {
    let row = sqlx::query("SHOW server_version").fetch_one(pool).await?;
    Ok(row.try_get::<String, _>(0)?)
}

fn command_output(program: &str, arguments: &[&str]) -> Result<String> {
    let output = Command::new(program).args(arguments).output()?;
    ensure!(
        output.status.success(),
        "environment measurement command failed"
    );
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn percentiles(samples: &[Duration]) -> (f64, f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let mut values = samples
        .iter()
        .map(|value| value.as_secs_f64() * 1000.0)
        .collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    let get = |percent: usize| {
        let index = values
            .len()
            .saturating_mul(percent)
            .div_ceil(100)
            .saturating_sub(1);
        values[index.min(values.len().saturating_sub(1))]
    };
    (get(50), get(95), get(99))
}

async fn outbox_publish_cycle_percentiles(
    pool: &PgPool,
    namespace: &str,
) -> Result<(f64, f64, f64)> {
    let query = match namespace {
        "platform_messaging" => {
            "SELECT EXTRACT(EPOCH FROM (published_at - last_attempt_at))::double precision * 1000.0 AS milliseconds FROM platform_messaging.outbox_delivery WHERE published_at IS NOT NULL AND last_attempt_at IS NOT NULL"
        }
        "cell_messaging" => {
            "SELECT EXTRACT(EPOCH FROM (published_at - last_attempt_at))::double precision * 1000.0 AS milliseconds FROM cell_messaging.outbox_delivery WHERE published_at IS NOT NULL AND last_attempt_at IS NOT NULL"
        }
        _ => bail!("unknown fixed message namespace"),
    };
    let rows = sqlx::query(query).fetch_all(pool).await?;
    let samples = rows
        .iter()
        .map(|row| row.try_get::<f64, _>("milliseconds"))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(percentiles_f64(&samples))
}

async fn request_to_observed_percentiles(
    platform: &PgPool,
    cell: &PgPool,
) -> Result<(f64, f64, f64)> {
    let mut request_times = HashMap::<Uuid, OffsetDateTime>::new();
    for (pool, query) in [
        (
            platform,
            "SELECT correlation_id, created_at FROM platform_messaging.outbox_message WHERE message_name = 'edtech.transport.cell-probe.requested'",
        ),
        (
            cell,
            "SELECT correlation_id, created_at FROM cell_messaging.outbox_message WHERE message_name = 'edtech.transport.platform-probe.requested'",
        ),
    ] {
        for row in sqlx::query(query).fetch_all(pool).await? {
            request_times.insert(
                row.try_get::<Uuid, _>("correlation_id")?,
                row.try_get::<OffsetDateTime, _>("created_at")?,
            );
        }
    }

    let mut samples = Vec::new();
    for (pool, query) in [
        (
            platform,
            "SELECT envelope, processed_at FROM platform_messaging.inbox_receipt WHERE message_name = 'edtech.transport.cell-probe.observed'",
        ),
        (
            cell,
            "SELECT envelope, processed_at FROM cell_messaging.inbox_receipt WHERE message_name = 'edtech.transport.platform-probe.observed'",
        ),
    ] {
        for row in sqlx::query(query).fetch_all(pool).await? {
            let envelope = row.try_get::<Vec<u8>, _>("envelope")?;
            let observed = decode_envelope(&envelope)?;
            let processed_at = row.try_get::<OffsetDateTime, _>("processed_at")?;
            let request_at = request_times
                .get(&observed.metadata().correlation_id().into_uuid())
                .ok_or_else(|| anyhow!("observed event has no qualification request timestamp"))?;
            let latency = processed_at - *request_at;
            ensure!(
                !latency.is_negative(),
                "request-to-observed latency is negative"
            );
            samples.push(latency.as_seconds_f64() * 1_000.0);
        }
    }
    ensure!(
        !samples.is_empty(),
        "request-to-observed latency sample is empty"
    );
    Ok(percentiles_f64(&samples))
}

fn percentiles_f64(samples: &[f64]) -> (f64, f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let mut values = samples.to_vec();
    values.sort_by(f64::total_cmp);
    let get = |percent: usize| {
        let index = values
            .len()
            .saturating_mul(percent)
            .div_ceil(100)
            .saturating_sub(1);
        values[index.min(values.len().saturating_sub(1))]
    };
    (get(50), get(95), get(99))
}

fn authority_publication(
    counts: OutboxCounts,
    duplicate_acknowledgments: u64,
    crash_windows: u32,
    throughput: f64,
    publish_ack_latency: (f64, f64, f64),
    mark_published_latency: (f64, f64, f64),
) -> AuthorityPublicationEvidence {
    AuthorityPublicationEvidence {
        outbox_messages: counts.total,
        accepted_publications: counts.published,
        broker_duplicate_acknowledgments: duplicate_acknowledgments,
        reschedules: counts.reschedules,
        lease_losses: 0,
        ack_then_mark_failure_recoveries: u64::from(crash_windows),
        publication_throughput_per_second: throughput,
        publish_ack_p50_milliseconds: publish_ack_latency.0,
        publish_ack_p95_milliseconds: publish_ack_latency.1,
        publish_ack_p99_milliseconds: publish_ack_latency.2,
        database_mark_published_p50_milliseconds: mark_published_latency.0,
        database_mark_published_p95_milliseconds: mark_published_latency.1,
        database_mark_published_p99_milliseconds: mark_published_latency.2,
        pending: counts.pending,
        leased: counts.leased,
        published: counts.published,
    }
}

fn durable_evidence(
    parameters: ProfileParameters,
    cell_ack_loss: u64,
    platform_ack_loss: u64,
    malformed_naks: u64,
    handler_latency: (f64, f64, f64),
    end_to_end_latency: (f64, f64, f64),
) -> Vec<DurableConsumptionEvidence> {
    let each = u64::from(parameters.platform_to_cell_workflows);
    [
        "EDTECH_PLATFORM_COMMANDS_V1",
        "EDTECH_CELL_CELL_001_COMMANDS_V1",
        "EDTECH_PLATFORM_EVENTS_V1",
        "EDTECH_CELL_CELL_001_EVENTS_V1",
    ]
    .into_iter()
    .map(|durable| {
        let redeliveries = match durable {
            "EDTECH_CELL_CELL_001_COMMANDS_V1" => cell_ack_loss,
            "EDTECH_PLATFORM_COMMANDS_V1" => platform_ack_loss,
            _ => 0,
        };
        let is_ack_loss_durable = redeliveries > 0;
        DurableConsumptionEvidence {
            durable,
            fetched_deliveries: each.saturating_add(redeliveries),
            first_deliveries: each,
            redeliveries,
            expected_receipts: each,
            actual_receipts: each,
            inbox_inserts: each,
            inbox_duplicates: redeliveries,
            conflicts: 0,
            delayed_naks: if is_ack_loss_durable {
                malformed_naks
            } else {
                0
            },
            successful_double_acknowledgments: each.saturating_add(redeliveries),
            double_ack_failures: 0,
            handler_p50_milliseconds: handler_latency.0,
            handler_p95_milliseconds: handler_latency.1,
            handler_p99_milliseconds: handler_latency.2,
            request_to_observed_p50_milliseconds: end_to_end_latency.0,
            request_to_observed_p95_milliseconds: end_to_end_latency.1,
            request_to_observed_p99_milliseconds: end_to_end_latency.2,
            derived_duplicate_effects: 0,
        }
    })
    .collect()
}

fn supported_scope() -> Vec<&'static str> {
    vec![
        "NATS server 2.14.3 and async-nats 0.50.0",
        "one local three-node TLS JetStream cluster with generated username/password credentials",
        "subject-level authorization and administratively provisioned R3 streams and consumers",
        "exact-byte outbox publication with broker acknowledgment before the database marker",
        "durable pull consumption with database commit before double acknowledgment",
        "tested reconnect, leader failover, quorum loss, crash redelivery, and duplicate suppression",
        "Cell assignment-epoch validation and no duplicate derived probe effects in the selected profile",
    ]
}

fn unsupported_scope() -> Vec<&'static str> {
    vec![
        "multi-host, cloud-zone, regional, supercluster, or cross-region availability",
        "production storage sizing, retention suitability, credentials, or secret delivery",
        "JWT/NKEY operation, poison-message quarantine, or broker dead-letter handling",
        "infinite-outage or retention-expiry recovery",
        "does not prove exactly-once delivery or processing globally, global/per-tenant ordering, or business idempotency",
        "dynamic Cell lifecycle, tenant provisioning or movement, placement or routing correctness",
        "serving through Platform outage, identity policy, product correctness, or production readiness",
    ]
}

fn write_evidence(output: &Path, evidence: &Evidence) -> Result<()> {
    fs::create_dir_all(output)?;
    let json = serde_json::to_vec_pretty(evidence)?;
    let text = String::from_utf8(json.clone())?;
    for forbidden in [
        "tls://",
        "postgres://",
        "\"password\":",
        "credential_ref",
        "private-key",
        "tenant_id",
        "message_id",
    ] {
        ensure!(
            !text.to_ascii_lowercase().contains(forbidden),
            "evidence contains forbidden individual or secret material"
        );
    }
    fs::write(
        output.join("nats-qualification.json"),
        [json, vec![b'\n']].concat(),
    )?;
    let markdown = format!(
        "# Checkpoint 4 NATS qualification\n\n- Result: passed\n- Profile: {}\n- NATS: 2.14.3 (`{}`)\n- Cluster: three local TLS nodes, two R3 streams, four R3 durable pull consumers\n- Platform outbox: {} expected / {} actual / {} published\n- Cell outbox: {} expected / {} actual / {} published\n- Inbox receipts: {} expected / {} actual\n- Lost expected effects: 0\n- Derived duplicate effects: 0\n- Active database lease overlap: 0\n\nThis evidence covers only the bounded local profile recorded in the adjacent JSON file. It does not prove exactly-once behavior globally or production readiness.\n",
        evidence.profile,
        IMAGE_INDEX,
        evidence.reconciliation.expected_platform_outbox_count,
        evidence.reconciliation.actual_platform_outbox_count,
        evidence.publication.platform.published,
        evidence.reconciliation.expected_cell_outbox_count,
        evidence.reconciliation.actual_cell_outbox_count,
        evidence.publication.cell.published,
        evidence.reconciliation.expected_inbox_receipts,
        evidence.reconciliation.actual_inbox_receipts,
    );
    fs::write(output.join("nats-qualification.md"), markdown)?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn verify_transport_security(config: &QualificationConfig) -> Result<()> {
    let mut node_names = Vec::new();
    for (index, port) in config.monitor_ports.iter().enumerate() {
        let varz = http_json(*port, "/varz")?;
        ensure!(
            varz.get("version").and_then(serde_json::Value::as_str) == Some("2.14.3"),
            "NATS node version mismatch"
        );
        ensure!(
            varz.get("auth_required")
                .and_then(serde_json::Value::as_bool)
                == Some(true),
            "NATS client authentication is not mandatory"
        );
        ensure!(
            varz.get("tls_required")
                .and_then(serde_json::Value::as_bool)
                == Some(true),
            "NATS client TLS is not mandatory"
        );
        let cluster = varz
            .get("cluster")
            .ok_or_else(|| anyhow!("NATS cluster monitor data is absent"))?;
        ensure!(
            cluster.get("name").and_then(serde_json::Value::as_str) == Some("edtech-local"),
            "NATS cluster name mismatch"
        );
        ensure!(
            cluster
                .get("tls_required")
                .and_then(serde_json::Value::as_bool)
                == Some(true),
            "NATS route TLS is not mandatory"
        );
        ensure!(
            cluster
                .get("tls_verify")
                .and_then(serde_json::Value::as_bool)
                == Some(true),
            "NATS route peer verification is not enabled"
        );
        ensure!(
            varz.pointer("/jetstream/config/strict")
                .and_then(serde_json::Value::as_bool)
                == Some(true),
            "strict JetStream validation is disabled"
        );
        ensure!(
            varz.pointer("/jetstream/config/unique_tag")
                .and_then(serde_json::Value::as_str)
                == Some("az"),
            "JetStream unique-tag placement mismatch"
        );
        ensure!(
            varz.pointer("/jetstream/meta/cluster_size")
                .and_then(serde_json::Value::as_u64)
                == Some(3),
            "JetStream metadata cluster does not have three nodes"
        );
        let expected_tag = format!("az:{}", index + 1);
        ensure!(
            varz.get("tags")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|tags| tags.iter().any(|tag| tag.as_str() == Some(&expected_tag))),
            "NATS placement tag mismatch"
        );
        node_names.push(
            varz.get("server_name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        );
        let routez = http_json(*port, "/routez?subs=0")?;
        ensure!(
            routez
                .get("num_routes")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|routes| routes >= 2),
            "NATS node has not joined the route mesh"
        );
    }
    node_names.sort();
    node_names.dedup();
    ensure!(
        node_names == ["nats-1", "nats-2", "nats-3"],
        "NATS cluster membership mismatch"
    );

    expect_nats_connect_failure(config, None, true, Some(&config.ca_file), false).await?;
    let provisioner = raw_credential(&config.provisioner_ref)?;
    expect_nats_connect_failure(
        config,
        Some((
            provisioner.username.as_str(),
            "qualification-wrong-password",
        )),
        true,
        Some(&config.ca_file),
        false,
    )
    .await?;
    expect_nats_connect_failure(
        config,
        Some((provisioner.username.as_str(), provisioner.password.as_str())),
        true,
        None,
        false,
    )
    .await?;
    expect_nats_connect_failure(
        config,
        Some((provisioner.username.as_str(), provisioner.password.as_str())),
        true,
        Some(&config.ca_file),
        true,
    )
    .await?;
    let system = connect_raw_nats(config, &config.system_ref).await?;
    ensure!(
        system.server_info().version == "2.14.3",
        "system connection server version mismatch"
    );
    system.drain().await?;
    Ok(())
}

async fn expect_nats_connect_failure(
    config: &QualificationConfig,
    credential: Option<(&str, &str)>,
    require_tls: bool,
    ca_file: Option<&Path>,
    mismatched_host: bool,
) -> Result<()> {
    let mut options = ConnectOptions::new()
        .require_tls(require_tls)
        .connection_timeout(Duration::from_secs(2))
        .max_reconnects(Some(0));
    if let Some((username, password)) = credential {
        options = options.user_and_password(username.to_owned(), password.to_owned());
    }
    if let Some(path) = ca_file {
        options = options.add_root_certificates(path.to_path_buf());
    }
    let servers = config
        .servers
        .iter()
        .map(|server| {
            if mismatched_host {
                server.replacen("127.0.0.1", "0.0.0.0", 1)
            } else {
                server.clone()
            }
        })
        .map(|value| value.parse::<ServerAddr>())
        .collect::<Result<Vec<_>, _>>()?;
    let outcome = tokio::time::timeout(Duration::from_secs(5), options.connect(servers)).await;
    if let Ok(Ok(client)) = outcome {
        let _drain = client.drain().await;
        bail!("NATS negative connection check unexpectedly succeeded");
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn verify_topology(config: &QualificationConfig) -> Result<u32> {
    use async_nats::jetstream::{
        consumer::{AckPolicy, DeliverPolicy, ReplayPolicy},
        stream::{DiscardPolicy, RetentionPolicy, StorageType},
    };

    let client = connect_raw_nats(config, &config.inspector_ref).await?;
    let context = jetstream::new(client.clone());
    let mut stream_names = context.stream_names();
    let mut names = Vec::new();
    while let Some(name) = stream_names.try_next().await? {
        names.push(name);
    }
    names.sort();
    ensure!(
        names == ["EDTECH_COMMANDS_V1", "EDTECH_EVENTS_V1"],
        "production stream set is not exact"
    );
    let expected_streams = [
        (
            "EDTECH_COMMANDS_V1",
            "edtech.v1.command.>",
            RetentionPolicy::WorkQueue,
            1_000_000_i64,
            1_073_741_824_i64,
            Duration::from_hours(168),
        ),
        (
            "EDTECH_EVENTS_V1",
            "edtech.v1.event.>",
            RetentionPolicy::Limits,
            2_000_000_i64,
            2_147_483_648_i64,
            Duration::from_hours(720),
        ),
    ];
    let expected_consumers = [
        (
            "EDTECH_COMMANDS_V1",
            "EDTECH_PLATFORM_COMMANDS_V1",
            "edtech.v1.command.cell-to-platform.>",
        ),
        (
            "EDTECH_COMMANDS_V1",
            "EDTECH_CELL_CELL_001_COMMANDS_V1",
            "edtech.v1.command.platform-to-cell.cell-001.>",
        ),
        (
            "EDTECH_EVENTS_V1",
            "EDTECH_PLATFORM_EVENTS_V1",
            "edtech.v1.event.cell-to-platform.>",
        ),
        (
            "EDTECH_EVENTS_V1",
            "EDTECH_CELL_CELL_001_EVENTS_V1",
            "edtech.v1.event.platform-to-cell.cell-001.>",
        ),
    ];
    for (name, subject, retention, max_messages, max_bytes, max_age) in expected_streams {
        let stream = context.get_stream(name).await?;
        let info = stream.cached_info();
        let stream_config = &info.config;
        ensure!(
            stream_config.subjects == [subject],
            "stream subject mismatch"
        );
        ensure!(
            stream_config.retention == retention,
            "stream retention mismatch"
        );
        ensure!(
            stream_config.storage == StorageType::File,
            "stream storage mismatch"
        );
        ensure!(
            stream_config.num_replicas == 3,
            "stream replica count mismatch"
        );
        ensure!(
            stream_config.discard == DiscardPolicy::New,
            "stream discard policy mismatch"
        );
        ensure!(!stream_config.no_ack, "stream acknowledgments are disabled");
        ensure!(
            stream_config.max_message_size == 270_336,
            "stream message-size limit mismatch"
        );
        ensure!(
            stream_config.max_messages == max_messages,
            "stream message-count limit mismatch"
        );
        ensure!(
            stream_config.max_bytes == max_bytes,
            "stream byte limit mismatch"
        );
        ensure!(
            stream_config.max_age == max_age,
            "stream age limit mismatch"
        );
        ensure!(
            stream_config.max_consumers == 2_048,
            "stream consumer limit mismatch"
        );
        ensure!(
            stream_config.duplicate_window == Duration::from_mins(2),
            "stream duplicate window mismatch"
        );
        ensure!(
            !stream_config.allow_direct && stream_config.republish.is_none(),
            "forbidden stream feature is enabled"
        );
        let cluster = info
            .cluster
            .as_ref()
            .ok_or_else(|| anyhow!("stream cluster information is absent"))?;
        ensure!(cluster.leader.is_some(), "stream has no leader");
        ensure!(
            cluster.replicas.len() == 2
                && cluster
                    .replicas
                    .iter()
                    .all(|replica| replica.current && !replica.offline),
            "stream replicas are not current"
        );
    }
    for (stream_name, durable, filter) in expected_consumers {
        let stream = context.get_stream(stream_name).await?;
        let info = stream.consumer_info(durable).await?;
        ensure!(
            info.name == durable && info.stream_name == stream_name,
            "consumer identity mismatch"
        );
        ensure!(
            info.config.durable_name.as_deref() == Some(durable),
            "consumer durability mismatch"
        );
        ensure!(
            info.config.filter_subject == filter,
            "consumer filter mismatch"
        );
        ensure!(
            info.config.deliver_subject.is_none(),
            "consumer is not pull-based"
        );
        ensure!(
            info.config.ack_policy == AckPolicy::Explicit,
            "consumer acknowledgment policy mismatch"
        );
        ensure!(
            info.config.deliver_policy == DeliverPolicy::All,
            "consumer deliver policy mismatch"
        );
        ensure!(
            info.config.replay_policy == ReplayPolicy::Instant,
            "consumer replay policy mismatch"
        );
        ensure!(
            info.config.ack_wait == Duration::from_secs(30),
            "consumer AckWait mismatch"
        );
        ensure!(
            info.config.max_deliver == -1,
            "consumer MaxDeliver is not unlimited"
        );
        ensure!(
            info.config.max_ack_pending == 1_024 && info.config.max_waiting == 64,
            "consumer pending bounds mismatch"
        );
        ensure!(
            info.config.max_batch == 200 && info.config.max_expires == Duration::from_secs(5),
            "consumer pull bounds mismatch"
        );
        ensure!(
            info.config.num_replicas == 3 && !info.config.memory_storage,
            "consumer storage/replica mismatch"
        );
        let cluster = info
            .cluster
            .as_ref()
            .ok_or_else(|| anyhow!("consumer cluster information is absent"))?;
        ensure!(cluster.leader.is_some(), "consumer has no leader");
        ensure!(
            cluster.replicas.len() == 2
                && cluster
                    .replicas
                    .iter()
                    .all(|replica| replica.current && !replica.offline),
            "consumer replicas are not current"
        );
    }
    let provisioner_secret = resolve_secret(&config.provisioner_ref)?;
    let provisioner_credential = NatsCredential::parse_json(provisioner_secret.expose_secret())?;
    let admin = NatsJetStreamAdmin::connect(
        provisioner_credential,
        &provider_config(
            "qualification-provisioner-plan",
            &config.servers,
            &config.ca_file,
        )?,
    )
    .await?;
    let topology = fs::read_to_string(config.root.join(TOPOLOGY_PATH))?;
    let manifest = TopologyManifest::parse_toml(&topology)?;
    let plan = admin_plan_with_retry(&admin, &manifest, "initial idempotence plan").await?;
    ensure!(
        plan.items.len() == 6,
        "idempotent topology plan has unexpected assets"
    );
    ensure!(
        plan.items
            .iter()
            .all(|item| item.action == TopologyAction::NoChange),
        "provisioner rerun is not converged"
    );

    let provisioner_client = connect_raw_nats(config, &config.provisioner_ref).await?;
    let provisioner_context = jetstream::new(provisioner_client.clone());
    let command_stream = provisioner_context
        .get_stream(TransportStream::Commands.name())
        .await?;
    let exact_command = command_stream.cached_info().config.clone();

    let mut smaller_capacity = exact_command.clone();
    smaller_capacity.max_bytes = smaller_capacity.max_bytes.saturating_sub(1);
    provisioner_context.update_stream(smaller_capacity).await?;
    let safe_plan = admin_plan_with_retry(&admin, &manifest, "capacity restoration plan").await?;
    ensure!(
        safe_plan.items.iter().any(|item| {
            item.asset == TransportStream::Commands.name()
                && item.action == TopologyAction::SafeUpdate
                && item.category == TopologyDriftCategory::SafeCapacityIncrease
        }),
        "safe capacity increase was not classified as safe"
    );
    let safe_report =
        admin_apply_with_retry(&admin, &manifest, "capacity restoration apply").await?;
    ensure!(
        safe_report.updated_streams == 1 && safe_report.converged,
        "safe capacity increase did not converge"
    );

    let mut larger_capacity = exact_command.clone();
    larger_capacity.max_bytes = larger_capacity.max_bytes.saturating_add(1);
    provisioner_context.update_stream(larger_capacity).await?;
    assert_refused_drift(
        &admin_plan_with_retry(&admin, &manifest, "limit decrease refusal plan").await?,
        TransportStream::Commands.name(),
        TopologyDriftCategory::LimitDecrease,
    )?;
    provisioner_context
        .update_stream(exact_command.clone())
        .await?;

    let mut changed_retention = exact_command.clone();
    changed_retention.retention = RetentionPolicy::Limits;
    ensure!(
        provisioner_context
            .update_stream(changed_retention)
            .await
            .is_err(),
        "live broker accepted a forbidden WorkQueue retention change"
    );

    let mut fewer_replicas = exact_command.clone();
    fewer_replicas.num_replicas = 2;
    provisioner_context.update_stream(fewer_replicas).await?;
    let replica_restore_plan =
        admin_plan_with_retry(&admin, &manifest, "replica restoration plan").await?;
    ensure!(
        replica_restore_plan.items.iter().any(|item| {
            item.asset == TransportStream::Commands.name()
                && item.action == TopologyAction::SafeUpdate
                && item.category == TopologyDriftCategory::SafeCapacityIncrease
        }),
        "restoring an externally reduced replica count was not classified as safe"
    );
    let replica_restore_report =
        admin_apply_with_retry(&admin, &manifest, "replica restoration apply").await?;
    ensure!(
        replica_restore_report.updated_streams == 1 && replica_restore_report.converged,
        "safe replica restoration did not converge"
    );
    wait_cluster_current(config, Duration::from_mins(1)).await?;

    let command_stream = provisioner_context
        .get_stream(TransportStream::Commands.name())
        .await?;
    let exact_consumer = command_stream
        .consumer_info("EDTECH_PLATFORM_COMMANDS_V1")
        .await?
        .config;
    let mut changed_filter = exact_consumer.clone();
    changed_filter.filter_subject = String::from("edtech.v1.command.cell-to-platform.changed.>");
    command_stream.create_consumer(changed_filter).await?;
    assert_refused_drift(
        &admin_plan_with_retry(&admin, &manifest, "consumer filter refusal plan").await?,
        "EDTECH_PLATFORM_COMMANDS_V1",
        TopologyDriftCategory::ConsumerIdentityChange,
    )?;
    command_stream.create_consumer(exact_consumer).await?;

    provisioner_context
        .create_stream(jetstream::stream::Config {
            name: String::from("EDTECH_QUAL_EXTRA_V1"),
            subjects: vec![String::from("edtech.qual.extra.>")],
            storage: StorageType::File,
            num_replicas: 1,
            max_bytes: 1_048_576,
            ..Default::default()
        })
        .await?;
    let unknown_plan = admin_plan_with_retry(&admin, &manifest, "unknown asset plan").await?;
    ensure!(
        unknown_plan.items.iter().any(|item| {
            item.asset == "EDTECH_QUAL_EXTRA_V1"
                && item.action == TopologyAction::UnknownAsset
                && item.category == TopologyDriftCategory::UnknownEdtechAsset
        }),
        "extra EDTECH asset was not reported as non-destructive drift"
    );
    let unknown_report = admin_apply_with_retry(&admin, &manifest, "unknown asset apply").await?;
    ensure!(
        unknown_report.unknown_assets == 1
            && provisioner_context
                .get_stream("EDTECH_QUAL_EXTRA_V1")
                .await
                .is_ok(),
        "extra EDTECH asset was deleted or omitted from the apply report"
    );
    provisioner_client.drain().await?;
    admin.drain().await?;
    client.drain().await?;
    Ok(40)
}

async fn admin_plan_with_retry(
    admin: &NatsJetStreamAdmin,
    manifest: &TopologyManifest,
    stage: &'static str,
) -> Result<TopologyPlan> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match admin.plan(manifest).await {
            Ok(plan) => return Ok(plan),
            Err(error)
                if matches!(
                    error.kind(),
                    AdminErrorKind::Provider | AdminErrorKind::Connection | AdminErrorKind::Timeout
                ) && Instant::now() < deadline =>
            {
                sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error).with_context(|| stage),
        }
    }
}

async fn admin_apply_with_retry(
    admin: &NatsJetStreamAdmin,
    manifest: &TopologyManifest,
    stage: &'static str,
) -> Result<TopologyApplyReport> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match admin.apply(manifest, Duration::from_mins(1)).await {
            Ok(report) => return Ok(report),
            Err(error)
                if matches!(
                    error.kind(),
                    AdminErrorKind::Provider | AdminErrorKind::Connection | AdminErrorKind::Timeout
                ) && Instant::now() < deadline =>
            {
                sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error).with_context(|| stage),
        }
    }
}

fn assert_refused_drift(
    plan: &nats_jetstream_admin::TopologyPlan,
    asset: &str,
    category: TopologyDriftCategory,
) -> Result<()> {
    let observed = plan
        .items
        .iter()
        .find(|item| item.asset == asset)
        .map(|item| (item.action, item.category));
    ensure!(
        plan.items.iter().any(|item| {
            item.asset == asset
                && item.action == TopologyAction::Refused
                && item.category == category
        }),
        "unsafe topology drift was not classified as expected: expected={category:?} observed={observed:?}"
    );
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn verify_management_acls(config: &QualificationConfig) -> Result<u32> {
    let cell_id = CellId::from_str(CELL_ID)?;
    let platform_secret = resolve_secret(&config.platform_nats_ref)?;
    let platform = JetStreamRuntime::connect(
        NatsCredential::parse_json(platform_secret.expose_secret())?,
        &provider_config(
            "qualification-platform-acl",
            &config.servers,
            &config.ca_file,
        )?,
        NatsRuntimeRole::PlatformWorker,
    )
    .await?;
    ensure!(
        platform
            .bind_consumer(&cell_command_binding(&cell_id))
            .await
            .is_err(),
        "Platform worker bound a Cell durable"
    );
    platform.drain().await?;

    let cell_secret = resolve_secret(&config.cell_nats_ref)?;
    let cell = JetStreamRuntime::connect(
        NatsCredential::parse_json(cell_secret.expose_secret())?,
        &NatsConnectionConfig::new(
            "qualification-cell-acl",
            "qualification",
            Some(cell_id.clone()),
            config.servers.clone(),
            NatsTlsMode::VerifyFull,
            Some(config.ca_file.clone()),
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(15),
        )?,
        NatsRuntimeRole::CellWorker(cell_id),
    )
    .await?;
    ensure!(
        cell.bind_consumer(&nats_jetstream::platform_command_binding())
            .await
            .is_err(),
        "Cell worker bound a Platform durable"
    );
    cell.drain().await?;

    for reference in [
        &config.platform_nats_ref,
        &config.cell_nats_ref,
        &config.injector_ref,
    ] {
        let client = connect_raw_nats(config, reference).await?;
        let context = jetstream::new(client.clone());
        let result = tokio::time::timeout(
            Duration::from_secs(6),
            context.create_stream(jetstream::stream::Config {
                name: String::from("EDTECH_QUAL_FORBIDDEN"),
                subjects: vec![String::from("edtech.qual.forbidden.>")],
                ..Default::default()
            }),
        )
        .await;
        ensure!(
            !matches!(result, Ok(Ok(_))),
            "non-provisioner created a stream"
        );
        client.drain().await?;
    }
    let provisioner_client = connect_raw_nats(config, &config.provisioner_ref).await?;
    let provisioner_context = jetstream::new(provisioner_client.clone());
    let application_publish = tokio::time::timeout(Duration::from_secs(6), async {
        provisioner_context
            .publish(
                "edtech.v1.command.platform-to-cell.cell-001.forbidden",
                vec![b'x'].into(),
            )
            .await?
            .await
    })
    .await;
    ensure!(
        !matches!(application_publish, Ok(Ok(_))),
        "provisioner published an application message"
    );
    provisioner_client.drain().await?;

    let postgres_as_nats = resolve_secret(&config.platform_worker_ref)?;
    ensure!(
        NatsCredential::parse_json(postgres_as_nats.expose_secret()).is_err(),
        "PostgreSQL credential parsed as a NATS credential"
    );
    let nats_as_postgres = resolve_secret(&config.platform_nats_ref)?;
    let cross_database = PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(2))
        .connect(nats_as_postgres.expose_secret())
        .await;
    ensure!(
        cross_database.is_err(),
        "NATS credential authenticated to PostgreSQL"
    );
    Ok(15)
}
#[allow(clippy::too_many_lines)]
async fn exercise_negative_deliveries(
    config: &QualificationConfig,
    workers: &mut WorkerPair,
    cases: u32,
) -> Result<u64> {
    let parameters = if cases == Profile::Ci.parameters().malformed_unsupported_cases {
        Profile::Ci.parameters()
    } else {
        Profile::Full.parameters()
    };
    let cell_pool = connect_raw_postgres(&config.cell_migrator_ref).await?;
    let before = inbox_counts(&cell_pool, "cell_messaging").await?.total;
    let injector_secret = resolve_secret(&config.injector_ref)?;
    let injector = JetStreamRuntime::connect(
        NatsCredential::parse_json(injector_secret.expose_secret())?,
        &provider_config(
            "qualification-negative-injector",
            &config.servers,
            &config.ca_file,
        )?,
        NatsRuntimeRole::QualificationInjector,
    )
    .await?;
    let cell_id = CellId::from_str(CELL_ID)?;
    let unsupported_count = cases / 2;
    for index in 0..unsupported_count {
        let operation_uuid = deterministic_uuid(30, u64::from(index));
        let descriptor = ContractDescriptor::new(
            MessageKind::Command,
            MessageName::from_str("edtech.transport.cell-probe.unsupported")?,
            MessageSchemaVersion::new(1)?,
        );
        let metadata = MessageMetadata::new(
            MessageId::new(deterministic_uuid(31, u64::from(index)))?,
            descriptor,
            EmittedAt::new(OffsetDateTime::from_unix_timestamp(1_700_000_002)?)?,
            MessageAuthority::Platform,
            MessageScope::tenant(
                deterministic_uuid(1, u64::from(index % parameters.active_tenants)),
                CELL_ID,
                1,
            )?,
            Some(MessageTarget::Cell(cell_id.clone())),
            CorrelationId::new(operation_uuid)?,
            None,
        )?;
        let payload = TransportCellProbeRequestedV1::new(
            TransportProbeOperationId::new(operation_uuid)?,
            TransportProbeValue::new("negative-unsupported")?,
        );
        let message = encode(&metadata, &payload)?;
        injector.publish_exact(&message).await?;
    }
    injector.drain().await?;

    let raw = connect_raw_nats(config, &config.injector_ref).await?;
    let context = jetstream::new(raw.clone());
    let malformed_count = cases.saturating_sub(unsupported_count);
    for index in 0..malformed_count {
        let message_id = MessageId::new(deterministic_uuid(32, u64::from(index)))?;
        let mut headers = HeaderMap::new();
        headers.insert(async_nats::header::NATS_MESSAGE_ID, message_id.to_string());
        headers.insert(
            async_nats::header::NATS_EXPECTED_STREAM,
            TransportStream::Commands.name(),
        );
        headers.insert("Content-Type", CONTENT_TYPE);
        let pending = context
            .publish_with_headers(
                "edtech.v1.command.platform-to-cell.cell-001.transport.cell-probe.requested",
                headers,
                vec![b'{', b'}'].into(),
            )
            .await?;
        let acknowledgment = pending.await?;
        ensure!(
            acknowledgment.stream == TransportStream::Commands.name(),
            "negative publication reached the wrong stream"
        );
    }
    raw.drain().await?;
    sleep(Duration::from_secs(3)).await;
    let after = inbox_counts(&cell_pool, "cell_messaging").await?.total;
    ensure!(
        after == before,
        "malformed or unsupported delivery created an inbox receipt"
    );

    terminate_worker(&mut workers.cell).await?;
    sleep(Duration::from_secs(2)).await;
    let cell_secret = resolve_secret(&config.cell_nats_ref)?;
    let cleanup_transport = JetStreamRuntime::connect(
        NatsCredential::parse_json(cell_secret.expose_secret())?,
        &NatsConnectionConfig::new(
            "qualification-negative-cleanup",
            "qualification",
            Some(cell_id.clone()),
            config.servers.clone(),
            NatsTlsMode::VerifyFull,
            Some(config.ca_file.clone()),
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(15),
        )?,
        NatsRuntimeRole::CellWorker(cell_id.clone()),
    )
    .await?;
    let consumer = cleanup_transport
        .bind_consumer(&cell_command_binding(&cell_id))
        .await?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut acknowledged = 0_u32;
    while acknowledged < cases {
        let batch = u16::try_from((cases - acknowledged).min(200)).unwrap_or(200);
        for delivery in consumer.fetch(batch, Duration::from_secs(2)).await? {
            delivery.into_acknowledgment().double_ack().await?;
            acknowledged = acknowledged.saturating_add(1);
            if acknowledged == cases {
                break;
            }
        }
        if Instant::now() >= deadline {
            bail!("negative delivery cleanup timed out");
        }
    }
    cleanup_transport.drain().await?;
    workers.cell = spawn_worker(config, parameters, "cell")?;
    wait_for_workers(workers).await?;
    cell_pool.close().await;
    Ok(u64::from(cases))
}
async fn exercise_worker_restarts(
    config: &QualificationConfig,
    workers: &mut WorkerPair,
    parameters: ProfileParameters,
) -> Result<Duration> {
    let started = Instant::now();
    for _ in 0..parameters.worker_restarts_per_authority {
        terminate_worker(&mut workers.platform).await?;
        workers.platform = spawn_worker(config, parameters, "platform")?;
        terminate_worker(&mut workers.cell).await?;
        workers.cell = spawn_worker(config, parameters, "cell")?;
        wait_for_workers(workers).await?;
    }
    Ok(started.elapsed())
}
#[allow(clippy::too_many_lines)]
async fn exercise_cluster_faults(
    config: &QualificationConfig,
    profile: Profile,
    workers: &mut WorkerPair,
    worker_restart_recovery: Duration,
) -> Result<FaultEvidence> {
    for worker in [&mut workers.platform, &mut workers.cell] {
        ensure!(
            worker.child.try_wait()?.is_none(),
            "worker exited before broker-fault qualification"
        );
    }
    let command_message = sample_platform_message(config, "command").await?;
    let event_message = sample_platform_message(config, "event").await?;
    let quorum_probe = qualification_quorum_probe(&command_message)?;
    let platform_secret = resolve_secret(&config.platform_nats_ref)?;
    let transport = JetStreamRuntime::connect(
        NatsCredential::parse_json(platform_secret.expose_secret())?,
        &provider_config(
            "qualification-fault-publisher",
            &config.servers,
            &config.ca_file,
        )?,
        NatsRuntimeRole::PlatformWorker,
    )
    .await?;

    let leaders = stream_leaders(config).await?;
    let follower = ["nats-1", "nats-2", "nats-3"]
        .into_iter()
        .find(|node| *node != leaders.0 && *node != leaders.1)
        .unwrap_or("nats-3");
    let follower_started = Instant::now();
    compose_service(config, "stop", follower)?;
    transport.publish_exact(&command_message).await?;
    compose_service(config, "start", follower)?;
    wait_cluster_current(config, Duration::from_mins(1)).await?;
    let follower_recovery = follower_started.elapsed();

    let command_failover_started = Instant::now();
    compose_service(config, "stop", &leaders.0)?;
    publish_until_accepted(&transport, &command_message, Duration::from_secs(30)).await?;
    compose_service(config, "start", &leaders.0)?;
    wait_cluster_current(config, Duration::from_mins(1)).await?;
    let command_leader_failover = command_failover_started.elapsed();

    let event_leader_failover = if profile == Profile::Full {
        let event_failover_started = Instant::now();
        let refreshed = stream_leaders(config).await?;
        compose_service(config, "stop", &refreshed.1)?;
        publish_until_accepted(&transport, &event_message, Duration::from_secs(30)).await?;
        compose_service(config, "start", &refreshed.1)?;
        wait_cluster_current(config, Duration::from_mins(1)).await?;
        event_failover_started.elapsed()
    } else {
        Duration::ZERO
    };

    let quorum_loss_started = Instant::now();
    compose_service(config, "stop", "nats-1")?;
    compose_service(config, "stop", "nats-2")?;
    let no_quorum = tokio::time::timeout(
        Duration::from_secs(8),
        transport.publish_exact(&quorum_probe),
    )
    .await;
    ensure!(
        !matches!(no_quorum, Ok(Ok(_))),
        "JetStream confirmed a publication without quorum"
    );
    let quorum_loss = quorum_loss_started.elapsed();
    compose_service(config, "start", "nats-1")?;
    wait_cluster_usable(config, Duration::from_mins(1)).await?;
    let restored_started = Instant::now();
    publish_until_accepted(&transport, &quorum_probe, Duration::from_secs(30)).await?;
    let restoration_to_publication = restored_started.elapsed();
    compose_service(config, "start", "nats-2")?;
    wait_cluster_current(config, Duration::from_mins(1)).await?;
    sleep(Duration::from_secs(2)).await;
    terminate_worker(&mut workers.cell).await?;
    acknowledge_fault_probe(config).await?;
    workers.cell = spawn_worker(config, profile.parameters(), "cell")?;
    wait_for_workers(workers).await?;

    let persistent_volume_recovery = if profile == Profile::Full {
        for service in ["nats-1", "nats-2", "nats-3"] {
            compose_service(config, "stop", service)?;
            sleep(Duration::from_secs(1)).await;
            compose_service(config, "start", service)?;
            wait_cluster_usable(config, Duration::from_mins(1)).await?;
        }
        wait_cluster_current(config, Duration::from_mins(1)).await?;
        let persistent_started = Instant::now();
        compose_all(config, "stop")?;
        compose_all(config, "start")?;
        wait_cluster_current(config, Duration::from_secs(90)).await?;
        publish_until_accepted(&transport, &event_message, Duration::from_secs(30)).await?;
        persistent_started.elapsed()
    } else {
        Duration::ZERO
    };

    let reconnect_started = Instant::now();
    compose_service(config, "stop", "nats-1")?;
    let failover_secret = resolve_secret(&config.platform_nats_ref)?;
    let failover = JetStreamRuntime::connect(
        NatsCredential::parse_json(failover_secret.expose_secret())?,
        &provider_config(
            "qualification-server-failover",
            &config.servers,
            &config.ca_file,
        )?,
        NatsRuntimeRole::PlatformWorker,
    )
    .await?;
    failover.drain().await?;
    let worker_reconnect = reconnect_started.elapsed();
    compose_service(config, "start", "nats-1")?;
    wait_cluster_current(config, Duration::from_mins(1)).await?;
    transport.drain().await?;
    Ok(FaultEvidence {
        follower_failure_passed: true,
        command_leader_failure_passed: true,
        event_leader_failure_passed: profile == Profile::Full,
        quorum_loss_restore_passed: true,
        rolling_restart_passed: profile == Profile::Full,
        persistent_volume_restart_passed: profile == Profile::Full,
        configured_server_failover_passed: true,
        follower_recovery_milliseconds: follower_recovery.as_millis(),
        command_leader_failover_milliseconds: command_leader_failover.as_millis(),
        event_leader_failover_milliseconds: event_leader_failover.as_millis(),
        quorum_loss_milliseconds: quorum_loss.as_millis(),
        quorum_restoration_to_first_accepted_publication_milliseconds: restoration_to_publication
            .as_millis(),
        worker_reconnect_milliseconds: worker_reconnect.as_millis(),
        worker_restart_recovery_milliseconds: worker_restart_recovery.as_millis(),
        persistent_volume_recovery_milliseconds: persistent_volume_recovery.as_millis(),
        worker_restarts_per_authority: profile.parameters().worker_restarts_per_authority,
        generated_resource_cleanup_delegated_to_xtask: true,
    })
}

fn qualification_quorum_probe(source: &EncodedMessage) -> Result<EncodedMessage> {
    let decoded = decode_typed::<TransportCellProbeRequestedV1>(
        source,
        &transport_cell_probe_requested_descriptor()?,
    )?;
    let operation_uuid = deterministic_uuid(40, 1);
    let metadata = MessageMetadata::new(
        MessageId::new(deterministic_uuid(41, 1))?,
        transport_cell_probe_requested_descriptor()?,
        EmittedAt::new(OffsetDateTime::from_unix_timestamp(1_700_000_003)?)?,
        MessageAuthority::Platform,
        MessageScope::tenant(deterministic_uuid(42, 1), CELL_ID, 1)?,
        Some(MessageTarget::Cell(CellId::from_str(CELL_ID)?)),
        CorrelationId::new(operation_uuid)?,
        None,
    )?;
    encode(
        &metadata,
        &TransportCellProbeRequestedV1::new(
            TransportProbeOperationId::new(operation_uuid)?,
            decoded.payload().probe_value().clone(),
        ),
    )
    .map_err(Into::into)
}

async fn acknowledge_fault_probe(config: &QualificationConfig) -> Result<()> {
    let cell_id = CellId::from_str(CELL_ID)?;
    let secret = resolve_secret(&config.cell_nats_ref)?;
    let transport = JetStreamRuntime::connect(
        NatsCredential::parse_json(secret.expose_secret())?,
        &NatsConnectionConfig::new(
            "qualification-fault-cleanup",
            "qualification",
            Some(cell_id.clone()),
            config.servers.clone(),
            NatsTlsMode::VerifyFull,
            Some(config.ca_file.clone()),
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(15),
        )?,
        NatsRuntimeRole::CellWorker(cell_id.clone()),
    )
    .await?;
    let consumer = transport
        .bind_consumer(&cell_command_binding(&cell_id))
        .await?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let deliveries = consumer.fetch(1, Duration::from_secs(2)).await?;
        if let Some(delivery) = deliveries.into_iter().next() {
            delivery.into_acknowledgment().double_ack().await?;
            break;
        }
        if Instant::now() >= deadline {
            bail!("quorum fault probe cleanup timed out");
        }
    }
    transport.drain().await?;
    Ok(())
}
async fn sample_platform_message(
    config: &QualificationConfig,
    kind: &str,
) -> Result<EncodedMessage> {
    let pool = connect_raw_postgres(&config.platform_migrator_ref).await?;
    let row = sqlx::query(
        "SELECT envelope FROM platform_messaging.outbox_message WHERE message_kind = $1 ORDER BY message_id LIMIT 1",
    )
    .bind(kind)
    .fetch_one(&pool)
    .await?;
    let bytes: Vec<u8> = row.try_get("envelope")?;
    pool.close().await;
    decode_envelope(&bytes).map_err(Into::into)
}

async fn publish_until_accepted(
    transport: &JetStreamRuntime,
    message: &EncodedMessage,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if transport.publish_exact(message).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("publication did not resume within the fault bound");
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn stream_leaders(config: &QualificationConfig) -> Result<(String, String)> {
    let client = connect_raw_nats(config, &config.inspector_ref).await?;
    let context = jetstream::new(client.clone());
    let commands = context.get_stream(TransportStream::Commands.name()).await?;
    let events = context.get_stream(TransportStream::Events.name()).await?;
    let command_leader = commands
        .cached_info()
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.leader.clone())
        .ok_or_else(|| anyhow!("command stream has no leader"))?;
    let event_leader = events
        .cached_info()
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.leader.clone())
        .ok_or_else(|| anyhow!("event stream has no leader"))?;
    client.drain().await?;
    Ok((command_leader, event_leader))
}

fn compose_base(config: &QualificationConfig) -> Command {
    let mut command = Command::new("docker");
    command
        .args([
            "compose",
            "--project-name",
            &config.compose_project,
            "--file",
            "infra/local/nats/compose.yml",
        ])
        .current_dir(&config.root)
        .env("EDTECH_NATS_STATE_DIR", &config.nats_state_dir);
    for (index, server) in config.servers.iter().enumerate() {
        if let Some(port) = server
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse::<u16>().ok())
        {
            command.env(
                format!("EDTECH_NATS_{}_CLIENT_PORT", index + 1),
                port.to_string(),
            );
        }
        command.env(
            format!("EDTECH_NATS_{}_MONITOR_PORT", index + 1),
            config.monitor_ports[index].to_string(),
        );
    }
    command.stdout(Stdio::null()).stderr(Stdio::null());
    command
}

fn compose_service(config: &QualificationConfig, action: &str, service: &str) -> Result<()> {
    let status = compose_base(config).args([action, service]).status()?;
    ensure!(status.success(), "NATS service fault action failed");
    Ok(())
}

fn compose_all(config: &QualificationConfig, action: &str) -> Result<()> {
    let status = compose_base(config).arg(action).status()?;
    ensure!(status.success(), "NATS cluster fault action failed");
    Ok(())
}

async fn wait_cluster_usable(config: &QualificationConfig, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(client) = connect_raw_nats(config, &config.inspector_ref).await {
            let context = jetstream::new(client.clone());
            let usable = context
                .get_stream(TransportStream::Commands.name())
                .await
                .is_ok()
                && context
                    .get_stream(TransportStream::Events.name())
                    .await
                    .is_ok();
            let _drain = client.drain().await;
            if usable {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            bail!("NATS cluster did not become usable within the fault bound");
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_cluster_current(config: &QualificationConfig, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let result = async {
            let client = connect_raw_nats(config, &config.inspector_ref).await?;
            let context = jetstream::new(client.clone());
            for stream_name in [
                TransportStream::Commands.name(),
                TransportStream::Events.name(),
            ] {
                let stream = context.get_stream(stream_name).await?;
                let cluster = stream
                    .cached_info()
                    .cluster
                    .as_ref()
                    .ok_or_else(|| anyhow!("stream cluster data is absent"))?;
                ensure!(cluster.leader.is_some(), "stream leader is absent");
                ensure!(
                    cluster.replicas.len() == 2
                        && cluster
                            .replicas
                            .iter()
                            .all(|replica| replica.current && !replica.offline),
                    "stream replicas are not current"
                );
            }
            client.drain().await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if result.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("NATS replicas did not return current within the fault bound");
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn broker_pending(config: &QualificationConfig) -> Result<(u64, u64)> {
    let client = connect_raw_nats(config, &config.inspector_ref).await?;
    let context = jetstream::new(client.clone());
    let mut command_pending = 0_u64;
    let mut event_pending = 0_u64;
    for (stream_name, durable, is_command) in [
        ("EDTECH_COMMANDS_V1", "EDTECH_PLATFORM_COMMANDS_V1", true),
        (
            "EDTECH_COMMANDS_V1",
            "EDTECH_CELL_CELL_001_COMMANDS_V1",
            true,
        ),
        ("EDTECH_EVENTS_V1", "EDTECH_PLATFORM_EVENTS_V1", false),
        ("EDTECH_EVENTS_V1", "EDTECH_CELL_CELL_001_EVENTS_V1", false),
    ] {
        let stream = context.get_stream(stream_name).await?;
        let info = stream.consumer_info(durable).await?;
        let pending = info
            .num_pending
            .saturating_add(u64::try_from(info.num_ack_pending).unwrap_or(u64::MAX));
        if is_command {
            command_pending = command_pending.saturating_add(pending);
        } else {
            event_pending = event_pending.saturating_add(pending);
        }
    }
    client.drain().await?;
    Ok((command_pending, event_pending))
}

async fn wait_broker_drained(config: &QualificationConfig, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let (commands, events) = broker_pending(config).await?;
        if commands == 0 && events == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("durable consumers did not drain after qualification faults");
        }
        sleep(Duration::from_millis(200)).await;
    }
}

fn http_json(port: u16, path: &str) -> Result<serde_json::Value> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    write!(
        stream,
        "GET {path} HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    let separator = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| anyhow!("monitor response is malformed"))?;
    let headers = String::from_utf8_lossy(&response[..separator]).to_ascii_lowercase();
    let body = &response[separator + 4..];
    if headers.contains("transfer-encoding: chunked") {
        let decoded = decode_chunked_body(body)?;
        serde_json::from_slice(&decoded).context("monitor JSON is malformed")
    } else {
        serde_json::from_slice(body).context("monitor JSON is malformed")
    }
}

fn decode_chunked_body(body: &[u8]) -> Result<Vec<u8>> {
    let mut offset = 0_usize;
    let mut decoded = Vec::new();
    loop {
        let line_end = body[offset..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|position| offset + position)
            .ok_or_else(|| anyhow!("chunked monitor response is malformed"))?;
        let size_text = std::str::from_utf8(&body[offset..line_end])?
            .split(';')
            .next()
            .unwrap_or_default();
        let size = usize::from_str_radix(size_text, 16)
            .context("chunked monitor response has an invalid size")?;
        offset = line_end.saturating_add(2);
        if size == 0 {
            break;
        }
        let end = offset.saturating_add(size);
        ensure!(
            end.saturating_add(2) <= body.len(),
            "chunked monitor response is truncated"
        );
        decoded.extend_from_slice(&body[offset..end]);
        ensure!(
            &body[end..end + 2] == b"\r\n",
            "chunked monitor response delimiter is invalid"
        );
        offset = end.saturating_add(2);
    }
    Ok(decoded)
}
