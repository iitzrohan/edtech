//! Administrative-only NATS `JetStream` topology planning, application, and drift verification.
//!
//! This crate must not publish or consume application payloads, access databases, or run workers.

use std::{collections::BTreeSet, fmt, str::FromStr, time::Duration};

use async_nats::{
    ConnectErrorKind, ConnectOptions, ServerAddr,
    jetstream::{
        self,
        consumer::{AckPolicy, DeliverPolicy, ReplayPolicy},
        stream::{Config as StreamConfig, DiscardPolicy, RetentionPolicy, StorageType},
    },
};
use futures_util::TryStreamExt;
use nats_jetstream::{
    ConsumerBinding, NatsConnectionConfig, NatsCredential, NatsRuntimeRole, NatsServerVersion,
    NatsTlsMode, TransportStream, cell_command_binding, cell_event_binding,
    platform_command_binding, platform_event_binding,
};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use tenancy_domain::CellId;

const TOPOLOGY_SCHEMA_VERSION: u32 = 1;
const TRANSPORT_CONTRACT_VERSION: u32 = 1;
const COMMAND_MAX_MESSAGES: i64 = 1_000_000;
const COMMAND_MAX_BYTES: i64 = 1_073_741_824;
const COMMAND_MAX_AGE_SECONDS: u64 = 7 * 24 * 60 * 60;
const EVENT_MAX_MESSAGES: i64 = 2_000_000;
const EVENT_MAX_BYTES: i64 = 2_147_483_648;
const EVENT_MAX_AGE_SECONDS: u64 = 30 * 24 * 60 * 60;
const MAX_MESSAGE_SIZE: i32 = 270_336;
const MAX_CONSUMERS: i32 = 2_048;
const DUPLICATE_WINDOW_SECONDS: u64 = 120;
const STREAM_REPLICAS: usize = 3;
const CONSUMER_REPLICAS: usize = 3;
const ACK_WAIT_SECONDS: u64 = 30;
const MAX_ACK_PENDING: i64 = 1_024;
const MAX_WAITING: i64 = 64;
const MAX_BATCH: i64 = 200;
const MAX_EXPIRES_SECONDS: u64 = 5;

/// Stable administrative failure categories.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AdminErrorKind {
    /// The strict topology manifest is malformed or inconsistent.
    Manifest,
    /// Connection configuration or role is invalid.
    Configuration,
    /// Authentication failed.
    Authentication,
    /// Authorization rejected an administrative request.
    Authorization,
    /// NATS could not be reached within the configured bound.
    Connection,
    /// The connected NATS server is outside the qualified interval.
    ServerVersion,
    /// Current topology contains a refused destructive or unsafe difference.
    UnsafeDrift,
    /// A bounded apply/readiness wait expired.
    Timeout,
    /// A `JetStream` topology operation failed safely.
    Provider,
}

impl AdminErrorKind {
    /// Returns the stable content-free category.
    #[must_use]
    pub const fn safe_category(self) -> &'static str {
        match self {
            Self::Manifest => "topology.manifest",
            Self::Configuration => "topology.configuration",
            Self::Authentication => "transport.authentication",
            Self::Authorization => "transport.authorization",
            Self::Connection => "transport.unavailable",
            Self::ServerVersion => "transport.server-version",
            Self::UnsafeDrift => "topology.unsafe-drift",
            Self::Timeout => "topology.timeout",
            Self::Provider => "topology.provider",
        }
    }
}

/// Content-free administrative error.
pub struct AdminError {
    kind: AdminErrorKind,
}

impl AdminError {
    const fn new(kind: AdminErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable category.
    #[must_use]
    pub const fn kind(&self) -> AdminErrorKind {
        self.kind
    }
}

impl fmt::Display for AdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "NATS topology error: {}",
            self.kind.safe_category()
        )
    }
}

impl fmt::Debug for AdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl std::error::Error for AdminError {}

/// Strict validated topology schema with fixed streams and declared Cells.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopologyManifest {
    cells: Vec<CellId>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTopologyManifest {
    schema_version: u32,
    transport_contract_version: u32,
    cells: Vec<String>,
    streams: RawStreams,
    consumers: RawConsumers,
    consumer_defaults: RawConsumerDefaults,
    cell_consumer_derivation: RawCellConsumerDerivation,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStreams {
    commands: RawStream,
    events: RawStream,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStream {
    name: String,
    subject: String,
    retention: String,
    storage: String,
    replicas: usize,
    discard: String,
    max_message_size: i32,
    max_messages: i64,
    max_bytes: i64,
    max_age_seconds: u64,
    max_consumers: i32,
    duplicate_window_seconds: u64,
    allow_direct: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConsumers {
    platform_commands: RawConsumer,
    platform_events: RawConsumer,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConsumer {
    stream: String,
    durable_name: String,
    filter_subject: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConsumerDefaults {
    delivery: String,
    durable: bool,
    ack_policy: String,
    deliver_policy: String,
    replay_policy: String,
    ack_wait_seconds: u64,
    max_deliver: i64,
    max_ack_pending: i64,
    max_waiting: i64,
    max_batch: i64,
    max_expires_seconds: u64,
    replicas: usize,
    memory_storage: bool,
    inactive_auto_delete: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCellConsumerDerivation {
    uppercase_cell_id: bool,
    replace_hyphen_with_underscore: bool,
    command_prefix: String,
    command_suffix: String,
    event_prefix: String,
    event_suffix: String,
}

impl TopologyManifest {
    /// Parses and validates topology schema version 1 with every fixed production bound.
    ///
    /// # Errors
    ///
    /// Rejects malformed TOML, unknown fields, duplicate/invalid Cells, or any changed fixed
    /// topology value.
    pub fn parse_toml(value: &str) -> Result<Self, AdminError> {
        let raw: RawTopologyManifest =
            toml::from_str(value).map_err(|_| AdminError::new(AdminErrorKind::Manifest))?;
        validate_raw_manifest(&raw)?;
        let cells = raw
            .cells
            .iter()
            .map(|cell| {
                CellId::from_str(cell).map_err(|_| AdminError::new(AdminErrorKind::Manifest))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let unique = cells.iter().map(CellId::as_str).collect::<BTreeSet<_>>();
        if cells.is_empty() || cells.len() > 1_000 || unique.len() != cells.len() {
            return Err(AdminError::new(AdminErrorKind::Manifest));
        }
        Ok(Self { cells })
    }

    /// Returns declared logical Cells in stable manifest order.
    #[must_use]
    pub fn cells(&self) -> &[CellId] {
        &self.cells
    }

    /// Returns the two exact production stream definitions.
    #[must_use]
    pub fn streams(&self) -> [DesiredStream; 2] {
        [desired_commands(), desired_events()]
    }

    /// Returns Platform and per-Cell consumer definitions in stable order.
    #[must_use]
    pub fn consumers(&self) -> Vec<DesiredConsumer> {
        let mut consumers = vec![
            DesiredConsumer::from_binding(&platform_command_binding()),
            DesiredConsumer::from_binding(&platform_event_binding()),
        ];
        for cell in &self.cells {
            consumers.push(DesiredConsumer::from_binding(&cell_command_binding(cell)));
            consumers.push(DesiredConsumer::from_binding(&cell_event_binding(cell)));
        }
        consumers
    }
}

fn validate_raw_manifest(raw: &RawTopologyManifest) -> Result<(), AdminError> {
    let command = &raw.streams.commands;
    let event = &raw.streams.events;
    let platform_commands = &raw.consumers.platform_commands;
    let platform_events = &raw.consumers.platform_events;
    let defaults = &raw.consumer_defaults;
    let derivation = &raw.cell_consumer_derivation;
    let valid = raw.schema_version == TOPOLOGY_SCHEMA_VERSION
        && raw.transport_contract_version == TRANSPORT_CONTRACT_VERSION
        && exact_raw_stream(
            command,
            "EDTECH_COMMANDS_V1",
            "edtech.v1.command.>",
            "work_queue",
            COMMAND_MAX_MESSAGES,
            COMMAND_MAX_BYTES,
            COMMAND_MAX_AGE_SECONDS,
        )
        && exact_raw_stream(
            event,
            "EDTECH_EVENTS_V1",
            "edtech.v1.event.>",
            "limits",
            EVENT_MAX_MESSAGES,
            EVENT_MAX_BYTES,
            EVENT_MAX_AGE_SECONDS,
        )
        && exact_raw_consumer(
            platform_commands,
            "EDTECH_COMMANDS_V1",
            "EDTECH_PLATFORM_COMMANDS_V1",
            "edtech.v1.command.cell-to-platform.>",
        )
        && exact_raw_consumer(
            platform_events,
            "EDTECH_EVENTS_V1",
            "EDTECH_PLATFORM_EVENTS_V1",
            "edtech.v1.event.cell-to-platform.>",
        )
        && defaults.delivery == "pull"
        && defaults.durable
        && defaults.ack_policy == "explicit"
        && defaults.deliver_policy == "all"
        && defaults.replay_policy == "instant"
        && defaults.ack_wait_seconds == ACK_WAIT_SECONDS
        && defaults.max_deliver == -1
        && defaults.max_ack_pending == MAX_ACK_PENDING
        && defaults.max_waiting == MAX_WAITING
        && defaults.max_batch == MAX_BATCH
        && defaults.max_expires_seconds == MAX_EXPIRES_SECONDS
        && defaults.replicas == CONSUMER_REPLICAS
        && !defaults.memory_storage
        && !defaults.inactive_auto_delete
        && derivation.uppercase_cell_id
        && derivation.replace_hyphen_with_underscore
        && derivation.command_prefix == "EDTECH_CELL_"
        && derivation.command_suffix == "_COMMANDS_V1"
        && derivation.event_prefix == "EDTECH_CELL_"
        && derivation.event_suffix == "_EVENTS_V1";
    if valid {
        Ok(())
    } else {
        Err(AdminError::new(AdminErrorKind::Manifest))
    }
}

#[allow(clippy::too_many_arguments)]
fn exact_raw_stream(
    raw: &RawStream,
    name: &str,
    subject: &str,
    retention: &str,
    max_messages: i64,
    max_bytes: i64,
    max_age_seconds: u64,
) -> bool {
    raw.name == name
        && raw.subject == subject
        && raw.retention == retention
        && raw.storage == "file"
        && raw.replicas == STREAM_REPLICAS
        && raw.discard == "new"
        && raw.max_message_size == MAX_MESSAGE_SIZE
        && raw.max_messages == max_messages
        && raw.max_bytes == max_bytes
        && raw.max_age_seconds == max_age_seconds
        && raw.max_consumers == MAX_CONSUMERS
        && raw.duplicate_window_seconds == DUPLICATE_WINDOW_SECONDS
        && !raw.allow_direct
}

fn exact_raw_consumer(raw: &RawConsumer, stream: &str, durable: &str, filter: &str) -> bool {
    raw.stream == stream && raw.durable_name == durable && raw.filter_subject == filter
}

/// Provider-neutral exact desired stream definition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DesiredStream {
    /// Fixed stream name.
    pub name: String,
    /// Exact stream subject.
    pub subject: String,
    /// Command work-queue or event limits retention.
    pub retention: String,
    /// Maximum retained messages.
    pub max_messages: i64,
    /// Maximum retained file bytes.
    pub max_bytes: i64,
    /// Maximum message age in seconds.
    pub max_age_seconds: u64,
}

impl DesiredStream {
    fn transport_stream(&self) -> TransportStream {
        if self.name == TransportStream::Commands.name() {
            TransportStream::Commands
        } else {
            TransportStream::Events
        }
    }

    fn provider_config(&self) -> StreamConfig {
        StreamConfig {
            name: self.name.clone(),
            description: Some(format!("EdTech {} transport v1", self.retention)),
            subjects: vec![self.subject.clone()],
            retention: match self.transport_stream() {
                TransportStream::Commands => RetentionPolicy::WorkQueue,
                TransportStream::Events => RetentionPolicy::Limits,
            },
            storage: StorageType::File,
            num_replicas: STREAM_REPLICAS,
            discard: DiscardPolicy::New,
            max_message_size: MAX_MESSAGE_SIZE,
            max_messages: self.max_messages,
            max_bytes: self.max_bytes,
            max_age: Duration::from_secs(self.max_age_seconds),
            max_consumers: MAX_CONSUMERS,
            duplicate_window: Duration::from_secs(DUPLICATE_WINDOW_SECONDS),
            no_ack: false,
            allow_direct: false,
            deny_delete: true,
            deny_purge: true,
            republish: None,
            mirror: None,
            sources: None,
            allow_batch_publish: false,
            ..Default::default()
        }
    }
}

fn desired_commands() -> DesiredStream {
    DesiredStream {
        name: String::from("EDTECH_COMMANDS_V1"),
        subject: String::from("edtech.v1.command.>"),
        retention: String::from("work_queue"),
        max_messages: COMMAND_MAX_MESSAGES,
        max_bytes: COMMAND_MAX_BYTES,
        max_age_seconds: COMMAND_MAX_AGE_SECONDS,
    }
}

fn desired_events() -> DesiredStream {
    DesiredStream {
        name: String::from("EDTECH_EVENTS_V1"),
        subject: String::from("edtech.v1.event.>"),
        retention: String::from("limits"),
        max_messages: EVENT_MAX_MESSAGES,
        max_bytes: EVENT_MAX_BYTES,
        max_age_seconds: EVENT_MAX_AGE_SECONDS,
    }
}

/// Provider-neutral exact desired durable consumer definition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DesiredConsumer {
    /// Fixed stream name.
    pub stream: String,
    /// Fixed durable name.
    pub durable_name: String,
    /// Exact non-overlapping filter.
    pub filter_subject: String,
}

impl DesiredConsumer {
    fn from_binding(binding: &ConsumerBinding) -> Self {
        Self {
            stream: binding.stream().name().to_owned(),
            durable_name: binding.durable_name().to_owned(),
            filter_subject: binding.filter_subject().to_owned(),
        }
    }

    fn provider_config(&self) -> jetstream::consumer::Config {
        jetstream::consumer::Config {
            deliver_subject: None,
            durable_name: Some(self.durable_name.clone()),
            name: Some(self.durable_name.clone()),
            description: Some(String::from("EdTech durable pull consumer v1")),
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::Explicit,
            ack_wait: Duration::from_secs(ACK_WAIT_SECONDS),
            max_deliver: -1,
            filter_subject: self.filter_subject.clone(),
            replay_policy: ReplayPolicy::Instant,
            max_waiting: MAX_WAITING,
            max_ack_pending: MAX_ACK_PENDING,
            max_batch: MAX_BATCH,
            max_expires: Duration::from_secs(MAX_EXPIRES_SECONDS),
            num_replicas: CONSUMER_REPLICAS,
            memory_storage: false,
            inactive_threshold: Duration::ZERO,
            rate_limit: 0,
            sample_frequency: 0,
            ..Default::default()
        }
    }
}

/// Planned administrative action for one asset.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyAction {
    /// Create a missing declared asset.
    Create,
    /// Apply an explicitly safe monotonic update.
    SafeUpdate,
    /// Asset already exactly matches.
    NoChange,
    /// Refuse a destructive or unsafe difference.
    Refused,
    /// Report but preserve an undeclared EDTECH-prefixed asset.
    UnknownAsset,
}

/// Safe drift category without raw provider configuration.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyDriftCategory {
    /// Declared asset is absent.
    Missing,
    /// Capacity or description may be increased safely.
    SafeCapacityIncrease,
    /// Asset exactly matches the manifest.
    Converged,
    /// Subjects differ.
    SubjectChange,
    /// Retention differs.
    RetentionChange,
    /// Storage differs.
    StorageChange,
    /// Replica count would decrease or otherwise conflicts.
    ReplicaChange,
    /// A configured bound would decrease.
    LimitDecrease,
    /// Durable/filter/delivery/ack identity differs.
    ConsumerIdentityChange,
    /// Forbidden topology capability is enabled.
    ForbiddenFeature,
    /// Undeclared EDTECH-prefixed asset remains present.
    UnknownEdtechAsset,
}

/// One safe topology plan item.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TopologyPlanItem {
    /// Fixed asset name only.
    pub asset: String,
    /// Safe action classification.
    pub action: TopologyAction,
    /// Stable drift category.
    pub category: TopologyDriftCategory,
}

/// Complete non-destructive topology plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TopologyPlan {
    /// Stable ordered plan items.
    pub items: Vec<TopologyPlanItem>,
}

impl TopologyPlan {
    /// Reports whether any change is explicitly refused.
    #[must_use]
    pub fn has_refused_change(&self) -> bool {
        self.items
            .iter()
            .any(|item| item.action == TopologyAction::Refused)
    }

    /// Reports convergence while allowing undeclared assets to remain visible.
    #[must_use]
    pub fn is_converged(&self) -> bool {
        self.items.iter().all(|item| {
            matches!(
                item.action,
                TopologyAction::NoChange | TopologyAction::UnknownAsset
            )
        })
    }

    fn action_for(&self, asset: &str) -> Option<TopologyAction> {
        self.items
            .iter()
            .find(|item| item.asset == asset)
            .map(|item| item.action)
    }
}

/// Aggregate safe apply result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TopologyApplyReport {
    /// Created stream count.
    pub created_streams: u32,
    /// Safely updated stream count.
    pub updated_streams: u32,
    /// Created consumer count.
    pub created_consumers: u32,
    /// Safely updated consumer count.
    pub updated_consumers: u32,
    /// Undeclared EDTECH assets reported but preserved.
    pub unknown_assets: u32,
    /// Final convergence result.
    pub converged: bool,
}

/// Separately privileged administrative `JetStream` connection.
pub struct NatsJetStreamAdmin {
    client: async_nats::Client,
    context: jetstream::Context,
    server_version: NatsServerVersion,
}

impl fmt::Debug for NatsJetStreamAdmin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsJetStreamAdmin")
            .field("server_version", &self.server_version)
            .finish_non_exhaustive()
    }
}

impl NatsJetStreamAdmin {
    /// Connects with the exact provisioner profile and verifies the qualified server version.
    ///
    /// # Errors
    ///
    /// Returns safe credential, connection, TLS, authorization, or version categories.
    pub async fn connect(
        credential: NatsCredential,
        config: &NatsConnectionConfig,
    ) -> Result<Self, AdminError> {
        credential
            .validate_for_role(&NatsRuntimeRole::Provisioner)
            .map_err(|_| AdminError::new(AdminErrorKind::Configuration))?;
        let (username, password) = credential.into_secret_parts();
        let servers = config
            .server_values()
            .iter()
            .map(|value| {
                value
                    .parse::<ServerAddr>()
                    .map_err(|_| AdminError::new(AdminErrorKind::Configuration))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut options = ConnectOptions::new()
            .name(config.safe_connection_name())
            .user_and_password(
                username.expose_secret().to_owned(),
                password.expose_secret().to_owned(),
            )
            .connection_timeout(config.connect_timeout())
            .request_timeout(Some(config.request_timeout()))
            .max_reconnects(None);
        if config.tls_mode() == NatsTlsMode::VerifyFull {
            options = options.require_tls(true);
            if let Some(path) = config.ca_certificate_file() {
                options = options.add_root_certificates(path.clone());
            }
        }
        let client = tokio::time::timeout(config.startup_timeout(), options.connect(servers))
            .await
            .map_err(|_| AdminError::new(AdminErrorKind::Timeout))?
            .map_err(|error| map_connect_error(&error))?;
        let server_version = NatsServerVersion::parse(&client.server_info().version)
            .and_then(|version| version.verify_qualified().map(|()| version))
            .map_err(|_| AdminError::new(AdminErrorKind::ServerVersion))?;
        let context = jetstream::context::ContextBuilder::new()
            .timeout(config.request_timeout())
            .build(client.clone());
        Ok(Self {
            client,
            context,
            server_version,
        })
    }

    /// Returns the qualified server version.
    #[must_use]
    pub const fn server_version(&self) -> NatsServerVersion {
        self.server_version
    }

    /// Plans changes without mutation, including unknown EDTECH-prefixed assets.
    ///
    /// # Errors
    ///
    /// Returns safe provider/authorization categories when topology cannot be inspected.
    pub async fn plan(&self, manifest: &TopologyManifest) -> Result<TopologyPlan, AdminError> {
        let mut items = Vec::new();
        let mut existing_streams = self.context.stream_names();
        let mut stream_names = BTreeSet::new();
        while let Some(name) = existing_streams
            .try_next()
            .await
            .map_err(|_| AdminError::new(AdminErrorKind::Provider))?
        {
            stream_names.insert(name);
        }
        let desired_streams = manifest.streams();
        for desired in &desired_streams {
            if stream_names.contains(&desired.name) {
                let stream = self
                    .context
                    .get_stream(&desired.name)
                    .await
                    .map_err(|_| AdminError::new(AdminErrorKind::Provider))?;
                items.push(classify_stream(desired, &stream.cached_info().config));
            } else {
                items.push(TopologyPlanItem {
                    asset: desired.name.clone(),
                    action: TopologyAction::Create,
                    category: TopologyDriftCategory::Missing,
                });
            }
        }
        for extra in stream_names.iter().filter(|name| {
            name.starts_with("EDTECH_")
                && !desired_streams.iter().any(|desired| &desired.name == *name)
        }) {
            items.push(TopologyPlanItem {
                asset: extra.clone(),
                action: TopologyAction::UnknownAsset,
                category: TopologyDriftCategory::UnknownEdtechAsset,
            });
        }

        let desired_consumers = manifest.consumers();
        for desired_stream in &desired_streams {
            if !stream_names.contains(&desired_stream.name) {
                for consumer in desired_consumers
                    .iter()
                    .filter(|consumer| consumer.stream == desired_stream.name)
                {
                    items.push(TopologyPlanItem {
                        asset: consumer.durable_name.clone(),
                        action: TopologyAction::Create,
                        category: TopologyDriftCategory::Missing,
                    });
                }
                continue;
            }
            let stream = self
                .context
                .get_stream(&desired_stream.name)
                .await
                .map_err(|_| AdminError::new(AdminErrorKind::Provider))?;
            let mut names = stream.consumer_names();
            let mut existing_consumers = BTreeSet::new();
            while let Some(name) = names
                .try_next()
                .await
                .map_err(|_| AdminError::new(AdminErrorKind::Provider))?
            {
                existing_consumers.insert(name);
            }
            for desired in desired_consumers
                .iter()
                .filter(|consumer| consumer.stream == desired_stream.name)
            {
                if existing_consumers.contains(&desired.durable_name) {
                    let info = stream
                        .consumer_info(&desired.durable_name)
                        .await
                        .map_err(|_| AdminError::new(AdminErrorKind::Provider))?;
                    items.push(classify_consumer(desired, &info.config));
                } else {
                    items.push(TopologyPlanItem {
                        asset: desired.durable_name.clone(),
                        action: TopologyAction::Create,
                        category: TopologyDriftCategory::Missing,
                    });
                }
            }
            for extra in existing_consumers.iter().filter(|name| {
                name.starts_with("EDTECH_")
                    && !desired_consumers
                        .iter()
                        .any(|desired| &desired.durable_name == *name)
            }) {
                items.push(TopologyPlanItem {
                    asset: extra.clone(),
                    action: TopologyAction::UnknownAsset,
                    category: TopologyDriftCategory::UnknownEdtechAsset,
                });
            }
        }
        Ok(TopologyPlan { items })
    }

    /// Applies only missing assets and explicitly safe monotonic changes, then waits for R3
    /// readiness and verifies convergence.
    ///
    /// # Errors
    ///
    /// Refuses the entire mutation phase when any unsafe change exists.
    pub async fn apply(
        &self,
        manifest: &TopologyManifest,
        timeout: Duration,
    ) -> Result<TopologyApplyReport, AdminError> {
        if timeout.is_zero() || timeout > Duration::from_mins(5) {
            return Err(AdminError::new(AdminErrorKind::Configuration));
        }
        let plan = self.plan(manifest).await?;
        if plan.has_refused_change() {
            return Err(AdminError::new(AdminErrorKind::UnsafeDrift));
        }
        let mut report = TopologyApplyReport {
            created_streams: 0,
            updated_streams: 0,
            created_consumers: 0,
            updated_consumers: 0,
            unknown_assets: u32::try_from(
                plan.items
                    .iter()
                    .filter(|item| item.action == TopologyAction::UnknownAsset)
                    .count(),
            )
            .unwrap_or(u32::MAX),
            converged: false,
        };
        for desired in manifest.streams() {
            match plan.action_for(&desired.name) {
                Some(TopologyAction::Create) => {
                    self.context
                        .create_stream(desired.provider_config())
                        .await
                        .map_err(|_| AdminError::new(AdminErrorKind::Provider))?;
                    report.created_streams = report.created_streams.saturating_add(1);
                }
                Some(TopologyAction::SafeUpdate) => {
                    self.context
                        .update_stream(desired.provider_config())
                        .await
                        .map_err(|_| AdminError::new(AdminErrorKind::Provider))?;
                    report.updated_streams = report.updated_streams.saturating_add(1);
                }
                Some(TopologyAction::NoChange | TopologyAction::UnknownAsset) | None => {}
                Some(TopologyAction::Refused) => {
                    return Err(AdminError::new(AdminErrorKind::UnsafeDrift));
                }
            }
        }
        for desired in manifest.consumers() {
            let stream = self
                .context
                .get_stream(&desired.stream)
                .await
                .map_err(|_| AdminError::new(AdminErrorKind::Provider))?;
            match plan.action_for(&desired.durable_name) {
                Some(TopologyAction::Create) => {
                    stream
                        .create_consumer(desired.provider_config())
                        .await
                        .map_err(|_| AdminError::new(AdminErrorKind::Provider))?;
                    report.created_consumers = report.created_consumers.saturating_add(1);
                }
                Some(TopologyAction::SafeUpdate) => {
                    stream
                        .create_consumer(desired.provider_config())
                        .await
                        .map_err(|_| AdminError::new(AdminErrorKind::Provider))?;
                    report.updated_consumers = report.updated_consumers.saturating_add(1);
                }
                Some(TopologyAction::NoChange | TopologyAction::UnknownAsset) | None => {}
                Some(TopologyAction::Refused) => {
                    return Err(AdminError::new(AdminErrorKind::UnsafeDrift));
                }
            }
        }
        self.wait_ready(manifest, timeout).await?;
        let final_plan = self.plan(manifest).await?;
        report.converged = final_plan.is_converged();
        if report.converged {
            Ok(report)
        } else {
            Err(AdminError::new(AdminErrorKind::Provider))
        }
    }

    /// Drains the provisioner connection before one-shot process exit.
    ///
    /// # Errors
    ///
    /// Returns a safe provider category.
    pub async fn drain(&self) -> Result<(), AdminError> {
        self.client
            .drain()
            .await
            .map_err(|_| AdminError::new(AdminErrorKind::Provider))
    }

    async fn wait_ready(
        &self,
        manifest: &TopologyManifest,
        timeout: Duration,
    ) -> Result<(), AdminError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.topology_ready(manifest).await? {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(AdminError::new(AdminErrorKind::Timeout));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn topology_ready(&self, manifest: &TopologyManifest) -> Result<bool, AdminError> {
        for desired in manifest.streams() {
            let stream = self
                .context
                .get_stream(&desired.name)
                .await
                .map_err(|_| AdminError::new(AdminErrorKind::Provider))?;
            if !cluster_ready(stream.cached_info().cluster.as_ref()) {
                return Ok(false);
            }
        }
        for desired in manifest.consumers() {
            let stream = self
                .context
                .get_stream(&desired.stream)
                .await
                .map_err(|_| AdminError::new(AdminErrorKind::Provider))?;
            let info = stream
                .consumer_info(&desired.durable_name)
                .await
                .map_err(|_| AdminError::new(AdminErrorKind::Provider))?;
            if !cluster_ready(info.cluster.as_ref()) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn map_connect_error(error: &async_nats::ConnectError) -> AdminError {
    let kind = match error.kind() {
        ConnectErrorKind::Authentication => AdminErrorKind::Authentication,
        ConnectErrorKind::AuthorizationViolation => AdminErrorKind::Authorization,
        ConnectErrorKind::ServerParse => AdminErrorKind::Configuration,
        ConnectErrorKind::Tls
        | ConnectErrorKind::Dns
        | ConnectErrorKind::Io
        | ConnectErrorKind::TimedOut
        | ConnectErrorKind::MaxReconnects => AdminErrorKind::Connection,
    };
    AdminError::new(kind)
}

fn cluster_ready(cluster: Option<&jetstream::stream::ClusterInfo>) -> bool {
    cluster.is_some_and(|cluster| {
        cluster.leader.is_some()
            && cluster.replicas.len() == STREAM_REPLICAS.saturating_sub(1)
            && cluster
                .replicas
                .iter()
                .all(|replica| replica.current && !replica.offline)
    })
}

fn classify_stream(desired: &DesiredStream, actual: &StreamConfig) -> TopologyPlanItem {
    let expected_retention = match desired.transport_stream() {
        TransportStream::Commands => RetentionPolicy::WorkQueue,
        TransportStream::Events => RetentionPolicy::Limits,
    };
    let expected_description = format!("EdTech {} transport v1", desired.retention);
    let (action, category) = if actual.subjects != [desired.subject.clone()] {
        (
            TopologyAction::Refused,
            TopologyDriftCategory::SubjectChange,
        )
    } else if actual.retention != expected_retention {
        (
            TopologyAction::Refused,
            TopologyDriftCategory::RetentionChange,
        )
    } else if actual.storage != StorageType::File {
        (
            TopologyAction::Refused,
            TopologyDriftCategory::StorageChange,
        )
    } else if actual.num_replicas > STREAM_REPLICAS {
        (
            TopologyAction::Refused,
            TopologyDriftCategory::ReplicaChange,
        )
    } else if actual.max_messages > desired.max_messages
        || actual.max_bytes > desired.max_bytes
        || actual.max_age > Duration::from_secs(desired.max_age_seconds)
        || actual.max_consumers > MAX_CONSUMERS
        || actual.max_message_size > MAX_MESSAGE_SIZE
        || actual.duplicate_window > Duration::from_secs(DUPLICATE_WINDOW_SECONDS)
    {
        (
            TopologyAction::Refused,
            TopologyDriftCategory::LimitDecrease,
        )
    } else if actual.discard != DiscardPolicy::New
        || actual.no_ack
        || actual.allow_direct
        || actual.republish.is_some()
        || actual.mirror.is_some()
        || actual.sources.is_some()
        || actual.allow_batch_publish
    {
        (
            TopologyAction::Refused,
            TopologyDriftCategory::ForbiddenFeature,
        )
    } else if actual.num_replicas < STREAM_REPLICAS
        || actual.max_messages < desired.max_messages
        || actual.max_bytes < desired.max_bytes
        || actual.max_age < Duration::from_secs(desired.max_age_seconds)
        || actual.max_consumers < MAX_CONSUMERS
        || actual.max_message_size < MAX_MESSAGE_SIZE
        || actual.duplicate_window != Duration::from_secs(DUPLICATE_WINDOW_SECONDS)
        || actual.description.as_deref() != Some(expected_description.as_str())
    {
        (
            TopologyAction::SafeUpdate,
            TopologyDriftCategory::SafeCapacityIncrease,
        )
    } else {
        (TopologyAction::NoChange, TopologyDriftCategory::Converged)
    };
    TopologyPlanItem {
        asset: desired.name.clone(),
        action,
        category,
    }
}

fn classify_consumer(
    desired: &DesiredConsumer,
    actual: &jetstream::consumer::Config,
) -> TopologyPlanItem {
    let identity_matches = actual.durable_name.as_deref() == Some(&desired.durable_name)
        && actual.name.as_deref() == Some(&desired.durable_name)
        && actual.deliver_subject.is_none()
        && actual.filter_subject == desired.filter_subject
        && actual.deliver_policy == DeliverPolicy::All
        && actual.ack_policy == AckPolicy::Explicit
        && actual.replay_policy == ReplayPolicy::Instant
        && actual.ack_wait == Duration::from_secs(ACK_WAIT_SECONDS)
        && actual.max_deliver == -1
        && actual.inactive_threshold.is_zero();
    let (action, category) = if !identity_matches {
        (
            TopologyAction::Refused,
            TopologyDriftCategory::ConsumerIdentityChange,
        )
    } else if actual.num_replicas > CONSUMER_REPLICAS {
        (
            TopologyAction::Refused,
            TopologyDriftCategory::ReplicaChange,
        )
    } else if actual.max_ack_pending > MAX_ACK_PENDING
        || actual.max_waiting > MAX_WAITING
        || actual.max_batch > MAX_BATCH
        || actual.max_expires > Duration::from_secs(MAX_EXPIRES_SECONDS)
    {
        (
            TopologyAction::Refused,
            TopologyDriftCategory::LimitDecrease,
        )
    } else if actual.memory_storage
        || actual.rate_limit != 0
        || actual.sample_frequency != 0
        || !actual.backoff.is_empty()
    {
        (
            TopologyAction::Refused,
            TopologyDriftCategory::ForbiddenFeature,
        )
    } else if actual.num_replicas < CONSUMER_REPLICAS
        || actual.max_ack_pending < MAX_ACK_PENDING
        || actual.max_waiting < MAX_WAITING
        || actual.max_batch < MAX_BATCH
        || actual.max_expires < Duration::from_secs(MAX_EXPIRES_SECONDS)
        || actual.description.as_deref() != Some("EdTech durable pull consumer v1")
    {
        (
            TopologyAction::SafeUpdate,
            TopologyDriftCategory::SafeCapacityIncrease,
        )
    } else {
        (TopologyAction::NoChange, TopologyDriftCategory::Converged)
    };
    TopologyPlanItem {
        asset: desired.durable_name.clone(),
        action,
        category,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = include_str!("../../../infra/local/nats/templates/topology.toml");

    #[test]
    fn manifest_has_exact_streams_consumers_and_cell_derivation() {
        let manifest = TopologyManifest::parse_toml(MANIFEST)
            .unwrap_or_else(|error| panic!("checked-in manifest: {error}"));
        assert_eq!(manifest.streams().len(), 2);
        assert_eq!(manifest.consumers().len(), 4);
        assert_eq!(
            manifest.cells().first().map(CellId::as_str),
            Some("cell-001")
        );
        let filters = manifest
            .consumers()
            .into_iter()
            .map(|consumer| consumer.filter_subject)
            .collect::<BTreeSet<_>>();
        assert_eq!(filters.len(), 4);
    }

    #[test]
    fn manifest_rejects_unknown_or_changed_fixed_values() {
        assert!(
            TopologyManifest::parse_toml(&MANIFEST.replace("replicas = 3", "replicas = 1"))
                .is_err()
        );
        assert!(TopologyManifest::parse_toml(&format!("{MANIFEST}\nunknown = true\n")).is_err());
    }

    #[test]
    fn stream_planning_allows_increases_and_refuses_destructive_changes() {
        let desired = desired_commands();
        let exact = desired.provider_config();
        assert_eq!(
            classify_stream(&desired, &exact).action,
            TopologyAction::NoChange
        );

        let mut smaller = exact.clone();
        smaller.max_bytes = desired.max_bytes - 1;
        assert_eq!(
            classify_stream(&desired, &smaller).action,
            TopologyAction::SafeUpdate
        );

        let mut larger = exact.clone();
        larger.max_bytes = desired.max_bytes + 1;
        assert_eq!(
            classify_stream(&desired, &larger).category,
            TopologyDriftCategory::LimitDecrease
        );

        let mut retention = exact.clone();
        retention.retention = RetentionPolicy::Limits;
        assert_eq!(
            classify_stream(&desired, &retention).category,
            TopologyDriftCategory::RetentionChange
        );

        let mut subjects = exact;
        subjects.subjects = vec![String::from("edtech.v1.other.>")];
        assert_eq!(
            classify_stream(&desired, &subjects).category,
            TopologyDriftCategory::SubjectChange
        );
    }

    #[test]
    fn consumer_planning_refuses_filter_or_capacity_decrease() {
        let desired = DesiredConsumer::from_binding(&platform_command_binding());
        let exact = desired.provider_config();
        assert_eq!(
            classify_consumer(&desired, &exact).action,
            TopologyAction::NoChange
        );

        let mut smaller = exact.clone();
        smaller.max_ack_pending = MAX_ACK_PENDING - 1;
        assert_eq!(
            classify_consumer(&desired, &smaller).action,
            TopologyAction::SafeUpdate
        );

        let mut filter = exact.clone();
        filter.filter_subject = String::from("edtech.v1.command.other.>");
        assert_eq!(
            classify_consumer(&desired, &filter).category,
            TopologyDriftCategory::ConsumerIdentityChange
        );
    }

    #[test]
    fn unknown_assets_are_visible_but_never_destructive() {
        let plan = TopologyPlan {
            items: vec![TopologyPlanItem {
                asset: String::from("EDTECH_EXTRA_V1"),
                action: TopologyAction::UnknownAsset,
                category: TopologyDriftCategory::UnknownEdtechAsset,
            }],
        };
        assert!(!plan.has_refused_change());
        assert!(plan.is_converged());
    }
}
