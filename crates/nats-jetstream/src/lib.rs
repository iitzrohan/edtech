//! Runtime-only NATS `JetStream` connection, routing, publication, and durable pull consumption.
//!
//! This crate must not own topology mutation, databases, business dispatch, or product behavior.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use async_nats::{
    ConnectErrorKind, ConnectOptions, Event, HeaderMap, ServerAddr,
    jetstream::{
        self, AckKind,
        consumer::{AckPolicy, DeliverPolicy, PullConsumer, ReplayPolicy},
        context::{ContextBuilder, PublishError, PublishErrorKind},
        stream::{DiscardPolicy, RetentionPolicy, StorageType},
    },
};
use futures_util::StreamExt;
use message_domain::{
    EncodedMessage, MESSAGE_CONTENT_TYPE, MessageAuthority, MessageId, MessageKind,
    MessageMetadata, MessageScope, MessageTarget,
};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use tenancy_domain::CellId;

/// Fixed transport subject prefix for envelope version 1.
pub const TRANSPORT_SUBJECT_PREFIX: &str = "edtech.v1";
/// Production command stream name.
pub const COMMAND_STREAM_NAME: &str = "EDTECH_COMMANDS_V1";
/// Production event stream name.
pub const EVENT_STREAM_NAME: &str = "EDTECH_EVENTS_V1";
/// Platform command durable name.
pub const PLATFORM_COMMAND_DURABLE: &str = "EDTECH_PLATFORM_COMMANDS_V1";
/// Platform event durable name.
pub const PLATFORM_EVENT_DURABLE: &str = "EDTECH_PLATFORM_EVENTS_V1";

const MINIMUM_SERVER_VERSION: NatsServerVersion = NatsServerVersion::new(2, 14, 3);
const MAXIMUM_SERVER_VERSION_EXCLUSIVE: NatsServerVersion = NatsServerVersion::new(2, 15, 0);
const MAX_SERVER_COUNT: usize = 8;
const MAX_SERVER_URL_BYTES: usize = 320;
const MAX_HOST_BYTES: usize = 253;
const MAX_SUBJECT_BYTES: usize = 512;
const MAX_CREDENTIAL_USERNAME_BYTES: usize = 128;
const MAX_CREDENTIAL_PASSWORD_BYTES: usize = 512;
const MIN_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_TIMEOUT: Duration = Duration::from_mins(5);
const ACK_WAIT: Duration = Duration::from_secs(30);

/// Stable safe `JetStream` failure categories.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TransportErrorKind {
    /// The client is temporarily disconnected.
    Disconnected,
    /// A bounded transport operation timed out.
    Timeout,
    /// The stream cluster could not form a quorum.
    NoQuorum,
    /// `JetStream` is temporarily unavailable.
    Unavailable,
    /// A configured broker capacity bound rejected work.
    Capacity,
    /// A publish acknowledgment did not arrive within its bound.
    AckTimeout,
    /// Authentication failed.
    Authentication,
    /// Authorization rejected the attempted operation.
    Authorization,
    /// TLS setup or peer verification failed.
    Tls,
    /// The connected server is outside the qualified version range.
    ServerVersion,
    /// Required streams or consumers are absent or incompatible.
    Topology,
    /// Message metadata does not define a supported subject route.
    Subject,
    /// A publication acknowledgment named another stream.
    WrongStreamAck,
    /// A credential file was malformed or used the wrong runtime profile.
    InvalidCredential,
    /// Transport configuration was invalid.
    InvalidConfig,
    /// Required or allowed transport headers were invalid.
    HeaderMismatch,
    /// Delivery metadata supplied by the broker was malformed.
    MalformedDelivery,
    /// A durable pull fetch failed.
    ConsumerFetch,
}

impl TransportErrorKind {
    /// Returns the stable persistence/logging category without provider text.
    #[must_use]
    pub const fn safe_category(self) -> &'static str {
        match self {
            Self::Disconnected => "transport.disconnected",
            Self::Timeout => "transport.timeout",
            Self::NoQuorum => "transport.no-quorum",
            Self::Unavailable => "transport.unavailable",
            Self::Capacity => "transport.capacity",
            Self::AckTimeout => "transport.ack-timeout",
            Self::Authentication => "transport.authentication",
            Self::Authorization => "transport.authorization",
            Self::Tls => "transport.tls",
            Self::ServerVersion => "transport.server-version",
            Self::Topology => "transport.topology",
            Self::Subject => "transport.subject",
            Self::WrongStreamAck => "transport.wrong-stream-ack",
            Self::InvalidCredential => "transport.credential",
            Self::InvalidConfig => "transport.configuration",
            Self::HeaderMismatch => "delivery.header-mismatch",
            Self::MalformedDelivery => "delivery.transport-metadata",
            Self::ConsumerFetch => "consumer.fetch",
        }
    }

    /// Reports whether a caller may safely reschedule or retry later.
    #[must_use]
    pub const fn is_transient(self) -> bool {
        matches!(
            self,
            Self::Disconnected
                | Self::Timeout
                | Self::NoQuorum
                | Self::Unavailable
                | Self::Capacity
                | Self::AckTimeout
                | Self::ConsumerFetch
        )
    }
}

/// Content-free provider error.
pub struct TransportError {
    kind: TransportErrorKind,
}

impl TransportError {
    const fn new(kind: TransportErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable safe category.
    #[must_use]
    pub const fn kind(&self) -> TransportErrorKind {
        self.kind
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "NATS transport error: {}",
            self.kind.safe_category()
        )
    }
}

impl fmt::Debug for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl std::error::Error for TransportError {}

/// Validated TLS behavior for one NATS connection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NatsTlsMode {
    /// Plain transport, permitted only by dev composition.
    Disable,
    /// TLS with CA chain and full peer name verification.
    VerifyFull,
}

/// Strict username/password credential loaded from a secret reference.
pub struct NatsCredential {
    username: SecretString,
    password: SecretString,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawNatsCredential {
    username: String,
    password: String,
}

impl NatsCredential {
    /// Parses strict credential JSON while keeping the resolved value secret at the composition
    /// boundary.
    ///
    /// # Errors
    ///
    /// Rejects malformed JSON, unknown fields, empty fields, and oversized fields.
    pub fn parse_secret_json(value: &impl ExposeSecret<str>) -> Result<Self, TransportError> {
        Self::parse_json(value.expose_secret())
    }

    /// Parses strict credential JSON without echoing any input on failure.
    ///
    /// # Errors
    ///
    /// Rejects malformed JSON, unknown fields, empty fields, and oversized fields.
    pub fn parse_json(value: &str) -> Result<Self, TransportError> {
        let raw: RawNatsCredential = serde_json::from_str(value)
            .map_err(|_| TransportError::new(TransportErrorKind::InvalidCredential))?;
        if !(1..=MAX_CREDENTIAL_USERNAME_BYTES).contains(&raw.username.len())
            || !(1..=MAX_CREDENTIAL_PASSWORD_BYTES).contains(&raw.password.len())
            || raw.username.chars().any(char::is_control)
            || raw.password.chars().any(char::is_control)
        {
            return Err(TransportError::new(TransportErrorKind::InvalidCredential));
        }
        Ok(Self {
            username: SecretString::from(raw.username),
            password: SecretString::from(raw.password),
        })
    }

    /// Verifies this credential names the exact generated profile for a runtime role.
    ///
    /// # Errors
    ///
    /// Rejects a qualification, provisioner, other-Cell, or other-authority profile.
    pub fn validate_for_role(&self, role: &NatsRuntimeRole) -> Result<(), TransportError> {
        let expected = role.expected_username();
        if self.username.expose_secret() == expected.as_str() {
            Ok(())
        } else {
            Err(TransportError::new(TransportErrorKind::InvalidCredential))
        }
    }

    /// Consumes the credential into still-redacted secret values for an approved provider.
    #[must_use]
    pub fn into_secret_parts(self) -> (SecretString, SecretString) {
        (self.username, self.password)
    }
}

impl fmt::Debug for NatsCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NatsCredential([REDACTED])")
    }
}

impl fmt::Display for NatsCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED NATS CREDENTIAL]")
    }
}

/// One validated server version triplet.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NatsServerVersion {
    major: u16,
    minor: u16,
    patch: u16,
}

impl NatsServerVersion {
    /// Constructs a numeric server version.
    #[must_use]
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Parses a strict three-component server version, ignoring a semver prerelease suffix.
    ///
    /// # Errors
    ///
    /// Rejects non-numeric or incomplete versions.
    pub fn parse(value: &str) -> Result<Self, TransportError> {
        let core = value.split_once('-').map_or(value, |(core, _)| core);
        let mut pieces = core.split('.');
        let major = pieces
            .next()
            .and_then(|item| item.parse::<u16>().ok())
            .ok_or_else(|| TransportError::new(TransportErrorKind::ServerVersion))?;
        let minor = pieces
            .next()
            .and_then(|item| item.parse::<u16>().ok())
            .ok_or_else(|| TransportError::new(TransportErrorKind::ServerVersion))?;
        let patch = pieces
            .next()
            .and_then(|item| item.parse::<u16>().ok())
            .ok_or_else(|| TransportError::new(TransportErrorKind::ServerVersion))?;
        if pieces.next().is_some() {
            return Err(TransportError::new(TransportErrorKind::ServerVersion));
        }
        Ok(Self::new(major, minor, patch))
    }

    /// Verifies the version lies in the qualified `[2.14.3, 2.15.0)` interval.
    ///
    /// # Errors
    ///
    /// Rejects an unqualified server version.
    pub fn verify_qualified(self) -> Result<(), TransportError> {
        if (MINIMUM_SERVER_VERSION..MAXIMUM_SERVER_VERSION_EXCLUSIVE).contains(&self) {
            Ok(())
        } else {
            Err(TransportError::new(TransportErrorKind::ServerVersion))
        }
    }
}

impl fmt::Display for NatsServerVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Runtime identity and permission profile of one transport client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NatsRuntimeRole {
    /// Platform worker publisher and Platform durable consumers.
    PlatformWorker,
    /// One logical Cell worker publisher and Cell-specific durable consumers.
    CellWorker(CellId),
    /// Separately privileged topology provisioner.
    Provisioner,
    /// Non-deployable negative-test injector.
    QualificationInjector,
}

impl NatsRuntimeRole {
    fn expected_username(&self) -> String {
        match self {
            Self::PlatformWorker => String::from("edtech_platform_worker"),
            Self::CellWorker(cell_id) => format!("edtech_cell_{}_worker", cell_id.as_str()),
            Self::Provisioner => String::from("edtech_nats_provisioner"),
            Self::QualificationInjector => String::from("edtech_qualification_injector"),
        }
    }
}

/// Provider-owned validated connection configuration.
pub struct NatsConnectionConfig {
    service_name: String,
    environment: String,
    cell_id: Option<CellId>,
    servers: Vec<String>,
    tls_mode: NatsTlsMode,
    ca_certificate_file: Option<PathBuf>,
    connect_timeout: Duration,
    request_timeout: Duration,
    publish_ack_timeout: Duration,
    startup_timeout: Duration,
}

impl NatsConnectionConfig {
    /// Validates one bounded NATS connection configuration.
    ///
    /// # Errors
    ///
    /// Rejects unsafe names, endpoint userinfo/path/query/fragment, duplicates, invalid timeout
    /// bounds, and TLS verification without a CA certificate path.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        service_name: impl Into<String>,
        environment: impl Into<String>,
        cell_id: Option<CellId>,
        servers: Vec<String>,
        tls_mode: NatsTlsMode,
        ca_certificate_file: Option<PathBuf>,
        connect_timeout: Duration,
        request_timeout: Duration,
        publish_ack_timeout: Duration,
        startup_timeout: Duration,
    ) -> Result<Self, TransportError> {
        let service_name = service_name.into();
        let environment = environment.into();
        if !safe_connection_component(&service_name, 64)
            || !safe_connection_component(&environment, 16)
            || !(1..=MAX_SERVER_COUNT).contains(&servers.len())
            || !bounded_timeout(connect_timeout)
            || !bounded_timeout(request_timeout)
            || !bounded_timeout(publish_ack_timeout)
            || !bounded_timeout(startup_timeout)
            || startup_timeout < connect_timeout
            || (tls_mode == NatsTlsMode::VerifyFull && ca_certificate_file.is_none())
            || (tls_mode == NatsTlsMode::Disable && ca_certificate_file.is_some())
        {
            return Err(TransportError::new(TransportErrorKind::InvalidConfig));
        }
        let mut unique = BTreeSet::new();
        for server in &servers {
            validate_server_url(server, tls_mode)?;
            if !unique.insert(server.clone()) {
                return Err(TransportError::new(TransportErrorKind::InvalidConfig));
            }
        }
        Ok(Self {
            service_name,
            environment,
            cell_id,
            servers,
            tls_mode,
            ca_certificate_file,
            connect_timeout,
            request_timeout,
            publish_ack_timeout,
            startup_timeout,
        })
    }

    /// Returns server strings only to approved provider composition code.
    #[must_use]
    pub fn server_values(&self) -> &[String] {
        &self.servers
    }

    /// Returns the validated TLS mode.
    #[must_use]
    pub const fn tls_mode(&self) -> NatsTlsMode {
        self.tls_mode
    }

    /// Returns the configured public CA path, never certificate content.
    #[must_use]
    pub fn ca_certificate_file(&self) -> Option<&PathBuf> {
        self.ca_certificate_file.as_ref()
    }

    /// Returns the connection establishment timeout.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the `JetStream` request timeout.
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the initial readiness timeout.
    #[must_use]
    pub const fn startup_timeout(&self) -> Duration {
        self.startup_timeout
    }

    /// Returns the bounded non-sensitive client connection name.
    #[must_use]
    pub fn safe_connection_name(&self) -> String {
        self.connection_name()
    }

    fn connection_name(&self) -> String {
        self.cell_id.as_ref().map_or_else(
            || format!("{}-{}", self.service_name, self.environment),
            |cell_id| {
                format!(
                    "{}-{}-{}",
                    self.service_name,
                    self.environment,
                    cell_id.as_str()
                )
            },
        )
    }
}

impl fmt::Debug for NatsConnectionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsConnectionConfig")
            .field("service_name", &self.service_name)
            .field("environment", &self.environment)
            .field("cell_id", &self.cell_id)
            .field("server_count", &self.servers.len())
            .field("servers", &"[REDACTED ENDPOINTS]")
            .field("tls_mode", &self.tls_mode)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("publish_ack_timeout", &self.publish_ack_timeout)
            .field("startup_timeout", &self.startup_timeout)
            .finish_non_exhaustive()
    }
}

fn safe_connection_component(value: &str, maximum: usize) -> bool {
    (1..=maximum).contains(&value.len())
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

fn bounded_timeout(value: Duration) -> bool {
    value.as_millis() >= MIN_TIMEOUT.as_millis() && value.as_millis() <= MAX_TIMEOUT.as_millis()
}

fn validate_server_url(value: &str, tls_mode: NatsTlsMode) -> Result<(), TransportError> {
    if !(1..=MAX_SERVER_URL_BYTES).contains(&value.len())
        || !value.is_ascii()
        || value.contains(['@', '?', '#'])
    {
        return Err(TransportError::new(TransportErrorKind::InvalidConfig));
    }
    let (scheme, authority) = value
        .split_once("://")
        .ok_or_else(|| TransportError::new(TransportErrorKind::InvalidConfig))?;
    if authority.contains(['/', '?', '#', '@']) {
        return Err(TransportError::new(TransportErrorKind::InvalidConfig));
    }
    if !matches!(scheme, "nats" | "tls")
        || (tls_mode == NatsTlsMode::VerifyFull && scheme != "tls")
        || (tls_mode == NatsTlsMode::Disable && scheme != "nats")
    {
        return Err(TransportError::new(TransportErrorKind::InvalidConfig));
    }
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| TransportError::new(TransportErrorKind::InvalidConfig))?;
    if !(1..=MAX_HOST_BYTES).contains(&host.len())
        || host.chars().any(char::is_whitespace)
        || port
            .parse::<u16>()
            .ok()
            .as_ref()
            .is_none_or(|number| *number == 0)
    {
        return Err(TransportError::new(TransportErrorKind::InvalidConfig));
    }
    Ok(())
}

/// One of the two fixed production streams.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TransportStream {
    /// Work-queue command stream.
    Commands,
    /// Limits-retained event stream.
    Events,
}

impl TransportStream {
    /// Returns the administratively fixed stream name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Commands => COMMAND_STREAM_NAME,
            Self::Events => EVENT_STREAM_NAME,
        }
    }

    /// Selects the stream from the immutable message kind.
    #[must_use]
    pub const fn for_kind(kind: MessageKind) -> Self {
        match kind {
            MessageKind::Command => Self::Commands,
            MessageKind::Event => Self::Events,
        }
    }
}

/// Validated concrete transport subject derived exclusively from envelope metadata.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransportSubject(String);

impl TransportSubject {
    /// Derives one of the four supported cross-authority routes.
    ///
    /// # Errors
    ///
    /// Rejects same-authority, Platform-scoped event, mismatched Cell, or otherwise unsupported
    /// routes.
    pub fn derive(metadata: &MessageMetadata) -> Result<Self, TransportError> {
        let suffix = metadata
            .descriptor()
            .name()
            .as_str()
            .strip_prefix("edtech.")
            .ok_or_else(|| TransportError::new(TransportErrorKind::Subject))?;
        let (direction, cell_id) = match (
            metadata.descriptor().kind(),
            metadata.source(),
            metadata.target(),
            metadata.scope(),
        ) {
            (
                MessageKind::Command,
                MessageAuthority::Platform,
                Some(MessageTarget::Cell(target)),
                MessageScope::Cell(scope) | MessageScope::Tenant { cell_id: scope, .. },
            ) if target == scope => ("command.platform-to-cell", target),
            (
                MessageKind::Command,
                MessageAuthority::Cell(source),
                Some(MessageTarget::Platform),
                MessageScope::Cell(scope) | MessageScope::Tenant { cell_id: scope, .. },
            ) if source == scope => ("command.cell-to-platform", source),
            (
                MessageKind::Event,
                MessageAuthority::Platform,
                None,
                MessageScope::Cell(scope) | MessageScope::Tenant { cell_id: scope, .. },
            ) => ("event.platform-to-cell", scope),
            (
                MessageKind::Event,
                MessageAuthority::Cell(source),
                None,
                MessageScope::Cell(scope) | MessageScope::Tenant { cell_id: scope, .. },
            ) if source == scope => ("event.cell-to-platform", source),
            _ => return Err(TransportError::new(TransportErrorKind::Subject)),
        };
        let value = format!(
            "{TRANSPORT_SUBJECT_PREFIX}.{direction}.{}.{}",
            cell_id.as_str(),
            suffix
        );
        validate_concrete_subject(&value)?;
        Ok(Self(value))
    }

    /// Borrows the validated concrete subject.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TransportSubject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for TransportSubject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("TransportSubject")
            .field(&self.0)
            .finish()
    }
}

fn validate_concrete_subject(value: &str) -> Result<(), TransportError> {
    if value.is_empty()
        || value.len() > MAX_SUBJECT_BYTES
        || !value.is_ascii()
        || value.starts_with('.')
        || value.ends_with('.')
        || value.contains("..")
        || value.contains(['*', '>', ' ', '\t', '\r', '\n'])
        || value.split('.').any(str::is_empty)
    {
        Err(TransportError::new(TransportErrorKind::Subject))
    } else {
        Ok(())
    }
}

/// Exact headers accepted from one application delivery.
#[derive(Clone, Eq, PartialEq)]
pub struct InboundHeaderSet {
    values: BTreeMap<String, Vec<String>>,
}

impl InboundHeaderSet {
    /// Constructs a bounded provider-neutral header set for validation and tests.
    ///
    /// # Errors
    ///
    /// Rejects empty, control-containing, duplicate-unbounded, or excessively large header data.
    pub fn from_pairs(
        pairs: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, TransportError> {
        let mut values = BTreeMap::<String, Vec<String>>::new();
        let mut count = 0_usize;
        for (name, value) in pairs {
            count = count.saturating_add(1);
            if count > 32
                || !(1..=128).contains(&name.len())
                || value.len() > 512
                || !name.is_ascii()
                || !value.is_ascii()
                || name.chars().any(char::is_control)
                || value.chars().any(char::is_control)
            {
                return Err(TransportError::new(TransportErrorKind::HeaderMismatch));
            }
            values.entry(name).or_default().push(value);
        }
        Ok(Self { values })
    }

    /// Validates required identity/content headers and the optional expected-stream precondition.
    ///
    /// # Errors
    ///
    /// Rejects missing, duplicated, unknown, or mismatched headers.
    pub fn validate(
        &self,
        message: &EncodedMessage,
        stream: TransportStream,
    ) -> Result<(), TransportError> {
        for name in self.values.keys() {
            if !is_known_delivery_header(name) {
                return Err(TransportError::new(TransportErrorKind::HeaderMismatch));
            }
        }
        let message_id = self.exactly_one("Nats-Msg-Id")?;
        let parsed = MessageId::from_str(message_id)
            .map_err(|_| TransportError::new(TransportErrorKind::HeaderMismatch))?;
        if parsed != message.metadata().message_id() {
            return Err(TransportError::new(TransportErrorKind::HeaderMismatch));
        }
        if self.exactly_one("Content-Type")? != MESSAGE_CONTENT_TYPE {
            return Err(TransportError::new(TransportErrorKind::HeaderMismatch));
        }
        if let Some(expected) = self.optional_one("Nats-Expected-Stream")?
            && expected != stream.name()
        {
            return Err(TransportError::new(TransportErrorKind::HeaderMismatch));
        }
        Ok(())
    }

    fn exactly_one(&self, name: &str) -> Result<&str, TransportError> {
        self.optional_one(name)?
            .ok_or_else(|| TransportError::new(TransportErrorKind::HeaderMismatch))
    }

    fn optional_one(&self, name: &str) -> Result<Option<&str>, TransportError> {
        match self.values.get(name).map(Vec::as_slice) {
            None => Ok(None),
            Some([value]) => Ok(Some(value.as_str())),
            Some(_) => Err(TransportError::new(TransportErrorKind::HeaderMismatch)),
        }
    }
}

impl fmt::Debug for InboundHeaderSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InboundHeaderSet")
            .field("header_count", &self.values.len())
            .field("values", &"[REDACTED]")
            .finish()
    }
}

fn is_known_delivery_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "nats-msg-id"
            | "nats-expected-stream"
            | "content-type"
            | "nats-stream"
            | "nats-sequence"
            | "nats-time-stamp"
            | "nats-subject"
            | "nats-last-sequence"
            | "nats-last-subject-sequence"
            | "nats-pending-messages"
            | "nats-pending-bytes"
    )
}

/// One accepted `JetStream` publication acknowledgment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishAcceptance {
    stream: TransportStream,
    stream_sequence: u64,
    broker_duplicate: bool,
}

impl PublishAcceptance {
    /// Validates an acknowledgment without provider types, useful for qualification and tests.
    ///
    /// # Errors
    ///
    /// Rejects another stream or sequence zero.
    pub fn validate(
        expected: TransportStream,
        acknowledged_stream: &str,
        stream_sequence: u64,
        broker_duplicate: bool,
    ) -> Result<Self, TransportError> {
        if acknowledged_stream != expected.name() {
            return Err(TransportError::new(TransportErrorKind::WrongStreamAck));
        }
        if stream_sequence == 0 {
            return Err(TransportError::new(TransportErrorKind::Topology));
        }
        Ok(Self {
            stream: expected,
            stream_sequence,
            broker_duplicate,
        })
    }

    /// Returns the accepted production stream.
    #[must_use]
    pub const fn stream(&self) -> TransportStream {
        self.stream
    }

    /// Returns the non-zero broker stream sequence as transport evidence only.
    #[must_use]
    pub const fn stream_sequence(&self) -> u64 {
        self.stream_sequence
    }

    /// Reports bounded broker duplicate-window suppression.
    #[must_use]
    pub const fn broker_duplicate(&self) -> bool {
        self.broker_duplicate
    }
}

/// Exact pre-provisioned durable pull-consumer binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerBinding {
    stream: TransportStream,
    durable_name: String,
    filter_subject: String,
}

impl ConsumerBinding {
    /// Returns the bound stream.
    #[must_use]
    pub const fn stream(&self) -> TransportStream {
        self.stream
    }

    /// Returns the fixed durable name.
    #[must_use]
    pub fn durable_name(&self) -> &str {
        &self.durable_name
    }

    /// Returns the administratively fixed subject filter.
    #[must_use]
    pub fn filter_subject(&self) -> &str {
        &self.filter_subject
    }
}

/// Returns the Platform command durable binding.
#[must_use]
pub fn platform_command_binding() -> ConsumerBinding {
    ConsumerBinding {
        stream: TransportStream::Commands,
        durable_name: String::from(PLATFORM_COMMAND_DURABLE),
        filter_subject: String::from("edtech.v1.command.cell-to-platform.>"),
    }
}

/// Returns the Platform event durable binding.
#[must_use]
pub fn platform_event_binding() -> ConsumerBinding {
    ConsumerBinding {
        stream: TransportStream::Events,
        durable_name: String::from(PLATFORM_EVENT_DURABLE),
        filter_subject: String::from("edtech.v1.event.cell-to-platform.>"),
    }
}

/// Derives the collision-free Cell durable token.
#[must_use]
pub fn cell_durable_token(cell_id: &CellId) -> String {
    cell_id.as_str().to_ascii_uppercase().replace('-', "_")
}

/// Returns one Cell command durable binding.
#[must_use]
pub fn cell_command_binding(cell_id: &CellId) -> ConsumerBinding {
    ConsumerBinding {
        stream: TransportStream::Commands,
        durable_name: format!("EDTECH_CELL_{}_COMMANDS_V1", cell_durable_token(cell_id)),
        filter_subject: format!("edtech.v1.command.platform-to-cell.{}.>", cell_id.as_str()),
    }
}

/// Returns one Cell event durable binding.
#[must_use]
pub fn cell_event_binding(cell_id: &CellId) -> ConsumerBinding {
    ConsumerBinding {
        stream: TransportStream::Events,
        durable_name: format!("EDTECH_CELL_{}_EVENTS_V1", cell_durable_token(cell_id)),
        filter_subject: format!("edtech.v1.event.platform-to-cell.{}.>", cell_id.as_str()),
    }
}

/// Redacted bounded delivery payload wrapper.
#[derive(Eq, PartialEq)]
pub struct InboundPayload(Vec<u8>);

impl InboundPayload {
    /// Borrows the exact broker payload bytes for canonical decoding.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for InboundPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InboundPayload")
            .field("byte_length", &self.0.len())
            .field("payload", &"[REDACTED]")
            .finish()
    }
}

/// Opaque post-commit acknowledgment capability.
pub struct DeliveryAcknowledgment {
    message: jetstream::Message,
}

impl DeliveryAcknowledgment {
    /// Sends an explicit acknowledgment and awaits the server confirmation.
    ///
    /// # Errors
    ///
    /// Returns a safe transient category without raw provider text.
    pub async fn double_ack(self) -> Result<(), TransportError> {
        self.message
            .double_ack()
            .await
            .map_err(|_| TransportError::new(TransportErrorKind::Unavailable))
    }

    /// Sends a bounded delayed negative acknowledgment.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive delays and maps provider failure safely.
    pub async fn nak_with_delay(self, delay: Duration) -> Result<(), TransportError> {
        if delay.is_zero() || delay > Duration::from_mins(5) {
            return Err(TransportError::new(TransportErrorKind::InvalidConfig));
        }
        self.message
            .ack_with(AckKind::Nak(Some(delay)))
            .await
            .map_err(|_| TransportError::new(TransportErrorKind::Unavailable))
    }
}

impl fmt::Debug for DeliveryAcknowledgment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryAcknowledgment(opaque)")
    }
}

/// Opaque delivery returned from one fixed durable pull consumer.
pub struct InboundDelivery {
    actual_subject: String,
    stream: TransportStream,
    durable_name: String,
    redelivery_count: u64,
    headers: InboundHeaderSet,
    payload: InboundPayload,
    acknowledgment: DeliveryAcknowledgment,
}

impl InboundDelivery {
    /// Returns the actual broker subject for envelope-derived comparison.
    #[must_use]
    pub fn actual_subject(&self) -> &str {
        &self.actual_subject
    }

    /// Returns the fixed stream.
    #[must_use]
    pub const fn stream(&self) -> TransportStream {
        self.stream
    }

    /// Returns the fixed durable name.
    #[must_use]
    pub fn durable_name(&self) -> &str {
        &self.durable_name
    }

    /// Returns the number of redeliveries after the first attempt.
    #[must_use]
    pub const fn redelivery_count(&self) -> u64 {
        self.redelivery_count
    }

    /// Borrows exact payload bytes through the redacted wrapper.
    #[must_use]
    pub const fn payload(&self) -> &InboundPayload {
        &self.payload
    }

    /// Validates required headers against the canonical envelope metadata.
    ///
    /// # Errors
    ///
    /// Returns a safe header mismatch category.
    pub fn validate_headers(&self, message: &EncodedMessage) -> Result<(), TransportError> {
        self.headers.validate(message, self.stream)
    }

    /// Consumes delivery data and returns the only available acknowledgment capability.
    #[must_use]
    pub fn into_acknowledgment(self) -> DeliveryAcknowledgment {
        self.acknowledgment
    }
}

impl fmt::Debug for InboundDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InboundDelivery")
            .field("actual_subject", &self.actual_subject)
            .field("stream", &self.stream)
            .field("durable_name", &self.durable_name)
            .field("redelivery_count", &self.redelivery_count)
            .field("payload", &self.payload)
            .finish_non_exhaustive()
    }
}

/// Opaque runtime `JetStream` client with no provider types in its public API.
#[derive(Clone)]
pub struct JetStreamRuntime {
    inner: Arc<JetStreamRuntimeInner>,
}

struct JetStreamRuntimeInner {
    client: async_nats::Client,
    context: jetstream::Context,
    role: NatsRuntimeRole,
    server_version: NatsServerVersion,
}

impl fmt::Debug for JetStreamRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JetStreamRuntime")
            .field("role", &self.inner.role)
            .field("server_version", &self.inner.server_version)
            .finish_non_exhaustive()
    }
}

impl JetStreamRuntime {
    /// Connects, authenticates, verifies TLS/server version, and performs read-only role topology
    /// checks before returning readiness.
    ///
    /// # Errors
    ///
    /// Returns safe configuration, credential, connection, server-version, or topology categories.
    pub async fn connect(
        credential: NatsCredential,
        config: &NatsConnectionConfig,
        role: NatsRuntimeRole,
    ) -> Result<Self, TransportError> {
        credential.validate_for_role(&role)?;
        let (username, password) = credential.into_secret_parts();
        let server_addrs = config
            .servers
            .iter()
            .map(|server| {
                server
                    .parse::<ServerAddr>()
                    .map_err(|_| TransportError::new(TransportErrorKind::InvalidConfig))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let options = connect_options(config, &username, &password);
        let client = tokio::time::timeout(config.startup_timeout, options.connect(server_addrs))
            .await
            .map_err(|_| TransportError::new(TransportErrorKind::Timeout))?
            .map_err(|error| map_connect_error(&error))?;
        let version = NatsServerVersion::parse(&client.server_info().version)?;
        version.verify_qualified()?;
        let context = ContextBuilder::new()
            .timeout(config.request_timeout)
            .ack_timeout(config.publish_ack_timeout)
            .max_ack_inflight(4_096)
            .backpressure_on_inflight(true)
            .build(client.clone());
        let runtime = Self {
            inner: Arc::new(JetStreamRuntimeInner {
                client,
                context,
                role,
                server_version: version,
            }),
        };
        runtime.verify_role_topology().await?;
        Ok(runtime)
    }

    /// Returns the qualified connected server version.
    #[must_use]
    pub fn server_version(&self) -> NatsServerVersion {
        self.inner.server_version
    }

    /// Publishes the exact stored envelope bytes and validates `JetStream` acceptance.
    ///
    /// # Errors
    ///
    /// Returns safe subject, transient provider, capacity, topology, or wrong-stream categories.
    pub async fn publish_exact(
        &self,
        message: &EncodedMessage,
    ) -> Result<PublishAcceptance, TransportError> {
        authorize_publish(&self.inner.role, message.metadata())?;
        let subject = TransportSubject::derive(message.metadata())?;
        let stream = TransportStream::for_kind(message.metadata().descriptor().kind());
        let mut headers = HeaderMap::new();
        headers.insert(
            async_nats::header::NATS_MESSAGE_ID,
            message.metadata().message_id().to_string(),
        );
        headers.insert(async_nats::header::NATS_EXPECTED_STREAM, stream.name());
        headers.insert("Content-Type", message.content_type());
        let pending = self
            .inner
            .context
            .publish_with_headers(
                subject.to_string(),
                headers,
                message.as_bytes().to_vec().into(),
            )
            .await
            .map_err(|error| map_publish_error(&error))?;
        let acknowledgment = pending.await.map_err(|error| map_publish_error(&error))?;
        PublishAcceptance::validate(
            stream,
            &acknowledgment.stream,
            acknowledgment.sequence,
            acknowledgment.duplicate,
        )
    }

    /// Binds to a pre-existing exact durable pull consumer and verifies its immutable safety
    /// settings without creating or changing topology.
    ///
    /// # Errors
    ///
    /// Returns a safe role or topology mismatch category.
    pub async fn bind_consumer(
        &self,
        binding: &ConsumerBinding,
    ) -> Result<BoundConsumer, TransportError> {
        if !role_allows_binding(&self.inner.role, binding) {
            return Err(TransportError::new(TransportErrorKind::Authorization));
        }
        let stream = self
            .inner
            .context
            .get_stream(binding.stream.name())
            .await
            .map_err(|_| TransportError::new(TransportErrorKind::Topology))?;
        let consumer: PullConsumer = stream
            .get_consumer(binding.durable_name())
            .await
            .map_err(|_| TransportError::new(TransportErrorKind::Topology))?;
        verify_consumer(binding, consumer.cached_info())?;
        Ok(BoundConsumer {
            consumer,
            binding: binding.clone(),
        })
    }

    /// Drains the underlying client after application tasks stop fetching and claiming work.
    ///
    /// # Errors
    ///
    /// Returns a safe temporary unavailable category.
    pub async fn drain(&self) -> Result<(), TransportError> {
        self.inner
            .client
            .drain()
            .await
            .map_err(|_| TransportError::new(TransportErrorKind::Unavailable))
    }

    async fn verify_role_topology(&self) -> Result<(), TransportError> {
        match &self.inner.role {
            NatsRuntimeRole::PlatformWorker => {
                self.verify_streams().await?;
                self.verify_binding(&platform_command_binding()).await?;
                self.verify_binding(&platform_event_binding()).await
            }
            NatsRuntimeRole::CellWorker(cell_id) => {
                self.verify_streams().await?;
                self.verify_binding(&cell_command_binding(cell_id)).await?;
                self.verify_binding(&cell_event_binding(cell_id)).await
            }
            NatsRuntimeRole::Provisioner | NatsRuntimeRole::QualificationInjector => Ok(()),
        }
    }

    async fn verify_streams(&self) -> Result<(), TransportError> {
        for expected in [TransportStream::Commands, TransportStream::Events] {
            let stream = self
                .inner
                .context
                .get_stream(expected.name())
                .await
                .map_err(|_| TransportError::new(TransportErrorKind::Topology))?;
            let config = &stream.cached_info().config;
            let retention_matches = match expected {
                TransportStream::Commands => config.retention == RetentionPolicy::WorkQueue,
                TransportStream::Events => config.retention == RetentionPolicy::Limits,
            };
            if config.storage != StorageType::File
                || config.num_replicas != 3
                || config.discard != DiscardPolicy::New
                || config.allow_direct
                || !retention_matches
            {
                return Err(TransportError::new(TransportErrorKind::Topology));
            }
        }
        Ok(())
    }

    async fn verify_binding(&self, binding: &ConsumerBinding) -> Result<(), TransportError> {
        self.bind_consumer(binding).await.map(|_| ())
    }
}

fn connect_options(
    config: &NatsConnectionConfig,
    username: &SecretString,
    password: &SecretString,
) -> ConnectOptions {
    let mut options = ConnectOptions::new()
        .name(config.connection_name())
        .user_and_password(
            username.expose_secret().to_owned(),
            password.expose_secret().to_owned(),
        )
        .connection_timeout(config.connect_timeout)
        .request_timeout(Some(config.request_timeout))
        .max_reconnects(None)
        .event_callback(|event| async move {
            tracing::info!(
                safe_transport_category = safe_connection_event(&event),
                "NATS connection event"
            );
        });
    if config.tls_mode == NatsTlsMode::VerifyFull {
        options = options.require_tls(true);
        if let Some(path) = config.ca_certificate_file.clone() {
            options = options.add_root_certificates(path);
        }
    }
    options
}

fn safe_connection_event(event: &Event) -> &'static str {
    match event {
        Event::Connected => "transport.connected",
        Event::Disconnected => "transport.disconnected",
        Event::LameDuckMode => "transport.lame-duck",
        Event::Draining => "transport.draining",
        Event::Closed => "transport.closed",
        Event::SlowConsumer(_) => "transport.slow-consumer",
        Event::ServerError(_) => "transport.server-error",
        Event::ClientError(_) => "transport.client-error",
    }
}

fn map_connect_error(error: &async_nats::ConnectError) -> TransportError {
    let kind = match error.kind() {
        ConnectErrorKind::Authentication => TransportErrorKind::Authentication,
        ConnectErrorKind::AuthorizationViolation => TransportErrorKind::Authorization,
        ConnectErrorKind::TimedOut => TransportErrorKind::Timeout,
        ConnectErrorKind::Tls => TransportErrorKind::Tls,
        ConnectErrorKind::Dns | ConnectErrorKind::Io | ConnectErrorKind::MaxReconnects => {
            TransportErrorKind::Unavailable
        }
        ConnectErrorKind::ServerParse => TransportErrorKind::InvalidConfig,
    };
    TransportError::new(kind)
}

fn map_publish_error(error: &PublishError) -> TransportError {
    let kind = match error.kind() {
        PublishErrorKind::TimedOut => TransportErrorKind::AckTimeout,
        PublishErrorKind::BrokenPipe => TransportErrorKind::Disconnected,
        PublishErrorKind::MaxAckPending | PublishErrorKind::MaxPayloadExceeded => {
            TransportErrorKind::Capacity
        }
        PublishErrorKind::StreamNotFound
        | PublishErrorKind::WrongLastMessageId
        | PublishErrorKind::WrongLastSequence => TransportErrorKind::Topology,
        PublishErrorKind::Other => TransportErrorKind::Unavailable,
    };
    TransportError::new(kind)
}

fn authorize_publish(
    role: &NatsRuntimeRole,
    metadata: &MessageMetadata,
) -> Result<(), TransportError> {
    let allowed = match (role, metadata.source()) {
        (NatsRuntimeRole::PlatformWorker, MessageAuthority::Platform)
        | (NatsRuntimeRole::QualificationInjector, _) => true,
        (NatsRuntimeRole::CellWorker(expected), MessageAuthority::Cell(actual)) => {
            expected == actual
        }
        (NatsRuntimeRole::Provisioner, _)
        | (NatsRuntimeRole::PlatformWorker, MessageAuthority::Cell(_))
        | (NatsRuntimeRole::CellWorker(_), MessageAuthority::Platform) => false,
    };
    if allowed {
        Ok(())
    } else {
        Err(TransportError::new(TransportErrorKind::Authorization))
    }
}

fn role_allows_binding(role: &NatsRuntimeRole, binding: &ConsumerBinding) -> bool {
    match role {
        NatsRuntimeRole::PlatformWorker => {
            binding == &platform_command_binding() || binding == &platform_event_binding()
        }
        NatsRuntimeRole::CellWorker(cell_id) => {
            binding == &cell_command_binding(cell_id) || binding == &cell_event_binding(cell_id)
        }
        NatsRuntimeRole::Provisioner | NatsRuntimeRole::QualificationInjector => false,
    }
}

fn verify_consumer(
    binding: &ConsumerBinding,
    info: &jetstream::consumer::Info,
) -> Result<(), TransportError> {
    let config = &info.config;
    if info.stream_name != binding.stream.name()
        || info.name != binding.durable_name
        || config.durable_name.as_deref() != Some(binding.durable_name())
        || config.deliver_subject.is_some()
        || config.filter_subject != binding.filter_subject
        || config.ack_policy != AckPolicy::Explicit
        || config.deliver_policy != DeliverPolicy::All
        || config.replay_policy != ReplayPolicy::Instant
        || config.ack_wait != ACK_WAIT
        || config.max_deliver != -1
        || config.max_ack_pending != 1_024
        || config.max_waiting != 64
        || config.max_batch != 200
        || config.max_expires != Duration::from_secs(5)
        || config.num_replicas != 3
        || config.memory_storage
        || !config.inactive_threshold.is_zero()
    {
        return Err(TransportError::new(TransportErrorKind::Topology));
    }
    Ok(())
}

/// Opaque handle bound to one verified pre-existing durable pull consumer.
pub struct BoundConsumer {
    consumer: PullConsumer,
    binding: ConsumerBinding,
}

impl fmt::Debug for BoundConsumer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundConsumer")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

impl BoundConsumer {
    /// Fetches at most one bounded batch from the fixed durable.
    ///
    /// # Errors
    ///
    /// Rejects invalid bounds, provider fetch failures, or malformed broker delivery metadata.
    pub async fn fetch(
        &self,
        batch_size: u16,
        expires: Duration,
    ) -> Result<Vec<InboundDelivery>, TransportError> {
        if !(1..=500).contains(&batch_size) || expires.is_zero() || expires > Duration::from_secs(5)
        {
            return Err(TransportError::new(TransportErrorKind::InvalidConfig));
        }
        let mut batch = self
            .consumer
            .batch()
            .max_messages(usize::from(batch_size))
            .expires(expires)
            .messages()
            .await
            .map_err(|_| TransportError::new(TransportErrorKind::ConsumerFetch))?;
        let mut deliveries = Vec::with_capacity(usize::from(batch_size));
        while let Some(result) = batch.next().await {
            let message =
                result.map_err(|_| TransportError::new(TransportErrorKind::ConsumerFetch))?;
            deliveries.push(delivery_from_message(message, &self.binding)?);
        }
        Ok(deliveries)
    }
}

fn delivery_from_message(
    message: jetstream::Message,
    binding: &ConsumerBinding,
) -> Result<InboundDelivery, TransportError> {
    let info = message
        .info()
        .map_err(|_| TransportError::new(TransportErrorKind::MalformedDelivery))?;
    if info.stream != binding.stream.name() || info.consumer != binding.durable_name() {
        return Err(TransportError::new(TransportErrorKind::MalformedDelivery));
    }
    let redelivery_count = u64::try_from(info.delivered.saturating_sub(1)).unwrap_or(u64::MAX);
    let actual_subject = message.subject.to_string();
    let payload = InboundPayload(message.payload.to_vec());
    let headers = message.headers.as_ref().map_or_else(
        || InboundHeaderSet::from_pairs(std::iter::empty()),
        |headers| {
            InboundHeaderSet::from_pairs(headers.iter().flat_map(|(name, values)| {
                values.iter().map(move |value| {
                    (
                        <async_nats::HeaderName as AsRef<str>>::as_ref(name).to_owned(),
                        value.as_str().to_owned(),
                    )
                })
            }))
        },
    )?;
    Ok(InboundDelivery {
        actual_subject,
        stream: binding.stream,
        durable_name: binding.durable_name.clone(),
        redelivery_count,
        headers,
        payload,
        acknowledgment: DeliveryAcknowledgment { message },
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use message_domain::{
        ContractDescriptor, CorrelationId, EmittedAt, MessageName, MessageSchemaVersion,
    };
    use tenancy_domain::{AssignmentEpoch, TenantId};
    use time::OffsetDateTime;
    use uuid::Uuid;

    use super::*;

    fn uuid(value: &str) -> Uuid {
        Uuid::from_str(value).unwrap_or_else(|error| panic!("static UUID fixture: {error}"))
    }

    fn metadata(
        kind: MessageKind,
        source: MessageAuthority,
        scope: MessageScope,
        target: Option<MessageTarget>,
    ) -> MessageMetadata {
        MessageMetadata::new(
            MessageId::new(uuid("01890f47-7cc2-7a1b-8d5d-7f6ebc9c2001"))
                .unwrap_or_else(|error| panic!("fixture id: {error}")),
            ContractDescriptor::new(
                kind,
                MessageName::from_str("edtech.transport.cell-probe.requested")
                    .unwrap_or_else(|error| panic!("fixture name: {error}")),
                MessageSchemaVersion::new(1)
                    .unwrap_or_else(|error| panic!("fixture version: {error}")),
            ),
            EmittedAt::new(OffsetDateTime::UNIX_EPOCH)
                .unwrap_or_else(|error| panic!("fixture time: {error}")),
            source,
            scope,
            target,
            CorrelationId::new(uuid("01890f47-7cc2-7a1b-8d5d-7f6ebc9c2002"))
                .unwrap_or_else(|error| panic!("fixture id: {error}")),
            None,
        )
        .unwrap_or_else(|error| panic!("fixture metadata: {error}"))
    }

    fn cell() -> CellId {
        CellId::from_str("cell-001").unwrap_or_else(|error| panic!("fixture cell: {error}"))
    }

    fn tenant_scope(cell_id: CellId) -> MessageScope {
        MessageScope::Tenant {
            tenant_id: TenantId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c2003")
                .unwrap_or_else(|error| panic!("fixture tenant: {error:?}")),
            cell_id,
            assignment_epoch: AssignmentEpoch::new(u64::MAX)
                .unwrap_or_else(|error| panic!("fixture epoch: {error}")),
        }
    }

    #[test]
    fn all_four_routes_have_exact_subjects_and_streams() {
        let cell = cell();
        let cases = [
            (
                metadata(
                    MessageKind::Command,
                    MessageAuthority::Platform,
                    tenant_scope(cell.clone()),
                    Some(MessageTarget::Cell(cell.clone())),
                ),
                "edtech.v1.command.platform-to-cell.cell-001.transport.cell-probe.requested",
            ),
            (
                metadata(
                    MessageKind::Command,
                    MessageAuthority::Cell(cell.clone()),
                    tenant_scope(cell.clone()),
                    Some(MessageTarget::Platform),
                ),
                "edtech.v1.command.cell-to-platform.cell-001.transport.cell-probe.requested",
            ),
            (
                metadata(
                    MessageKind::Event,
                    MessageAuthority::Platform,
                    tenant_scope(cell.clone()),
                    None,
                ),
                "edtech.v1.event.platform-to-cell.cell-001.transport.cell-probe.requested",
            ),
            (
                metadata(
                    MessageKind::Event,
                    MessageAuthority::Cell(cell.clone()),
                    tenant_scope(cell.clone()),
                    None,
                ),
                "edtech.v1.event.cell-to-platform.cell-001.transport.cell-probe.requested",
            ),
        ];
        for (metadata, expected) in cases {
            let subject = TransportSubject::derive(&metadata);
            assert_eq!(
                subject.ok().map(|value| value.to_string()),
                Some(expected.to_owned())
            );
            assert!(!expected.contains("01890f47"));
            assert!(!expected.contains(&u64::MAX.to_string()));
        }
        assert_eq!(
            TransportStream::for_kind(MessageKind::Command),
            TransportStream::Commands
        );
        assert_eq!(
            TransportStream::for_kind(MessageKind::Event),
            TransportStream::Events
        );
    }

    #[test]
    fn unsupported_same_authority_routes_are_rejected() {
        let platform_command = metadata(
            MessageKind::Command,
            MessageAuthority::Platform,
            MessageScope::Platform,
            Some(MessageTarget::Platform),
        );
        let platform_event = metadata(
            MessageKind::Event,
            MessageAuthority::Platform,
            MessageScope::Platform,
            None,
        );
        assert_eq!(
            TransportSubject::derive(&platform_command)
                .err()
                .map(|error| error.kind()),
            Some(TransportErrorKind::Subject)
        );
        assert_eq!(
            TransportSubject::derive(&platform_event)
                .err()
                .map(|error| error.kind()),
            Some(TransportErrorKind::Subject)
        );
    }

    #[test]
    fn server_urls_tls_and_credentials_are_strict_and_redacted() {
        let ca = PathBuf::from("/tmp/local-nats-ca.pem");
        let valid = NatsConnectionConfig::new(
            "platform-worker",
            "dev",
            None,
            vec![String::from("tls://localhost:4222")],
            NatsTlsMode::VerifyFull,
            Some(ca),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(30),
        );
        assert!(valid.is_ok());
        for server in [
            "tls://user:pass@localhost:4222",
            "http://localhost:4222",
            "tls://localhost:4222/path",
            "tls://localhost:0",
        ] {
            assert!(
                NatsConnectionConfig::new(
                    "platform-worker",
                    "dev",
                    None,
                    vec![server.to_owned()],
                    NatsTlsMode::VerifyFull,
                    Some(PathBuf::from("/tmp/ca.pem")),
                    Duration::from_secs(5),
                    Duration::from_secs(5),
                    Duration::from_secs(5),
                    Duration::from_secs(30),
                )
                .is_err()
            );
        }
        let secret = "unique-password-sentinel";
        let credential = NatsCredential::parse_json(&format!(
            r#"{{"username":"edtech_platform_worker","password":"{secret}"}}"#
        ));
        let rendered = format!("{credential:?}");
        assert!(!rendered.contains(secret));
        assert!(credential.as_ref().is_ok_and(|value| {
            value
                .validate_for_role(&NatsRuntimeRole::PlatformWorker)
                .is_ok()
        }));
        assert!(
            NatsCredential::parse_json(r#"{"username":"x","password":"y","extra":1}"#).is_err()
        );
    }

    #[test]
    fn headers_require_exact_identity_content_type_and_known_names() {
        let cell = cell();
        let metadata = metadata(
            MessageKind::Command,
            MessageAuthority::Platform,
            tenant_scope(cell.clone()),
            Some(MessageTarget::Cell(cell)),
        );
        let encoded = EncodedMessage::new(metadata.clone(), b"{}".to_vec())
            .unwrap_or_else(|error| panic!("fixture bytes: {error}"));
        let valid = InboundHeaderSet::from_pairs([
            (
                String::from("Nats-Msg-Id"),
                metadata.message_id().to_string(),
            ),
            (
                String::from("Content-Type"),
                String::from(MESSAGE_CONTENT_TYPE),
            ),
            (
                String::from("Nats-Expected-Stream"),
                String::from(COMMAND_STREAM_NAME),
            ),
        ]);
        assert!(valid.is_ok_and(|headers| {
            headers
                .validate(&encoded, TransportStream::Commands)
                .is_ok()
        }));

        let missing = InboundHeaderSet::from_pairs([(
            String::from("Nats-Msg-Id"),
            metadata.message_id().to_string(),
        )]);
        assert!(missing.is_ok_and(|headers| {
            headers
                .validate(&encoded, TransportStream::Commands)
                .is_err()
        }));
        let unknown = InboundHeaderSet::from_pairs([
            (
                String::from("Nats-Msg-Id"),
                metadata.message_id().to_string(),
            ),
            (
                String::from("Content-Type"),
                String::from(MESSAGE_CONTENT_TYPE),
            ),
            (String::from("TenantId"), String::from("forbidden")),
        ]);
        assert!(unknown.is_ok_and(|headers| {
            headers
                .validate(&encoded, TransportStream::Commands)
                .is_err()
        }));
    }

    #[test]
    fn acknowledgments_validate_stream_sequence_and_duplicate_state() {
        assert!(
            PublishAcceptance::validate(TransportStream::Commands, COMMAND_STREAM_NAME, 1, false)
                .is_ok()
        );
        let duplicate =
            PublishAcceptance::validate(TransportStream::Events, EVENT_STREAM_NAME, 2, true);
        assert!(duplicate.is_ok_and(|value| value.broker_duplicate()));
        assert_eq!(
            PublishAcceptance::validate(TransportStream::Commands, EVENT_STREAM_NAME, 1, false,)
                .err()
                .map(|error| error.kind()),
            Some(TransportErrorKind::WrongStreamAck)
        );
        assert!(
            PublishAcceptance::validate(TransportStream::Commands, COMMAND_STREAM_NAME, 0, false,)
                .is_err()
        );
    }

    #[test]
    fn consumer_derivation_is_collision_free_for_cell_grammar() {
        let first =
            CellId::from_str("cell-001").unwrap_or_else(|error| panic!("fixture cell: {error}"));
        let second =
            CellId::from_str("cell-002").unwrap_or_else(|error| panic!("fixture cell: {error}"));
        assert_eq!(cell_durable_token(&first), "CELL_001");
        assert_ne!(cell_command_binding(&first), cell_command_binding(&second));
        assert_eq!(
            cell_event_binding(&first).durable_name(),
            "EDTECH_CELL_CELL_001_EVENTS_V1"
        );
    }

    #[test]
    fn server_version_bounds_are_exact() {
        for accepted in ["2.14.3", "2.14.99"] {
            assert!(
                NatsServerVersion::parse(accepted)
                    .and_then(NatsServerVersion::verify_qualified)
                    .is_ok()
            );
        }
        for rejected in ["2.14.2", "2.15.0", "3.0.0", "2.14"] {
            assert!(
                NatsServerVersion::parse(rejected)
                    .and_then(NatsServerVersion::verify_qualified)
                    .is_err()
            );
        }
    }

    #[test]
    fn safe_errors_never_include_provider_or_endpoint_text() {
        let error = TransportError::new(TransportErrorKind::Authentication);
        let rendered = format!("{error:?} {error}");
        assert!(rendered.contains("Authentication"));
        assert!(!rendered.contains("localhost"));
        assert!(!rendered.contains("password"));
    }
}
