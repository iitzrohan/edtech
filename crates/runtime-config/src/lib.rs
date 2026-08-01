//! Typed startup configuration at the process-composition boundary.
//!
//! The same schema supports `dev`, `npr`, and `prd`; the selected environment is runtime data.
//! Raw deserialization stays private, database credentials remain external secret references, and
//! domain and application code must never depend on this crate.

use std::{collections::BTreeMap, fmt, fs, path::PathBuf, str::FromStr, time::Duration};

use config::{Config, File, FileFormat};
use serde::Deserialize;
use tenancy_domain::CellId;
use thiserror::Error;

const ENVIRONMENT_PREFIX: &str = "EDTECH__";
const CONFIG_FILE_VARIABLE: &str = "EDTECH_CONFIG_FILE";
const DEFAULT_LOG_FILTER: &str = "info";
const DEFAULT_SHUTDOWN_GRACE_MS: u64 = 30_000;
const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_MIN_CONNECTIONS: u32 = 0;
const DEFAULT_ACQUIRE_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_LOCK_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_MAX_LIFETIME_MS: u64 = 1_800_000;
const DEFAULT_MIGRATION_TIMEOUT_MS: u64 = 600_000;
const DEFAULT_TRANSPORT_CONNECT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_TRANSPORT_REQUEST_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_PUBLISH_ACK_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_TRANSPORT_STARTUP_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_OUTBOX_POLL_INTERVAL_MS: u64 = 100;
const DEFAULT_OUTBOX_CLAIM_BATCH_SIZE: u16 = 100;
const DEFAULT_OUTBOX_LEASE_MS: u64 = 30_000;
const DEFAULT_PUBLISH_CONCURRENCY: u16 = 16;
const DEFAULT_RETRY_BASE_MS: u64 = 250;
const DEFAULT_RETRY_MAX_MS: u64 = 30_000;
const DEFAULT_CONSUMER_FETCH_BATCH_SIZE: u16 = 100;
const DEFAULT_CONSUMER_FETCH_EXPIRES_MS: u64 = 1_000;
const DEFAULT_CONSUMER_HANDLER_TIMEOUT_MS: u64 = 20_000;
const DEFAULT_CONSUMER_NAK_DELAY_MS: u64 = 5_000;
const DEFAULT_CONSUMER_MAX_IN_FLIGHT: u16 = 64;
const DEFAULT_TOPOLOGY_APPLY_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_EXPECTED_SERVER_MINIMUM: &str = "2.14.3";
const DEFAULT_EXPECTED_SERVER_MAXIMUM_EXCLUSIVE: &str = "2.15.0";
const MIN_SHUTDOWN_GRACE_MS: u64 = 100;
const MAX_SHUTDOWN_GRACE_MS: u64 = 300_000;
const MIN_DATABASE_TIMEOUT_MS: u64 = 100;
const MAX_DATABASE_TIMEOUT_MS: u64 = 3_600_000;
const MIN_MAX_LIFETIME_MS: u64 = 1_000;
const MAX_MAX_LIFETIME_MS: u64 = 86_400_000;
const MIN_MIGRATION_TIMEOUT_MS: u64 = 1_000;
const MAX_MIGRATION_TIMEOUT_MS: u64 = 3_600_000;
const MAX_DATABASE_CONNECTIONS: u32 = 100;
const MAX_LOG_FILTER_LENGTH: usize = 256;
const MAX_SECRET_REFERENCE_LENGTH: usize = 512;
const MAX_TRANSPORT_SERVERS: usize = 8;
const MAX_TRANSPORT_SERVER_URL_LENGTH: usize = 320;
const MAX_TRANSPORT_HOST_LENGTH: usize = 253;
const MAX_TRANSPORT_PATH_LENGTH: usize = 1_024;
const MAX_TRANSPORT_TIMEOUT_MS: u64 = 300_000;
const MIN_TRANSPORT_TIMEOUT_MS: u64 = 100;
const MAX_CONSUMER_ACK_WAIT_MS: u64 = 30_000;

/// An isolated deployment environment selected at runtime.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeploymentEnvironment {
    /// Developer environment.
    Dev,
    /// Non-production environment.
    Npr,
    /// Production environment.
    Prd,
}

impl fmt::Display for DeploymentEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Dev => "dev",
            Self::Npr => "npr",
            Self::Prd => "prd",
        })
    }
}

/// One of the seven fixed process composition roots.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ServiceKind {
    /// Platform API process.
    PlatformApi,
    /// Platform worker process.
    PlatformWorker,
    /// Database-free tenant router process.
    TenantRouter,
    /// Cell API process.
    CellApi,
    /// Cell worker process.
    CellWorker,
    /// Separately privileged database migrator process.
    DbMigrator,
    /// Separately privileged one-shot NATS topology provisioner.
    NatsProvisioner,
}

impl ServiceKind {
    /// Returns the fixed service name used in safe startup records.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PlatformApi => "platform-api",
            Self::PlatformWorker => "platform-worker",
            Self::TenantRouter => "tenant-router",
            Self::CellApi => "cell-api",
            Self::CellWorker => "cell-worker",
            Self::DbMigrator => "db-migrator",
            Self::NatsProvisioner => "nats-provisioner",
        }
    }

    const fn accepts_platform_config(self) -> bool {
        matches!(self, Self::PlatformApi | Self::PlatformWorker)
    }

    const fn accepts_cell_config(self) -> bool {
        matches!(self, Self::CellApi | Self::CellWorker)
    }
}

impl fmt::Display for ServiceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A validated, bounded tracing filter expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogFilter(String);

impl LogFilter {
    fn new(value: String) -> Result<Self, RuntimeConfigError> {
        if value.is_empty() || value.len() > MAX_LOG_FILTER_LENGTH {
            return Err(RuntimeConfigError::InvalidField {
                field: "log_filter",
                reason: "must contain between 1 and 256 bytes",
            });
        }
        if value.chars().any(char::is_control) {
            return Err(RuntimeConfigError::InvalidField {
                field: "log_filter",
                reason: "must not contain control characters",
            });
        }
        Ok(Self(value))
    }

    /// Returns the validated filter text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Runtime settings shared by every process type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseRuntimeConfig {
    environment: DeploymentEnvironment,
    log_filter: LogFilter,
    shutdown_grace: Duration,
}

impl BaseRuntimeConfig {
    /// Returns the isolated deployment environment.
    #[must_use]
    pub const fn environment(&self) -> DeploymentEnvironment {
        self.environment
    }

    /// Returns the validated structured-logging filter.
    #[must_use]
    pub const fn log_filter(&self) -> &LogFilter {
        &self.log_filter
    }

    /// Returns the bounded graceful-shutdown duration.
    #[must_use]
    pub const fn shutdown_grace(&self) -> Duration {
        self.shutdown_grace
    }
}

/// `PostgreSQL` transport verification mode.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DatabaseTlsMode {
    /// Disable TLS. Valid only in the developer environment.
    Disable,
    /// Require TLS with certificate and hostname verification.
    VerifyFull,
}

impl fmt::Display for DatabaseTlsMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Disable => "disable",
            Self::VerifyFull => "verify_full",
        })
    }
}

/// An opaque, bounded reference to a secret held by an external authority.
#[derive(Clone, Eq, PartialEq)]
pub struct SecretReference(String);

/// A safe validation failure for [`SecretReference`] that never includes the supplied value.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SecretReferenceError {
    /// The reference is empty.
    #[error("secret reference must not be empty")]
    Empty,
    /// The reference exceeds its 512-byte bound.
    #[error("secret reference exceeds the maximum length")]
    TooLong,
    /// The reference includes control characters.
    #[error("secret reference must not contain control characters")]
    ControlCharacter,
}

impl SecretReference {
    /// Validates and constructs an opaque secret reference.
    ///
    /// # Errors
    ///
    /// Returns a [`SecretReferenceError`] without echoing `value`.
    pub fn new(value: impl Into<String>) -> Result<Self, SecretReferenceError> {
        let value = value.into();
        if value.is_empty() {
            return Err(SecretReferenceError::Empty);
        }
        if value.len() > MAX_SECRET_REFERENCE_LENGTH {
            return Err(SecretReferenceError::TooLong);
        }
        if value.chars().any(char::is_control) {
            return Err(SecretReferenceError::ControlCharacter);
        }
        Ok(Self(value))
    }

    /// Exposes the reference only to a secret resolver.
    ///
    /// This is a locator, not resolved secret material, but callers must still avoid logging it.
    #[must_use]
    pub fn as_str_for_resolution(&self) -> &str {
        &self.0
    }
}

impl FromStr for SecretReference {
    type Err = SecretReferenceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl fmt::Debug for SecretReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretReference([REDACTED])")
    }
}

/// Bounded database connection and session configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseConfig {
    credential_ref: SecretReference,
    tls_mode: DatabaseTlsMode,
    max_connections: u32,
    min_connections: u32,
    acquire_timeout: Duration,
    connect_timeout: Duration,
    statement_timeout: Duration,
    lock_timeout: Duration,
    idle_in_transaction_timeout: Duration,
    max_lifetime: Duration,
}

impl DatabaseConfig {
    /// Returns the opaque credential reference without resolving it.
    #[must_use]
    pub const fn credential_ref(&self) -> &SecretReference {
        &self.credential_ref
    }

    /// Returns the validated TLS verification mode.
    #[must_use]
    pub const fn tls_mode(&self) -> DatabaseTlsMode {
        self.tls_mode
    }

    /// Returns the maximum pool size.
    #[must_use]
    pub const fn max_connections(&self) -> u32 {
        self.max_connections
    }

    /// Returns the minimum pool size.
    #[must_use]
    pub const fn min_connections(&self) -> u32 {
        self.min_connections
    }

    /// Returns the bounded pool-acquisition timeout.
    #[must_use]
    pub const fn acquire_timeout(&self) -> Duration {
        self.acquire_timeout
    }

    /// Returns the bounded connection-establishment timeout.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the per-session statement timeout.
    #[must_use]
    pub const fn statement_timeout(&self) -> Duration {
        self.statement_timeout
    }

    /// Returns the per-session lock timeout.
    #[must_use]
    pub const fn lock_timeout(&self) -> Duration {
        self.lock_timeout
    }

    /// Returns the idle-in-transaction session timeout.
    #[must_use]
    pub const fn idle_in_transaction_timeout(&self) -> Duration {
        self.idle_in_transaction_timeout
    }

    /// Returns the maximum lifetime of a pooled connection.
    #[must_use]
    pub const fn max_lifetime(&self) -> Duration {
        self.max_lifetime
    }
}

/// NATS client TLS verification mode.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TransportTlsMode {
    /// Plain NATS transport, accepted only for dev.
    Disable,
    /// TLS with a configured CA and full hostname verification.
    VerifyFull,
}

/// Bounded worker/provisioner NATS settings with no arbitrary subject input.
#[derive(Clone, Eq, PartialEq)]
pub struct TransportConfig {
    servers: Vec<String>,
    credential_ref: SecretReference,
    tls_mode: TransportTlsMode,
    ca_certificate_file: Option<PathBuf>,
    connect_timeout: Duration,
    request_timeout: Duration,
    publish_ack_timeout: Duration,
    startup_timeout: Duration,
    outbox_poll_interval: Duration,
    outbox_claim_batch_size: u16,
    outbox_lease: Duration,
    publish_concurrency: u16,
    retry_base: Duration,
    retry_max: Duration,
    consumer_fetch_batch_size: u16,
    consumer_fetch_expires: Duration,
    consumer_handler_timeout: Duration,
    consumer_nak_delay: Duration,
    consumer_max_in_flight: u16,
    expected_server_minimum: String,
    expected_server_maximum_exclusive: String,
}

impl TransportConfig {
    /// Returns validated NATS server URLs. Values must never be logged.
    #[must_use]
    pub fn servers(&self) -> &[String] {
        &self.servers
    }

    /// Returns the opaque NATS credential reference without resolving it.
    #[must_use]
    pub const fn credential_ref(&self) -> &SecretReference {
        &self.credential_ref
    }

    /// Returns the validated TLS mode.
    #[must_use]
    pub const fn tls_mode(&self) -> TransportTlsMode {
        self.tls_mode
    }

    /// Returns the configured CA certificate file path.
    #[must_use]
    pub fn ca_certificate_file(&self) -> Option<&PathBuf> {
        self.ca_certificate_file.as_ref()
    }

    /// Returns the connection timeout.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the `JetStream` request timeout.
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the bounded publication acknowledgment timeout.
    #[must_use]
    pub const fn publish_ack_timeout(&self) -> Duration {
        self.publish_ack_timeout
    }

    /// Returns the overall initial transport readiness timeout.
    #[must_use]
    pub const fn startup_timeout(&self) -> Duration {
        self.startup_timeout
    }

    /// Returns the empty-outbox polling interval.
    #[must_use]
    pub const fn outbox_poll_interval(&self) -> Duration {
        self.outbox_poll_interval
    }

    /// Returns the bounded outbox claim batch size.
    #[must_use]
    pub const fn outbox_claim_batch_size(&self) -> u16 {
        self.outbox_claim_batch_size
    }

    /// Returns the whole-second outbox lease duration.
    #[must_use]
    pub const fn outbox_lease(&self) -> Duration {
        self.outbox_lease
    }

    /// Returns the per-authority publication concurrency.
    #[must_use]
    pub const fn publish_concurrency(&self) -> u16 {
        self.publish_concurrency
    }

    /// Returns the minimum retry delay.
    #[must_use]
    pub const fn retry_base(&self) -> Duration {
        self.retry_base
    }

    /// Returns the maximum retry delay.
    #[must_use]
    pub const fn retry_max(&self) -> Duration {
        self.retry_max
    }

    /// Returns the durable-consumer fetch batch size.
    #[must_use]
    pub const fn consumer_fetch_batch_size(&self) -> u16 {
        self.consumer_fetch_batch_size
    }

    /// Returns the bounded durable pull request expiry.
    #[must_use]
    pub const fn consumer_fetch_expires(&self) -> Duration {
        self.consumer_fetch_expires
    }

    /// Returns the local database handler timeout.
    #[must_use]
    pub const fn consumer_handler_timeout(&self) -> Duration {
        self.consumer_handler_timeout
    }

    /// Returns the bounded poison/stale delivery NAK delay.
    #[must_use]
    pub const fn consumer_nak_delay(&self) -> Duration {
        self.consumer_nak_delay
    }

    /// Returns the in-task consumer concurrency bound.
    #[must_use]
    pub const fn consumer_max_in_flight(&self) -> u16 {
        self.consumer_max_in_flight
    }

    /// Returns the inclusive qualified minimum server version.
    #[must_use]
    pub fn expected_server_minimum(&self) -> &str {
        &self.expected_server_minimum
    }

    /// Returns the exclusive qualified maximum server version.
    #[must_use]
    pub fn expected_server_maximum_exclusive(&self) -> &str {
        &self.expected_server_maximum_exclusive
    }
}

impl fmt::Debug for TransportConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportConfig")
            .field("server_count", &self.servers.len())
            .field("servers", &"[REDACTED ENDPOINTS]")
            .field("credential_ref", &self.credential_ref)
            .field("tls_mode", &self.tls_mode)
            .field("ca_certificate_file", &"[REDACTED PATH]")
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("publish_ack_timeout", &self.publish_ack_timeout)
            .field("startup_timeout", &self.startup_timeout)
            .field("outbox_poll_interval", &self.outbox_poll_interval)
            .field("outbox_claim_batch_size", &self.outbox_claim_batch_size)
            .field("outbox_lease", &self.outbox_lease)
            .field("publish_concurrency", &self.publish_concurrency)
            .field("retry_base", &self.retry_base)
            .field("retry_max", &self.retry_max)
            .field("consumer_fetch_batch_size", &self.consumer_fetch_batch_size)
            .field("consumer_fetch_expires", &self.consumer_fetch_expires)
            .field("consumer_handler_timeout", &self.consumer_handler_timeout)
            .field("consumer_nak_delay", &self.consumer_nak_delay)
            .field("consumer_max_in_flight", &self.consumer_max_in_flight)
            .field("expected_server_minimum", &self.expected_server_minimum)
            .field(
                "expected_server_maximum_exclusive",
                &self.expected_server_maximum_exclusive,
            )
            .finish()
    }
}

/// Validated configuration for a Platform-authority runtime process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlatformRuntimeConfig {
    base: BaseRuntimeConfig,
    database: DatabaseConfig,
    transport: Option<TransportConfig>,
}

impl PlatformRuntimeConfig {
    /// Returns settings common to all process types.
    #[must_use]
    pub const fn base(&self) -> &BaseRuntimeConfig {
        &self.base
    }

    /// Returns the single Platform database configuration.
    #[must_use]
    pub const fn database(&self) -> &DatabaseConfig {
        &self.database
    }

    /// Returns transport settings only for platform-worker.
    #[must_use]
    pub const fn transport(&self) -> Option<&TransportConfig> {
        self.transport.as_ref()
    }
}

/// Validated configuration for the database-free tenant router.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouterRuntimeConfig {
    base: BaseRuntimeConfig,
}

impl RouterRuntimeConfig {
    /// Returns settings common to all process types.
    #[must_use]
    pub const fn base(&self) -> &BaseRuntimeConfig {
        &self.base
    }
}

/// Validated configuration for a Cell-authority runtime process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CellRuntimeConfig {
    base: BaseRuntimeConfig,
    cell_id: CellId,
    database: DatabaseConfig,
    transport: Option<TransportConfig>,
}

impl CellRuntimeConfig {
    /// Returns settings common to all process types.
    #[must_use]
    pub const fn base(&self) -> &BaseRuntimeConfig {
        &self.base
    }

    /// Returns the topology-neutral logical Cell identity.
    #[must_use]
    pub const fn cell_id(&self) -> &CellId {
        &self.cell_id
    }

    /// Returns the single Cell database configuration.
    #[must_use]
    pub const fn database(&self) -> &DatabaseConfig {
        &self.database
    }

    /// Returns transport settings only for cell-worker.
    #[must_use]
    pub const fn transport(&self) -> Option<&TransportConfig> {
        self.transport.as_ref()
    }
}

/// Validated one-shot NATS topology provisioner configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NatsProvisionerRuntimeConfig {
    base: BaseRuntimeConfig,
    transport: TransportConfig,
    topology_file: PathBuf,
    topology_apply_timeout: Duration,
}

impl NatsProvisionerRuntimeConfig {
    /// Returns common process settings.
    #[must_use]
    pub const fn base(&self) -> &BaseRuntimeConfig {
        &self.base
    }

    /// Returns the required NATS connection settings.
    #[must_use]
    pub const fn transport(&self) -> &TransportConfig {
        &self.transport
    }

    /// Returns the topology manifest path without reading it.
    #[must_use]
    pub const fn topology_file(&self) -> &PathBuf {
        &self.topology_file
    }

    /// Returns the bounded topology apply/readiness timeout.
    #[must_use]
    pub const fn topology_apply_timeout(&self) -> Duration {
        self.topology_apply_timeout
    }
}

/// Database authority selected for a migration process.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MigrationScope {
    /// Platform database authority.
    Platform,
    /// One logical Cell database authority.
    Cell,
}

impl fmt::Display for MigrationScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Platform => "platform",
            Self::Cell => "cell",
        })
    }
}

/// Validated configuration for the separately privileged migration process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigratorRuntimeConfig {
    base: BaseRuntimeConfig,
    scope: MigrationScope,
    cell_id: Option<CellId>,
    database: DatabaseConfig,
    migration_timeout: Duration,
}

impl MigratorRuntimeConfig {
    /// Returns settings common to all process types.
    #[must_use]
    pub const fn base(&self) -> &BaseRuntimeConfig {
        &self.base
    }

    /// Returns the selected database authority scope.
    #[must_use]
    pub const fn scope(&self) -> MigrationScope {
        self.scope
    }

    /// Returns the logical Cell only for Cell-scoped migration authority.
    #[must_use]
    pub const fn cell_id(&self) -> Option<&CellId> {
        self.cell_id.as_ref()
    }

    /// Returns the single migration database configuration.
    #[must_use]
    pub const fn database(&self) -> &DatabaseConfig {
        &self.database
    }

    /// Returns the bounded overall migration timeout.
    #[must_use]
    pub const fn migration_timeout(&self) -> Duration {
        self.migration_timeout
    }
}

/// Explicit, testable configuration sources ordered as defaults, optional TOML, then environment.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct ConfigSources {
    toml: Option<String>,
    environment: BTreeMap<String, String>,
}

impl ConfigSources {
    /// Constructs sources from explicit key-value input without reading global process state.
    #[must_use]
    pub fn new(environment: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            toml: None,
            environment: environment.into_iter().collect(),
        }
    }

    /// Adds lower-precedence TOML file contents to these sources.
    #[must_use]
    pub fn with_toml(mut self, contents: impl Into<String>) -> Self {
        self.toml = Some(contents.into());
        self
    }
}

/// A startup-configuration failure that never includes supplied field values.
#[derive(Debug, Error)]
pub enum RuntimeConfigError {
    /// A required configuration field is absent.
    #[error("required configuration field `{0}` is missing")]
    MissingField(&'static str),
    /// A source contains a field outside the single supported schema.
    #[error("unknown configuration field `{0}`")]
    UnknownField(String),
    /// A known field fails bounded or semantic validation.
    #[error("invalid configuration field `{field}`: {reason}")]
    InvalidField {
        /// Safe schema field name.
        field: &'static str,
        /// Safe validation description that never includes the value.
        reason: &'static str,
    },
    /// A field is valid in another process schema but forbidden for this service.
    #[error("configuration field `{0}` is not valid for this service")]
    UnexpectedField(&'static str),
    /// The typed loader does not match the fixed service kind.
    #[error("service `{service}` cannot use {configuration_kind} configuration")]
    ServiceMismatch {
        /// Fixed, trusted service kind.
        service: ServiceKind,
        /// Static configuration category.
        configuration_kind: &'static str,
    },
    /// TOML or merged configuration could not be decoded safely.
    #[error("configuration source is malformed or contains an unknown field")]
    InvalidSource,
    /// The optional configuration file could not be read.
    #[error("configuration file could not be read: {path}")]
    ConfigFileRead {
        /// User-selected path, never file contents.
        path: PathBuf,
    },
    /// An applicable environment key or value is not Unicode.
    #[error("an EDTECH configuration environment entry is not valid Unicode")]
    NonUnicodeEnvironment,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDatabaseConfig {
    credential_ref: Option<String>,
    tls_mode: Option<String>,
    max_connections: Option<u32>,
    min_connections: Option<u32>,
    acquire_timeout_ms: Option<u64>,
    connect_timeout_ms: Option<u64>,
    statement_timeout_ms: Option<u64>,
    lock_timeout_ms: Option<u64>,
    idle_in_transaction_timeout_ms: Option<u64>,
    max_lifetime_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTransportConfig {
    servers: Option<Vec<String>>,
    credential_ref: Option<String>,
    tls_mode: Option<String>,
    ca_certificate_file: Option<String>,
    connect_timeout_ms: Option<u64>,
    request_timeout_ms: Option<u64>,
    publish_ack_timeout_ms: Option<u64>,
    startup_timeout_ms: Option<u64>,
    outbox_poll_interval_ms: Option<u64>,
    outbox_claim_batch_size: Option<u16>,
    outbox_lease_ms: Option<u64>,
    publish_concurrency: Option<u16>,
    retry_base_ms: Option<u64>,
    retry_max_ms: Option<u64>,
    consumer_fetch_batch_size: Option<u16>,
    consumer_fetch_expires_ms: Option<u64>,
    consumer_handler_timeout_ms: Option<u64>,
    consumer_nak_delay_ms: Option<u64>,
    consumer_max_in_flight: Option<u16>,
    expected_server_minimum: Option<String>,
    expected_server_maximum_exclusive: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    environment: Option<String>,
    log_filter: Option<String>,
    shutdown_grace_ms: Option<u64>,
    cell_id: Option<String>,
    migration_scope: Option<String>,
    migration_timeout_ms: Option<u64>,
    database: Option<RawDatabaseConfig>,
    transport: Option<RawTransportConfig>,
    topology_file: Option<String>,
    topology_apply_timeout_ms: Option<u64>,
}

/// Loads Platform configuration from the real process environment and optional file.
///
/// # Errors
///
/// Returns [`RuntimeConfigError`] for missing, unknown, unsafe, or incompatible input.
pub fn load_platform(service: ServiceKind) -> Result<PlatformRuntimeConfig, RuntimeConfigError> {
    let sources = process_sources()?;
    load_platform_from_sources(service, &sources)
}

/// Loads tenant-router configuration from the real process environment and optional file.
///
/// # Errors
///
/// Returns [`RuntimeConfigError`] for missing, unknown, unsafe, or incompatible input.
pub fn load_router() -> Result<RouterRuntimeConfig, RuntimeConfigError> {
    let sources = process_sources()?;
    load_router_from_sources(&sources)
}

/// Loads Cell configuration from the real process environment and optional file.
///
/// # Errors
///
/// Returns [`RuntimeConfigError`] for missing, unknown, unsafe, or incompatible input.
pub fn load_cell(service: ServiceKind) -> Result<CellRuntimeConfig, RuntimeConfigError> {
    let sources = process_sources()?;
    load_cell_from_sources(service, &sources)
}

/// Loads migrator configuration from the real process environment and optional file.
///
/// # Errors
///
/// Returns [`RuntimeConfigError`] for missing, unknown, unsafe, or incompatible input.
pub fn load_migrator() -> Result<MigratorRuntimeConfig, RuntimeConfigError> {
    let sources = process_sources()?;
    load_migrator_from_sources(&sources)
}

/// Loads NATS provisioner configuration from the real process environment and optional file.
///
/// # Errors
///
/// Returns [`RuntimeConfigError`] for missing, unknown, unsafe, or incompatible input.
pub fn load_nats_provisioner() -> Result<NatsProvisionerRuntimeConfig, RuntimeConfigError> {
    let sources = process_sources()?;
    load_nats_provisioner_from_sources(&sources)
}

/// Loads Platform configuration from explicit, deterministic sources.
///
/// # Errors
///
/// Returns [`RuntimeConfigError`] for missing, unknown, unsafe, or incompatible input.
pub fn load_platform_from_sources(
    service: ServiceKind,
    sources: &ConfigSources,
) -> Result<PlatformRuntimeConfig, RuntimeConfigError> {
    if !service.accepts_platform_config() {
        return Err(RuntimeConfigError::ServiceMismatch {
            service,
            configuration_kind: "Platform",
        });
    }
    let raw = deserialize_sources(sources)?;
    reject_migration_fields(&raw)?;
    reject_provisioner_fields(&raw)?;
    if raw.cell_id.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("cell_id"));
    }
    let base = validate_base(&raw)?;
    let database = validate_database(raw.database.as_ref(), base.environment())?;
    let transport = match service {
        ServiceKind::PlatformWorker => Some(validate_transport(
            raw.transport.as_ref(),
            base.environment(),
        )?),
        ServiceKind::PlatformApi => {
            if raw.transport.is_some() {
                return Err(RuntimeConfigError::UnexpectedField("transport"));
            }
            None
        }
        _ => {
            return Err(RuntimeConfigError::ServiceMismatch {
                service,
                configuration_kind: "Platform",
            });
        }
    };
    Ok(PlatformRuntimeConfig {
        base,
        database,
        transport,
    })
}

/// Loads database-free tenant-router configuration from explicit, deterministic sources.
///
/// # Errors
///
/// Returns [`RuntimeConfigError`] for missing, unknown, unsafe, or incompatible input.
pub fn load_router_from_sources(
    sources: &ConfigSources,
) -> Result<RouterRuntimeConfig, RuntimeConfigError> {
    let raw = deserialize_sources(sources)?;
    reject_migration_fields(&raw)?;
    reject_provisioner_fields(&raw)?;
    if raw.cell_id.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("cell_id"));
    }
    if raw.database.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("database"));
    }
    if raw.transport.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("transport"));
    }
    Ok(RouterRuntimeConfig {
        base: validate_base(&raw)?,
    })
}

/// Loads Cell configuration from explicit, deterministic sources.
///
/// # Errors
///
/// Returns [`RuntimeConfigError`] for missing, unknown, unsafe, or incompatible input.
pub fn load_cell_from_sources(
    service: ServiceKind,
    sources: &ConfigSources,
) -> Result<CellRuntimeConfig, RuntimeConfigError> {
    if !service.accepts_cell_config() {
        return Err(RuntimeConfigError::ServiceMismatch {
            service,
            configuration_kind: "Cell",
        });
    }
    let raw = deserialize_sources(sources)?;
    reject_migration_fields(&raw)?;
    reject_provisioner_fields(&raw)?;
    let cell_id = validate_cell_id(raw.cell_id.as_deref())?;
    let base = validate_base(&raw)?;
    let database = validate_database(raw.database.as_ref(), base.environment())?;
    let transport = match service {
        ServiceKind::CellWorker => Some(validate_transport(
            raw.transport.as_ref(),
            base.environment(),
        )?),
        ServiceKind::CellApi => {
            if raw.transport.is_some() {
                return Err(RuntimeConfigError::UnexpectedField("transport"));
            }
            None
        }
        _ => {
            return Err(RuntimeConfigError::ServiceMismatch {
                service,
                configuration_kind: "Cell",
            });
        }
    };
    Ok(CellRuntimeConfig {
        base,
        cell_id,
        database,
        transport,
    })
}

/// Loads migrator configuration from explicit, deterministic sources.
///
/// # Errors
///
/// Returns [`RuntimeConfigError`] for missing, unknown, unsafe, or incompatible input.
pub fn load_migrator_from_sources(
    sources: &ConfigSources,
) -> Result<MigratorRuntimeConfig, RuntimeConfigError> {
    let raw = deserialize_sources(sources)?;
    if raw.transport.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("transport"));
    }
    reject_provisioner_fields(&raw)?;
    let scope = match raw.migration_scope.as_deref() {
        Some("platform") => MigrationScope::Platform,
        Some("cell") => MigrationScope::Cell,
        Some(_) => {
            return Err(RuntimeConfigError::InvalidField {
                field: "migration_scope",
                reason: "must be `platform` or `cell`",
            });
        }
        None => return Err(RuntimeConfigError::MissingField("migration_scope")),
    };

    let cell_id = match (scope, raw.cell_id.as_deref()) {
        (MigrationScope::Platform, Some(_)) => {
            return Err(RuntimeConfigError::UnexpectedField("cell_id"));
        }
        (MigrationScope::Platform, None) => None,
        (MigrationScope::Cell, None) => {
            return Err(RuntimeConfigError::MissingField("cell_id"));
        }
        (MigrationScope::Cell, Some(value)) => Some(validate_cell_id(Some(value))?),
    };

    let base = validate_base(&raw)?;
    let database = validate_database(raw.database.as_ref(), base.environment())?;
    let migration_timeout_ms = raw
        .migration_timeout_ms
        .unwrap_or(DEFAULT_MIGRATION_TIMEOUT_MS);
    if !(MIN_MIGRATION_TIMEOUT_MS..=MAX_MIGRATION_TIMEOUT_MS).contains(&migration_timeout_ms) {
        return Err(RuntimeConfigError::InvalidField {
            field: "migration_timeout_ms",
            reason: "must be between 1000 and 3600000 milliseconds",
        });
    }

    Ok(MigratorRuntimeConfig {
        base,
        scope,
        cell_id,
        database,
        migration_timeout: Duration::from_millis(migration_timeout_ms),
    })
}

/// Loads NATS provisioner configuration from explicit deterministic sources.
///
/// # Errors
///
/// Rejects database/Cell/migration fields and requires strict transport and topology settings.
pub fn load_nats_provisioner_from_sources(
    sources: &ConfigSources,
) -> Result<NatsProvisionerRuntimeConfig, RuntimeConfigError> {
    let raw = deserialize_sources(sources)?;
    reject_migration_fields(&raw)?;
    if raw.database.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("database"));
    }
    if raw.cell_id.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("cell_id"));
    }
    let base = validate_base(&raw)?;
    let transport = validate_transport(raw.transport.as_ref(), base.environment())?;
    let topology_file = raw
        .topology_file
        .as_deref()
        .ok_or(RuntimeConfigError::MissingField("topology_file"))
        .and_then(|value| validate_configuration_path(value, "topology_file"))?;
    let timeout_ms = raw
        .topology_apply_timeout_ms
        .unwrap_or(DEFAULT_TOPOLOGY_APPLY_TIMEOUT_MS);
    let topology_apply_timeout = transport_timeout(timeout_ms, "topology_apply_timeout_ms")?;
    Ok(NatsProvisionerRuntimeConfig {
        base,
        transport,
        topology_file,
        topology_apply_timeout,
    })
}

fn reject_migration_fields(raw: &RawConfig) -> Result<(), RuntimeConfigError> {
    if raw.migration_scope.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("migration_scope"));
    }
    if raw.migration_timeout_ms.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("migration_timeout_ms"));
    }
    Ok(())
}

fn reject_provisioner_fields(raw: &RawConfig) -> Result<(), RuntimeConfigError> {
    if raw.topology_file.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("topology_file"));
    }
    if raw.topology_apply_timeout_ms.is_some() {
        return Err(RuntimeConfigError::UnexpectedField(
            "topology_apply_timeout_ms",
        ));
    }
    Ok(())
}

fn validate_cell_id(raw: Option<&str>) -> Result<CellId, RuntimeConfigError> {
    raw.ok_or(RuntimeConfigError::MissingField("cell_id"))?
        .parse::<CellId>()
        .map_err(|_| RuntimeConfigError::InvalidField {
            field: "cell_id",
            reason: "must be a valid topology-neutral logical Cell identifier",
        })
}

fn process_sources() -> Result<ConfigSources, RuntimeConfigError> {
    let mut environment = BTreeMap::new();
    for (key, value) in std::env::vars_os() {
        let Some(key) = key.to_str() else {
            continue;
        };
        if key.starts_with(ENVIRONMENT_PREFIX) {
            let value = value
                .into_string()
                .map_err(|_| RuntimeConfigError::NonUnicodeEnvironment)?;
            environment.insert(key.to_owned(), value);
        }
    }

    let mut sources = ConfigSources::new(environment);
    if let Some(path) = std::env::var_os(CONFIG_FILE_VARIABLE) {
        let path = PathBuf::from(path);
        let contents = fs::read_to_string(&path)
            .map_err(|_| RuntimeConfigError::ConfigFileRead { path: path.clone() })?;
        sources = sources.with_toml(contents);
    }
    Ok(sources)
}

#[allow(clippy::too_many_lines)]
fn deserialize_sources(sources: &ConfigSources) -> Result<RawConfig, RuntimeConfigError> {
    if !sources.environment.contains_key("EDTECH__ENVIRONMENT") {
        return Err(RuntimeConfigError::MissingField("environment"));
    }

    let mut builder = Config::builder()
        .set_default("log_filter", DEFAULT_LOG_FILTER)
        .map_err(|_| RuntimeConfigError::InvalidSource)?
        .set_default("shutdown_grace_ms", DEFAULT_SHUTDOWN_GRACE_MS)
        .map_err(|_| RuntimeConfigError::InvalidSource)?;

    if let Some(contents) = &sources.toml {
        builder = builder.add_source(File::from_str(contents, FileFormat::Toml));
    }

    for (key, value) in &sources.environment {
        let Some(raw_field) = key.strip_prefix(ENVIRONMENT_PREFIX) else {
            continue;
        };
        let field = raw_field.to_ascii_lowercase();
        builder = match field.as_str() {
            "environment"
            | "log_filter"
            | "cell_id"
            | "migration_scope"
            | "topology_file"
            | "database__credential_ref"
            | "database__tls_mode"
            | "transport__credential_ref"
            | "transport__tls_mode"
            | "transport__ca_certificate_file"
            | "transport__expected_server_minimum"
            | "transport__expected_server_maximum_exclusive" => builder
                .set_override(field.replace("__", "."), value.clone())
                .map_err(|_| RuntimeConfigError::InvalidSource)?,
            "transport__servers" => {
                let servers = parse_environment_servers(value)?;
                builder
                    .set_override(field.replace("__", "."), servers)
                    .map_err(|_| RuntimeConfigError::InvalidSource)?
            }
            "shutdown_grace_ms"
            | "migration_timeout_ms"
            | "topology_apply_timeout_ms"
            | "database__acquire_timeout_ms"
            | "database__connect_timeout_ms"
            | "database__statement_timeout_ms"
            | "database__lock_timeout_ms"
            | "database__idle_in_transaction_timeout_ms"
            | "database__max_lifetime_ms"
            | "transport__connect_timeout_ms"
            | "transport__request_timeout_ms"
            | "transport__publish_ack_timeout_ms"
            | "transport__startup_timeout_ms"
            | "transport__outbox_poll_interval_ms"
            | "transport__outbox_lease_ms"
            | "transport__retry_base_ms"
            | "transport__retry_max_ms"
            | "transport__consumer_fetch_expires_ms"
            | "transport__consumer_handler_timeout_ms"
            | "transport__consumer_nak_delay_ms" => {
                let number =
                    value
                        .parse::<u64>()
                        .map_err(|_| RuntimeConfigError::InvalidField {
                            field: numeric_field_name(field.as_str()),
                            reason: "must be an unsigned integer",
                        })?;
                builder
                    .set_override(field.replace("__", "."), number)
                    .map_err(|_| RuntimeConfigError::InvalidSource)?
            }
            "database__max_connections" | "database__min_connections" => {
                let number =
                    value
                        .parse::<u32>()
                        .map_err(|_| RuntimeConfigError::InvalidField {
                            field: numeric_field_name(field.as_str()),
                            reason: "must be an unsigned integer",
                        })?;
                builder
                    .set_override(field.replace("__", "."), number)
                    .map_err(|_| RuntimeConfigError::InvalidSource)?
            }
            "transport__outbox_claim_batch_size"
            | "transport__publish_concurrency"
            | "transport__consumer_fetch_batch_size"
            | "transport__consumer_max_in_flight" => {
                let number =
                    value
                        .parse::<u16>()
                        .map_err(|_| RuntimeConfigError::InvalidField {
                            field: numeric_field_name(field.as_str()),
                            reason: "must be an unsigned integer no greater than 65535",
                        })?;
                builder
                    .set_override(field.replace("__", "."), number)
                    .map_err(|_| RuntimeConfigError::InvalidSource)?
            }
            _ => return Err(RuntimeConfigError::UnknownField(field.replace("__", "."))),
        };
    }

    builder
        .build()
        .and_then(Config::try_deserialize::<RawConfig>)
        .map_err(|_| RuntimeConfigError::InvalidSource)
}

fn parse_environment_servers(value: &str) -> Result<Vec<String>, RuntimeConfigError> {
    let servers = value
        .split(',')
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if servers.iter().any(String::is_empty) {
        return Err(RuntimeConfigError::InvalidField {
            field: "transport.servers",
            reason: "must be a non-empty comma-separated server list",
        });
    }
    Ok(servers)
}

fn numeric_field_name(field: &str) -> &'static str {
    match field {
        "shutdown_grace_ms" => "shutdown_grace_ms",
        "migration_timeout_ms" => "migration_timeout_ms",
        "topology_apply_timeout_ms" => "topology_apply_timeout_ms",
        "database__max_connections" => "database.max_connections",
        "database__min_connections" => "database.min_connections",
        "database__acquire_timeout_ms" => "database.acquire_timeout_ms",
        "database__connect_timeout_ms" => "database.connect_timeout_ms",
        "database__statement_timeout_ms" => "database.statement_timeout_ms",
        "database__lock_timeout_ms" => "database.lock_timeout_ms",
        "database__idle_in_transaction_timeout_ms" => "database.idle_in_transaction_timeout_ms",
        "database__max_lifetime_ms" => "database.max_lifetime_ms",
        "transport__connect_timeout_ms" => "transport.connect_timeout_ms",
        "transport__request_timeout_ms" => "transport.request_timeout_ms",
        "transport__publish_ack_timeout_ms" => "transport.publish_ack_timeout_ms",
        "transport__startup_timeout_ms" => "transport.startup_timeout_ms",
        "transport__outbox_poll_interval_ms" => "transport.outbox_poll_interval_ms",
        "transport__outbox_claim_batch_size" => "transport.outbox_claim_batch_size",
        "transport__outbox_lease_ms" => "transport.outbox_lease_ms",
        "transport__publish_concurrency" => "transport.publish_concurrency",
        "transport__retry_base_ms" => "transport.retry_base_ms",
        "transport__retry_max_ms" => "transport.retry_max_ms",
        "transport__consumer_fetch_batch_size" => "transport.consumer_fetch_batch_size",
        "transport__consumer_fetch_expires_ms" => "transport.consumer_fetch_expires_ms",
        "transport__consumer_handler_timeout_ms" => "transport.consumer_handler_timeout_ms",
        "transport__consumer_nak_delay_ms" => "transport.consumer_nak_delay_ms",
        "transport__consumer_max_in_flight" => "transport.consumer_max_in_flight",
        _ => "numeric configuration",
    }
}

fn validate_base(raw: &RawConfig) -> Result<BaseRuntimeConfig, RuntimeConfigError> {
    let environment = match raw.environment.as_deref() {
        Some("dev") => DeploymentEnvironment::Dev,
        Some("npr") => DeploymentEnvironment::Npr,
        Some("prd") => DeploymentEnvironment::Prd,
        Some(_) => {
            return Err(RuntimeConfigError::InvalidField {
                field: "environment",
                reason: "must be `dev`, `npr`, or `prd`",
            });
        }
        None => return Err(RuntimeConfigError::MissingField("environment")),
    };
    let log_filter = raw
        .log_filter
        .clone()
        .ok_or(RuntimeConfigError::MissingField("log_filter"))
        .and_then(LogFilter::new)?;
    let grace_ms = raw
        .shutdown_grace_ms
        .ok_or(RuntimeConfigError::MissingField("shutdown_grace_ms"))?;
    if !(MIN_SHUTDOWN_GRACE_MS..=MAX_SHUTDOWN_GRACE_MS).contains(&grace_ms) {
        return Err(RuntimeConfigError::InvalidField {
            field: "shutdown_grace_ms",
            reason: "must be between 100 and 300000 milliseconds",
        });
    }

    Ok(BaseRuntimeConfig {
        environment,
        log_filter,
        shutdown_grace: Duration::from_millis(grace_ms),
    })
}

fn validate_database(
    raw: Option<&RawDatabaseConfig>,
    environment: DeploymentEnvironment,
) -> Result<DatabaseConfig, RuntimeConfigError> {
    let raw = raw.ok_or(RuntimeConfigError::MissingField("database"))?;
    let credential_ref = raw
        .credential_ref
        .as_deref()
        .ok_or(RuntimeConfigError::MissingField("database.credential_ref"))?
        .parse::<SecretReference>()
        .map_err(|_| RuntimeConfigError::InvalidField {
            field: "database.credential_ref",
            reason: "must be a bounded opaque secret reference",
        })?;
    let tls_mode = match raw.tls_mode.as_deref().unwrap_or("verify_full") {
        "disable" => DatabaseTlsMode::Disable,
        "verify_full" => DatabaseTlsMode::VerifyFull,
        _ => {
            return Err(RuntimeConfigError::InvalidField {
                field: "database.tls_mode",
                reason: "must be `disable` or `verify_full`",
            });
        }
    };
    if tls_mode == DatabaseTlsMode::Disable && environment != DeploymentEnvironment::Dev {
        return Err(RuntimeConfigError::InvalidField {
            field: "database.tls_mode",
            reason: "must be `verify_full` outside the dev environment",
        });
    }

    let max_connections = raw.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);
    let min_connections = raw.min_connections.unwrap_or(DEFAULT_MIN_CONNECTIONS);
    if !(1..=MAX_DATABASE_CONNECTIONS).contains(&max_connections) {
        return Err(RuntimeConfigError::InvalidField {
            field: "database.max_connections",
            reason: "must be between 1 and 100",
        });
    }
    if min_connections > max_connections {
        return Err(RuntimeConfigError::InvalidField {
            field: "database.min_connections",
            reason: "must not exceed database.max_connections",
        });
    }

    let acquire_timeout = database_timeout(
        raw.acquire_timeout_ms.unwrap_or(DEFAULT_ACQUIRE_TIMEOUT_MS),
        "database.acquire_timeout_ms",
    )?;
    let connect_timeout = database_timeout(
        raw.connect_timeout_ms.unwrap_or(DEFAULT_CONNECT_TIMEOUT_MS),
        "database.connect_timeout_ms",
    )?;
    let statement_timeout = database_timeout(
        raw.statement_timeout_ms
            .unwrap_or(DEFAULT_STATEMENT_TIMEOUT_MS),
        "database.statement_timeout_ms",
    )?;
    let lock_timeout = database_timeout(
        raw.lock_timeout_ms.unwrap_or(DEFAULT_LOCK_TIMEOUT_MS),
        "database.lock_timeout_ms",
    )?;
    let idle_in_transaction_timeout = database_timeout(
        raw.idle_in_transaction_timeout_ms
            .unwrap_or(DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS),
        "database.idle_in_transaction_timeout_ms",
    )?;
    let max_lifetime_ms = raw.max_lifetime_ms.unwrap_or(DEFAULT_MAX_LIFETIME_MS);
    if !(MIN_MAX_LIFETIME_MS..=MAX_MAX_LIFETIME_MS).contains(&max_lifetime_ms) {
        return Err(RuntimeConfigError::InvalidField {
            field: "database.max_lifetime_ms",
            reason: "must be between 1000 and 86400000 milliseconds",
        });
    }

    Ok(DatabaseConfig {
        credential_ref,
        tls_mode,
        max_connections,
        min_connections,
        acquire_timeout,
        connect_timeout,
        statement_timeout,
        lock_timeout,
        idle_in_transaction_timeout,
        max_lifetime: Duration::from_millis(max_lifetime_ms),
    })
}

#[allow(clippy::too_many_lines)]
fn validate_transport(
    raw: Option<&RawTransportConfig>,
    environment: DeploymentEnvironment,
) -> Result<TransportConfig, RuntimeConfigError> {
    let raw = raw.ok_or(RuntimeConfigError::MissingField("transport"))?;
    let tls_mode = match raw.tls_mode.as_deref().unwrap_or("verify_full") {
        "disable" => TransportTlsMode::Disable,
        "verify_full" => TransportTlsMode::VerifyFull,
        _ => {
            return Err(RuntimeConfigError::InvalidField {
                field: "transport.tls_mode",
                reason: "must be `disable` or `verify_full`",
            });
        }
    };
    if tls_mode == TransportTlsMode::Disable && environment != DeploymentEnvironment::Dev {
        return Err(RuntimeConfigError::InvalidField {
            field: "transport.tls_mode",
            reason: "must be `verify_full` outside the dev environment",
        });
    }

    let servers = raw
        .servers
        .as_ref()
        .ok_or(RuntimeConfigError::MissingField("transport.servers"))?;
    if !(1..=MAX_TRANSPORT_SERVERS).contains(&servers.len()) {
        return Err(RuntimeConfigError::InvalidField {
            field: "transport.servers",
            reason: "must contain between 1 and 8 server URLs",
        });
    }
    let mut unique_servers = std::collections::BTreeSet::new();
    for server in servers {
        validate_transport_server(server, tls_mode)?;
        if !unique_servers.insert(server) {
            return Err(RuntimeConfigError::InvalidField {
                field: "transport.servers",
                reason: "must not contain duplicate server URLs",
            });
        }
    }

    let credential_ref = raw
        .credential_ref
        .as_deref()
        .ok_or(RuntimeConfigError::MissingField("transport.credential_ref"))?
        .parse::<SecretReference>()
        .map_err(|_| RuntimeConfigError::InvalidField {
            field: "transport.credential_ref",
            reason: "must be a bounded opaque secret reference",
        })?;
    let ca_certificate_file = match (tls_mode, raw.ca_certificate_file.as_deref()) {
        (TransportTlsMode::VerifyFull, Some(value)) => Some(validate_configuration_path(
            value,
            "transport.ca_certificate_file",
        )?),
        (TransportTlsMode::VerifyFull, None) => {
            return Err(RuntimeConfigError::MissingField(
                "transport.ca_certificate_file",
            ));
        }
        (TransportTlsMode::Disable, Some(_)) => {
            return Err(RuntimeConfigError::UnexpectedField(
                "transport.ca_certificate_file",
            ));
        }
        (TransportTlsMode::Disable, None) => None,
    };

    let connect_timeout = transport_timeout(
        raw.connect_timeout_ms
            .unwrap_or(DEFAULT_TRANSPORT_CONNECT_TIMEOUT_MS),
        "transport.connect_timeout_ms",
    )?;
    let request_timeout = transport_timeout(
        raw.request_timeout_ms
            .unwrap_or(DEFAULT_TRANSPORT_REQUEST_TIMEOUT_MS),
        "transport.request_timeout_ms",
    )?;
    let publish_ack_timeout = transport_timeout(
        raw.publish_ack_timeout_ms
            .unwrap_or(DEFAULT_PUBLISH_ACK_TIMEOUT_MS),
        "transport.publish_ack_timeout_ms",
    )?;
    let startup_timeout = transport_timeout(
        raw.startup_timeout_ms
            .unwrap_or(DEFAULT_TRANSPORT_STARTUP_TIMEOUT_MS),
        "transport.startup_timeout_ms",
    )?;
    if startup_timeout < connect_timeout {
        return Err(RuntimeConfigError::InvalidField {
            field: "transport.startup_timeout_ms",
            reason: "must be greater than or equal to transport.connect_timeout_ms",
        });
    }

    let outbox_poll_interval = positive_transport_interval(
        raw.outbox_poll_interval_ms
            .unwrap_or(DEFAULT_OUTBOX_POLL_INTERVAL_MS),
        "transport.outbox_poll_interval_ms",
    )?;
    let outbox_claim_batch_size = raw
        .outbox_claim_batch_size
        .unwrap_or(DEFAULT_OUTBOX_CLAIM_BATCH_SIZE);
    validate_u16_range(
        outbox_claim_batch_size,
        1,
        500,
        "transport.outbox_claim_batch_size",
        "must be between 1 and 500",
    )?;
    let outbox_lease_ms = raw.outbox_lease_ms.unwrap_or(DEFAULT_OUTBOX_LEASE_MS);
    if !(1_000..=300_000).contains(&outbox_lease_ms) || outbox_lease_ms % 1_000 != 0 {
        return Err(RuntimeConfigError::InvalidField {
            field: "transport.outbox_lease_ms",
            reason: "must be between 1000 and 300000 whole milliseconds in one-second increments",
        });
    }
    if u128::from(outbox_lease_ms) <= publish_ack_timeout.as_millis() {
        return Err(RuntimeConfigError::InvalidField {
            field: "transport.outbox_lease_ms",
            reason: "must exceed the maximum configured publish acknowledgment window",
        });
    }
    let publish_concurrency = raw
        .publish_concurrency
        .unwrap_or(DEFAULT_PUBLISH_CONCURRENCY);
    validate_u16_range(
        publish_concurrency,
        1,
        128,
        "transport.publish_concurrency",
        "must be between 1 and 128",
    )?;

    let retry_base_ms = raw.retry_base_ms.unwrap_or(DEFAULT_RETRY_BASE_MS);
    let retry_max_ms = raw.retry_max_ms.unwrap_or(DEFAULT_RETRY_MAX_MS);
    if retry_base_ms == 0 || retry_base_ms > MAX_TRANSPORT_TIMEOUT_MS {
        return Err(RuntimeConfigError::InvalidField {
            field: "transport.retry_base_ms",
            reason: "must be between 1 and 300000 milliseconds",
        });
    }
    if !(retry_base_ms..=MAX_TRANSPORT_TIMEOUT_MS).contains(&retry_max_ms) {
        return Err(RuntimeConfigError::InvalidField {
            field: "transport.retry_max_ms",
            reason: "must be at least retry_base_ms and no greater than 300000 milliseconds",
        });
    }

    let consumer_fetch_batch_size = raw
        .consumer_fetch_batch_size
        .unwrap_or(DEFAULT_CONSUMER_FETCH_BATCH_SIZE);
    validate_u16_range(
        consumer_fetch_batch_size,
        1,
        500,
        "transport.consumer_fetch_batch_size",
        "must be between 1 and 500",
    )?;
    let consumer_fetch_expires = positive_transport_interval(
        raw.consumer_fetch_expires_ms
            .unwrap_or(DEFAULT_CONSUMER_FETCH_EXPIRES_MS),
        "transport.consumer_fetch_expires_ms",
    )?;
    let handler_timeout_ms = raw
        .consumer_handler_timeout_ms
        .unwrap_or(DEFAULT_CONSUMER_HANDLER_TIMEOUT_MS);
    if handler_timeout_ms == 0 || handler_timeout_ms >= MAX_CONSUMER_ACK_WAIT_MS {
        return Err(RuntimeConfigError::InvalidField {
            field: "transport.consumer_handler_timeout_ms",
            reason: "must be positive and strictly less than the 30000 millisecond AckWait",
        });
    }
    let consumer_nak_delay = positive_transport_interval(
        raw.consumer_nak_delay_ms
            .unwrap_or(DEFAULT_CONSUMER_NAK_DELAY_MS),
        "transport.consumer_nak_delay_ms",
    )?;
    let consumer_max_in_flight = raw
        .consumer_max_in_flight
        .unwrap_or(DEFAULT_CONSUMER_MAX_IN_FLIGHT);
    validate_u16_range(
        consumer_max_in_flight,
        1,
        1_024,
        "transport.consumer_max_in_flight",
        "must be between 1 and 1024",
    )?;

    let expected_server_minimum = raw
        .expected_server_minimum
        .as_deref()
        .unwrap_or(DEFAULT_EXPECTED_SERVER_MINIMUM);
    let expected_server_maximum_exclusive = raw
        .expected_server_maximum_exclusive
        .as_deref()
        .unwrap_or(DEFAULT_EXPECTED_SERVER_MAXIMUM_EXCLUSIVE);
    if expected_server_minimum != DEFAULT_EXPECTED_SERVER_MINIMUM {
        return Err(RuntimeConfigError::InvalidField {
            field: "transport.expected_server_minimum",
            reason: "must match the qualified minimum server version",
        });
    }
    if expected_server_maximum_exclusive != DEFAULT_EXPECTED_SERVER_MAXIMUM_EXCLUSIVE {
        return Err(RuntimeConfigError::InvalidField {
            field: "transport.expected_server_maximum_exclusive",
            reason: "must match the qualified exclusive maximum server version",
        });
    }

    Ok(TransportConfig {
        servers: servers.clone(),
        credential_ref,
        tls_mode,
        ca_certificate_file,
        connect_timeout,
        request_timeout,
        publish_ack_timeout,
        startup_timeout,
        outbox_poll_interval,
        outbox_claim_batch_size,
        outbox_lease: Duration::from_millis(outbox_lease_ms),
        publish_concurrency,
        retry_base: Duration::from_millis(retry_base_ms),
        retry_max: Duration::from_millis(retry_max_ms),
        consumer_fetch_batch_size,
        consumer_fetch_expires,
        consumer_handler_timeout: Duration::from_millis(handler_timeout_ms),
        consumer_nak_delay,
        consumer_max_in_flight,
        expected_server_minimum: expected_server_minimum.to_owned(),
        expected_server_maximum_exclusive: expected_server_maximum_exclusive.to_owned(),
    })
}

fn validate_transport_server(
    value: &str,
    tls_mode: TransportTlsMode,
) -> Result<(), RuntimeConfigError> {
    let invalid = || RuntimeConfigError::InvalidField {
        field: "transport.servers",
        reason: "must contain bounded nats:// or tls:// host:port URLs without credentials, paths, queries, or fragments",
    };
    if !(1..=MAX_TRANSPORT_SERVER_URL_LENGTH).contains(&value.len())
        || !value.is_ascii()
        || value.contains(['@', '?', '#'])
    {
        return Err(invalid());
    }
    let (scheme, authority) = value.split_once("://").ok_or_else(invalid)?;
    if authority.contains(['/', '?', '#', '@'])
        || !matches!(scheme, "nats" | "tls")
        || (tls_mode == TransportTlsMode::VerifyFull && scheme != "tls")
        || (tls_mode == TransportTlsMode::Disable && scheme != "nats")
    {
        return Err(invalid());
    }
    let (host, port) = authority.rsplit_once(':').ok_or_else(invalid)?;
    if !(1..=MAX_TRANSPORT_HOST_LENGTH).contains(&host.len())
        || host.chars().any(char::is_whitespace)
        || port
            .parse::<u16>()
            .ok()
            .as_ref()
            .is_none_or(|number| *number == 0)
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_configuration_path(
    value: &str,
    field: &'static str,
) -> Result<PathBuf, RuntimeConfigError> {
    if value.is_empty()
        || value.len() > MAX_TRANSPORT_PATH_LENGTH
        || value.chars().any(char::is_control)
    {
        return Err(RuntimeConfigError::InvalidField {
            field,
            reason: "must be a bounded non-empty path without control characters",
        });
    }
    Ok(PathBuf::from(value))
}

fn transport_timeout(
    milliseconds: u64,
    field: &'static str,
) -> Result<Duration, RuntimeConfigError> {
    if !(MIN_TRANSPORT_TIMEOUT_MS..=MAX_TRANSPORT_TIMEOUT_MS).contains(&milliseconds) {
        return Err(RuntimeConfigError::InvalidField {
            field,
            reason: "must be between 100 and 300000 milliseconds",
        });
    }
    Ok(Duration::from_millis(milliseconds))
}

fn positive_transport_interval(
    milliseconds: u64,
    field: &'static str,
) -> Result<Duration, RuntimeConfigError> {
    if !(1..=MAX_TRANSPORT_TIMEOUT_MS).contains(&milliseconds) {
        return Err(RuntimeConfigError::InvalidField {
            field,
            reason: "must be between 1 and 300000 milliseconds",
        });
    }
    Ok(Duration::from_millis(milliseconds))
}

fn validate_u16_range(
    value: u16,
    minimum: u16,
    maximum: u16,
    field: &'static str,
    reason: &'static str,
) -> Result<(), RuntimeConfigError> {
    if !(minimum..=maximum).contains(&value) {
        return Err(RuntimeConfigError::InvalidField { field, reason });
    }
    Ok(())
}

fn database_timeout(
    milliseconds: u64,
    field: &'static str,
) -> Result<Duration, RuntimeConfigError> {
    if !(MIN_DATABASE_TIMEOUT_MS..=MAX_DATABASE_TIMEOUT_MS).contains(&milliseconds) {
        return Err(RuntimeConfigError::InvalidField {
            field,
            reason: "must be between 100 and 3600000 milliseconds",
        });
    }
    Ok(Duration::from_millis(milliseconds))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use super::{
        ConfigSources, DatabaseTlsMode, DeploymentEnvironment, MigrationScope, RuntimeConfigError,
        SecretReference, ServiceKind, TransportTlsMode, load_cell_from_sources,
        load_migrator_from_sources, load_nats_provisioner_from_sources, load_platform_from_sources,
        load_router_from_sources,
    };

    fn source(entries: &[(&str, &str)]) -> ConfigSources {
        ConfigSources::new(
            entries
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
        )
    }

    fn database_entries(environment: &str) -> Vec<(&'static str, &'static str)> {
        vec![
            (
                "EDTECH__ENVIRONMENT",
                match environment {
                    "dev" => "dev",
                    "npr" => "npr",
                    _ => "prd",
                },
            ),
            (
                "EDTECH__DATABASE__CREDENTIAL_REF",
                "file:/run/secrets/edtech-database",
            ),
            (
                "EDTECH__DATABASE__TLS_MODE",
                if environment == "dev" {
                    "disable"
                } else {
                    "verify_full"
                },
            ),
        ]
    }

    fn transport_entries(environment: &str) -> Vec<(&'static str, &'static str)> {
        let verify_full = environment != "dev";
        let mut entries = vec![
            (
                "EDTECH__TRANSPORT__SERVERS",
                if verify_full {
                    "tls://nats-1:4222,tls://nats-2:4222,tls://nats-3:4222"
                } else {
                    "nats://nats-1:4222,nats://nats-2:4222,nats://nats-3:4222"
                },
            ),
            (
                "EDTECH__TRANSPORT__CREDENTIAL_REF",
                "file:/run/secrets/edtech-nats",
            ),
            (
                "EDTECH__TRANSPORT__TLS_MODE",
                if verify_full {
                    "verify_full"
                } else {
                    "disable"
                },
            ),
        ];
        if verify_full {
            entries.push((
                "EDTECH__TRANSPORT__CA_CERTIFICATE_FILE",
                "/run/edtech-nats/ca.pem",
            ));
        }
        entries
    }

    #[test]
    fn every_service_environment_combination_obeys_database_scope() {
        for (environment, expected) in [
            ("dev", DeploymentEnvironment::Dev),
            ("npr", DeploymentEnvironment::Npr),
            ("prd", DeploymentEnvironment::Prd),
        ] {
            let entries = database_entries(environment);
            let platform_api =
                load_platform_from_sources(ServiceKind::PlatformApi, &source(&entries));
            assert_eq!(
                platform_api.ok().map(|item| item.base().environment()),
                Some(expected)
            );
            let mut worker_entries = entries.clone();
            worker_entries.extend(transport_entries(environment));
            let platform_worker =
                load_platform_from_sources(ServiceKind::PlatformWorker, &source(&worker_entries));
            assert_eq!(
                platform_worker
                    .as_ref()
                    .ok()
                    .map(|item| item.base().environment()),
                Some(expected)
            );
            assert!(
                platform_worker
                    .ok()
                    .and_then(|item| item.transport)
                    .is_some()
            );

            let mut cell_entries = entries.clone();
            cell_entries.push(("EDTECH__CELL_ID", "cell-001"));
            let cell_api = load_cell_from_sources(ServiceKind::CellApi, &source(&cell_entries));
            assert_eq!(
                cell_api.ok().map(|item| item.base().environment()),
                Some(expected)
            );
            cell_entries.extend(transport_entries(environment));
            let cell_worker =
                load_cell_from_sources(ServiceKind::CellWorker, &source(&cell_entries));
            assert_eq!(
                cell_worker.ok().map(|item| item.base().environment()),
                Some(expected)
            );

            let router = load_router_from_sources(&source(&[("EDTECH__ENVIRONMENT", environment)]));
            assert_eq!(
                router.ok().map(|item| item.base().environment()),
                Some(expected)
            );
        }
    }

    #[test]
    fn transport_scope_tls_and_bounds_are_enforced_without_exposing_values() {
        let sentinel = "nats://private-user:private-password@private.example:4222";
        for service in [ServiceKind::PlatformApi, ServiceKind::PlatformWorker] {
            let mut entries = database_entries("dev");
            entries.extend(transport_entries("dev"));
            if service == ServiceKind::PlatformWorker {
                entries.retain(|(key, _)| *key != "EDTECH__TRANSPORT__SERVERS");
                entries.push(("EDTECH__TRANSPORT__SERVERS", sentinel));
            }
            let rendered = load_platform_from_sources(service, &source(&entries))
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default();
            assert!(!rendered.contains("private-user"));
            assert!(!rendered.contains("private-password"));
            assert!(if service == ServiceKind::PlatformApi {
                rendered.contains("not valid for this service")
            } else {
                rendered.contains("transport.servers")
            });
        }

        let mut npr_entries = database_entries("npr");
        npr_entries.extend(transport_entries("dev"));
        assert!(matches!(
            load_platform_from_sources(ServiceKind::PlatformWorker, &source(&npr_entries)),
            Err(RuntimeConfigError::InvalidField {
                field: "transport.tls_mode",
                ..
            })
        ));

        let mut bounded = database_entries("dev");
        bounded.extend(transport_entries("dev"));
        bounded.push(("EDTECH__TRANSPORT__OUTBOX_CLAIM_BATCH_SIZE", "501"));
        assert!(matches!(
            load_platform_from_sources(ServiceKind::PlatformWorker, &source(&bounded)),
            Err(RuntimeConfigError::InvalidField {
                field: "transport.outbox_claim_batch_size",
                ..
            })
        ));
    }

    #[test]
    fn transport_timing_invariants_and_redaction_are_deterministic() {
        let cases = [
            (
                "EDTECH__TRANSPORT__CONSUMER_HANDLER_TIMEOUT_MS",
                "30000",
                "transport.consumer_handler_timeout_ms",
            ),
            (
                "EDTECH__TRANSPORT__OUTBOX_LEASE_MS",
                "5000",
                "transport.outbox_lease_ms",
            ),
            (
                "EDTECH__TRANSPORT__RETRY_BASE_MS",
                "0",
                "transport.retry_base_ms",
            ),
        ];
        for (key, value, expected_field) in cases {
            let mut entries = database_entries("dev");
            entries.extend(transport_entries("dev"));
            entries.push((key, value));
            assert!(matches!(
                load_platform_from_sources(ServiceKind::PlatformWorker, &source(&entries)),
                Err(RuntimeConfigError::InvalidField { field, .. }) if field == expected_field
            ));
        }

        let mut entries = database_entries("dev");
        entries.extend(transport_entries("dev"));
        let config = load_platform_from_sources(ServiceKind::PlatformWorker, &source(&entries));
        assert_eq!(
            config
                .as_ref()
                .ok()
                .and_then(|item| item.transport())
                .map(super::TransportConfig::tls_mode),
            Some(TransportTlsMode::Disable)
        );
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("nats-1"));
        assert!(!rendered.contains("/run/secrets/edtech-nats"));
    }

    #[test]
    fn nats_provisioner_has_only_transport_and_topology_configuration() {
        for environment in ["dev", "npr", "prd"] {
            let mut entries = vec![("EDTECH__ENVIRONMENT", environment)];
            entries.extend(transport_entries(environment));
            entries.push(("EDTECH__TOPOLOGY_FILE", "/run/edtech-nats/topology.toml"));
            let config = load_nats_provisioner_from_sources(&source(&entries));
            assert_eq!(
                config.ok().map(|item| item.base().environment()),
                Some(match environment {
                    "dev" => DeploymentEnvironment::Dev,
                    "npr" => DeploymentEnvironment::Npr,
                    _ => DeploymentEnvironment::Prd,
                })
            );
        }

        let mut with_database = vec![("EDTECH__ENVIRONMENT", "dev")];
        with_database.extend(transport_entries("dev"));
        with_database.extend(database_entries("dev"));
        with_database.push(("EDTECH__TOPOLOGY_FILE", "/tmp/topology.toml"));
        assert!(matches!(
            load_nats_provisioner_from_sources(&source(&with_database)),
            Err(RuntimeConfigError::UnexpectedField("database"))
        ));
    }

    #[test]
    fn database_enabled_services_require_database_configuration() {
        let no_database = source(&[("EDTECH__ENVIRONMENT", "dev")]);
        assert!(matches!(
            load_platform_from_sources(ServiceKind::PlatformApi, &no_database),
            Err(RuntimeConfigError::MissingField("database"))
        ));
        assert!(matches!(
            load_cell_from_sources(
                ServiceKind::CellApi,
                &source(&[
                    ("EDTECH__ENVIRONMENT", "dev"),
                    ("EDTECH__CELL_ID", "cell-001")
                ])
            ),
            Err(RuntimeConfigError::MissingField("database"))
        ));
        assert!(matches!(
            load_migrator_from_sources(&source(&[
                ("EDTECH__ENVIRONMENT", "dev"),
                ("EDTECH__MIGRATION_SCOPE", "platform")
            ])),
            Err(RuntimeConfigError::MissingField("database"))
        ));
    }

    #[test]
    fn tenant_router_rejects_every_database_field() {
        for field in [
            "EDTECH__DATABASE__CREDENTIAL_REF",
            "EDTECH__DATABASE__TLS_MODE",
            "EDTECH__DATABASE__MAX_CONNECTIONS",
            "EDTECH__DATABASE__MIN_CONNECTIONS",
            "EDTECH__DATABASE__ACQUIRE_TIMEOUT_MS",
            "EDTECH__DATABASE__CONNECT_TIMEOUT_MS",
            "EDTECH__DATABASE__STATEMENT_TIMEOUT_MS",
            "EDTECH__DATABASE__LOCK_TIMEOUT_MS",
            "EDTECH__DATABASE__IDLE_IN_TRANSACTION_TIMEOUT_MS",
            "EDTECH__DATABASE__MAX_LIFETIME_MS",
        ] {
            let config = load_router_from_sources(&source(&[
                ("EDTECH__ENVIRONMENT", "dev"),
                (field, "1000"),
            ]));
            assert!(matches!(
                config,
                Err(RuntimeConfigError::UnexpectedField("database"))
            ));
        }
    }

    #[test]
    fn tls_disable_is_confined_to_dev() {
        for environment in ["npr", "prd"] {
            let mut entries = database_entries(environment);
            if let Some(value) = entries
                .iter_mut()
                .find(|(key, _)| *key == "EDTECH__DATABASE__TLS_MODE")
            {
                value.1 = "disable";
            }
            assert!(matches!(
                load_platform_from_sources(ServiceKind::PlatformApi, &source(&entries)),
                Err(RuntimeConfigError::InvalidField {
                    field: "database.tls_mode",
                    ..
                })
            ));
        }
        let dev =
            load_platform_from_sources(ServiceKind::PlatformApi, &source(&database_entries("dev")));
        assert_eq!(
            dev.ok().map(|item| item.database().tls_mode()),
            Some(DatabaseTlsMode::Disable)
        );
    }

    #[test]
    fn pool_bounds_and_timeouts_are_bounded() {
        let cases = [
            (
                "EDTECH__DATABASE__MAX_CONNECTIONS",
                "0",
                "database.max_connections",
            ),
            (
                "EDTECH__DATABASE__ACQUIRE_TIMEOUT_MS",
                "0",
                "database.acquire_timeout_ms",
            ),
            (
                "EDTECH__DATABASE__MAX_LIFETIME_MS",
                "0",
                "database.max_lifetime_ms",
            ),
        ];
        for (key, value, expected_field) in cases {
            let mut entries = database_entries("dev");
            entries.push((key, value));
            assert!(matches!(
                load_platform_from_sources(ServiceKind::PlatformApi, &source(&entries)),
                Err(RuntimeConfigError::InvalidField { field, .. }) if field == expected_field
            ));
        }

        let mut entries = database_entries("dev");
        entries.push(("EDTECH__DATABASE__MIN_CONNECTIONS", "11"));
        entries.push(("EDTECH__DATABASE__MAX_CONNECTIONS", "10"));
        assert!(matches!(
            load_platform_from_sources(ServiceKind::PlatformApi, &source(&entries)),
            Err(RuntimeConfigError::InvalidField {
                field: "database.min_connections",
                ..
            })
        ));
    }

    #[test]
    fn nested_unknown_and_raw_secret_fields_fail_without_echoing_values() {
        let sentinel = "unique-password-sentinel";
        for key in [
            "EDTECH__DATABASE__SURPRISE",
            "EDTECH__DATABASE_URL",
            "EDTECH__DATABASE__PASSWORD",
        ] {
            let rendered = load_platform_from_sources(
                ServiceKind::PlatformApi,
                &source(&[("EDTECH__ENVIRONMENT", "dev"), (key, sentinel)]),
            )
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
            assert!(!rendered.contains(sentinel));
        }

        let raw_secret = ConfigSources::new(BTreeMap::from([(
            String::from("EDTECH__ENVIRONMENT"),
            String::from("dev"),
        )]))
        .with_toml(format!("[database]\npassword = \"{sentinel}\""));
        let rendered = load_platform_from_sources(ServiceKind::PlatformApi, &raw_secret)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(!rendered.contains(sentinel));
    }

    #[test]
    fn cell_process_requires_a_valid_cell_id() {
        let entries = database_entries("dev");
        assert!(matches!(
            load_cell_from_sources(ServiceKind::CellApi, &source(&entries)),
            Err(RuntimeConfigError::MissingField("cell_id"))
        ));
        let mut invalid = entries;
        invalid.push(("EDTECH__CELL_ID", "Cell_01"));
        assert!(matches!(
            load_cell_from_sources(ServiceKind::CellApi, &source(&invalid)),
            Err(RuntimeConfigError::InvalidField {
                field: "cell_id",
                ..
            })
        ));
    }

    #[test]
    fn migrator_validates_scope_cell_and_timeout() {
        let mut platform_entries = database_entries("dev");
        platform_entries.push(("EDTECH__MIGRATION_SCOPE", "platform"));
        let platform = load_migrator_from_sources(&source(&platform_entries));
        assert_eq!(
            platform
                .as_ref()
                .ok()
                .map(super::MigratorRuntimeConfig::scope),
            Some(MigrationScope::Platform)
        );
        assert_eq!(
            platform.ok().map(|item| item.migration_timeout()),
            Some(Duration::from_mins(10))
        );

        let mut cell_entries = database_entries("dev");
        cell_entries.push(("EDTECH__MIGRATION_SCOPE", "cell"));
        cell_entries.push(("EDTECH__CELL_ID", "cell-001"));
        assert!(load_migrator_from_sources(&source(&cell_entries)).is_ok());

        let mut missing_cell = database_entries("dev");
        missing_cell.push(("EDTECH__MIGRATION_SCOPE", "cell"));
        assert!(matches!(
            load_migrator_from_sources(&source(&missing_cell)),
            Err(RuntimeConfigError::MissingField("cell_id"))
        ));

        let mut platform_with_cell = platform_entries;
        platform_with_cell.push(("EDTECH__CELL_ID", "cell-001"));
        assert!(matches!(
            load_migrator_from_sources(&source(&platform_with_cell)),
            Err(RuntimeConfigError::UnexpectedField("cell_id"))
        ));
    }

    #[test]
    fn secret_reference_debug_is_redacted() {
        let reference = SecretReference::new("file:/run/secrets/database");
        assert_eq!(
            reference.as_ref().map(|value| format!("{value:?}")).ok(),
            Some(String::from("SecretReference([REDACTED])"))
        );
        assert!(!format!("{reference:?}").contains("/run/secrets/database"));
    }

    #[test]
    fn precedence_is_defaults_then_file_then_environment() {
        let sources = ConfigSources::new(BTreeMap::from([
            (String::from("EDTECH__ENVIRONMENT"), String::from("dev")),
            (String::from("EDTECH__LOG_FILTER"), String::from("debug")),
            (
                String::from("EDTECH__SHUTDOWN_GRACE_MS"),
                String::from("7000"),
            ),
        ]))
        .with_toml("environment = \"npr\"\nlog_filter = \"warn\"\nshutdown_grace_ms = 5000");
        let config = load_router_from_sources(&sources);
        assert_eq!(
            config.as_ref().ok().map(|item| item.base().environment()),
            Some(DeploymentEnvironment::Dev)
        );
        assert_eq!(
            config
                .as_ref()
                .ok()
                .map(|item| item.base().log_filter().as_str()),
            Some("debug")
        );
        assert_eq!(
            config
                .as_ref()
                .ok()
                .map(|item| item.base().shutdown_grace()),
            Some(Duration::from_secs(7))
        );
    }

    #[test]
    fn check_config_data_contains_only_a_reference_not_credential_material() {
        let config =
            load_platform_from_sources(ServiceKind::PlatformApi, &source(&database_entries("dev")));
        assert_eq!(
            config
                .as_ref()
                .ok()
                .map(|item| item.database().max_connections()),
            Some(10)
        );
        assert_eq!(
            config
                .as_ref()
                .ok()
                .map(|item| item.database().statement_timeout()),
            Some(Duration::from_secs(30))
        );
        assert!(!format!("{config:?}").contains("/run/secrets/edtech-database"));
    }
}
