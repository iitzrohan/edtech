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

/// One of the six fixed process composition roots.
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

/// Validated configuration for a Platform-authority runtime process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlatformRuntimeConfig {
    base: BaseRuntimeConfig,
    database: DatabaseConfig,
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
struct RawConfig {
    environment: Option<String>,
    log_filter: Option<String>,
    shutdown_grace_ms: Option<u64>,
    cell_id: Option<String>,
    migration_scope: Option<String>,
    migration_timeout_ms: Option<u64>,
    database: Option<RawDatabaseConfig>,
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
    if raw.cell_id.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("cell_id"));
    }
    let base = validate_base(&raw)?;
    let database = validate_database(raw.database.as_ref(), base.environment())?;
    Ok(PlatformRuntimeConfig { base, database })
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
    if raw.cell_id.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("cell_id"));
    }
    if raw.database.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("database"));
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
    let cell_id = validate_cell_id(raw.cell_id.as_deref())?;
    let base = validate_base(&raw)?;
    let database = validate_database(raw.database.as_ref(), base.environment())?;
    Ok(CellRuntimeConfig {
        base,
        cell_id,
        database,
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

fn reject_migration_fields(raw: &RawConfig) -> Result<(), RuntimeConfigError> {
    if raw.migration_scope.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("migration_scope"));
    }
    if raw.migration_timeout_ms.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("migration_timeout_ms"));
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
            | "database__credential_ref"
            | "database__tls_mode" => builder
                .set_override(field.replace("__", "."), value.clone())
                .map_err(|_| RuntimeConfigError::InvalidSource)?,
            "shutdown_grace_ms"
            | "migration_timeout_ms"
            | "database__acquire_timeout_ms"
            | "database__connect_timeout_ms"
            | "database__statement_timeout_ms"
            | "database__lock_timeout_ms"
            | "database__idle_in_transaction_timeout_ms"
            | "database__max_lifetime_ms" => {
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
            _ => return Err(RuntimeConfigError::UnknownField(field.replace("__", "."))),
        };
    }

    builder
        .build()
        .and_then(Config::try_deserialize::<RawConfig>)
        .map_err(|_| RuntimeConfigError::InvalidSource)
}

fn numeric_field_name(field: &str) -> &'static str {
    match field {
        "shutdown_grace_ms" => "shutdown_grace_ms",
        "migration_timeout_ms" => "migration_timeout_ms",
        "database__max_connections" => "database.max_connections",
        "database__min_connections" => "database.min_connections",
        "database__acquire_timeout_ms" => "database.acquire_timeout_ms",
        "database__connect_timeout_ms" => "database.connect_timeout_ms",
        "database__statement_timeout_ms" => "database.statement_timeout_ms",
        "database__lock_timeout_ms" => "database.lock_timeout_ms",
        "database__idle_in_transaction_timeout_ms" => "database.idle_in_transaction_timeout_ms",
        "database__max_lifetime_ms" => "database.max_lifetime_ms",
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
        SecretReference, ServiceKind, load_cell_from_sources, load_migrator_from_sources,
        load_platform_from_sources, load_router_from_sources,
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

    #[test]
    fn every_service_environment_combination_obeys_database_scope() {
        for (environment, expected) in [
            ("dev", DeploymentEnvironment::Dev),
            ("npr", DeploymentEnvironment::Npr),
            ("prd", DeploymentEnvironment::Prd),
        ] {
            let entries = database_entries(environment);
            for service in [ServiceKind::PlatformApi, ServiceKind::PlatformWorker] {
                let config = load_platform_from_sources(service, &source(&entries));
                assert_eq!(
                    config.ok().map(|item| item.base().environment()),
                    Some(expected)
                );
            }

            let mut cell_entries = entries.clone();
            cell_entries.push(("EDTECH__CELL_ID", "cell-001"));
            for service in [ServiceKind::CellApi, ServiceKind::CellWorker] {
                let config = load_cell_from_sources(service, &source(&cell_entries));
                assert_eq!(
                    config.ok().map(|item| item.base().environment()),
                    Some(expected)
                );
            }

            let router = load_router_from_sources(&source(&[("EDTECH__ENVIRONMENT", environment)]));
            assert_eq!(
                router.ok().map(|item| item.base().environment()),
                Some(expected)
            );
        }
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
