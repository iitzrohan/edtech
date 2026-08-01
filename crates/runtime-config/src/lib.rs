//! Typed startup configuration at the process-composition boundary.
//!
//! The same schema supports `dev`, `npr`, and `prd`; the selected environment is runtime data.
//! Raw deserialization stays private, secret references are opaque and redacted, and domain code
//! must never depend on this crate.

use std::{collections::BTreeMap, fmt, fs, path::PathBuf, str::FromStr, time::Duration};

use config::{Config, File, FileFormat};
use serde::Deserialize;
use tenancy_domain::CellId;
use thiserror::Error;

const ENVIRONMENT_PREFIX: &str = "EDTECH__";
const CONFIG_FILE_VARIABLE: &str = "EDTECH_CONFIG_FILE";
const DEFAULT_LOG_FILTER: &str = "info";
const DEFAULT_SHUTDOWN_GRACE_MS: u64 = 30_000;
const MIN_SHUTDOWN_GRACE_MS: u64 = 100;
const MAX_SHUTDOWN_GRACE_MS: u64 = 300_000;
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
    /// Platform API process shell.
    PlatformApi,
    /// Platform worker process shell.
    PlatformWorker,
    /// Tenant router process shell.
    TenantRouter,
    /// Cell API process shell.
    CellApi,
    /// Cell worker process shell.
    CellWorker,
    /// Separately privileged database migrator process shell.
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
        matches!(
            self,
            Self::PlatformApi | Self::PlatformWorker | Self::TenantRouter
        )
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

/// Validated configuration for a Platform-authority process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlatformRuntimeConfig {
    base: BaseRuntimeConfig,
}

impl PlatformRuntimeConfig {
    /// Returns settings common to all process types.
    #[must_use]
    pub const fn base(&self) -> &BaseRuntimeConfig {
        &self.base
    }
}

/// Validated configuration for a Cell-authority process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CellRuntimeConfig {
    base: BaseRuntimeConfig,
    cell_id: CellId,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    environment: Option<String>,
    log_filter: Option<String>,
    shutdown_grace_ms: Option<u64>,
    cell_id: Option<String>,
    migration_scope: Option<String>,
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
    if raw.cell_id.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("cell_id"));
    }
    if raw.migration_scope.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("migration_scope"));
    }
    Ok(PlatformRuntimeConfig {
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
    if raw.migration_scope.is_some() {
        return Err(RuntimeConfigError::UnexpectedField("migration_scope"));
    }
    let cell_id = raw
        .cell_id
        .as_deref()
        .ok_or(RuntimeConfigError::MissingField("cell_id"))?
        .parse::<CellId>()
        .map_err(|_| RuntimeConfigError::InvalidField {
            field: "cell_id",
            reason: "must be a valid topology-neutral logical Cell identifier",
        })?;
    Ok(CellRuntimeConfig {
        base: validate_base(&raw)?,
        cell_id,
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
        (MigrationScope::Cell, Some(value)) => {
            Some(
                value
                    .parse::<CellId>()
                    .map_err(|_| RuntimeConfigError::InvalidField {
                        field: "cell_id",
                        reason: "must be a valid topology-neutral logical Cell identifier",
                    })?,
            )
        }
    };

    Ok(MigratorRuntimeConfig {
        base: validate_base(&raw)?,
        scope,
        cell_id,
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
            "environment" | "log_filter" | "cell_id" | "migration_scope" => builder
                .set_override(field, value.clone())
                .map_err(|_| RuntimeConfigError::InvalidSource)?,
            "shutdown_grace_ms" => {
                let milliseconds =
                    value
                        .parse::<u64>()
                        .map_err(|_| RuntimeConfigError::InvalidField {
                            field: "shutdown_grace_ms",
                            reason: "must be an unsigned integer",
                        })?;
                builder
                    .set_override(field, milliseconds)
                    .map_err(|_| RuntimeConfigError::InvalidSource)?
            }
            _ => return Err(RuntimeConfigError::UnknownField(field)),
        };
    }

    builder
        .build()
        .and_then(Config::try_deserialize::<RawConfig>)
        .map_err(|_| RuntimeConfigError::InvalidSource)
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

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use super::{
        ConfigSources, DeploymentEnvironment, MigrationScope, RuntimeConfigError, SecretReference,
        ServiceKind, load_cell_from_sources, load_migrator_from_sources,
        load_platform_from_sources,
    };

    fn source(entries: &[(&str, &str)]) -> ConfigSources {
        ConfigSources::new(
            entries
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
        )
    }

    #[test]
    fn each_environment_is_valid_runtime_data() {
        let cases = [
            ("dev", DeploymentEnvironment::Dev),
            ("npr", DeploymentEnvironment::Npr),
            ("prd", DeploymentEnvironment::Prd),
        ];
        for (value, expected) in cases {
            let config = load_platform_from_sources(
                ServiceKind::PlatformApi,
                &source(&[("EDTECH__ENVIRONMENT", value)]),
            );
            assert_eq!(
                config.ok().map(|item| item.base().environment()),
                Some(expected)
            );
        }
    }

    #[test]
    fn missing_and_unknown_environments_fail() {
        assert!(matches!(
            load_platform_from_sources(ServiceKind::PlatformApi, &ConfigSources::default()),
            Err(RuntimeConfigError::MissingField("environment"))
        ));
        assert!(matches!(
            load_platform_from_sources(
                ServiceKind::PlatformApi,
                &source(&[("EDTECH__ENVIRONMENT", "staging")])
            ),
            Err(RuntimeConfigError::InvalidField {
                field: "environment",
                ..
            })
        ));
    }

    #[test]
    fn unknown_key_fails() {
        assert!(matches!(
            load_platform_from_sources(
                ServiceKind::PlatformApi,
                &source(&[
                    ("EDTECH__ENVIRONMENT", "dev"),
                    ("EDTECH__SURPRISE", "value")
                ])
            ),
            Err(RuntimeConfigError::UnknownField(field)) if field == "surprise"
        ));
    }

    #[test]
    fn cell_process_requires_a_valid_cell_id() {
        assert!(matches!(
            load_cell_from_sources(
                ServiceKind::CellApi,
                &source(&[("EDTECH__ENVIRONMENT", "dev")])
            ),
            Err(RuntimeConfigError::MissingField("cell_id"))
        ));
        assert!(matches!(
            load_cell_from_sources(
                ServiceKind::CellApi,
                &source(&[
                    ("EDTECH__ENVIRONMENT", "dev"),
                    ("EDTECH__CELL_ID", "Cell_01")
                ])
            ),
            Err(RuntimeConfigError::InvalidField {
                field: "cell_id",
                ..
            })
        ));
    }

    #[test]
    fn platform_process_rejects_cell_only_field() {
        assert!(matches!(
            load_platform_from_sources(
                ServiceKind::PlatformWorker,
                &source(&[
                    ("EDTECH__ENVIRONMENT", "npr"),
                    ("EDTECH__CELL_ID", "cell-001")
                ])
            ),
            Err(RuntimeConfigError::UnexpectedField("cell_id"))
        ));
    }

    #[test]
    fn migrator_validates_scope_and_cell_combination() {
        let platform = load_migrator_from_sources(&source(&[
            ("EDTECH__ENVIRONMENT", "prd"),
            ("EDTECH__MIGRATION_SCOPE", "platform"),
        ]));
        assert_eq!(
            platform.ok().map(|item| item.scope()),
            Some(MigrationScope::Platform)
        );

        let cell = load_migrator_from_sources(&source(&[
            ("EDTECH__ENVIRONMENT", "prd"),
            ("EDTECH__MIGRATION_SCOPE", "cell"),
            ("EDTECH__CELL_ID", "cell-001"),
        ]));
        assert_eq!(
            cell.as_ref().ok().map(super::MigratorRuntimeConfig::scope),
            Some(MigrationScope::Cell)
        );
        assert_eq!(
            cell.as_ref()
                .ok()
                .and_then(|item| item.cell_id())
                .map(ToString::to_string),
            Some(String::from("cell-001"))
        );

        assert!(matches!(
            load_migrator_from_sources(&source(&[
                ("EDTECH__ENVIRONMENT", "prd"),
                ("EDTECH__MIGRATION_SCOPE", "cell")
            ])),
            Err(RuntimeConfigError::MissingField("cell_id"))
        ));
        assert!(matches!(
            load_migrator_from_sources(&source(&[
                ("EDTECH__ENVIRONMENT", "prd"),
                ("EDTECH__MIGRATION_SCOPE", "platform"),
                ("EDTECH__CELL_ID", "cell-001")
            ])),
            Err(RuntimeConfigError::UnexpectedField("cell_id"))
        ));
    }

    #[test]
    fn secret_reference_debug_is_redacted() {
        let reference = SecretReference::new("secret-manager://path/to/value");
        assert_eq!(
            reference.as_ref().map(|value| format!("{value:?}")).ok(),
            Some(String::from("SecretReference([REDACTED])"))
        );
        assert!(!format!("{reference:?}").contains("path/to/value"));
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
        let config = load_platform_from_sources(ServiceKind::TenantRouter, &sources);
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

        let defaults = load_platform_from_sources(
            ServiceKind::PlatformApi,
            &source(&[("EDTECH__ENVIRONMENT", "dev")]),
        );
        assert_eq!(
            defaults
                .as_ref()
                .ok()
                .map(|item| item.base().log_filter().as_str()),
            Some("info")
        );
        assert_eq!(
            defaults
                .as_ref()
                .ok()
                .map(|item| item.base().shutdown_grace()),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn errors_keep_field_context_without_secret_material() {
        let secret = "do-not-echo-this-secret";
        let sources = source(&[
            ("EDTECH__ENVIRONMENT", "dev"),
            ("EDTECH__DATABASE_PASSWORD", secret),
        ]);
        let rendered = load_platform_from_sources(ServiceKind::PlatformApi, &sources)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(rendered.contains("database_password"));
        assert!(!rendered.contains(secret));

        let malformed = ConfigSources::new(BTreeMap::from([(
            String::from("EDTECH__ENVIRONMENT"),
            String::from("dev"),
        )]))
        .with_toml(format!("database_password = \"{secret}\""));
        let rendered = load_platform_from_sources(ServiceKind::PlatformApi, &malformed)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(!rendered.contains(secret));
    }
}
