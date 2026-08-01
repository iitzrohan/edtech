//! Shared `SQLx`/`PostgreSQL` mechanics for concrete database provider adapters.
//!
//! This crate owns sanitized connection parsing, TLS selection, bounded pooling, session setup,
//! server-version qualification, and generic role checks. It must not know Platform or Cell
//! tables, tenant policies, application use cases, migration directories, deployment-environment
//! enums, or secret-reference providers.

use std::{fmt, str::FromStr, time::Duration};

use secrecy::ExposeSecret;
use sqlx::{
    ConnectOptions, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};
use thiserror::Error;

const MIN_SUPPORTED_SERVER_VERSION: i32 = 180_004;
const MAX_SUPPORTED_SERVER_VERSION_EXCLUSIVE: i32 = 190_000;
const MAX_APPLICATION_NAME_BYTES: usize = 128;

/// `PostgreSQL` TLS behavior selected by validated composition configuration.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PostgresTlsMode {
    /// Disable TLS without fallback.
    Disable,
    /// Require TLS with certificate and hostname verification.
    VerifyFull,
}

/// Validated bounds for a `PostgreSQL` connection pool and initialized sessions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolSettings {
    max_connections: u32,
    min_connections: u32,
    acquire_timeout: Duration,
    connect_timeout: Duration,
    statement_timeout: Duration,
    lock_timeout: Duration,
    idle_in_transaction_timeout: Duration,
    max_lifetime: Duration,
}

impl PoolSettings {
    /// Constructs pool settings after checking every non-zero bound.
    ///
    /// # Errors
    ///
    /// Returns [`PoolSettingsError`] when a pool or timeout bound is invalid.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_connections: u32,
        min_connections: u32,
        acquire_timeout: Duration,
        connect_timeout: Duration,
        statement_timeout: Duration,
        lock_timeout: Duration,
        idle_in_transaction_timeout: Duration,
        max_lifetime: Duration,
    ) -> Result<Self, PoolSettingsError> {
        if max_connections == 0 {
            return Err(PoolSettingsError::ZeroMaxConnections);
        }
        if min_connections > max_connections {
            return Err(PoolSettingsError::MinimumExceedsMaximum);
        }
        if [
            acquire_timeout,
            connect_timeout,
            statement_timeout,
            lock_timeout,
            idle_in_transaction_timeout,
            max_lifetime,
        ]
        .contains(&Duration::ZERO)
        {
            return Err(PoolSettingsError::ZeroTimeout);
        }
        Ok(Self {
            max_connections,
            min_connections,
            acquire_timeout,
            connect_timeout,
            statement_timeout,
            lock_timeout,
            idle_in_transaction_timeout,
            max_lifetime,
        })
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
}

/// A pool-bound validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PoolSettingsError {
    /// The maximum pool size is zero.
    #[error("maximum connections must be non-zero")]
    ZeroMaxConnections,
    /// The minimum pool size exceeds the maximum.
    #[error("minimum connections must not exceed maximum connections")]
    MinimumExceedsMaximum,
    /// A timeout is zero and therefore unbounded or immediately expiring.
    #[error("database timeouts must be non-zero")]
    ZeroTimeout,
}

/// A bounded, safe `PostgreSQL` application name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationName(String);

impl ApplicationName {
    /// Builds a fixed application name from trusted composition components.
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationNameError`] for empty, oversized, or unsafe components.
    pub fn compose(
        service: &str,
        environment: &str,
        cell_id: Option<&str>,
    ) -> Result<Self, ApplicationNameError> {
        if !valid_component(service) || !valid_component(environment) {
            return Err(ApplicationNameError);
        }
        if cell_id.is_some_and(|value| !valid_component(value)) {
            return Err(ApplicationNameError);
        }
        let mut value = format!("edtech-{service}-{environment}");
        if let Some(cell_id) = cell_id {
            value.push('-');
            value.push_str(cell_id);
        }
        if value.len() > MAX_APPLICATION_NAME_BYTES {
            return Err(ApplicationNameError);
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

fn valid_component(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// An invalid fixed application-name component.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("database application name is invalid")]
pub struct ApplicationNameError;

/// Complete provider connection settings without credential material.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresConnectionConfig {
    tls_mode: PostgresTlsMode,
    application_name: ApplicationName,
    pool: PoolSettings,
}

/// A redacted credential that can be exposed only at the connection-provider boundary.
pub trait DatabaseCredential {
    /// Exposes resolved credential material to the `PostgreSQL` connection parser.
    #[doc(hidden)]
    fn expose_for_connection(&self) -> &str;
}

impl<T> DatabaseCredential for T
where
    T: ExposeSecret<str> + ?Sized,
{
    fn expose_for_connection(&self) -> &str {
        self.expose_secret()
    }
}

impl PostgresConnectionConfig {
    /// Constructs provider settings from independently validated values.
    #[must_use]
    pub const fn new(
        tls_mode: PostgresTlsMode,
        application_name: ApplicationName,
        pool: PoolSettings,
    ) -> Self {
        Self {
            tls_mode,
            application_name,
            pool,
        }
    }
}

/// An opaque pool retained within concrete `PostgreSQL` provider and migration crates.
#[derive(Clone)]
pub struct PostgresPool(PgPool);

impl fmt::Debug for PostgresPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PostgresPool([OPAQUE])")
    }
}

impl PostgresPool {
    /// Borrows the pool for implementation inside a `PostgreSQL` provider or migration crate.
    ///
    /// Architecture enforcement prevents composition roots and application/domain crates from
    /// receiving this type or depending directly on `SQLx`.
    #[must_use]
    pub fn sqlx_pool(&self) -> &PgPool {
        &self.0
    }

    /// Closes all pooled connections.
    pub async fn close(&self) {
        self.0.close().await;
    }
}

/// Safe provider error categories suitable for logs and process output.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProviderErrorKind {
    /// Credential syntax or authentication is invalid.
    InvalidCredential,
    /// A connection could not be established or retained.
    Connection,
    /// TLS negotiation or verification failed.
    Tls,
    /// The server version is outside the qualified range.
    UnsupportedServer,
    /// The connected role violates the required profile.
    PrivilegeMismatch,
    /// A bounded provider operation timed out.
    Timeout,
    /// The connected database authority is not the expected authority.
    AuthorityMismatch,
    /// The required schema contract is missing or unsupported.
    SchemaContractMismatch,
    /// A bounded database operation failed.
    Database,
}

impl ProviderErrorKind {
    /// Returns a stable safe category label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidCredential => "invalid_credential",
            Self::Connection => "connection_failure",
            Self::Tls => "tls_failure",
            Self::UnsupportedServer => "unsupported_server",
            Self::PrivilegeMismatch => "privilege_mismatch",
            Self::Timeout => "timeout",
            Self::AuthorityMismatch => "authority_mismatch",
            Self::SchemaContractMismatch => "schema_contract_mismatch",
            Self::Database => "database_failure",
        }
    }
}

/// A sanitized provider failure that retains an internal `SQLx` error without exposing it.
pub struct ProviderError {
    kind: ProviderErrorKind,
    internal: Option<sqlx::Error>,
}

impl ProviderError {
    /// Constructs a category-only provider failure.
    #[must_use]
    pub const fn category(kind: ProviderErrorKind) -> Self {
        Self {
            kind,
            internal: None,
        }
    }

    /// Sanitizes an `SQLx` error and retains it only for internal ownership and drop.
    #[must_use]
    pub fn from_sqlx(error: sqlx::Error) -> Self {
        let kind = classify_sqlx_error(&error);
        Self {
            kind,
            internal: Some(error),
        }
    }

    /// Returns the stable safe category.
    #[must_use]
    pub const fn kind(&self) -> ProviderErrorKind {
        self.kind
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "postgres provider error: {}", self.kind.as_str())
    }
}

impl fmt::Debug for ProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl std::error::Error for ProviderError {}

impl Drop for ProviderError {
    fn drop(&mut self) {
        let _ = self.internal.take();
    }
}

fn classify_sqlx_error(error: &sqlx::Error) -> ProviderErrorKind {
    match error {
        sqlx::Error::Configuration(_) | sqlx::Error::InvalidArgument(_) => {
            ProviderErrorKind::InvalidCredential
        }
        sqlx::Error::Tls(_) => ProviderErrorKind::Tls,
        sqlx::Error::PoolTimedOut => ProviderErrorKind::Timeout,
        sqlx::Error::Database(database_error)
            if database_error
                .code()
                .is_some_and(|code| code.starts_with("28")) =>
        {
            ProviderErrorKind::InvalidCredential
        }
        sqlx::Error::Io(_) | sqlx::Error::PoolClosed | sqlx::Error::WorkerCrashed => {
            ProviderErrorKind::Connection
        }
        _ => ProviderErrorKind::Database,
    }
}

/// Parses a resolved credential, applies fixed TLS/session behavior, and opens a bounded pool.
///
/// # Errors
///
/// Returns a sanitized [`ProviderError`] without exposing credential or connection details.
pub async fn connect(
    credential: &impl DatabaseCredential,
    config: &PostgresConnectionConfig,
) -> Result<PostgresPool, ProviderError> {
    let options = PgConnectOptions::from_str(credential.expose_for_connection())
        .map_err(ProviderError::from_sqlx)?
        .ssl_mode(match config.tls_mode {
            PostgresTlsMode::Disable => PgSslMode::Disable,
            PostgresTlsMode::VerifyFull => PgSslMode::VerifyFull,
        })
        .application_name(config.application_name.as_str())
        .disable_statement_logging();

    let statement_timeout = milliseconds_text(config.pool.statement_timeout);
    let lock_timeout = milliseconds_text(config.pool.lock_timeout);
    let idle_timeout = milliseconds_text(config.pool.idle_in_transaction_timeout);
    let effective_acquire_timeout = config.pool.acquire_timeout.min(config.pool.connect_timeout);
    let pool = PgPoolOptions::new()
        .max_connections(config.pool.max_connections)
        .min_connections(config.pool.min_connections)
        .acquire_timeout(effective_acquire_timeout)
        .max_lifetime(Some(config.pool.max_lifetime))
        .after_connect(move |connection, _metadata| {
            let statement_timeout = statement_timeout.clone();
            let lock_timeout = lock_timeout.clone();
            let idle_timeout = idle_timeout.clone();
            Box::pin(async move {
                sqlx::query(
                    "SELECT pg_catalog.set_config('TimeZone', 'UTC', false), \
                     pg_catalog.set_config('row_security', 'on', false), \
                     pg_catalog.set_config('statement_timeout', $1, false), \
                     pg_catalog.set_config('lock_timeout', $2, false), \
                     pg_catalog.set_config('idle_in_transaction_session_timeout', $3, false), \
                     pg_catalog.set_config('search_path', 'pg_catalog', false)",
                )
                .bind(statement_timeout)
                .bind(lock_timeout)
                .bind(idle_timeout)
                .execute(connection)
                .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .map_err(ProviderError::from_sqlx)?;
    Ok(PostgresPool(pool))
}

fn milliseconds_text(duration: Duration) -> String {
    format!("{}ms", duration.as_millis())
}

/// Verifies that the connected server is `PostgreSQL` 18.4 or newer within major version 18.
///
/// # Errors
///
/// Returns a sanitized failure for query, parsing, or unsupported-version results.
pub async fn verify_server_version(pool: &PostgresPool) -> Result<u32, ProviderError> {
    let version_text =
        sqlx::query_scalar::<_, String>("SELECT pg_catalog.current_setting('server_version_num')")
            .fetch_one(pool.sqlx_pool())
            .await
            .map_err(ProviderError::from_sqlx)?;
    let version = version_text
        .parse::<i32>()
        .map_err(|_| ProviderError::category(ProviderErrorKind::UnsupportedServer))?;
    if !(MIN_SUPPORTED_SERVER_VERSION..MAX_SUPPORTED_SERVER_VERSION_EXCLUSIVE).contains(&version) {
        return Err(ProviderError::category(
            ProviderErrorKind::UnsupportedServer,
        ));
    }
    u32::try_from(version)
        .map_err(|_| ProviderError::category(ProviderErrorKind::UnsupportedServer))
}

/// Verifies a connected runtime role against generic least-privilege invariants.
///
/// # Errors
///
/// Returns a sanitized privilege mismatch or provider failure.
pub async fn verify_runtime_role(
    pool: &PostgresPool,
    expected_role: &str,
    migration_role: &str,
    application_schemas: &[&str],
) -> Result<(), ProviderError> {
    let schema_names = application_schemas
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    let row = sqlx::query(
        "SELECT r.rolname = $1 AS expected_role, r.rolsuper, r.rolbypassrls, r.rolcreatedb, \
         r.rolcreaterole, r.rolreplication, \
         pg_catalog.has_database_privilege(CURRENT_USER, \
             pg_catalog.current_database(), 'CREATE') AS database_create, \
         pg_catalog.has_database_privilege(CURRENT_USER, \
             pg_catalog.current_database(), 'TEMP') AS database_temp, \
         EXISTS (SELECT 1 FROM pg_catalog.pg_namespace AS n \
             WHERE n.nspname = ANY($2::text[]) AND n.nspowner = r.oid) AS owns_schema, \
         EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c \
             JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
             WHERE n.nspname = ANY($2::text[]) AND c.relowner = r.oid) AS owns_table, \
         EXISTS (SELECT 1 FROM pg_catalog.pg_namespace AS n \
             WHERE n.nspname = ANY($2::text[]) \
             AND pg_catalog.has_schema_privilege(r.oid, n.oid, 'CREATE')) AS schema_create, \
         COALESCE(pg_catalog.pg_has_role(r.oid, pg_catalog.to_regrole($3), 'MEMBER'), false) \
             AS migration_member, \
         COALESCE(pg_catalog.pg_has_role(r.oid, pg_catalog.to_regrole($3), 'SET'), false) \
             AS can_set_migration_role \
         FROM pg_catalog.pg_roles AS r WHERE r.rolname = CURRENT_USER",
    )
    .bind(expected_role)
    .bind(schema_names)
    .bind(migration_role)
    .fetch_one(pool.sqlx_pool())
    .await
    .map_err(ProviderError::from_sqlx)?;

    let unsafe_profile = !row.get::<bool, _>("expected_role")
        || row.get::<bool, _>("rolsuper")
        || row.get::<bool, _>("rolbypassrls")
        || row.get::<bool, _>("rolcreatedb")
        || row.get::<bool, _>("rolcreaterole")
        || row.get::<bool, _>("rolreplication")
        || row.get::<bool, _>("database_create")
        || row.get::<bool, _>("database_temp")
        || row.get::<bool, _>("owns_schema")
        || row.get::<bool, _>("owns_table")
        || row.get::<bool, _>("schema_create")
        || row.get::<bool, _>("migration_member")
        || row.get::<bool, _>("can_set_migration_role");
    if unsafe_profile {
        return Err(ProviderError::category(
            ProviderErrorKind::PrivilegeMismatch,
        ));
    }
    Ok(())
}

/// Verifies a connected migration role before any application DDL is attempted.
///
/// # Errors
///
/// Returns a sanitized privilege mismatch or provider failure.
pub async fn verify_migration_role(
    pool: &PostgresPool,
    expected_role: &str,
) -> Result<(), ProviderError> {
    let row = sqlx::query(
        "SELECT r.rolname = $1 AS expected_role, r.rolsuper, r.rolbypassrls, r.rolcreatedb, \
         r.rolcreaterole, r.rolreplication, \
         pg_catalog.has_database_privilege(CURRENT_USER, \
             pg_catalog.current_database(), 'CREATE') AS database_create \
         FROM pg_catalog.pg_roles AS r WHERE r.rolname = CURRENT_USER",
    )
    .bind(expected_role)
    .fetch_one(pool.sqlx_pool())
    .await
    .map_err(ProviderError::from_sqlx)?;
    let unsafe_profile = !row.get::<bool, _>("expected_role")
        || row.get::<bool, _>("rolsuper")
        || row.get::<bool, _>("rolbypassrls")
        || row.get::<bool, _>("rolcreatedb")
        || row.get::<bool, _>("rolcreaterole")
        || row.get::<bool, _>("rolreplication")
        || !row.get::<bool, _>("database_create");
    if unsafe_profile {
        return Err(ProviderError::category(
            ProviderErrorKind::PrivilegeMismatch,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        ApplicationName, PoolSettings, PoolSettingsError, ProviderError, ProviderErrorKind,
    };

    #[test]
    fn pool_bounds_reject_zero_and_inverted_values() {
        let duration = Duration::from_secs(1);
        assert!(matches!(
            PoolSettings::new(
                0, 0, duration, duration, duration, duration, duration, duration
            ),
            Err(PoolSettingsError::ZeroMaxConnections)
        ));
        assert!(matches!(
            PoolSettings::new(
                1, 2, duration, duration, duration, duration, duration, duration
            ),
            Err(PoolSettingsError::MinimumExceedsMaximum)
        ));
        assert!(matches!(
            PoolSettings::new(
                1,
                0,
                Duration::ZERO,
                duration,
                duration,
                duration,
                duration,
                duration
            ),
            Err(PoolSettingsError::ZeroTimeout)
        ));
    }

    #[test]
    fn application_name_is_bounded_and_uses_safe_components() {
        let valid = ApplicationName::compose("cell-api", "dev", Some("cell-001"));
        assert_eq!(
            valid.as_ref().ok().map(super::ApplicationName::as_str),
            Some("edtech-cell-api-dev-cell-001")
        );
        assert!(ApplicationName::compose("Cell API", "dev", None).is_err());
        assert!(ApplicationName::compose("cell-api", "dev/other", None).is_err());
    }

    #[test]
    fn provider_errors_render_only_safe_categories() {
        let sentinel = "unique-database-password-sentinel";
        let error = ProviderError {
            kind: ProviderErrorKind::Connection,
            internal: Some(sqlx::Error::Protocol(String::from(sentinel))),
        };
        let display = error.to_string();
        let debug = format!("{error:?}");
        assert_eq!(display, "postgres provider error: connection_failure");
        assert!(!display.contains(sentinel));
        assert!(!debug.contains(sentinel));
    }
}
