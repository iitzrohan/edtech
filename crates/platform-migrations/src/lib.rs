//! Embedded, separately privileged Platform `PostgreSQL` migrations.
//!
//! This crate owns Platform migration authority validation, locking, execution, and safe
//! reporting. It must not provide runtime data access or be imported by Platform runtime
//! processes.

use std::fmt;

use postgres_runtime::{
    PostgresConnectionConfig, PostgresPool, ProviderError, ProviderErrorKind, connect,
    verify_migration_role, verify_server_version,
};
use secrecy::ExposeSecret;
use sqlx::{Executor, PgConnection, Row};
use thiserror::Error;

const MIGRATION_ROLE: &str = "edtech_platform_migrator";
const MIGRATION_ADVISORY_LOCK: i64 = 7_202_000_001;
const MIGRATION_FILES: &[&str] = &["0001_platform_foundation.sql"];
const SUPPORTED_CONTRACT_VERSION: u32 = 1;

/// Safe summary of a completed Platform migration run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlatformMigrationReport {
    latest_version: i64,
    applied_count: u64,
    contract_version: u32,
}

impl PlatformMigrationReport {
    /// Returns the latest applied migration version.
    #[must_use]
    pub const fn latest_version(self) -> i64 {
        self.latest_version
    }

    /// Returns the total number of applied migrations.
    #[must_use]
    pub const fn applied_count(self) -> u64 {
        self.applied_count
    }

    /// Returns the resulting Platform schema-contract version.
    #[must_use]
    pub const fn contract_version(self) -> u32 {
        self.contract_version
    }
}

/// Safe summary of a read-only Platform migration-authority check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlatformMigrationCheck {
    server_version: u32,
    contract_version: u32,
}

impl PlatformMigrationCheck {
    /// Returns `server_version_num`.
    #[must_use]
    pub const fn server_version(self) -> u32 {
        self.server_version
    }

    /// Returns the compatible schema-contract version.
    #[must_use]
    pub const fn contract_version(self) -> u32 {
        self.contract_version
    }
}

/// A safe Platform migration failure category.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PlatformMigrationErrorKind {
    /// Credential, connection, TLS, timeout, or server qualification failed.
    Provider,
    /// The bootstrap marker is absent or is not Platform authority.
    AuthorityMismatch,
    /// The connected role is not the dedicated Platform migrator.
    PrivilegeMismatch,
    /// Migration DDL or history management failed.
    Execution,
    /// The resulting Platform schema contract is absent or incompatible.
    ContractMismatch,
    /// Embedded migration filenames violate the monotonic convention.
    InvalidMigrationSet,
}

impl PlatformMigrationErrorKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Provider => "provider_failure",
            Self::AuthorityMismatch => "authority_mismatch",
            Self::PrivilegeMismatch => "privilege_mismatch",
            Self::Execution => "migration_execution_failure",
            Self::ContractMismatch => "schema_contract_mismatch",
            Self::InvalidMigrationSet => "invalid_migration_set",
        }
    }
}

/// A sanitized Platform migration error.
pub struct PlatformMigrationError {
    kind: PlatformMigrationErrorKind,
}

impl PlatformMigrationError {
    const fn new(kind: PlatformMigrationErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable safe error category.
    #[must_use]
    pub const fn kind(&self) -> PlatformMigrationErrorKind {
        self.kind
    }
}

impl fmt::Display for PlatformMigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "platform migration error: {}",
            self.kind.as_str()
        )
    }
}

impl fmt::Debug for PlatformMigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlatformMigrationError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl std::error::Error for PlatformMigrationError {}

/// A migration-filename validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("migration filenames must be monotonically numbered forward SQL files")]
pub struct MigrationFilenameError;

/// Runs the embedded Platform migrations once and closes the migration pool.
///
/// # Errors
///
/// Returns a sanitized failure. Authority and role checks occur before any application DDL.
pub async fn migrate(
    credential: &impl ExposeSecret<str>,
    config: &PostgresConnectionConfig,
) -> Result<PlatformMigrationReport, PlatformMigrationError> {
    validate_migration_filenames(MIGRATION_FILES).map_err(|_| {
        PlatformMigrationError::new(PlatformMigrationErrorKind::InvalidMigrationSet)
    })?;
    let pool = connect(credential, config)
        .await
        .map_err(map_provider_error)?;
    let result = migrate_with_pool(&pool).await;
    pool.close().await;
    result
}

/// Performs a read-only Platform migration-authority and schema-state check.
///
/// # Errors
///
/// Returns a sanitized failure and executes no DDL.
pub async fn check_database(
    credential: &impl ExposeSecret<str>,
    config: &PostgresConnectionConfig,
) -> Result<PlatformMigrationCheck, PlatformMigrationError> {
    let pool = connect(credential, config)
        .await
        .map_err(map_provider_error)?;
    let result = check_with_pool(&pool).await;
    pool.close().await;
    result
}

async fn migrate_with_pool(
    pool: &PostgresPool,
) -> Result<PlatformMigrationReport, PlatformMigrationError> {
    verify_pre_ddl(pool).await?;
    let mut connection = pool
        .sqlx_pool()
        .acquire()
        .await
        .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::Execution))?;
    sqlx::query("SELECT pg_catalog.pg_advisory_lock($1)")
        .bind(MIGRATION_ADVISORY_LOCK)
        .execute(&mut *connection)
        .await
        .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::Execution))?;
    prepare_migration_schema(&mut connection).await?;
    sqlx::query(
        "SELECT pg_catalog.set_config('search_path', 'edtech_migrations,pg_catalog', false)",
    )
    .execute(&mut *connection)
    .await
    .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::Execution))?;

    let mut migrator = sqlx::migrate!("./migrations");
    migrator.dangerous_set_table_name("edtech_migrations._sqlx_migrations");
    migrator
        .run(&mut *connection)
        .await
        .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::Execution))?;
    connection
        .execute(
            "REVOKE ALL ON ALL TABLES IN SCHEMA edtech_migrations FROM PUBLIC, \
             edtech_platform_api, edtech_platform_worker",
        )
        .await
        .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::Execution))?;

    let (latest_version, applied_count) = sqlx::query_as::<_, (i64, i64)>(
        "SELECT COALESCE(pg_catalog.max(version), 0), pg_catalog.count(*) \
             FROM edtech_migrations._sqlx_migrations WHERE success",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::Execution))?;
    sqlx::query("SELECT pg_catalog.pg_advisory_unlock($1)")
        .bind(MIGRATION_ADVISORY_LOCK)
        .execute(&mut *connection)
        .await
        .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::Execution))?;
    drop(connection);
    let contract_version = verify_contract(pool).await?;
    let applied_count = u64::try_from(applied_count)
        .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::Execution))?;
    Ok(PlatformMigrationReport {
        latest_version,
        applied_count,
        contract_version,
    })
}

async fn check_with_pool(
    pool: &PostgresPool,
) -> Result<PlatformMigrationCheck, PlatformMigrationError> {
    let server_version = verify_pre_ddl(pool).await?;
    let contract_version = verify_contract(pool).await?;
    Ok(PlatformMigrationCheck {
        server_version,
        contract_version,
    })
}

async fn verify_pre_ddl(pool: &PostgresPool) -> Result<u32, PlatformMigrationError> {
    let server_version = verify_server_version(pool)
        .await
        .map_err(map_provider_error)?;
    verify_platform_marker(pool).await?;
    verify_migration_role(pool, MIGRATION_ROLE)
        .await
        .map_err(map_provider_error)?;
    Ok(server_version)
}

async fn verify_platform_marker(pool: &PostgresPool) -> Result<(), PlatformMigrationError> {
    let row = sqlx::query(
        "SELECT authority_kind, cell_id FROM edtech_bootstrap.authority_identity \
         WHERE singleton",
    )
    .fetch_optional(pool.sqlx_pool())
    .await
    .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::AuthorityMismatch))?;
    let matches = row.is_some_and(|row| {
        row.get::<String, _>("authority_kind") == "platform"
            && row.get::<Option<String>, _>("cell_id").is_none()
    });
    if !matches {
        return Err(PlatformMigrationError::new(
            PlatformMigrationErrorKind::AuthorityMismatch,
        ));
    }
    Ok(())
}

async fn prepare_migration_schema(
    connection: &mut PgConnection,
) -> Result<(), PlatformMigrationError> {
    connection
        .execute(
            "CREATE SCHEMA IF NOT EXISTS edtech_migrations \
             AUTHORIZATION edtech_platform_migrator",
        )
        .await
        .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::Execution))?;
    connection
        .execute(
            "REVOKE ALL ON SCHEMA edtech_migrations FROM PUBLIC, \
             edtech_platform_api, edtech_platform_worker",
        )
        .await
        .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::Execution))?;
    let owned = sqlx::query_scalar::<_, bool>(
        "SELECT n.nspowner = r.oid FROM pg_catalog.pg_namespace AS n \
         JOIN pg_catalog.pg_roles AS r ON r.rolname = 'edtech_platform_migrator' \
         WHERE n.nspname = 'edtech_migrations'",
    )
    .fetch_optional(connection)
    .await
    .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::Execution))?;
    if owned != Some(true) {
        return Err(PlatformMigrationError::new(
            PlatformMigrationErrorKind::PrivilegeMismatch,
        ));
    }
    Ok(())
}

async fn verify_contract(pool: &PostgresPool) -> Result<u32, PlatformMigrationError> {
    let row = sqlx::query(
        "SELECT contract_name, contract_version FROM platform_control.schema_contract \
         WHERE singleton",
    )
    .fetch_optional(pool.sqlx_pool())
    .await
    .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::ContractMismatch))?;
    let Some(row) = row else {
        return Err(PlatformMigrationError::new(
            PlatformMigrationErrorKind::ContractMismatch,
        ));
    };
    let name = row.get::<String, _>("contract_name");
    let version = row.get::<i32, _>("contract_version");
    if name != "platform" || u32::try_from(version).ok() != Some(SUPPORTED_CONTRACT_VERSION) {
        return Err(PlatformMigrationError::new(
            PlatformMigrationErrorKind::ContractMismatch,
        ));
    }
    u32::try_from(version)
        .map_err(|_| PlatformMigrationError::new(PlatformMigrationErrorKind::ContractMismatch))
}

fn map_provider_error(error: ProviderError) -> PlatformMigrationError {
    let kind = match error.kind() {
        ProviderErrorKind::AuthorityMismatch => PlatformMigrationErrorKind::AuthorityMismatch,
        ProviderErrorKind::PrivilegeMismatch => PlatformMigrationErrorKind::PrivilegeMismatch,
        _ => PlatformMigrationErrorKind::Provider,
    };
    drop(error);
    PlatformMigrationError::new(kind)
}

/// Validates monotonically increasing, forward-only `.sql` migration filenames.
///
/// # Errors
///
/// Returns [`MigrationFilenameError`] for malformed, duplicate, or decreasing versions.
pub fn validate_migration_filenames(files: &[&str]) -> Result<(), MigrationFilenameError> {
    let mut previous = None;
    for file in files {
        let (prefix, remainder) = file.split_once('_').ok_or(MigrationFilenameError)?;
        let extension = std::path::Path::new(remainder)
            .extension()
            .and_then(std::ffi::OsStr::to_str);
        if remainder.is_empty() || extension != Some("sql") || remainder.contains(".down.") {
            return Err(MigrationFilenameError);
        }
        let version = prefix.parse::<u64>().map_err(|_| MigrationFilenameError)?;
        if version == 0 || previous.is_some_and(|prior| version <= prior) {
            return Err(MigrationFilenameError);
        }
        previous = Some(version);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{MIGRATION_FILES, validate_migration_filenames};

    #[test]
    fn embedded_platform_migration_filenames_are_monotonic() {
        assert!(validate_migration_filenames(MIGRATION_FILES).is_ok());
        assert!(validate_migration_filenames(&["0002_second.sql", "0001_first.sql"]).is_err());
        assert!(validate_migration_filenames(&["0001_first.down.sql"]).is_err());
    }
}
