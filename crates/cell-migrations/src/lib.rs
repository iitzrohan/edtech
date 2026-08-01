//! Embedded, separately privileged Cell `PostgreSQL` migrations.
//!
//! This crate owns Cell migration authority validation, logical Cell binding, locking, execution,
//! and safe reporting. It must not provide runtime tenant access or be imported by Cell runtime
//! processes.

use std::fmt;

use postgres_runtime::{
    PostgresConnectionConfig, PostgresPool, ProviderError, ProviderErrorKind, connect,
    verify_migration_role, verify_server_version,
};
use secrecy::ExposeSecret;
use sqlx::{Executor, PgConnection, Row};
use tenancy_domain::CellId;
use thiserror::Error;

const MIGRATION_ROLE: &str = "edtech_cell_migrator";
const MIGRATION_ADVISORY_LOCK: i64 = 7_202_000_002;
const MIGRATION_FILES: &[&str] = &["0001_cell_foundation.sql"];
const SUPPORTED_CONTRACT_VERSION: u32 = 1;

/// Safe summary of a completed Cell migration run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CellMigrationReport {
    latest_version: i64,
    applied_count: u64,
    contract_version: u32,
}

impl CellMigrationReport {
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

    /// Returns the resulting Cell schema-contract version.
    #[must_use]
    pub const fn contract_version(self) -> u32 {
        self.contract_version
    }
}

/// Safe summary of a read-only Cell migration-authority check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CellMigrationCheck {
    server_version: u32,
    contract_version: u32,
}

impl CellMigrationCheck {
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

/// A safe Cell migration failure category.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CellMigrationErrorKind {
    /// Credential, connection, TLS, timeout, or server qualification failed.
    Provider,
    /// The bootstrap marker is absent, is not Cell authority, or has another `cell_id`.
    AuthorityMismatch,
    /// The connected role is not the dedicated Cell migrator.
    PrivilegeMismatch,
    /// Migration DDL or history management failed.
    Execution,
    /// The resulting Cell schema contract is absent or incompatible.
    ContractMismatch,
    /// Embedded migration filenames violate the monotonic convention.
    InvalidMigrationSet,
}

impl CellMigrationErrorKind {
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

/// A sanitized Cell migration error.
pub struct CellMigrationError {
    kind: CellMigrationErrorKind,
}

impl CellMigrationError {
    const fn new(kind: CellMigrationErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable safe error category.
    #[must_use]
    pub const fn kind(&self) -> CellMigrationErrorKind {
        self.kind
    }
}

impl fmt::Display for CellMigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "cell migration error: {}", self.kind.as_str())
    }
}

impl fmt::Debug for CellMigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CellMigrationError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl std::error::Error for CellMigrationError {}

/// A migration-filename validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("migration filenames must be monotonically numbered forward SQL files")]
pub struct MigrationFilenameError;

/// Runs the embedded Cell migrations once and closes the migration pool.
///
/// # Errors
///
/// Returns a sanitized failure. Authority, Cell identity, and role checks occur before DDL.
pub async fn migrate(
    credential: &impl ExposeSecret<str>,
    config: &PostgresConnectionConfig,
    cell_id: &CellId,
) -> Result<CellMigrationReport, CellMigrationError> {
    validate_migration_filenames(MIGRATION_FILES)
        .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::InvalidMigrationSet))?;
    let pool = connect(credential, config)
        .await
        .map_err(map_provider_error)?;
    let result = migrate_with_pool(&pool, cell_id).await;
    pool.close().await;
    result
}

/// Performs a read-only Cell migration-authority and schema-state check.
///
/// # Errors
///
/// Returns a sanitized failure and executes no DDL.
pub async fn check_database(
    credential: &impl ExposeSecret<str>,
    config: &PostgresConnectionConfig,
    cell_id: &CellId,
) -> Result<CellMigrationCheck, CellMigrationError> {
    let pool = connect(credential, config)
        .await
        .map_err(map_provider_error)?;
    let result = check_with_pool(&pool, cell_id).await;
    pool.close().await;
    result
}

async fn migrate_with_pool(
    pool: &PostgresPool,
    cell_id: &CellId,
) -> Result<CellMigrationReport, CellMigrationError> {
    verify_pre_ddl(pool, cell_id).await?;
    let mut connection = pool
        .sqlx_pool()
        .acquire()
        .await
        .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::Execution))?;
    sqlx::query("SELECT pg_catalog.pg_advisory_lock($1)")
        .bind(MIGRATION_ADVISORY_LOCK)
        .execute(&mut *connection)
        .await
        .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::Execution))?;
    prepare_migration_schema(&mut connection).await?;
    sqlx::query(
        "SELECT pg_catalog.set_config('search_path', 'edtech_migrations,pg_catalog', false)",
    )
    .execute(&mut *connection)
    .await
    .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::Execution))?;

    let mut migrator = sqlx::migrate!("./migrations");
    migrator.dangerous_set_table_name("edtech_migrations._sqlx_migrations");
    migrator
        .run(&mut *connection)
        .await
        .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::Execution))?;
    connection
        .execute(
            "REVOKE ALL ON ALL TABLES IN SCHEMA edtech_migrations FROM PUBLIC, \
             edtech_cell_api, edtech_cell_worker",
        )
        .await
        .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::Execution))?;

    let (latest_version, applied_count) = sqlx::query_as::<_, (i64, i64)>(
        "SELECT COALESCE(pg_catalog.max(version), 0), pg_catalog.count(*) \
             FROM edtech_migrations._sqlx_migrations WHERE success",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::Execution))?;
    sqlx::query("SELECT pg_catalog.pg_advisory_unlock($1)")
        .bind(MIGRATION_ADVISORY_LOCK)
        .execute(&mut *connection)
        .await
        .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::Execution))?;
    drop(connection);
    let contract_version = verify_contract(pool).await?;
    let applied_count = u64::try_from(applied_count)
        .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::Execution))?;
    Ok(CellMigrationReport {
        latest_version,
        applied_count,
        contract_version,
    })
}

async fn check_with_pool(
    pool: &PostgresPool,
    cell_id: &CellId,
) -> Result<CellMigrationCheck, CellMigrationError> {
    let server_version = verify_pre_ddl(pool, cell_id).await?;
    let contract_version = verify_contract(pool).await?;
    Ok(CellMigrationCheck {
        server_version,
        contract_version,
    })
}

async fn verify_pre_ddl(pool: &PostgresPool, cell_id: &CellId) -> Result<u32, CellMigrationError> {
    let server_version = verify_server_version(pool)
        .await
        .map_err(map_provider_error)?;
    verify_cell_marker(pool, cell_id).await?;
    verify_migration_role(pool, MIGRATION_ROLE)
        .await
        .map_err(map_provider_error)?;
    Ok(server_version)
}

async fn verify_cell_marker(
    pool: &PostgresPool,
    cell_id: &CellId,
) -> Result<(), CellMigrationError> {
    let row = sqlx::query(
        "SELECT authority_kind, cell_id FROM edtech_bootstrap.authority_identity \
         WHERE singleton",
    )
    .fetch_optional(pool.sqlx_pool())
    .await
    .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::AuthorityMismatch))?;
    let matches = row.is_some_and(|row| {
        row.get::<String, _>("authority_kind") == "cell"
            && row.get::<Option<String>, _>("cell_id").as_deref() == Some(cell_id.as_str())
    });
    if !matches {
        return Err(CellMigrationError::new(
            CellMigrationErrorKind::AuthorityMismatch,
        ));
    }
    Ok(())
}

async fn prepare_migration_schema(connection: &mut PgConnection) -> Result<(), CellMigrationError> {
    connection
        .execute("CREATE SCHEMA IF NOT EXISTS edtech_migrations AUTHORIZATION edtech_cell_migrator")
        .await
        .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::Execution))?;
    connection
        .execute(
            "REVOKE ALL ON SCHEMA edtech_migrations FROM PUBLIC, \
             edtech_cell_api, edtech_cell_worker",
        )
        .await
        .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::Execution))?;
    let owned = sqlx::query_scalar::<_, bool>(
        "SELECT n.nspowner = r.oid FROM pg_catalog.pg_namespace AS n \
         JOIN pg_catalog.pg_roles AS r ON r.rolname = 'edtech_cell_migrator' \
         WHERE n.nspname = 'edtech_migrations'",
    )
    .fetch_optional(connection)
    .await
    .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::Execution))?;
    if owned != Some(true) {
        return Err(CellMigrationError::new(
            CellMigrationErrorKind::PrivilegeMismatch,
        ));
    }
    Ok(())
}

async fn verify_contract(pool: &PostgresPool) -> Result<u32, CellMigrationError> {
    let row = sqlx::query(
        "SELECT contract_name, contract_version FROM cell_control.schema_contract WHERE singleton",
    )
    .fetch_optional(pool.sqlx_pool())
    .await
    .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::ContractMismatch))?;
    let Some(row) = row else {
        return Err(CellMigrationError::new(
            CellMigrationErrorKind::ContractMismatch,
        ));
    };
    let name = row.get::<String, _>("contract_name");
    let version = row.get::<i32, _>("contract_version");
    if name != "cell" || u32::try_from(version).ok() != Some(SUPPORTED_CONTRACT_VERSION) {
        return Err(CellMigrationError::new(
            CellMigrationErrorKind::ContractMismatch,
        ));
    }
    u32::try_from(version)
        .map_err(|_| CellMigrationError::new(CellMigrationErrorKind::ContractMismatch))
}

fn map_provider_error(error: ProviderError) -> CellMigrationError {
    let kind = match error.kind() {
        ProviderErrorKind::AuthorityMismatch => CellMigrationErrorKind::AuthorityMismatch,
        ProviderErrorKind::PrivilegeMismatch => CellMigrationErrorKind::PrivilegeMismatch,
        _ => CellMigrationErrorKind::Provider,
    };
    drop(error);
    CellMigrationError::new(kind)
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
    fn embedded_cell_migration_filenames_are_monotonic() {
        assert!(validate_migration_filenames(MIGRATION_FILES).is_ok());
        assert!(validate_migration_filenames(&["0001_first.sql", "0001_duplicate.sql"]).is_err());
        assert!(validate_migration_filenames(&["migration.sql"]).is_err());
    }
}
