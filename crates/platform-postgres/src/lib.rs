//! Runtime `PostgreSQL` adapter for the Platform database authority.
//!
//! This crate verifies the Platform bootstrap marker, qualified server, runtime role, and schema
//! contract behind an opaque handle. It must not contain DDL, migrations, Cell dependencies,
//! application workflows, configuration loading, or public `SQLx` types.

use postgres_runtime::{
    DatabaseCredential, PostgresConnectionConfig, PostgresPool, ProviderError, ProviderErrorKind,
    connect, verify_runtime_role, verify_server_version,
};
use sqlx::Row;

const MIGRATION_ROLE: &str = "edtech_platform_migrator";
const SUPPORTED_CONTRACT_VERSION: u32 = 1;
const PLATFORM_SCHEMAS: &[&str] = &["edtech_bootstrap", "edtech_migrations", "platform_control"];

/// The separately scoped Platform runtime role expected on a connection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PlatformRuntimeRole {
    /// Platform API runtime role.
    Api,
    /// Platform worker runtime role.
    Worker,
}

impl PlatformRuntimeRole {
    const fn database_role(self) -> &'static str {
        match self {
            Self::Api => "edtech_platform_api",
            Self::Worker => "edtech_platform_worker",
        }
    }
}

/// Safe information proven when a Platform database becomes ready.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlatformDatabaseCheck {
    server_version: u32,
    contract_version: u32,
}

impl PlatformDatabaseCheck {
    /// Returns `server_version_num`.
    #[must_use]
    pub const fn server_version(self) -> u32 {
        self.server_version
    }

    /// Returns the supported Platform schema-contract version.
    #[must_use]
    pub const fn contract_version(self) -> u32 {
        self.contract_version
    }
}

/// Opaque, verified Platform runtime database handle.
pub struct PlatformDatabase {
    pool: PostgresPool,
    check: PlatformDatabaseCheck,
}

impl std::fmt::Debug for PlatformDatabase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlatformDatabase")
            .field("check", &self.check)
            .finish_non_exhaustive()
    }
}

impl PlatformDatabase {
    /// Connects and fails closed unless authority, server, role, and contract all match.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`ProviderError`] without connection or credential details.
    pub async fn connect(
        credential: &impl DatabaseCredential,
        config: &PostgresConnectionConfig,
        role: PlatformRuntimeRole,
    ) -> Result<Self, ProviderError> {
        let pool = connect(credential, config).await?;
        match verify_ready(&pool, role).await {
            Ok(check) => Ok(Self { pool, check }),
            Err(error) => {
                pool.close().await;
                Err(error)
            }
        }
    }

    /// Returns the safe readiness facts established at connection time.
    #[must_use]
    pub const fn check(&self) -> PlatformDatabaseCheck {
        self.check
    }

    /// Closes all Platform runtime connections.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

/// Performs the full one-shot Platform runtime database check and closes its pool.
///
/// # Errors
///
/// Returns a sanitized [`ProviderError`] without connection or credential details.
pub async fn check_database(
    credential: &impl DatabaseCredential,
    config: &PostgresConnectionConfig,
    role: PlatformRuntimeRole,
) -> Result<PlatformDatabaseCheck, ProviderError> {
    let database = PlatformDatabase::connect(credential, config, role).await?;
    let check = database.check();
    database.close().await;
    Ok(check)
}

async fn verify_ready(
    pool: &PostgresPool,
    role: PlatformRuntimeRole,
) -> Result<PlatformDatabaseCheck, ProviderError> {
    let server_version = verify_server_version(pool).await?;
    verify_platform_marker(pool).await?;
    verify_runtime_role(pool, role.database_role(), MIGRATION_ROLE, PLATFORM_SCHEMAS).await?;
    let contract_version = verify_contract(pool).await?;
    Ok(PlatformDatabaseCheck {
        server_version,
        contract_version,
    })
}

async fn verify_platform_marker(pool: &PostgresPool) -> Result<(), ProviderError> {
    let row = sqlx::query(
        "SELECT authority_kind, cell_id FROM edtech_bootstrap.authority_identity \
         WHERE singleton",
    )
    .fetch_optional(pool.sqlx_pool())
    .await
    .map_err(ProviderError::from_sqlx)?;
    let matches = row.is_some_and(|row| {
        row.get::<String, _>("authority_kind") == "platform"
            && row.get::<Option<String>, _>("cell_id").is_none()
    });
    if !matches {
        return Err(ProviderError::category(
            ProviderErrorKind::AuthorityMismatch,
        ));
    }
    Ok(())
}

async fn verify_contract(pool: &PostgresPool) -> Result<u32, ProviderError> {
    let row = sqlx::query(
        "SELECT contract_name, contract_version FROM platform_control.schema_contract \
         WHERE singleton",
    )
    .fetch_optional(pool.sqlx_pool())
    .await
    .map_err(|_| ProviderError::category(ProviderErrorKind::SchemaContractMismatch))?;
    let Some(row) = row else {
        return Err(ProviderError::category(
            ProviderErrorKind::SchemaContractMismatch,
        ));
    };
    validate_contract(
        &row.get::<String, _>("contract_name"),
        row.get::<i32, _>("contract_version"),
    )
}

fn validate_contract(name: &str, version: i32) -> Result<u32, ProviderError> {
    if name != "platform" || version != 1 {
        return Err(ProviderError::category(
            ProviderErrorKind::SchemaContractMismatch,
        ));
    }
    let version = u32::try_from(version)
        .map_err(|_| ProviderError::category(ProviderErrorKind::SchemaContractMismatch))?;
    if version != SUPPORTED_CONTRACT_VERSION {
        return Err(ProviderError::category(
            ProviderErrorKind::SchemaContractMismatch,
        ));
    }
    Ok(version)
}

#[cfg(test)]
mod tests {
    use postgres_runtime::ProviderErrorKind;

    use super::validate_contract;

    #[test]
    fn platform_contract_compatibility_fails_closed() {
        assert_eq!(validate_contract("platform", 1).ok(), Some(1));
        for (name, version) in [("cell", 1), ("platform", 0), ("platform", 2)] {
            assert_eq!(
                validate_contract(name, version)
                    .err()
                    .map(|error| error.kind()),
                Some(ProviderErrorKind::SchemaContractMismatch)
            );
        }
    }
}
