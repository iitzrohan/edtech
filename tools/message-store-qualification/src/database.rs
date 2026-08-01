//! Credential-reference loading and safe qualification connections.

use std::{env, time::Duration};

use anyhow::{Result, anyhow};
use postgres_runtime::{
    ApplicationName, PoolSettings, PostgresConnectionConfig, PostgresPool, PostgresTlsMode,
};
use secret_resolution::{ResolvedCredential, resolve_reference};

pub(crate) struct Credentials {
    pub(crate) platform_migrator: ResolvedCredential,
    pub(crate) platform_api: ResolvedCredential,
    pub(crate) platform_worker: ResolvedCredential,
    pub(crate) cell_migrator: ResolvedCredential,
    pub(crate) cell_api: ResolvedCredential,
    pub(crate) cell_worker: ResolvedCredential,
}

impl Credentials {
    pub(crate) fn load() -> Result<Self> {
        Ok(Self {
            platform_migrator: load("EDTECH_QUAL_PLATFORM_MIGRATOR_REF", "Platform migrator")?,
            platform_api: load("EDTECH_QUAL_PLATFORM_API_REF", "Platform API")?,
            platform_worker: load("EDTECH_QUAL_PLATFORM_WORKER_REF", "Platform worker")?,
            cell_migrator: load("EDTECH_QUAL_CELL_MIGRATOR_REF", "Cell migrator")?,
            cell_api: load("EDTECH_QUAL_CELL_API_REF", "Cell API")?,
            cell_worker: load("EDTECH_QUAL_CELL_WORKER_REF", "Cell worker")?,
        })
    }
}

fn load(variable: &str, label: &str) -> Result<ResolvedCredential> {
    let reference = env::var(variable)
        .map_err(|_| anyhow!("{label} qualification credential reference is missing"))?;
    resolve_reference(&reference)
        .map_err(|_| anyhow!("{label} qualification credential resolution failed"))
}

pub(crate) fn provider_config(
    application: &str,
    cell_id: Option<&str>,
    max_connections: u32,
) -> Result<PostgresConnectionConfig> {
    let timeout = Duration::from_secs(30);
    let pool = PoolSettings::new(
        max_connections,
        0,
        timeout,
        timeout,
        Duration::from_mins(5),
        timeout,
        Duration::from_mins(1),
        Duration::from_mins(10),
    )?;
    let application_name = ApplicationName::compose(application, "dev", cell_id)?;
    Ok(PostgresConnectionConfig::new(
        PostgresTlsMode::Disable,
        application_name,
        pool,
    ))
}

pub(crate) async fn raw_pool(
    credential: &ResolvedCredential,
    max_connections: u32,
) -> Result<PostgresPool> {
    let config = provider_config("message-store-qualification", None, max_connections)?;
    postgres_runtime::connect(credential, &config)
        .await
        .map_err(|_| anyhow!("qualification database connection failed"))
}
