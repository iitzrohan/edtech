//! Credential loading and safe direct qualification connections.

use std::{env, str::FromStr, time::Duration};

use anyhow::{Result, anyhow};
use postgres_runtime::{ApplicationName, PoolSettings, PostgresConnectionConfig, PostgresTlsMode};
use secrecy::ExposeSecret;
use secret_resolution::{ResolvedCredential, resolve_reference};
use sqlx::{
    ConnectOptions, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};

pub(crate) struct Credentials {
    pub(crate) platform_bootstrap: ResolvedCredential,
    pub(crate) platform_migrator: ResolvedCredential,
    pub(crate) platform_api: ResolvedCredential,
    pub(crate) platform_worker: ResolvedCredential,
    pub(crate) cell_bootstrap: ResolvedCredential,
    pub(crate) cell_migrator: ResolvedCredential,
    pub(crate) cell_api: ResolvedCredential,
    pub(crate) cell_worker: ResolvedCredential,
}

impl Credentials {
    pub(crate) fn load() -> Result<Self> {
        Ok(Self {
            platform_bootstrap: load_credential(
                "EDTECH_QUAL_PLATFORM_BOOTSTRAP_REF",
                "Platform bootstrap",
            )?,
            platform_migrator: load_credential(
                "EDTECH_QUAL_PLATFORM_MIGRATOR_REF",
                "Platform migrator",
            )?,
            platform_api: load_credential("EDTECH_QUAL_PLATFORM_API_REF", "Platform API")?,
            platform_worker: load_credential("EDTECH_QUAL_PLATFORM_WORKER_REF", "Platform worker")?,
            cell_bootstrap: load_credential("EDTECH_QUAL_CELL_BOOTSTRAP_REF", "Cell bootstrap")?,
            cell_migrator: load_credential("EDTECH_QUAL_CELL_MIGRATOR_REF", "Cell migrator")?,
            cell_api: load_credential("EDTECH_QUAL_CELL_API_REF", "Cell API")?,
            cell_worker: load_credential("EDTECH_QUAL_CELL_WORKER_REF", "Cell worker")?,
        })
    }
}

fn load_credential(variable: &str, label: &str) -> Result<ResolvedCredential> {
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
) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(credential.expose_secret())
        .map_err(|_| anyhow!("qualification database credential is invalid"))?
        .ssl_mode(PgSslMode::Disable)
        .application_name("edtech-postgres-qualification-dev")
        .disable_statement_logging();
    PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(0)
        .acquire_timeout(Duration::from_secs(30))
        .after_connect(|connection, _metadata| {
            Box::pin(async move {
                sqlx::query(
                    "SELECT pg_catalog.set_config('TimeZone', 'UTC', false), \
                     pg_catalog.set_config('row_security', 'on', false), \
                     pg_catalog.set_config('statement_timeout', '300000ms', false), \
                     pg_catalog.set_config('lock_timeout', '30000ms', false), \
                     pg_catalog.set_config('idle_in_transaction_session_timeout', '60000ms', false), \
                     pg_catalog.set_config('search_path', 'pg_catalog', false)",
                )
                .execute(connection)
                .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .map_err(|_| anyhow!("qualification database connection failed"))
}

pub(crate) fn safe_database_error(
    stage: &'static str,
) -> impl FnOnce(sqlx::Error) -> anyhow::Error {
    move |_error| anyhow!("qualification database stage failed: {stage}")
}
