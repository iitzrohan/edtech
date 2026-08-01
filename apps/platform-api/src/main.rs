//! Composition root for the Platform API runtime process.
//!
//! This process resolves one Platform credential and retains an opaque, verified Platform
//! database handle. It contains no DDL, migration code, Cell adapter, or direct `SQLx` usage.

use std::{env, ffi::OsStr};

use anyhow::{Result, anyhow, bail};
use platform_postgres::{PlatformDatabase, PlatformRuntimeRole, check_database};
use postgres_runtime::{ApplicationName, PoolSettings, PostgresConnectionConfig, PostgresTlsMode};
use process_lifecycle::{TaskSupervisor, shutdown_signal};
use runtime_config::{DatabaseConfig, DatabaseTlsMode, ServiceKind, load_platform};
use secret_resolution::resolve;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Clone, Copy)]
enum Mode {
    Run,
    CheckConfig,
    CheckDatabase,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mode = parse_mode()?;
    let service = ServiceKind::PlatformApi;
    let config = load_platform(service)?;
    let logging_filter = parse_logging_filter(config.base().log_filter().as_str())?;

    if matches!(mode, Mode::CheckConfig) {
        println!(
            "configuration valid: service={service} environment={}",
            config.base().environment()
        );
        return Ok(());
    }

    let credential = resolve(config.database().credential_ref())?;
    let provider_config = provider_config(
        service,
        &config.base().environment().to_string(),
        config.database(),
    )?;
    if matches!(mode, Mode::CheckDatabase) {
        let check = check_database(&credential, &provider_config, PlatformRuntimeRole::Api).await?;
        println!(
            "database valid: service={service} authority=platform server_version={} contract_version={} message_store_available={}",
            check.server_version(),
            check.contract_version(),
            check.message_store_available()
        );
        return Ok(());
    }

    initialize_logging(logging_filter)?;
    let database =
        PlatformDatabase::connect(&credential, &provider_config, PlatformRuntimeRole::Api).await?;
    info!(
        service = service.as_str(),
        environment = %config.base().environment(),
        authority_kind = "platform",
        schema_contract_version = database.check().contract_version(),
        "database ready"
    );
    let mut supervisor = TaskSupervisor::new();
    let run_result = supervisor
        .run_until_shutdown(shutdown_signal(), config.base().shutdown_grace())
        .await;
    database.close().await;
    let reason = run_result?;
    info!(
        service = service.as_str(),
        ?reason,
        "process stopped cleanly"
    );
    Ok(())
}

fn provider_config(
    service: ServiceKind,
    environment: &str,
    database: &DatabaseConfig,
) -> Result<PostgresConnectionConfig> {
    let application_name = ApplicationName::compose(service.as_str(), environment, None)?;
    let pool = PoolSettings::new(
        database.max_connections(),
        database.min_connections(),
        database.acquire_timeout(),
        database.connect_timeout(),
        database.statement_timeout(),
        database.lock_timeout(),
        database.idle_in_transaction_timeout(),
        database.max_lifetime(),
    )?;
    let tls = match database.tls_mode() {
        DatabaseTlsMode::Disable => PostgresTlsMode::Disable,
        DatabaseTlsMode::VerifyFull => PostgresTlsMode::VerifyFull,
    };
    Ok(PostgresConnectionConfig::new(tls, application_name, pool))
}

fn parse_mode() -> Result<Mode> {
    let mut arguments = env::args_os().skip(1);
    match (arguments.next(), arguments.next()) {
        (None, None) => Ok(Mode::Run),
        (Some(argument), None) if argument == OsStr::new("--check-config") => Ok(Mode::CheckConfig),
        (Some(argument), None) if argument == OsStr::new("--check-database") => {
            Ok(Mode::CheckDatabase)
        }
        _ => bail!("unsupported arguments; use `--check-config` or `--check-database`"),
    }
}

fn parse_logging_filter(filter: &str) -> Result<EnvFilter> {
    EnvFilter::try_new(filter).map_err(|_| anyhow!("invalid log_filter syntax"))
}

fn initialize_logging(filter: EnvFilter) -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .try_init()
        .map_err(|_| anyhow!("structured logging subscriber could not be initialized"))
}
