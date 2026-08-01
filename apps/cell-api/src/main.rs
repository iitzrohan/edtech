//! Composition root for the Cell API runtime process.
//!
//! This process resolves one Cell credential and retains an opaque, verified Cell database
//! handle. It contains no DDL, migration code, Platform adapter, or direct `SQLx` usage.

use std::{env, ffi::OsStr};

use anyhow::{Result, anyhow, bail};
use cell_postgres::{CellDatabase, CellRuntimeRole, check_database};
use postgres_runtime::{ApplicationName, PoolSettings, PostgresConnectionConfig, PostgresTlsMode};
use process_lifecycle::{TaskSupervisor, shutdown_signal};
use runtime_config::{DatabaseConfig, DatabaseTlsMode, ServiceKind, load_cell};
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
    let service = ServiceKind::CellApi;
    let config = load_cell(service)?;
    let logging_filter = parse_logging_filter(config.base().log_filter().as_str())?;

    if matches!(mode, Mode::CheckConfig) {
        println!(
            "configuration valid: service={service} environment={} cell_id={}",
            config.base().environment(),
            config.cell_id()
        );
        return Ok(());
    }

    let credential = resolve(config.database().credential_ref())?;
    let provider_config = provider_config(
        service,
        &config.base().environment().to_string(),
        config.cell_id().as_str(),
        config.database(),
    )?;
    if matches!(mode, Mode::CheckDatabase) {
        let check = check_database(
            &credential,
            &provider_config,
            config.cell_id(),
            CellRuntimeRole::Api,
        )
        .await?;
        println!(
            "database valid: authority=cell cell_id={} server_version={} contract_version={}",
            check.cell_id(),
            check.server_version(),
            check.contract_version()
        );
        return Ok(());
    }

    initialize_logging(logging_filter)?;
    let database = CellDatabase::connect(
        &credential,
        &provider_config,
        config.cell_id(),
        CellRuntimeRole::Api,
    )
    .await?;
    info!(
        service = service.as_str(),
        environment = %config.base().environment(),
        authority_kind = "cell",
        cell_id = %database.check().cell_id(),
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
    cell_id: &str,
    database: &DatabaseConfig,
) -> Result<PostgresConnectionConfig> {
    let application_name = ApplicationName::compose(service.as_str(), environment, Some(cell_id))?;
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
