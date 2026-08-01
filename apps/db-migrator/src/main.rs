//! One-shot, separately privileged Platform or Cell migration composition root.
//!
//! One invocation resolves exactly one credential, verifies exactly one authority, runs only the
//! selected embedded migration set, reports a safe summary, and exits. Runtime processes cannot
//! import either migration crate.

use std::{env, ffi::OsStr};

use anyhow::{Result, anyhow, bail};
use postgres_runtime::{ApplicationName, PoolSettings, PostgresConnectionConfig, PostgresTlsMode};
use runtime_config::{DatabaseConfig, DatabaseTlsMode, MigrationScope, ServiceKind, load_migrator};
use secret_resolution::resolve;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Clone, Copy)]
enum Mode {
    Run,
    CheckConfig,
    CheckDatabase,
}

#[derive(Clone, Copy)]
enum MigrationOutcome {
    Platform {
        latest_version: i64,
        applied_count: u64,
        contract_version: u32,
    },
    Cell {
        latest_version: i64,
        applied_count: u64,
        contract_version: u32,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mode = parse_mode()?;
    let service = ServiceKind::DbMigrator;
    let config = load_migrator()?;
    let logging_filter = parse_logging_filter(config.base().log_filter().as_str())?;

    if matches!(mode, Mode::CheckConfig) {
        print_config_check(&config, service);
        return Ok(());
    }

    let credential = resolve(config.database().credential_ref())?;
    #[allow(clippy::redundant_closure_for_method_calls)]
    let cell_id = config.cell_id().map(|cell_id| cell_id.as_str());
    let provider_config = provider_config(
        service,
        &config.base().environment().to_string(),
        cell_id,
        config.database(),
    )?;
    if matches!(mode, Mode::CheckDatabase) {
        match config.scope() {
            MigrationScope::Platform => {
                let check = tokio::time::timeout(
                    config.migration_timeout(),
                    platform_migrations::check_database(&credential, &provider_config),
                )
                .await
                .map_err(|_| anyhow!("migration database check timed out"))??;
                println!(
                    "database valid: authority=platform server_version={} contract_version={}",
                    check.server_version(),
                    check.contract_version()
                );
            }
            MigrationScope::Cell => {
                let cell_id = config
                    .cell_id()
                    .ok_or_else(|| anyhow!("cell_id is required"))?;
                let check = tokio::time::timeout(
                    config.migration_timeout(),
                    cell_migrations::check_database(&credential, &provider_config, cell_id),
                )
                .await
                .map_err(|_| anyhow!("migration database check timed out"))??;
                println!(
                    "database valid: authority=cell cell_id={cell_id} server_version={} contract_version={}",
                    check.server_version(),
                    check.contract_version()
                );
            }
        }
        return Ok(());
    }

    initialize_logging(logging_filter)?;
    let outcome = match config.scope() {
        MigrationScope::Platform => {
            let report = tokio::time::timeout(
                config.migration_timeout(),
                platform_migrations::migrate(&credential, &provider_config),
            )
            .await
            .map_err(|_| anyhow!("migration operation timed out"))??;
            MigrationOutcome::Platform {
                latest_version: report.latest_version(),
                applied_count: report.applied_count(),
                contract_version: report.contract_version(),
            }
        }
        MigrationScope::Cell => {
            let cell_id = config
                .cell_id()
                .ok_or_else(|| anyhow!("cell_id is required"))?;
            let report = tokio::time::timeout(
                config.migration_timeout(),
                cell_migrations::migrate(&credential, &provider_config, cell_id),
            )
            .await
            .map_err(|_| anyhow!("migration operation timed out"))??;
            MigrationOutcome::Cell {
                latest_version: report.latest_version(),
                applied_count: report.applied_count(),
                contract_version: report.contract_version(),
            }
        }
    };
    report_outcome(&config, service, outcome);
    Ok(())
}

fn print_config_check(config: &runtime_config::MigratorRuntimeConfig, service: ServiceKind) {
    if let Some(cell_id) = config.cell_id() {
        println!(
            "configuration valid: service={service} environment={} migration_scope={} cell_id={cell_id}",
            config.base().environment(),
            config.scope()
        );
    } else {
        println!(
            "configuration valid: service={service} environment={} migration_scope={}",
            config.base().environment(),
            config.scope()
        );
    }
}

fn report_outcome(
    config: &runtime_config::MigratorRuntimeConfig,
    service: ServiceKind,
    outcome: MigrationOutcome,
) {
    match outcome {
        MigrationOutcome::Platform {
            latest_version,
            applied_count,
            contract_version,
        } => info!(
            service = service.as_str(),
            environment = %config.base().environment(),
            authority_kind = "platform",
            migration_version = latest_version,
            migration_count = applied_count,
            schema_contract_version = contract_version,
            "migration complete"
        ),
        MigrationOutcome::Cell {
            latest_version,
            applied_count,
            contract_version,
        } => info!(
            service = service.as_str(),
            environment = %config.base().environment(),
            authority_kind = "cell",
            cell_id = config.cell_id().map(ToString::to_string),
            migration_version = latest_version,
            migration_count = applied_count,
            schema_contract_version = contract_version,
            "migration complete"
        ),
    }
}

fn provider_config(
    service: ServiceKind,
    environment: &str,
    cell_id: Option<&str>,
    database: &DatabaseConfig,
) -> Result<PostgresConnectionConfig> {
    let application_name = ApplicationName::compose(service.as_str(), environment, cell_id)?;
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
