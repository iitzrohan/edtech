//! Composition root for the future Cell API process.
//!
//! Checkpoint 1 contains configuration, logging, supervision, and signal handling only: no HTTP,
//! database, broker, identity-provider, cache, or tenant product behavior belongs here.

use std::{env, ffi::OsStr};

use anyhow::{Result, anyhow, bail};
use process_lifecycle::{TaskSupervisor, shutdown_signal};
use runtime_config::{ServiceKind, load_cell};
use tracing::info;
use tracing_subscriber::EnvFilter;

enum Mode {
    Run,
    CheckConfig,
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

    initialize_logging(logging_filter)?;
    let mut supervisor = TaskSupervisor::new();
    info!(
        service = service.as_str(),
        environment = %config.base().environment(),
        cell_id = %config.cell_id(),
        "process started"
    );
    let reason = supervisor
        .run_until_shutdown(shutdown_signal(), config.base().shutdown_grace())
        .await?;
    info!(
        service = service.as_str(),
        ?reason,
        "process stopped cleanly"
    );
    Ok(())
}

fn parse_mode() -> Result<Mode> {
    let mut arguments = env::args_os().skip(1);
    match (arguments.next(), arguments.next()) {
        (None, None) => Ok(Mode::Run),
        (Some(argument), None) if argument == OsStr::new("--check-config") => Ok(Mode::CheckConfig),
        _ => bail!("unsupported arguments; the only supported argument is `--check-config`"),
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
