//! Composition root for the Platform worker runtime process.
//!
//! The process owns exactly one Platform database handle, one opaque `JetStream` runtime, and three
//! supervised message tasks. It contains no topology mutation, Cell provider, DDL, or direct SQL.

use std::{env, ffi::OsStr, sync::Arc};

use anyhow::{Result, anyhow, bail};
use nats_jetstream::{
    JetStreamRuntime, NatsConnectionConfig, NatsCredential, NatsRuntimeRole, NatsTlsMode,
};
use platform_message_runtime::{
    ConsumerSettings, PublisherSettings, create_publisher_instance_id, run_command_consumer,
    run_event_consumer, run_outbox_publisher,
};
use platform_postgres::{PlatformDatabase, PlatformRuntimeRole, check_database};
use postgres_runtime::{ApplicationName, PoolSettings, PostgresConnectionConfig, PostgresTlsMode};
use process_lifecycle::{TaskSupervisor, shutdown_signal};
use runtime_config::{
    DatabaseConfig, DatabaseTlsMode, ServiceKind, TransportConfig, TransportTlsMode, load_platform,
};
use runtime_identity::{RuntimeIdentitySource, SystemRuntimeIdentity};
use secret_resolution::resolve;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Clone, Copy)]
enum Mode {
    Run,
    CheckConfig,
    CheckDatabase,
    CheckTransport,
    CheckRuntime,
}

#[tokio::main(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    let mode = parse_mode()?;
    let service = ServiceKind::PlatformWorker;
    let config = load_platform(service)?;
    let logging_filter = parse_logging_filter(config.base().log_filter().as_str())?;
    let transport_config = config
        .transport()
        .ok_or_else(|| anyhow!("platform-worker transport configuration is missing"))?;

    if matches!(mode, Mode::CheckConfig) {
        println!(
            "configuration valid: service={service} environment={}",
            config.base().environment()
        );
        return Ok(());
    }

    let nats_provider_config = nats_provider_config(
        service,
        &config.base().environment().to_string(),
        transport_config,
    )?;
    if matches!(mode, Mode::CheckTransport) {
        let transport = connect_transport(transport_config, &nats_provider_config).await?;
        println!(
            "transport valid: service={service} authority=platform server_version={}",
            transport.server_version()
        );
        transport.drain().await?;
        return Ok(());
    }

    let database_credential = resolve(config.database().credential_ref())?;
    let database_provider_config = database_provider_config(
        service,
        &config.base().environment().to_string(),
        config.database(),
    )?;
    if matches!(mode, Mode::CheckDatabase) {
        let check = check_database(
            &database_credential,
            &database_provider_config,
            PlatformRuntimeRole::Worker,
        )
        .await?;
        println!(
            "database valid: service={service} authority=platform server_version={} contract_version={} message_store_available={}",
            check.server_version(),
            check.contract_version(),
            check.message_store_available()
        );
        return Ok(());
    }

    let database = Arc::new(
        PlatformDatabase::connect(
            &database_credential,
            &database_provider_config,
            PlatformRuntimeRole::Worker,
        )
        .await?,
    );
    let transport = match connect_transport(transport_config, &nats_provider_config).await {
        Ok(transport) => transport,
        Err(error) => {
            database.close().await;
            return Err(error);
        }
    };
    if matches!(mode, Mode::CheckRuntime) {
        println!(
            "runtime valid: service={service} authority=platform database_contract_version={} nats_server_version={}",
            database.check().contract_version(),
            transport.server_version()
        );
        let drain_result = transport.drain().await;
        database.close().await;
        drain_result?;
        return Ok(());
    }

    initialize_logging(logging_filter)?;
    info!(
        service = service.as_str(),
        environment = %config.base().environment(),
        authority_kind = "platform",
        schema_contract_version = database.check().contract_version(),
        nats_server_version = %transport.server_version(),
        "worker runtime ready"
    );

    let publisher_settings = publisher_settings(transport_config)?;
    let consumer_settings = consumer_settings(transport_config)?;
    let system_identity = SystemRuntimeIdentity;
    let publisher = create_publisher_instance_id(&system_identity)?;
    let identity: Arc<dyn RuntimeIdentitySource> = Arc::new(system_identity);
    let mut supervisor = TaskSupervisor::new();
    supervisor.spawn(
        "platform-outbox-publisher",
        run_outbox_publisher(
            Arc::clone(&database),
            transport.clone(),
            publisher,
            Arc::clone(&identity),
            publisher_settings,
            supervisor.child_token(),
        ),
    )?;
    supervisor.spawn(
        "platform-command-consumer",
        run_command_consumer(
            Arc::clone(&database),
            transport.clone(),
            Arc::clone(&identity),
            consumer_settings.clone(),
            supervisor.child_token(),
        ),
    )?;
    supervisor.spawn(
        "platform-event-consumer",
        run_event_consumer(
            Arc::clone(&database),
            transport.clone(),
            identity,
            consumer_settings,
            supervisor.child_token(),
        ),
    )?;

    let run_result = supervisor
        .run_until_shutdown(shutdown_signal(), config.base().shutdown_grace())
        .await;
    let drain_result = tokio::time::timeout(config.base().shutdown_grace(), transport.drain())
        .await
        .map_err(|_| anyhow!("NATS drain exceeded the configured shutdown grace"));
    database.close().await;
    let reason = run_result?;
    drain_result??;
    info!(
        service = service.as_str(),
        ?reason,
        "process stopped cleanly"
    );
    Ok(())
}

fn database_provider_config(
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

fn nats_provider_config(
    service: ServiceKind,
    environment: &str,
    transport: &TransportConfig,
) -> Result<NatsConnectionConfig> {
    let tls = match transport.tls_mode() {
        TransportTlsMode::Disable => NatsTlsMode::Disable,
        TransportTlsMode::VerifyFull => NatsTlsMode::VerifyFull,
    };
    Ok(NatsConnectionConfig::new(
        service.as_str(),
        environment,
        None,
        transport.servers().to_vec(),
        tls,
        transport.ca_certificate_file().cloned(),
        transport.connect_timeout(),
        transport.request_timeout(),
        transport.publish_ack_timeout(),
        transport.startup_timeout(),
    )?)
}

async fn connect_transport(
    config: &TransportConfig,
    provider: &NatsConnectionConfig,
) -> Result<JetStreamRuntime> {
    let resolved = resolve(config.credential_ref())?;
    let credential = NatsCredential::parse_secret_json(&resolved)?;
    Ok(JetStreamRuntime::connect(credential, provider, NatsRuntimeRole::PlatformWorker).await?)
}

fn publisher_settings(transport: &TransportConfig) -> Result<PublisherSettings> {
    Ok(PublisherSettings::new(
        transport.outbox_poll_interval(),
        transport.outbox_claim_batch_size(),
        transport.outbox_lease(),
        transport.publish_concurrency(),
        transport.retry_base(),
        transport.retry_max(),
    )?)
}

fn consumer_settings(transport: &TransportConfig) -> Result<ConsumerSettings> {
    Ok(ConsumerSettings::new(
        transport.consumer_fetch_batch_size(),
        transport.consumer_fetch_expires(),
        transport.consumer_handler_timeout(),
        transport.consumer_nak_delay(),
        transport.consumer_max_in_flight(),
    )?)
}

fn parse_mode() -> Result<Mode> {
    let mut arguments = env::args_os().skip(1);
    match (arguments.next(), arguments.next()) {
        (None, None) => Ok(Mode::Run),
        (Some(argument), None) if argument == OsStr::new("--check-config") => Ok(Mode::CheckConfig),
        (Some(argument), None) if argument == OsStr::new("--check-database") => {
            Ok(Mode::CheckDatabase)
        }
        (Some(argument), None) if argument == OsStr::new("--check-transport") => {
            Ok(Mode::CheckTransport)
        }
        (Some(argument), None) if argument == OsStr::new("--check-runtime") => {
            Ok(Mode::CheckRuntime)
        }
        _ => bail!(
            "unsupported arguments; use `--check-config`, `--check-database`, `--check-transport`, or `--check-runtime`"
        ),
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
