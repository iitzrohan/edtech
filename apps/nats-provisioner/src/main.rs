//! One-shot composition root for privileged, non-destructive NATS topology provisioning.

use std::{env, ffi::OsStr, fs::File, io::Read};

use anyhow::{Result, anyhow, bail};
use nats_jetstream::{NatsConnectionConfig, NatsCredential, NatsTlsMode};
use nats_jetstream_admin::{NatsJetStreamAdmin, TopologyManifest, TopologyPlan};
use runtime_config::{
    NatsProvisionerRuntimeConfig, ServiceKind, TransportConfig, TransportTlsMode,
    load_nats_provisioner,
};
use secret_resolution::resolve;
use tracing::info;
use tracing_subscriber::EnvFilter;

const MAX_TOPOLOGY_FILE_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy)]
enum Mode {
    Apply,
    CheckConfig,
    CheckTransport,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mode = parse_mode()?;
    let config = load_nats_provisioner()?;
    let logging_filter = parse_logging_filter(config.base().log_filter().as_str())?;
    let topology_text = read_topology(&config)?;
    let manifest = TopologyManifest::parse_toml(&topology_text)?;

    if matches!(mode, Mode::CheckConfig) {
        println!(
            "configuration valid: service={} environment={} topology_schema=1 declared_cells={}",
            ServiceKind::NatsProvisioner,
            config.base().environment(),
            manifest.cells().len()
        );
        return Ok(());
    }

    if matches!(mode, Mode::Apply) {
        initialize_logging(logging_filter)?;
    }
    let provider = nats_provider_config(&config)?;
    let resolved = resolve(config.transport().credential_ref())?;
    let credential = NatsCredential::parse_secret_json(&resolved)?;
    let admin = NatsJetStreamAdmin::connect(credential, &provider).await?;

    let operation_result = match mode {
        Mode::CheckTransport => inspect_only(&admin, &manifest).await,
        Mode::Apply => apply_topology(&admin, &manifest, &config).await,
        Mode::CheckConfig => Ok(()),
    };
    let drain_result = admin.drain().await;
    operation_result?;
    drain_result?;
    Ok(())
}

async fn inspect_only(admin: &NatsJetStreamAdmin, manifest: &TopologyManifest) -> Result<()> {
    let plan = admin.plan(manifest).await?;
    print_plan(&plan);
    println!(
        "transport valid: service={} server_version={} refused_changes={} converged={}",
        ServiceKind::NatsProvisioner,
        admin.server_version(),
        plan.has_refused_change(),
        plan.is_converged()
    );
    Ok(())
}

async fn apply_topology(
    admin: &NatsJetStreamAdmin,
    manifest: &TopologyManifest,
    config: &NatsProvisionerRuntimeConfig,
) -> Result<()> {
    let plan = admin.plan(manifest).await?;
    print_plan(&plan);
    if plan.has_refused_change() {
        bail!("NATS topology contains a refused non-destructive drift category");
    }
    let report = admin
        .apply(manifest, config.topology_apply_timeout())
        .await?;
    println!(
        "topology applied: server_version={} created_streams={} updated_streams={} created_consumers={} updated_consumers={} unknown_assets={} converged={}",
        admin.server_version(),
        report.created_streams,
        report.updated_streams,
        report.created_consumers,
        report.updated_consumers,
        report.unknown_assets,
        report.converged
    );
    info!(
        created_streams = report.created_streams,
        updated_streams = report.updated_streams,
        created_consumers = report.created_consumers,
        updated_consumers = report.updated_consumers,
        unknown_assets = report.unknown_assets,
        converged = report.converged,
        "NATS topology apply complete"
    );
    Ok(())
}

fn print_plan(plan: &TopologyPlan) {
    for item in &plan.items {
        println!(
            "topology plan: asset={} action={:?} category={:?}",
            item.asset, item.action, item.category
        );
    }
}

fn read_topology(config: &NatsProvisionerRuntimeConfig) -> Result<String> {
    let file = File::open(config.topology_file())
        .map_err(|_| anyhow!("topology file could not be read"))?;
    let maximum = u64::try_from(MAX_TOPOLOGY_FILE_BYTES)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut bytes = Vec::with_capacity(MAX_TOPOLOGY_FILE_BYTES);
    file.take(maximum)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow!("topology file could not be read"))?;
    if bytes.len() > MAX_TOPOLOGY_FILE_BYTES {
        bail!("topology file exceeds the configured size bound");
    }
    String::from_utf8(bytes).map_err(|_| anyhow!("topology file is not valid UTF-8"))
}

fn nats_provider_config(config: &NatsProvisionerRuntimeConfig) -> Result<NatsConnectionConfig> {
    let transport: &TransportConfig = config.transport();
    let tls = match transport.tls_mode() {
        TransportTlsMode::Disable => NatsTlsMode::Disable,
        TransportTlsMode::VerifyFull => NatsTlsMode::VerifyFull,
    };
    Ok(NatsConnectionConfig::new(
        ServiceKind::NatsProvisioner.as_str(),
        config.base().environment().to_string(),
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

fn parse_mode() -> Result<Mode> {
    let mut arguments = env::args_os().skip(1);
    match (arguments.next(), arguments.next()) {
        (None, None) => Ok(Mode::Apply),
        (Some(argument), None) if argument == OsStr::new("--check-config") => Ok(Mode::CheckConfig),
        (Some(argument), None) if argument == OsStr::new("--check-transport") => {
            Ok(Mode::CheckTransport)
        }
        _ => bail!("unsupported arguments; use `--check-config` or `--check-transport`"),
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
