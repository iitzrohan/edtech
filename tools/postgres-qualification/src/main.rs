//! Non-deployable real-PostgreSQL correctness and tenancy qualification tool.
//!
//! This tool owns authority, migration, privilege, forced-RLS, schema-inspector,
//! schema-per-tenant candidate, benchmark, and credential-free evidence checks. It must never be
//! imported by deployable binaries or production library crates.

mod database;
mod inspector;
mod migration_checks;
mod model;
mod rls;
mod schema_candidate;

use std::{
    env,
    ffi::OsString,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, anyhow, bail};

use crate::{
    database::{Credentials, raw_pool},
    model::{CandidateMetrics, CheckBook, Profile, QualificationEvidence},
};

struct Arguments {
    profile: Profile,
    output: PathBuf,
    replace: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let arguments = parse_arguments(env::args_os().skip(1))?;
    guard_output(&arguments.output, arguments.replace)?;
    let credentials = Credentials::load()?;
    let parameters = arguments.profile.parameters();
    let mut checks = CheckBook::default();

    println!(
        "qualification: authority and migration checks (profile={})",
        arguments.profile.as_str()
    );
    let server_version = migration_checks::run(&credentials, &mut checks).await?;

    println!("qualification: mandatory forced-RLS checks and measurements");
    let shared_rls = rls::run(&credentials, parameters, &mut checks).await?;

    println!("qualification: tenant schema inspector and unsafe fixtures");
    let cell_migrator = raw_pool(&credentials.cell_migrator, 2).await?;
    let cell_bootstrap = raw_pool(&credentials.cell_bootstrap, 2).await?;
    inspector::inspect_selected_schema(&cell_migrator, &cell_bootstrap, &mut checks).await?;
    cell_migrator.close().await;
    cell_bootstrap.close().await;

    println!("qualification: schema-per-tenant candidate and measurements");
    let schema_per_tenant = schema_candidate::run(&credentials, parameters, &mut checks).await?;

    let rust_version = rust_version()?;
    let evidence = QualificationEvidence::new(
        arguments.profile,
        server_version,
        rust_version,
        checks.into_checks(),
        shared_rls,
        schema_per_tenant,
    );
    write_evidence(&arguments.output, &evidence)?;
    println!(
        "qualification: profile={} passed; correctness_checks={} evidence_written",
        arguments.profile.as_str(),
        evidence.correctness.passed
    );
    Ok(())
}

fn parse_arguments(arguments: impl Iterator<Item = OsString>) -> Result<Arguments> {
    let values = arguments.collect::<Vec<_>>();
    let mut index = 0;
    let mut profile = None;
    let mut output = None;
    let mut replace = false;
    while index < values.len() {
        let argument = values[index]
            .to_str()
            .ok_or_else(|| anyhow!("qualification arguments must be UTF-8"))?;
        match argument {
            "--profile" => {
                index = index.saturating_add(1);
                let value = values
                    .get(index)
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| anyhow!("--profile requires a value"))?;
                if profile.replace(Profile::parse(value)?).is_some() {
                    bail!("--profile may be supplied only once");
                }
            }
            "--output" => {
                index = index.saturating_add(1);
                let value = values
                    .get(index)
                    .ok_or_else(|| anyhow!("--output requires a value"))?;
                if output.replace(PathBuf::from(value)).is_some() {
                    bail!("--output may be supplied only once");
                }
            }
            "--replace" if !replace => replace = true,
            _ => bail!("unsupported qualification argument"),
        }
        index = index.saturating_add(1);
    }
    Ok(Arguments {
        profile: profile.ok_or_else(|| anyhow!("--profile is required"))?,
        output: output.ok_or_else(|| anyhow!("--output is required"))?,
        replace,
    })
}

fn guard_output(output: &Path, replace: bool) -> Result<()> {
    let json = output.join("postgres-qualification.json");
    let markdown = output.join("postgres-qualification.md");
    if !replace && (json.exists() || markdown.exists()) {
        bail!("qualification evidence exists; pass --replace to overwrite it intentionally");
    }
    Ok(())
}

fn rust_version() -> Result<String> {
    let output = Command::new("rustc")
        .arg("--version")
        .output()
        .context("could not run rustc for qualification evidence")?;
    if !output.status.success() {
        bail!("rustc version command failed");
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .context("rustc version output was not UTF-8")
}

fn write_evidence(output: &Path, evidence: &QualificationEvidence) -> Result<()> {
    fs::create_dir_all(output)
        .with_context(|| format!("could not create evidence directory {}", output.display()))?;
    let json = serde_json::to_string_pretty(evidence)
        .context("could not serialize qualification evidence")?;
    reject_sensitive_evidence(&json)?;
    let markdown = render_markdown(evidence)?;
    reject_sensitive_evidence(&markdown)?;
    fs::write(
        output.join("postgres-qualification.json"),
        format!("{json}\n"),
    )
    .context("could not write JSON qualification evidence")?;
    fs::write(output.join("postgres-qualification.md"), markdown)
        .context("could not write Markdown qualification evidence")?;
    Ok(())
}

fn reject_sensitive_evidence(contents: &str) -> Result<()> {
    let lowercase = contents.to_ascii_lowercase();
    for forbidden in [
        "postgres://",
        "postgresql://",
        "password",
        "credential_ref",
        "host_port",
        "container_id",
        "edtech_platform_api",
        "edtech_platform_worker",
        "edtech_platform_migrator",
        "edtech_cell_api",
        "edtech_cell_worker",
        "edtech_cell_migrator",
    ] {
        if lowercase.contains(forbidden) {
            bail!("generated qualification evidence contains a forbidden sensitive field");
        }
    }
    Ok(())
}

fn render_markdown(evidence: &QualificationEvidence) -> Result<String> {
    let mut markdown = String::new();
    markdown.push_str("# Checkpoint 02 PostgreSQL qualification\n\n");
    write!(
        &mut markdown,
        "Profile: `{}`. All {} correctness checks passed.\n\n",
        evidence.profile.as_str(),
        evidence.correctness.passed
    )
    .map_err(|_| anyhow!("could not render qualification summary"))?;
    markdown.push_str("The selected baseline is shared tenant tables with `tenant_id` and forced RLS. The schema-per-tenant candidate remains qualification-only.\n\n");
    markdown.push_str("## Qualified versions\n\n");
    write!(
        &mut markdown,
        "- PostgreSQL server version number: `{}`\n- PostgreSQL image: `{}`\n- Rust: `{}`\n- SQLx: `{}`\n- Host OS/architecture: `{}/{}`\n- Available parallelism: `{}`\n\n",
        evidence.versions.postgres_server_version_num,
        evidence.versions.postgres_image,
        evidence.versions.rust,
        evidence.versions.sqlx,
        evidence.host.operating_system,
        evidence.host.architecture,
        evidence.host.available_parallelism
    )
    .map_err(|_| anyhow!("could not render qualification versions"))?;
    markdown.push_str("## Profile parameters\n\n");
    write!(
        &mut markdown,
        "- Tenants: {}\n- Logical tables: {}\n- Secondary indexes per table: {}\n- Rows per tenant: {}\n- Alternating switches: {}\n- Concurrency: {}\n\n",
        evidence.parameters.tenants,
        evidence.parameters.logical_tables,
        evidence.parameters.secondary_indexes_per_table,
        evidence.parameters.rows_per_tenant,
        evidence.parameters.alternating_switches,
        evidence.parameters.concurrency
    )
    .map_err(|_| anyhow!("could not render qualification parameters"))?;
    render_candidate_metrics(&mut markdown, "Shared forced RLS", &evidence.shared_rls)?;
    render_candidate_metrics(
        &mut markdown,
        "Schema per tenant (qualification only)",
        &evidence.schema_per_tenant,
    )?;
    markdown.push_str("## Limitations\n\n");
    markdown.push_str(evidence.timing_limitations);
    markdown.push_str(" These local measurements do not establish production capacity, availability, hardening, backup, recovery, network isolation, or protection against a completely compromised Cell runtime.\n");
    Ok(markdown)
}

fn render_candidate_metrics(
    markdown: &mut String,
    heading: &str,
    metrics: &CandidateMetrics,
) -> Result<()> {
    write!(markdown, "## {heading}\n\n")
        .map_err(|_| anyhow!("could not render qualification heading"))?;
    write!(
        markdown,
        "| Measurement | Value |\n|---|---:|\n| Clean candidate creation | {} ms |\n| Initial schema migration | {} ms |\n| Incremental migration | {} ms |\n| Tenant provisioning | {} ms |\n| Schemas | {} |\n| Tables | {} |\n| Indexes | {} |\n| Relevant pg_class rows | {} |\n| Relevant pg_attribute rows | {} |\n| Database size | {} bytes |\n| Insert throughput | {} rows/s |\n| Read throughput | {} rows/s |\n| Tenant switch p50 | {} us |\n| Tenant switch p95 | {} us |\n| Tenant switch p99 | {} us |\n| Prepared-query alternation | {} |\n| Concurrent isolation | {} |\n| Probe export | {} us |\n| Probe import | {} us |\n| Cleanup | {} ms |\n\n",
        metrics.clean_candidate_creation_ms,
        metrics.initial_schema_migration_ms,
        metrics.incremental_migration_ms,
        metrics.tenant_provisioning_ms,
        metrics.total_schema_count,
        metrics.total_table_count,
        metrics.total_index_count,
        metrics.relevant_pg_class_rows,
        metrics.relevant_pg_attribute_rows,
        metrics.database_size_bytes,
        metrics.insert_rows_per_second,
        metrics.read_rows_per_second,
        metrics.tenant_switch_p50_microseconds,
        metrics.tenant_switch_p95_microseconds,
        metrics.tenant_switch_p99_microseconds,
        metrics.prepared_query_alternation_passed,
        metrics.concurrent_isolation_passed,
        metrics.single_tenant_probe_export_microseconds,
        metrics.single_tenant_probe_import_microseconds,
        metrics.cleanup_ms
    )
    .map_err(|_| anyhow!("could not render qualification metrics"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::parse_arguments;
    use crate::model::Profile;

    #[test]
    fn qualification_arguments_are_explicit_and_bounded() {
        let arguments = parse_arguments(
            [
                "--profile",
                "ci",
                "--output",
                "target/evidence",
                "--replace",
            ]
            .into_iter()
            .map(OsString::from),
        );
        assert!(
            arguments
                .as_ref()
                .is_ok_and(|arguments| { arguments.profile == Profile::Ci && arguments.replace })
        );
        assert!(parse_arguments([OsString::from("--unknown")].into_iter()).is_err());
    }
}
