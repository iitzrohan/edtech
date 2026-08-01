//! Local two-authority `PostgreSQL` lifecycle and qualification orchestration.

use std::{
    env, fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result, anyhow, bail};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

const IMAGE_REFERENCE: &str = "postgres:18.4-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296";
const COMPOSE_FILE: &str = "infra/local/postgres/compose.yml";
const LOCAL_STATE_FILE: &str = "state.json";
const DEFAULT_PLATFORM_PORT: u16 = 55_432;
const DEFAULT_CELL_PORT: u16 = 55_433;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum QualificationProfile {
    #[default]
    Ci,
    Full,
}

impl QualificationProfile {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ci => "ci",
            Self::Full => "full",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LocalState {
    project: String,
    platform_port: u16,
    cell_port: u16,
}

#[derive(Clone, Copy)]
struct CredentialSpec {
    authority: &'static str,
    purpose: &'static str,
    role: &'static str,
    database: &'static str,
}

const CREDENTIAL_SPECS: &[CredentialSpec] = &[
    CredentialSpec {
        authority: "platform",
        purpose: "bootstrap",
        role: "edtech_platform_bootstrap",
        database: "edtech_platform",
    },
    CredentialSpec {
        authority: "platform",
        purpose: "migrator",
        role: "edtech_platform_migrator",
        database: "edtech_platform",
    },
    CredentialSpec {
        authority: "platform",
        purpose: "api",
        role: "edtech_platform_api",
        database: "edtech_platform",
    },
    CredentialSpec {
        authority: "platform",
        purpose: "worker",
        role: "edtech_platform_worker",
        database: "edtech_platform",
    },
    CredentialSpec {
        authority: "cell",
        purpose: "bootstrap",
        role: "edtech_cell_bootstrap",
        database: "edtech_cell",
    },
    CredentialSpec {
        authority: "cell",
        purpose: "migrator",
        role: "edtech_cell_migrator",
        database: "edtech_cell",
    },
    CredentialSpec {
        authority: "cell",
        purpose: "api",
        role: "edtech_cell_api",
        database: "edtech_cell",
    },
    CredentialSpec {
        authority: "cell",
        purpose: "worker",
        role: "edtech_cell_worker",
        database: "edtech_cell",
    },
];

struct LocalProject<'a> {
    root: &'a Path,
    state: LocalState,
    secret_dir: PathBuf,
    cleanup_armed: bool,
}

impl<'a> LocalProject<'a> {
    fn prepare(root: &'a Path, state: LocalState, cleanup_armed: bool) -> Result<Self> {
        validate_project_name(&state.project)?;
        validate_ports(state.platform_port, state.cell_port)?;
        let secret_dir = root.join("target/local-postgres").join(&state.project);
        if secret_dir.exists() {
            bail!(
                "local PostgreSQL state already exists for project `{}`; run postgres-down first",
                state.project
            );
        }
        fs::create_dir_all(&secret_dir).with_context(|| {
            format!(
                "could not create local PostgreSQL state directory {}",
                secret_dir.display()
            )
        })?;
        restrict_directory(&secret_dir)?;
        let canonical_secret_dir = fs::canonicalize(&secret_dir).with_context(|| {
            format!(
                "could not resolve local PostgreSQL state directory {}",
                secret_dir.display()
            )
        })?;
        let project = Self {
            root,
            state,
            secret_dir: canonical_secret_dir,
            cleanup_armed,
        };
        if let Err(error) = project.write_credentials() {
            let _remove_result = fs::remove_dir_all(&project.secret_dir);
            return Err(error);
        }
        Ok(project)
    }

    fn from_state(root: &'a Path, project_name: &str) -> Result<Option<Self>> {
        validate_project_name(project_name)?;
        let secret_dir = root.join("target/local-postgres").join(project_name);
        let state_path = secret_dir.join(LOCAL_STATE_FILE);
        if !state_path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&state_path)
            .with_context(|| format!("could not read {}", state_path.display()))?;
        let state: LocalState = serde_json::from_str(&contents)
            .with_context(|| format!("could not parse {}", state_path.display()))?;
        if state.project != project_name {
            bail!("local PostgreSQL state does not match the requested project");
        }
        validate_ports(state.platform_port, state.cell_port)?;
        Ok(Some(Self {
            root,
            state,
            secret_dir,
            cleanup_armed: false,
        }))
    }

    fn write_credentials(&self) -> Result<()> {
        for specification in CREDENTIAL_SPECS {
            let password = random_hex(32)?;
            let password_path = self.password_path(*specification);
            write_private_file(&password_path, password.as_bytes())?;

            let port = if specification.authority == "platform" {
                self.state.platform_port
            } else {
                self.state.cell_port
            };
            let url = database_url(specification.role, &password, port, specification.database);
            let url_path = self.url_path(*specification);
            write_private_file(&url_path, url.as_bytes())?;
        }
        Ok(())
    }

    fn save_state(&self) -> Result<()> {
        let state_path = self.secret_dir.join(LOCAL_STATE_FILE);
        let bytes = serde_json::to_vec_pretty(&self.state)
            .context("could not serialize local PostgreSQL state")?;
        write_private_file(&state_path, &bytes)
    }

    fn start(&self) -> Result<()> {
        let mut command = self.compose_command();
        command.args(["up", "--detach", "--wait", "--wait-timeout", "90"]);
        run_status(&mut command, "docker compose up")
    }

    fn stop_and_remove(&self) -> Result<()> {
        let mut command = self.compose_command();
        command.args(["down", "--volumes", "--remove-orphans", "--timeout", "10"]);
        run_status(&mut command, "docker compose down")
    }

    fn compose_command(&self) -> Command {
        let mut command = Command::new("docker");
        command
            .args([
                "compose",
                "--project-name",
                &self.state.project,
                "--file",
                COMPOSE_FILE,
            ])
            .current_dir(self.root)
            .env("EDTECH_POSTGRES_SECRET_DIR", &self.secret_dir)
            .env(
                "EDTECH_PLATFORM_POSTGRES_PORT",
                self.state.platform_port.to_string(),
            )
            .env(
                "EDTECH_CELL_POSTGRES_PORT",
                self.state.cell_port.to_string(),
            );
        command
    }

    fn cleanup(&mut self) -> Result<()> {
        let stop_result = self.stop_and_remove();
        let remove_result = if self.secret_dir.exists() {
            fs::remove_dir_all(&self.secret_dir).with_context(|| {
                format!(
                    "could not remove generated credential directory {}",
                    self.secret_dir.display()
                )
            })
        } else {
            Ok(())
        };
        self.cleanup_armed = false;
        match (stop_result, remove_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(stop_error), Err(remove_error)) => Err(anyhow!(
                "PostgreSQL cleanup failed: {stop_error}; credential cleanup also failed: {remove_error}"
            )),
        }
    }

    fn disarm_cleanup(&mut self) {
        self.cleanup_armed = false;
    }

    fn password_path(&self, specification: CredentialSpec) -> PathBuf {
        self.secret_dir.join(format!(
            "{}-{}-password",
            specification.authority, specification.purpose
        ))
    }

    fn url_path(&self, specification: CredentialSpec) -> PathBuf {
        self.secret_dir.join(format!(
            "{}-{}-url",
            specification.authority, specification.purpose
        ))
    }

    fn reference(&self, authority: &str, purpose: &str) -> Result<String> {
        let specification = CREDENTIAL_SPECS
            .iter()
            .find(|specification| {
                specification.authority == authority && specification.purpose == purpose
            })
            .ok_or_else(|| anyhow!("unknown local PostgreSQL credential purpose"))?;
        Ok(format!("file:{}", self.url_path(*specification).display()))
    }
}

impl Drop for LocalProject<'_> {
    fn drop(&mut self) {
        if self.cleanup_armed {
            let _stop_result = self.stop_and_remove();
            let _remove_result = fs::remove_dir_all(&self.secret_dir);
        }
    }
}

pub fn doctor(root: &Path) -> Result<()> {
    let required = [
        COMPOSE_FILE,
        "infra/local/postgres/platform/init/001-bootstrap.sh",
        "infra/local/postgres/cell/init/001-bootstrap.sh",
    ];
    let missing = required
        .iter()
        .filter(|path| !root.join(path).is_file())
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "local PostgreSQL infrastructure is incomplete; missing: {}",
            missing.join(", ")
        );
    }

    let docker_version = capture_safe(root, "docker", &["--version"], "docker version")?;
    let daemon_version = capture_safe(
        root,
        "docker",
        &["info", "--format", "{{.ServerVersion}}"],
        "Docker daemon",
    )?;
    let compose_version = capture_safe(
        root,
        "docker",
        &["compose", "version", "--short"],
        "docker compose",
    )?;

    let image_present = Command::new("docker")
        .args(["image", "inspect", IMAGE_REFERENCE])
        .current_dir(root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("could not inspect the pinned PostgreSQL image")?
        .success();
    if !image_present {
        println!("doctor-postgres: pulling pinned PostgreSQL 18.4 image");
        let mut pull = Command::new("docker");
        pull.args(["pull", IMAGE_REFERENCE]).current_dir(root);
        run_status(&mut pull, "docker pull pinned PostgreSQL image")?;
    }

    println!("doctor-postgres: docker={docker_version}");
    println!("doctor-postgres: daemon={daemon_version}");
    println!("doctor-postgres: compose={compose_version}");
    println!("doctor-postgres: pinned PostgreSQL image available");
    println!("doctor-postgres: local infrastructure complete");
    Ok(())
}

pub fn up(
    root: &Path,
    project_name: &str,
    platform_port: Option<u16>,
    cell_port: Option<u16>,
) -> Result<()> {
    doctor(root)?;
    let state = LocalState {
        project: project_name.to_owned(),
        platform_port: platform_port.unwrap_or(DEFAULT_PLATFORM_PORT),
        cell_port: cell_port.unwrap_or(DEFAULT_CELL_PORT),
    };
    let mut project = LocalProject::prepare(root, state, true)?;
    project.start()?;
    project.save_state()?;
    project.disarm_cleanup();

    println!("postgres-up: platform-postgres=healthy");
    println!("postgres-up: cell-postgres=healthy");
    println!("postgres-up: platform-port={}", project.state.platform_port);
    println!("postgres-up: cell-port={}", project.state.cell_port);
    for specification in CREDENTIAL_SPECS {
        println!(
            "postgres-up: {}-{}-reference=file:{}",
            specification.authority,
            specification.purpose,
            project.url_path(*specification).display()
        );
    }
    println!("next: cargo xtask migrate-local --project {project_name}");
    println!("cleanup: cargo xtask postgres-down --project {project_name}");
    Ok(())
}

pub fn down(root: &Path, project_name: &str) -> Result<()> {
    let Some(mut project) = LocalProject::from_state(root, project_name)? else {
        println!("postgres-down: project `{project_name}` is already absent");
        return Ok(());
    };
    project.cleanup()?;
    println!("postgres-down: containers, volumes, and generated credentials removed");
    Ok(())
}

pub fn migrate_local(root: &Path, project_name: &str) -> Result<()> {
    let project = LocalProject::from_state(root, project_name)?.ok_or_else(|| {
        anyhow!("manual PostgreSQL project is not running; run postgres-up first")
    })?;
    require_healthy(&project)?;
    run_migrations(&project)
}

pub fn verify(root: &Path, profile: QualificationProfile) -> Result<()> {
    doctor(root)?;
    run_disposable(root, profile, None, true)
}

pub fn qualify(
    root: &Path,
    profile: QualificationProfile,
    output: &Path,
    replace: bool,
) -> Result<()> {
    doctor(root)?;
    let output = if output.is_absolute() {
        output.to_path_buf()
    } else {
        root.join(output)
    };
    guard_evidence_replacement(&output, replace)?;
    run_disposable(root, profile, Some(&output), false)
}

fn run_disposable(
    root: &Path,
    profile: QualificationProfile,
    requested_output: Option<&Path>,
    run_binary_checks: bool,
) -> Result<()> {
    let (platform_port, cell_port) = allocate_loopback_ports()?;
    let state = LocalState {
        project: unique_project_name()?,
        platform_port,
        cell_port,
    };
    let mut project = LocalProject::prepare(root, state, true)?;
    let output = requested_output.map_or_else(
        || project.secret_dir.join("qualification-evidence"),
        Path::to_path_buf,
    );

    let operation = (|| {
        println!(
            "verify-postgres: starting disposable Platform and Cell authorities (profile={})",
            profile.as_str()
        );
        project.start()?;
        run_migrations(&project)?;
        run_qualification(&project, profile, &output, requested_output.is_some())?;
        if run_binary_checks {
            run_database_binary_checks(&project)?;
            verify_router_rejects_database(&project)?;
        }
        Ok(())
    })();
    let cleanup = project.cleanup();
    match (operation, cleanup) {
        (Ok(()), Ok(())) => {
            println!(
                "verify-postgres: profile={} passed; disposable authorities removed",
                profile.as_str()
            );
            Ok(())
        }
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(operation_error), Err(cleanup_error)) => Err(anyhow!(
            "PostgreSQL verification failed: {operation_error}; cleanup also failed: {cleanup_error}"
        )),
    }
}

fn run_migrations(project: &LocalProject<'_>) -> Result<()> {
    println!("postgres: migrating Platform authority");
    run_database_binary(
        project,
        "db-migrator",
        None,
        &[
            ("EDTECH__MIGRATION_SCOPE", "platform"),
            (
                "EDTECH__DATABASE__CREDENTIAL_REF",
                &project.reference("platform", "migrator")?,
            ),
        ],
    )?;
    println!("postgres: migrating Cell authority cell-001");
    run_database_binary(
        project,
        "db-migrator",
        None,
        &[
            ("EDTECH__MIGRATION_SCOPE", "cell"),
            ("EDTECH__CELL_ID", "cell-001"),
            (
                "EDTECH__DATABASE__CREDENTIAL_REF",
                &project.reference("cell", "migrator")?,
            ),
        ],
    )
}

fn run_qualification(
    project: &LocalProject<'_>,
    profile: QualificationProfile,
    output: &Path,
    replace: bool,
) -> Result<()> {
    let mut command = Command::new("cargo");
    command
        .args([
            "run",
            "--quiet",
            "--locked",
            "--package",
            "postgres-qualification",
            "--",
            "--profile",
            profile.as_str(),
            "--output",
        ])
        .arg(output)
        .current_dir(project.root);
    if replace {
        command.arg("--replace");
    }
    for (name, authority, purpose) in [
        (
            "EDTECH_QUAL_PLATFORM_BOOTSTRAP_REF",
            "platform",
            "bootstrap",
        ),
        ("EDTECH_QUAL_PLATFORM_MIGRATOR_REF", "platform", "migrator"),
        ("EDTECH_QUAL_PLATFORM_API_REF", "platform", "api"),
        ("EDTECH_QUAL_PLATFORM_WORKER_REF", "platform", "worker"),
        ("EDTECH_QUAL_CELL_BOOTSTRAP_REF", "cell", "bootstrap"),
        ("EDTECH_QUAL_CELL_MIGRATOR_REF", "cell", "migrator"),
        ("EDTECH_QUAL_CELL_API_REF", "cell", "api"),
        ("EDTECH_QUAL_CELL_WORKER_REF", "cell", "worker"),
    ] {
        command.env(name, project.reference(authority, purpose)?);
    }
    run_status(&mut command, "PostgreSQL qualification")
}

fn run_database_binary_checks(project: &LocalProject<'_>) -> Result<()> {
    println!("postgres: checking all database-enabled process roots");
    for (package, authority, purpose, cell_id) in [
        ("platform-api", "platform", "api", None),
        ("platform-worker", "platform", "worker", None),
        ("cell-api", "cell", "api", Some("cell-001")),
        ("cell-worker", "cell", "worker", Some("cell-001")),
    ] {
        let reference = project.reference(authority, purpose)?;
        let mut environment = vec![("EDTECH__DATABASE__CREDENTIAL_REF", reference.as_str())];
        if let Some(cell_id) = cell_id {
            environment.push(("EDTECH__CELL_ID", cell_id));
        }
        run_database_binary(project, package, Some("--check-database"), &environment)?;
    }
    for (authority, extra) in [
        ("platform", None),
        ("cell", Some(("EDTECH__CELL_ID", "cell-001"))),
    ] {
        let reference = project.reference(authority, "migrator")?;
        let mut environment = vec![
            ("EDTECH__MIGRATION_SCOPE", authority),
            ("EDTECH__DATABASE__CREDENTIAL_REF", reference.as_str()),
        ];
        if let Some(extra) = extra {
            environment.push(extra);
        }
        run_database_binary(
            project,
            "db-migrator",
            Some("--check-database"),
            &environment,
        )?;
    }
    Ok(())
}

fn run_database_binary(
    project: &LocalProject<'_>,
    package: &str,
    argument: Option<&str>,
    environment: &[(&str, &str)],
) -> Result<()> {
    let mut command = Command::new("cargo");
    command
        .args(["run", "--quiet", "--locked", "--package", package, "--"])
        .current_dir(project.root);
    if let Some(argument) = argument {
        command.arg(argument);
    }
    clear_edtech_environment(&mut command);
    command
        .env("EDTECH__ENVIRONMENT", "dev")
        .env("EDTECH__DATABASE__TLS_MODE", "disable")
        .env("EDTECH__DATABASE__MAX_CONNECTIONS", "4")
        .env("EDTECH__DATABASE__MIN_CONNECTIONS", "0");
    if package == "db-migrator" {
        command.env("EDTECH__MIGRATION_TIMEOUT_MS", "600000");
    }
    for (key, value) in environment {
        command.env(key, value);
    }
    run_status(&mut command, package)
}

fn verify_router_rejects_database(project: &LocalProject<'_>) -> Result<()> {
    let reference = project.reference("platform", "api")?;
    let mut command = Command::new("cargo");
    command
        .args([
            "run",
            "--quiet",
            "--locked",
            "--package",
            "tenant-router",
            "--",
            "--check-config",
        ])
        .current_dir(project.root);
    clear_edtech_environment(&mut command);
    command
        .env("EDTECH__ENVIRONMENT", "dev")
        .env("EDTECH__DATABASE__CREDENTIAL_REF", reference)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let status = command
        .status()
        .context("could not start tenant-router database-rejection check")?;
    if status.success() {
        bail!("tenant-router accepted forbidden database configuration");
    }
    println!("postgres: tenant-router rejected database configuration");
    Ok(())
}

fn require_healthy(project: &LocalProject<'_>) -> Result<()> {
    let mut command = project.compose_command();
    command
        .args(["ps", "--status", "running", "--format", "json"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let output = command
        .output()
        .context("could not inspect local PostgreSQL services")?;
    if !output.status.success() {
        bail!("manual PostgreSQL project is not healthy");
    }
    let rendered =
        String::from_utf8(output.stdout).context("docker compose service state was not UTF-8")?;
    if !rendered.contains("platform-postgres") || !rendered.contains("cell-postgres") {
        bail!("manual PostgreSQL project does not have both authorities running");
    }
    Ok(())
}

fn guard_evidence_replacement(output: &Path, replace: bool) -> Result<()> {
    let json = output.join("postgres-qualification.json");
    let markdown = output.join("postgres-qualification.md");
    if !replace && (json.exists() || markdown.exists()) {
        bail!(
            "qualification evidence already exists; pass --replace to overwrite it intentionally"
        );
    }
    Ok(())
}

fn unique_project_name() -> Result<String> {
    let suffix = random_hex(4)?;
    Ok(format!("edtech-pg-{}-{suffix}", std::process::id()))
}

fn allocate_loopback_ports() -> Result<(u16, u16)> {
    let platform = TcpListener::bind(("127.0.0.1", 0))
        .context("could not allocate a loopback port for Platform PostgreSQL")?;
    let cell = TcpListener::bind(("127.0.0.1", 0))
        .context("could not allocate a loopback port for Cell PostgreSQL")?;
    let platform_port = platform
        .local_addr()
        .context("could not inspect the Platform loopback port")?
        .port();
    let cell_port = cell
        .local_addr()
        .context("could not inspect the Cell loopback port")?
        .port();
    drop((platform, cell));
    validate_ports(platform_port, cell_port)?;
    Ok((platform_port, cell_port))
}

fn validate_project_name(value: &str) -> Result<()> {
    let valid = (3..=63).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && !value.contains("--");
    if !valid {
        bail!("Compose project name must be a safe lowercase identifier");
    }
    Ok(())
}

fn validate_ports(platform_port: u16, cell_port: u16) -> Result<()> {
    if platform_port == 0 || cell_port == 0 || platform_port == cell_port {
        bail!("Platform and Cell PostgreSQL require distinct non-zero loopback ports");
    }
    Ok(())
}

fn random_hex(byte_count: usize) -> Result<String> {
    let mut bytes = vec![0_u8; byte_count];
    getrandom::fill(&mut bytes)
        .map_err(|_| anyhow!("secure random credential generation failed"))?;
    Ok(hex_encode(&bytes))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn database_url(role: &str, password: &str, port: u16, database: &str) -> String {
    let mut value = String::from("postgres");
    value.push_str("ql://");
    value.push_str(role);
    value.push(':');
    value.push_str(password);
    value.push_str("@127.0.0.1:");
    value.push_str(&port.to_string());
    value.push('/');
    value.push_str(database);
    value
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::write(path, bytes).with_context(|| format!("could not write {}", path.display()))?;
    restrict_file(path)
}

#[cfg(unix)]
fn restrict_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("could not restrict permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn restrict_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn restrict_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("could not restrict permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn restrict_file(_path: &Path) -> Result<()> {
    Ok(())
}

fn clear_edtech_environment(command: &mut Command) {
    for (key, _) in env::vars_os()
        .filter(|(key, _)| key.to_str().is_some_and(|key| key.starts_with("EDTECH__")))
    {
        command.env_remove(key);
    }
    command.env_remove("EDTECH_CONFIG_FILE");
}

fn capture_safe(root: &Path, program: &str, arguments: &[&str], label: &str) -> Result<String> {
    let output = Command::new(program)
        .args(arguments)
        .current_dir(root)
        .stderr(Stdio::null())
        .output()
        .with_context(|| format!("could not start {label}"))?;
    if !output.status.success() {
        bail!("{label} check failed with exit status {}", output.status);
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .with_context(|| format!("{label} output was not UTF-8"))
}

fn run_status(command: &mut Command, label: &str) -> Result<()> {
    let status = command
        .status()
        .with_context(|| format!("could not start {label}"))?;
    if status.success() {
        Ok(())
    } else {
        bail!("{label} failed with exit status {status}")
    }
}

#[cfg(test)]
mod tests {
    use super::{hex_encode, validate_ports, validate_project_name};

    #[test]
    fn local_credential_hex_encoding_is_lowercase_and_exact() {
        assert_eq!(hex_encode(&[0x00, 0x0f, 0x10, 0xab, 0xff]), "000f10abff");
    }

    #[test]
    fn project_names_and_ports_are_bounded() {
        assert!(validate_project_name("edtech-local").is_ok());
        assert!(validate_project_name("../unsafe").is_err());
        assert!(validate_project_name("UPPERCASE").is_err());
        assert!(validate_ports(55_432, 55_433).is_ok());
        assert!(validate_ports(55_432, 55_432).is_err());
    }
}
