//! Local three-node TLS NATS lifecycle and qualification orchestration.

use std::{
    env, fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

const COMPOSE_FILE: &str = "infra/local/nats/compose.yml";
const IMAGE_LOCK_FILE: &str = "infra/local/nats/nats-image.lock.toml";
const SERVER_TEMPLATE_FILE: &str = "infra/local/nats/templates/nats-server.conf.tmpl";
const TOPOLOGY_FILE: &str = "infra/local/nats/templates/topology.toml";
const STATE_FILE: &str = "state.json";
const DEFAULT_CLIENT_PORTS: [u16; 3] = [54_222, 54_223, 54_224];
const DEFAULT_MONITOR_PORTS: [u16; 3] = [58_222, 58_223, 58_224];

#[derive(Debug, Deserialize)]
struct ImageLock {
    repository: String,
    tag: String,
    index_digest: String,
    server_version: String,
    required_platforms: Vec<String>,
}

#[derive(Deserialize)]
struct RawImageIndex {
    #[serde(rename = "mediaType")]
    media_type: String,
}

impl ImageLock {
    fn reference(&self) -> String {
        format!("{}:{}@{}", self.repository, self.tag, self.index_digest)
    }

    fn tag_reference(&self) -> String {
        format!("{}:{}", self.repository, self.tag)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LocalState {
    project: String,
    client_ports: [u16; 3],
    monitor_ports: [u16; 3],
}

#[derive(Serialize)]
struct CredentialFile<'a> {
    username: &'a str,
    password: &'a str,
}

pub struct NatsProject<'a> {
    root: &'a Path,
    state: LocalState,
    state_dir: PathBuf,
    cleanup_armed: bool,
}

impl<'a> NatsProject<'a> {
    fn prepare(root: &'a Path, state: LocalState, cleanup_armed: bool) -> Result<NatsProject<'a>> {
        validate_project_name(&state.project)?;
        validate_ports(state.client_ports, state.monitor_ports)?;
        let state_dir = root.join("target/local-nats").join(&state.project);
        if state_dir.exists() {
            bail!(
                "local NATS state already exists for project `{}`; run nats-down first",
                state.project
            );
        }
        for relative in ["config", "credentials", "tls"] {
            let directory = state_dir.join(relative);
            fs::create_dir_all(&directory).with_context(|| {
                format!("could not create local NATS generated directory {relative}")
            })?;
            restrict_directory(&directory)?;
        }
        restrict_directory(&state_dir)?;
        let state_dir = fs::canonicalize(&state_dir)
            .context("could not canonicalize local NATS generated directory")?;
        let project = NatsProject {
            root,
            state,
            state_dir,
            cleanup_armed,
        };
        if let Err(error) = project.generate() {
            let _remove_result = fs::remove_dir_all(&project.state_dir);
            return Err(error);
        }
        Ok(project)
    }

    pub fn from_state(root: &'a Path, project_name: &str) -> Result<Option<NatsProject<'a>>> {
        validate_project_name(project_name)?;
        let state_dir = root.join("target/local-nats").join(project_name);
        let state_path = state_dir.join(STATE_FILE);
        if !state_path.exists() {
            return Ok(None);
        }
        let contents =
            fs::read_to_string(&state_path).context("could not read local NATS state metadata")?;
        let state: LocalState = serde_json::from_str(&contents)
            .context("could not decode local NATS state metadata")?;
        if state.project != project_name {
            bail!("local NATS state does not match the requested project");
        }
        validate_ports(state.client_ports, state.monitor_ports)?;
        Ok(Some(NatsProject {
            root,
            state,
            state_dir,
            cleanup_armed: false,
        }))
    }

    fn generate(&self) -> Result<()> {
        let route_password = random_hex(32)?;
        let system_password = random_hex(32)?;
        let provisioner_password = random_hex(32)?;
        let platform_password = random_hex(32)?;
        let cell_password = random_hex(32)?;
        let qualification_password = random_hex(32)?;
        let inspector_password = random_hex(32)?;

        self.generate_certificates()?;
        let template = fs::read_to_string(self.root.join(SERVER_TEMPLATE_FILE))
            .context("could not read NATS server template")?;
        for (index, server_name) in ["nats-1", "nats-2", "nats-3"].iter().enumerate() {
            let mut rendered = template.clone();
            for (placeholder, value) in [
                ("{{SERVER_NAME}}", *server_name),
                (
                    "{{AZ_NUMBER}}",
                    &u8::try_from(index + 1).unwrap_or(3).to_string(),
                ),
                ("{{ROUTE_USERNAME}}", "edtech_route"),
                ("{{ROUTE_PASSWORD}}", route_password.as_str()),
                ("{{SYSTEM_USERNAME}}", "edtech_system_inspector"),
                ("{{SYSTEM_PASSWORD}}", system_password.as_str()),
                ("{{PROVISIONER_PASSWORD}}", provisioner_password.as_str()),
                ("{{PLATFORM_WORKER_PASSWORD}}", platform_password.as_str()),
                ("{{CELL_WORKER_PASSWORD}}", cell_password.as_str()),
                (
                    "{{QUALIFICATION_PASSWORD}}",
                    qualification_password.as_str(),
                ),
                ("{{INSPECTOR_PASSWORD}}", inspector_password.as_str()),
            ] {
                rendered = rendered.replace(placeholder, value);
            }
            if rendered.contains("{{") || rendered.contains("}}") {
                bail!("NATS server template contains an unresolved placeholder");
            }
            write_private_file(
                &self
                    .state_dir
                    .join("config")
                    .join(format!("{server_name}.conf")),
                rendered.as_bytes(),
            )?;
        }

        for (name, username, password) in [
            (
                "system",
                "edtech_system_inspector",
                system_password.as_str(),
            ),
            (
                "provisioner",
                "edtech_nats_provisioner",
                provisioner_password.as_str(),
            ),
            (
                "platform-worker",
                "edtech_platform_worker",
                platform_password.as_str(),
            ),
            (
                "cell-worker",
                "edtech_cell_cell-001_worker",
                cell_password.as_str(),
            ),
            (
                "qualification-injector",
                "edtech_qualification_injector",
                qualification_password.as_str(),
            ),
            (
                "qualification-inspector",
                "edtech_qualification_inspector",
                inspector_password.as_str(),
            ),
        ] {
            let bytes = serde_json::to_vec(&CredentialFile { username, password })
                .context("could not encode generated NATS credential")?;
            write_private_file(&self.credential_path(name), &bytes)?;
        }
        let state =
            serde_json::to_vec_pretty(&self.state).context("could not encode local NATS state")?;
        write_private_file(&self.state_dir.join(STATE_FILE), &state)
    }

    fn generate_certificates(&self) -> Result<()> {
        let tls = self.state_dir.join("tls");
        let mut ca = Command::new("openssl");
        ca.args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-days",
            "7",
            "-subj",
            "/CN=edtech-local-nats-ca",
            "-keyout",
        ])
        .arg(tls.join("ca-key.pem"))
        .arg("-out")
        .arg(tls.join("ca.pem"));
        run_quiet(&mut ca, "OpenSSL local CA generation")?;

        for server_name in ["nats-1", "nats-2", "nats-3"] {
            let key = tls.join(format!("{server_name}-key.pem"));
            let request = tls.join(format!("{server_name}.csr"));
            let certificate = tls.join(format!("{server_name}.pem"));
            let extension = format!("subjectAltName=DNS:{server_name},DNS:localhost,IP:127.0.0.1");
            let mut csr = Command::new("openssl");
            csr.args([
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-sha256",
                "-nodes",
                "-subj",
                &format!("/CN={server_name}"),
                "-addext",
                &extension,
                "-keyout",
            ])
            .arg(&key)
            .arg("-out")
            .arg(&request);
            run_quiet(&mut csr, "OpenSSL server request generation")?;

            let mut sign = Command::new("openssl");
            sign.args([
                "x509",
                "-req",
                "-sha256",
                "-days",
                "7",
                "-copy_extensions",
                "copy",
            ])
            .arg("-in")
            .arg(&request)
            .arg("-CA")
            .arg(tls.join("ca.pem"))
            .arg("-CAkey")
            .arg(tls.join("ca-key.pem"))
            .arg("-CAcreateserial")
            .arg("-out")
            .arg(&certificate);
            run_quiet(&mut sign, "OpenSSL server certificate signing")?;
            restrict_file(&key)?;
            restrict_file(&certificate)?;
            restrict_file(&request)?;
        }
        restrict_file(&tls.join("ca-key.pem"))?;
        restrict_file(&tls.join("ca.pem"))?;
        Ok(())
    }

    pub fn start(&self) -> Result<()> {
        let mut command = self.compose_command();
        command.args(["up", "--detach", "--wait", "--wait-timeout", "120"]);
        let status = command
            .status()
            .context("could not start docker compose NATS up")?;
        if status.success() {
            return Ok(());
        }
        let mut logs = self.compose_command();
        let output = logs
            .args(["logs", "--no-color", "--tail", "30"])
            .output()
            .context("could not inspect failed NATS startup")?;
        let combined = format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let safe = combined
            .lines()
            .filter(|line| {
                let normalized = line.to_ascii_lowercase();
                (normalized.contains("error")
                    || normalized.contains("unknown field")
                    || normalized.contains("parse"))
                    && !normalized.contains("password")
                    && !normalized.contains("credential")
            })
            .take(6)
            .collect::<Vec<_>>()
            .join(" | ");
        if safe.is_empty() {
            bail!("docker compose NATS up failed with exit status {status}");
        }
        bail!("docker compose NATS up failed: {safe}")
    }

    pub fn stop_and_remove(&self) -> Result<()> {
        let mut command = self.compose_command();
        command.args(["down", "--volumes", "--remove-orphans", "--timeout", "10"]);
        run_status(&mut command, "docker compose NATS down")
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
            .env("EDTECH_NATS_STATE_DIR", &self.state_dir);
        for index in 0..3 {
            command.env(
                format!("EDTECH_NATS_{}_CLIENT_PORT", index + 1),
                self.state.client_ports[index].to_string(),
            );
            command.env(
                format!("EDTECH_NATS_{}_MONITOR_PORT", index + 1),
                self.state.monitor_ports[index].to_string(),
            );
        }
        command
    }

    pub fn cleanup(&mut self) -> Result<()> {
        let stop_result = self.stop_and_remove();
        let remove_result = if self.state_dir.exists() {
            fs::remove_dir_all(&self.state_dir)
                .context("could not remove generated NATS credentials and configuration")
        } else {
            Ok(())
        };
        self.cleanup_armed = false;
        match (stop_result, remove_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(first), Err(second)) => Err(anyhow!(
                "NATS cleanup failed: {first}; generated-state cleanup also failed: {second}"
            )),
        }
    }

    fn disarm_cleanup(&mut self) {
        self.cleanup_armed = false;
    }

    pub fn project_name(&self) -> &str {
        &self.state.project
    }

    pub fn server_list(&self) -> String {
        self.state
            .client_ports
            .iter()
            .map(|port| format!("tls://127.0.0.1:{port}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn ca_path(&self) -> PathBuf {
        self.state_dir.join("tls/ca.pem")
    }

    pub fn state_directory(&self) -> &Path {
        &self.state_dir
    }

    pub fn monitor_ports(&self) -> [u16; 3] {
        self.state.monitor_ports
    }

    pub fn credential_path(&self, name: &str) -> PathBuf {
        self.state_dir
            .join("credentials")
            .join(format!("{name}.json"))
    }

    pub fn credential_reference(&self, name: &str) -> String {
        format!("file:{}", self.credential_path(name).display())
    }
}

impl Drop for NatsProject<'_> {
    fn drop(&mut self) {
        if self.cleanup_armed {
            let _stop_result = self.stop_and_remove();
            let _remove_result = fs::remove_dir_all(&self.state_dir);
        }
    }
}

#[allow(clippy::too_many_lines)]
pub fn doctor(root: &Path) -> Result<()> {
    let required = [
        COMPOSE_FILE,
        IMAGE_LOCK_FILE,
        SERVER_TEMPLATE_FILE,
        TOPOLOGY_FILE,
        "infra/local/nats/README.md",
    ];
    let missing = required
        .iter()
        .filter(|path| !root.join(path).is_file())
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "local NATS infrastructure is incomplete: {}",
            missing.join(", ")
        );
    }
    let lock = read_image_lock(root)?;
    if lock.server_version != "2.14.3"
        || lock.required_platforms != [String::from("linux/amd64"), String::from("linux/arm64/v8")]
    {
        bail!("NATS image lock has an unexpected version or platform set");
    }
    let compose = fs::read_to_string(root.join(COMPOSE_FILE))
        .context("could not read local NATS Compose file")?;
    if compose.matches(&lock.reference()).count() != 3 {
        bail!("NATS Compose image references do not exactly match the image lock");
    }

    let docker = capture_safe(root, "docker", &["--version"], "Docker version")?;
    let daemon = capture_safe(
        root,
        "docker",
        &["info", "--format", "{{.ServerVersion}}"],
        "Docker daemon",
    )?;
    let compose_version = capture_safe(
        root,
        "docker",
        &["compose", "version", "--short"],
        "Docker Compose",
    )?;
    let openssl = capture_safe(root, "openssl", &["version"], "OpenSSL")?;

    let present = Command::new("docker")
        .args(["image", "inspect", &lock.reference()])
        .current_dir(root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("could not inspect pinned NATS image")?
        .success();
    if !present {
        let mut pull = Command::new("docker");
        pull.args(["pull", &lock.reference()]).current_dir(root);
        run_status(&mut pull, "docker pull pinned NATS image")?;
    }

    let index = capture_safe(
        root,
        "docker",
        &["buildx", "imagetools", "inspect", &lock.tag_reference()],
        "NATS image index",
    )?;
    if !index.contains(&format!("Digest:    {}", lock.index_digest))
        || !index.contains("linux/amd64")
        || !index.contains("linux/arm64/v8")
    {
        bail!("online NATS tag resolution does not match the locked multi-platform index");
    }
    let raw_index = capture_safe(
        root,
        "docker",
        &[
            "buildx",
            "imagetools",
            "inspect",
            "--raw",
            &lock.tag_reference(),
        ],
        "NATS raw image index",
    )?;
    let raw_index: RawImageIndex =
        serde_json::from_str(&raw_index).context("could not decode the raw NATS image index")?;
    if raw_index.media_type != "application/vnd.oci.image.index.v1+json" {
        bail!("locked NATS digest is not an OCI multi-platform image index");
    }

    let tracked = capture_safe(
        root,
        "git",
        &["ls-files", "target/local-nats"],
        "generated NATS tracking check",
    )?;
    if !tracked.is_empty() {
        bail!("a generated NATS credential directory is tracked by Git");
    }
    let _ports = allocate_loopback_ports()?;

    println!("doctor-nats: docker={docker}");
    println!("doctor-nats: daemon={daemon}");
    println!("doctor-nats: compose={compose_version}");
    println!("doctor-nats: openssl={openssl}");
    println!(
        "doctor-nats: image={} index={} platforms=amd64,arm64",
        lock.tag_reference(),
        lock.index_digest
    );
    println!("doctor-nats: templates and loopback port allocation passed");
    Ok(())
}

pub fn up(
    root: &Path,
    project_name: &str,
    client_ports: Option<[u16; 3]>,
    monitor_ports: Option<[u16; 3]>,
) -> Result<()> {
    doctor(root)?;
    let state = LocalState {
        project: project_name.to_owned(),
        client_ports: client_ports.unwrap_or(DEFAULT_CLIENT_PORTS),
        monitor_ports: monitor_ports.unwrap_or(DEFAULT_MONITOR_PORTS),
    };
    let mut project = NatsProject::prepare(root, state, true)?;
    project.start()?;
    project.disarm_cleanup();
    println!("nats-up: three-node TLS JetStream cluster healthy");
    for (index, port) in project.state.client_ports.iter().enumerate() {
        println!("nats-up: nats-{} client-port={port}", index + 1);
    }
    for name in [
        "provisioner",
        "platform-worker",
        "cell-worker",
        "qualification-injector",
        "qualification-inspector",
    ] {
        println!(
            "nats-up: {name}-reference={}",
            project.credential_reference(name)
        );
    }
    println!("next: cargo xtask provision-nats-local --project {project_name}");
    println!("cleanup: cargo xtask nats-down --project {project_name}");
    Ok(())
}

pub fn down(root: &Path, project_name: &str) -> Result<()> {
    let Some(mut project) = NatsProject::from_state(root, project_name)? else {
        println!("nats-down: project `{project_name}` is already absent");
        return Ok(());
    };
    project.cleanup()?;
    println!(
        "nats-down: containers, network, volumes, TLS, configuration, and credentials removed"
    );
    Ok(())
}

pub fn provision_local(root: &Path, project_name: &str) -> Result<()> {
    let project = NatsProject::from_state(root, project_name)?
        .ok_or_else(|| anyhow!("manual NATS project is absent; run nats-up first"))?;
    run_provisioner(&project, None)?;
    run_provisioner(&project, Some("--check-transport"))
}

pub fn run_provisioner(project: &NatsProject<'_>, argument: Option<&str>) -> Result<()> {
    let mut command = Command::new("cargo");
    command
        .args([
            "run",
            "--quiet",
            "--locked",
            "--package",
            "nats-provisioner",
            "--",
        ])
        .current_dir(project.root);
    if let Some(argument) = argument {
        command.arg(argument);
    }
    clear_edtech_environment(&mut command);
    command
        .env("EDTECH__ENVIRONMENT", "dev")
        .env("EDTECH__TRANSPORT__SERVERS", project.server_list())
        .env(
            "EDTECH__TRANSPORT__CREDENTIAL_REF",
            project.credential_reference("provisioner"),
        )
        .env("EDTECH__TRANSPORT__TLS_MODE", "verify_full")
        .env("EDTECH__TRANSPORT__CA_CERTIFICATE_FILE", project.ca_path())
        .env("EDTECH__TOPOLOGY_FILE", project.root.join(TOPOLOGY_FILE))
        .env("EDTECH__TOPOLOGY_APPLY_TIMEOUT_MS", "120000");
    run_status(&mut command, "NATS topology provisioner")
}

pub fn disposable(root: &Path) -> Result<NatsProject<'_>> {
    let (client_ports, monitor_ports) = allocate_loopback_ports()?;
    let state = LocalState {
        project: unique_project_name()?,
        client_ports,
        monitor_ports,
    };
    NatsProject::prepare(root, state, true)
}

fn read_image_lock(root: &Path) -> Result<ImageLock> {
    let contents =
        fs::read_to_string(root.join(IMAGE_LOCK_FILE)).context("could not read NATS image lock")?;
    toml::from_str(&contents).context("could not decode NATS image lock")
}

fn unique_project_name() -> Result<String> {
    Ok(format!(
        "edtech-nats-{}-{}",
        std::process::id(),
        random_hex(4)?
    ))
}

fn allocate_loopback_ports() -> Result<([u16; 3], [u16; 3])> {
    let listeners = (0..6)
        .map(|_| {
            TcpListener::bind(("127.0.0.1", 0)).context("could not allocate a NATS loopback port")
        })
        .collect::<Result<Vec<_>>>()?;
    let ports = listeners
        .iter()
        .map(|listener| {
            listener
                .local_addr()
                .map(|address| address.port())
                .context("could not inspect a NATS loopback port")
        })
        .collect::<Result<Vec<_>>>()?;
    let client_ports = [ports[0], ports[1], ports[2]];
    let monitor_ports = [ports[3], ports[4], ports[5]];
    drop(listeners);
    validate_ports(client_ports, monitor_ports)?;
    Ok((client_ports, monitor_ports))
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

fn validate_ports(client: [u16; 3], monitor: [u16; 3]) -> Result<()> {
    let mut ports = client.iter().chain(&monitor).copied().collect::<Vec<_>>();
    ports.sort_unstable();
    ports.dedup();
    if ports.len() != 6 || ports.contains(&0) {
        bail!("NATS requires six distinct non-zero loopback ports");
    }
    Ok(())
}

fn random_hex(byte_count: usize) -> Result<String> {
    let mut bytes = vec![0_u8; byte_count];
    getrandom::fill(&mut bytes).map_err(|_| anyhow!("secure credential generation failed"))?;
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

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::write(path, bytes).context("could not write generated NATS file")?;
    restrict_file(path)
}

#[cfg(unix)]
fn restrict_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .context("could not restrict generated NATS directory")
}

#[cfg(not(unix))]
fn restrict_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn restrict_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .context("could not restrict generated NATS file")
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

fn run_quiet(command: &mut Command, label: &str) -> Result<()> {
    command.stdout(Stdio::null()).stderr(Stdio::null());
    run_status(command, label)
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
    fn local_names_ports_and_secret_encoding_are_bounded() {
        assert!(validate_project_name("edtech-nats-local").is_ok());
        assert!(validate_project_name("../unsafe").is_err());
        assert!(validate_ports([42_221, 42_222, 42_223], [52_221, 52_222, 52_223]).is_ok());
        assert!(validate_ports([42_221, 42_221, 42_223], [52_221, 52_222, 52_223]).is_err());
        assert_eq!(hex_encode(&[0, 15, 16, 255]), "000f10ff");
    }
}
