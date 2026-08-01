//! Canonical, cross-platform repository workflows for Checkpoint 1.
//!
//! `xtask` performs toolchain diagnostics, static architecture enforcement, configuration smoke
//! checks, and the deterministic full verification sequence without external services.

use std::{
    collections::{BTreeSet, HashSet},
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use serde::Deserialize;

const REQUIRED_RUST_VERSION: &str = "1.97.1";
const RULES_PATH: &str = "architecture/dependency-rules.json";

#[derive(Debug, Parser)]
#[command(name = "xtask", about = "Canonical repository workflows")]
struct Cli {
    #[command(subcommand)]
    command: RepositoryCommand,
}

#[derive(Debug, Subcommand)]
enum RepositoryCommand {
    /// Report whether the pinned local development prerequisites are present.
    Doctor,
    /// Enforce workspace dependency and source-boundary rules.
    VerifyArchitecture,
    /// Validate every process composition root with synthetic configuration.
    Smoke,
    /// Run the complete deterministic repository verification sequence.
    Verify,
}

fn main() -> Result<()> {
    let root = workspace_root()?;
    match Cli::parse().command {
        RepositoryCommand::Doctor => doctor(&root),
        RepositoryCommand::VerifyArchitecture => verify_architecture(&root),
        RepositoryCommand::Smoke => smoke(&root),
        RepositoryCommand::Verify => verify(&root),
    }
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_directory = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_directory
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("xtask is not located under tools/xtask"))
}

fn doctor(root: &Path) -> Result<()> {
    let rustc_version = capture(root, "rustc", &["--version"])?;
    if !rustc_version.starts_with(&format!("rustc {REQUIRED_RUST_VERSION} ")) {
        bail!("selected compiler is `{rustc_version}`; expected rustc {REQUIRED_RUST_VERSION}");
    }

    let cargo_version = capture(root, "cargo", &["--version"])?;
    let verbose_rustc = capture(root, "rustc", &["-vV"])?;
    let host = verbose_rustc
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or_else(|| anyhow!("rustc -vV did not report a host target"))?;
    let rustfmt_version = capture(root, "rustfmt", &["--version"])?;
    let clippy_version = capture(root, "cargo", &["clippy", "--version"])?;

    let required_paths = [
        ".cargo/config.toml",
        ".github/workflows/ci.yml",
        "architecture/dependency-rules.json",
        "apps/platform-api",
        "apps/platform-worker",
        "apps/tenant-router",
        "apps/cell-api",
        "apps/cell-worker",
        "apps/db-migrator",
        "crates/tenancy-domain",
        "crates/provisioning-domain",
        "crates/auth-context",
        "crates/audit-domain",
        "crates/platform-application",
        "crates/cell-application",
        "crates/routing-application",
        "crates/runtime-config",
        "crates/process-lifecycle",
        "crates/test-support",
        "docs/adr/0001-workspace-foundation.md",
        "docs/architecture/invariants.md",
        "docs/checkpoints/01-workspace-foundation.md",
        "tools/xtask",
        ".editorconfig",
        ".gitignore",
        "Cargo.lock",
        "Cargo.toml",
        "CONTRIBUTING.md",
        "README.md",
        "rust-toolchain.toml",
        "rustfmt.toml",
    ];
    let missing = required_paths
        .iter()
        .filter(|path| !root.join(path).exists())
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "repository structure is incomplete; missing: {}",
            missing.join(", ")
        );
    }

    capture(
        root,
        "cargo",
        &["metadata", "--format-version", "1", "--locked", "--no-deps"],
    )?;

    println!("doctor: rustc={rustc_version}");
    println!("doctor: cargo={cargo_version}");
    println!("doctor: host={host}");
    println!("doctor: rustfmt={rustfmt_version}");
    println!("doctor: clippy={clippy_version}");
    println!("doctor: Cargo.lock present");
    println!("doctor: cargo metadata loaded");
    println!("doctor: repository structure complete");
    Ok(())
}

fn verify(root: &Path) -> Result<()> {
    doctor(root)?;
    run_checked(
        root,
        "cargo fmt --all -- --check",
        "cargo",
        &["fmt", "--all", "--", "--check"],
    )?;
    run_checked(
        root,
        "cargo check --workspace --all-targets --locked",
        "cargo",
        &["check", "--workspace", "--all-targets", "--locked"],
    )?;
    run_checked(
        root,
        "cargo clippy --workspace --all-targets --locked -- -D warnings",
        "cargo",
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ],
    )?;
    run_checked(
        root,
        "cargo test --workspace --all-targets --locked",
        "cargo",
        &["test", "--workspace", "--all-targets", "--locked"],
    )?;
    verify_architecture(root)?;
    smoke(root)
}

fn capture(root: &Path, program: &str, arguments: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(arguments)
        .current_dir(root)
        .output()
        .with_context(|| format!("could not start `{program} {}`", arguments.join(" ")))?;
    checked_output(program, arguments, output)
}

fn checked_output(program: &str, arguments: &[&str], output: Output) -> Result<String> {
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "`{program} {}` failed with {}: {}",
            arguments.join(" "),
            output.status,
            stderr.trim()
        );
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .context("command output was not UTF-8")
}

fn run_checked(root: &Path, label: &str, program: &str, arguments: &[&str]) -> Result<()> {
    println!("verify: {label}");
    let status = Command::new(program)
        .args(arguments)
        .current_dir(root)
        .status()
        .with_context(|| format!("could not start `{label}`"))?;
    if status.success() {
        Ok(())
    } else {
        bail!("`{label}` failed with exit status {status}")
    }
}

#[derive(Debug, Deserialize)]
struct DependencyRules {
    schema_version: u32,
    classes: RuleClasses,
    forbidden_package_names: BTreeSet<String>,
    allowed_workspace_edges: Vec<AllowedWorkspaceEdge>,
    allowed_external_dependencies: Vec<AllowedExternalDependency>,
}

#[derive(Debug, Default, Deserialize)]
struct RuleClasses {
    domain: BTreeSet<String>,
    application: BTreeSet<String>,
    platform_binary: BTreeSet<String>,
    cell_binary: BTreeSet<String>,
    deployable_binary: BTreeSet<String>,
    composition_support: BTreeSet<String>,
    test_support: BTreeSet<String>,
    tool: BTreeSet<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct AllowedWorkspaceEdge {
    from: String,
    to: String,
    kinds: BTreeSet<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct AllowedExternalDependency {
    from: String,
    dependency: String,
    kinds: BTreeSet<String>,
}

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
    workspace_members: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CargoPackage {
    id: String,
    name: String,
    manifest_path: PathBuf,
    dependencies: Vec<CargoDependency>,
}

#[derive(Debug, Deserialize)]
struct CargoDependency {
    name: String,
    source: Option<String>,
    req: String,
    kind: Option<String>,
    path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct GraphPackage {
    name: String,
    dependencies: Vec<GraphDependency>,
}

#[derive(Clone, Debug)]
struct GraphDependency {
    name: String,
    kind: String,
    requirement: String,
    workspace: bool,
    git: bool,
    outside_workspace_path: bool,
}

fn verify_architecture(root: &Path) -> Result<()> {
    let rules = read_rules(root)?;
    let metadata = read_metadata(root)?;
    let graph = graph_from_metadata(root, &metadata);
    let mut violations = validate_graph(&graph, &rules);
    violations.extend(validate_member_manifests(&metadata, root)?);
    violations.extend(validate_root_dependencies(root)?);
    violations.extend(validate_source_boundaries(root)?);
    violations.sort();
    violations.dedup();

    if violations.is_empty() {
        println!(
            "verify-architecture: {} workspace packages satisfy the declared dependency and source rules",
            graph.len()
        );
        Ok(())
    } else {
        let rendered = violations
            .iter()
            .map(|violation| format!("  - {violation}"))
            .collect::<Vec<_>>()
            .join("\n");
        bail!("architecture verification failed:\n{rendered}")
    }
}

fn read_rules(root: &Path) -> Result<DependencyRules> {
    let path = root.join(RULES_PATH);
    let contents =
        fs::read_to_string(&path).with_context(|| format!("could not read {}", path.display()))?;
    let rules: DependencyRules = serde_json::from_str(&contents)
        .with_context(|| format!("could not parse {}", path.display()))?;
    if rules.schema_version != 1 {
        bail!(
            "unsupported dependency-rule schema {}",
            rules.schema_version
        );
    }
    Ok(rules)
}

fn read_metadata(root: &Path) -> Result<CargoMetadata> {
    let json = capture(
        root,
        "cargo",
        &["metadata", "--format-version", "1", "--locked", "--no-deps"],
    )?;
    serde_json::from_str(&json).context("cargo metadata output could not be decoded")
}

fn graph_from_metadata(root: &Path, metadata: &CargoMetadata) -> Vec<GraphPackage> {
    let workspace_ids = metadata.workspace_members.iter().collect::<HashSet<_>>();
    let workspace_names = metadata
        .packages
        .iter()
        .filter(|package| workspace_ids.contains(&package.id))
        .map(|package| package.name.as_str())
        .collect::<HashSet<_>>();

    metadata
        .packages
        .iter()
        .filter(|package| workspace_ids.contains(&package.id))
        .map(|package| GraphPackage {
            name: package.name.clone(),
            dependencies: package
                .dependencies
                .iter()
                .map(|dependency| {
                    let path_is_inside = dependency
                        .path
                        .as_deref()
                        .and_then(|path| fs::canonicalize(path).ok())
                        .is_none_or(|path| path.starts_with(root));
                    let workspace = dependency.source.is_none()
                        && dependency.path.is_some()
                        && workspace_names.contains(dependency.name.as_str())
                        && path_is_inside;
                    GraphDependency {
                        name: dependency.name.clone(),
                        kind: dependency
                            .kind
                            .clone()
                            .unwrap_or_else(|| String::from("normal")),
                        requirement: dependency.req.clone(),
                        workspace,
                        git: dependency
                            .source
                            .as_deref()
                            .is_some_and(|source| source.starts_with("git+")),
                        outside_workspace_path: dependency.path.is_some() && !path_is_inside,
                    }
                })
                .collect(),
        })
        .collect()
}

fn validate_graph(graph: &[GraphPackage], rules: &DependencyRules) -> Vec<String> {
    let mut violations = Vec::new();

    for package in graph {
        if rules.forbidden_package_names.contains(&package.name) {
            violations.push(format!(
                "package `{}` uses a forbidden generic crate name",
                package.name
            ));
        }

        for dependency in &package.dependencies {
            if !is_exact_requirement(&dependency.requirement) {
                violations.push(format!(
                    "`{}` dependency `{}` has non-exact requirement `{}`",
                    package.name, dependency.name, dependency.requirement
                ));
            }
            if dependency.git {
                violations.push(format!(
                    "`{}` dependency `{}` is a forbidden Git dependency",
                    package.name, dependency.name
                ));
            }
            if dependency.outside_workspace_path {
                violations.push(format!(
                    "`{}` dependency `{}` points outside the workspace",
                    package.name, dependency.name
                ));
            }

            if dependency.workspace {
                validate_workspace_edge(package, dependency, rules, &mut violations);
            } else {
                validate_external_edge(package, dependency, rules, &mut violations);
            }
        }
    }

    violations
}

fn validate_workspace_edge(
    package: &GraphPackage,
    dependency: &GraphDependency,
    rules: &DependencyRules,
    violations: &mut Vec<String>,
) {
    let explicitly_allowed = rules.allowed_workspace_edges.iter().any(|edge| {
        edge.from == package.name
            && edge.to == dependency.name
            && edge.kinds.contains(&dependency.kind)
    });
    if !explicitly_allowed {
        violations.push(format!(
            "workspace edge `{} -[{}]-> {}` is not explicitly allowed in {RULES_PATH}",
            package.name, dependency.kind, dependency.name
        ));
    }

    if rules.classes.domain.contains(&package.name)
        && (rules.classes.application.contains(&dependency.name)
            || rules.classes.composition_support.contains(&dependency.name)
            || rules.classes.deployable_binary.contains(&dependency.name)
            || rules.classes.tool.contains(&dependency.name))
    {
        violations.push(format!(
            "domain crate `{}` depends outward on `{}`",
            package.name, dependency.name
        ));
    }
    if rules.classes.application.contains(&package.name)
        && rules.classes.application.contains(&dependency.name)
    {
        violations.push(format!(
            "application crate `{}` depends on application crate `{}`",
            package.name, dependency.name
        ));
    }
    if rules.classes.platform_binary.contains(&package.name)
        && dependency.name == "cell-application"
    {
        violations.push(format!(
            "Platform binary `{}` imports the Cell application boundary",
            package.name
        ));
    }
    if rules.classes.cell_binary.contains(&package.name)
        && dependency.name == "platform-application"
    {
        violations.push(format!(
            "Cell binary `{}` imports the Platform application boundary",
            package.name
        ));
    }
    if rules.classes.deployable_binary.contains(&package.name)
        && rules.classes.test_support.contains(&dependency.name)
        && dependency.kind == "normal"
    {
        violations.push(format!(
            "deployable binary `{}` has test-support as a normal dependency",
            package.name
        ));
    }
}

fn validate_external_edge(
    package: &GraphPackage,
    dependency: &GraphDependency,
    rules: &DependencyRules,
    violations: &mut Vec<String>,
) {
    let explicitly_allowed = rules.allowed_external_dependencies.iter().any(|allowed| {
        allowed.from == package.name
            && allowed.dependency == dependency.name
            && allowed.kinds.contains(&dependency.kind)
    });
    if !explicitly_allowed {
        violations.push(format!(
            "external dependency `{} -[{}]-> {}` is not explicitly allowed in {RULES_PATH}",
            package.name, dependency.kind, dependency.name
        ));
    }

    if rules.classes.domain.contains(&package.name) && dependency.name == "serde" {
        violations.push(format!(
            "domain crate `{}` must not depend on serde",
            package.name
        ));
    }
    if (rules.classes.domain.contains(&package.name)
        || rules.classes.application.contains(&package.name))
        && dependency.name == "anyhow"
        && dependency.kind == "normal"
    {
        violations.push(format!(
            "`{}` must not use anyhow as a normal dependency",
            package.name
        ));
    }
}

fn is_exact_requirement(requirement: &str) -> bool {
    requirement.strip_prefix('=').is_some_and(|version| {
        !version.is_empty()
            && !version
                .chars()
                .any(|character| "*^~<>,| ".contains(character))
    })
}

fn validate_member_manifests(metadata: &CargoMetadata, root: &Path) -> Result<Vec<String>> {
    let workspace_ids = metadata.workspace_members.iter().collect::<HashSet<_>>();
    let mut violations = Vec::new();

    for package in metadata
        .packages
        .iter()
        .filter(|package| workspace_ids.contains(&package.id))
    {
        let contents = fs::read_to_string(&package.manifest_path)
            .with_context(|| format!("could not read {}", package.manifest_path.display()))?;
        for marker in [
            "version.workspace = true",
            "edition.workspace = true",
            "rust-version.workspace = true",
            "publish.workspace = true",
            "[lints]",
            "workspace = true",
        ] {
            if !contents.contains(marker) {
                violations.push(format!(
                    "`{}` manifest does not inherit required workspace metadata marker `{marker}`",
                    package.name
                ));
            }
        }

        let mut dependency_section = false;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                dependency_section = is_dependency_section(trimmed);
                continue;
            }
            if dependency_section
                && !trimmed.is_empty()
                && !trimmed.starts_with('#')
                && trimmed.contains('=')
                && !trimmed.contains(".workspace = true")
                && !trimmed.contains("workspace = true")
            {
                violations.push(format!(
                    "`{}` declares a dependency outside the root workspace table: `{trimmed}`",
                    package.name
                ));
            }
        }

        let package_directory = package.manifest_path.parent().unwrap_or(root);
        if package_directory.join("build.rs").exists() {
            violations.push(format!(
                "`{}` introduces a forbidden build.rs",
                package.name
            ));
        }
    }

    Ok(violations)
}

fn is_dependency_section(section: &str) -> bool {
    matches!(
        section,
        "[dependencies]" | "[dev-dependencies]" | "[build-dependencies]"
    ) || (section.starts_with("[target.")
        && (section.ends_with(".dependencies]")
            || section.ends_with(".dev-dependencies]")
            || section.ends_with(".build-dependencies]")))
}

fn validate_root_dependencies(root: &Path) -> Result<Vec<String>> {
    let manifest_path = root.join("Cargo.toml");
    let contents = fs::read_to_string(&manifest_path)
        .with_context(|| format!("could not read {}", manifest_path.display()))?;
    let mut in_workspace_dependencies = false;
    let mut violations = Vec::new();

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_workspace_dependencies = trimmed == "[workspace.dependencies]";
            continue;
        }
        if !in_workspace_dependencies
            || trimmed.is_empty()
            || trimmed.starts_with('#')
            || !trimmed.contains('=')
        {
            continue;
        }

        let dependency_name = trimmed
            .split_once('=')
            .map_or("unknown", |(name, _)| name.trim());
        if trimmed.contains("git =") {
            violations.push(format!(
                "root workspace dependency `{dependency_name}` uses a forbidden Git source"
            ));
        }

        match quoted_attribute(trimmed, "version")
            .or_else(|| shorthand_requirement(trimmed))
        {
            Some(requirement) if is_exact_requirement(requirement) => {}
            Some(requirement) => violations.push(format!(
                "root workspace dependency `{dependency_name}` has non-exact requirement `{requirement}`"
            )),
            None => violations.push(format!(
                "root workspace dependency `{dependency_name}` has no exact version requirement"
            )),
        }

        if let Some(path) = quoted_attribute(trimmed, "path") {
            let resolved = root.join(path);
            match fs::canonicalize(&resolved) {
                Ok(canonical) if canonical.starts_with(root) => {}
                Ok(_) => violations.push(format!(
                    "root workspace dependency `{dependency_name}` points outside the workspace"
                )),
                Err(_) => violations.push(format!(
                    "root workspace dependency `{dependency_name}` has an unresolved path `{path}`"
                )),
            }
        }
    }

    Ok(violations)
}

fn quoted_attribute<'a>(line: &'a str, attribute: &str) -> Option<&'a str> {
    let marker = format!("{attribute} = \"");
    let remainder = line.split_once(&marker)?.1;
    remainder.split_once('"').map(|(value, _)| value)
}

fn shorthand_requirement(line: &str) -> Option<&str> {
    let value = line.split_once('=')?.1.trim();
    value
        .strip_prefix('"')?
        .split_once('"')
        .map(|(requirement, _)| requirement)
}

fn validate_source_boundaries(root: &Path) -> Result<Vec<String>> {
    let mut rust_files = Vec::new();
    collect_rust_files(root, &mut rust_files)?;
    let mut violations = Vec::new();
    let placeholder_patterns = [["todo", "!"].concat(), ["unimplemented", "!"].concat()];
    let direct_spawn_pattern = ["tokio", "::", "spawn"].concat();

    for path in rust_files {
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("could not read {}", path.display()))?;
        let is_test_file = path
            .components()
            .any(|component| component.as_os_str() == "tests")
            || path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .is_some_and(|stem| stem.ends_with("_test"));
        if !is_test_file {
            for pattern in &placeholder_patterns {
                if contents.contains(pattern) {
                    violations.push(format!(
                        "non-test source `{}` contains forbidden placeholder macro `{pattern}`",
                        relative_display(root, &path)
                    ));
                }
            }
        }

        if path.starts_with(root.join("apps")) && contents.contains(&direct_spawn_pattern) {
            violations.push(format!(
                "application binary source `{}` starts a Tokio task directly",
                relative_display(root, &path)
            ));
        }
    }

    Ok(violations)
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("could not inspect {}", directory.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = entry.file_name();
            if name != "target" && name != ".git" {
                collect_rust_files(&path, files)?;
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    Ok(())
}

fn relative_display<'a>(root: &'a Path, path: &'a Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}

struct SmokeCase<'a> {
    package: &'a str,
    environment: &'a [(&'a str, &'a str)],
}

fn smoke(root: &Path) -> Result<()> {
    let cases = [
        SmokeCase {
            package: "platform-api",
            environment: &[("EDTECH__ENVIRONMENT", "dev")],
        },
        SmokeCase {
            package: "platform-worker",
            environment: &[("EDTECH__ENVIRONMENT", "npr")],
        },
        SmokeCase {
            package: "tenant-router",
            environment: &[("EDTECH__ENVIRONMENT", "prd")],
        },
        SmokeCase {
            package: "cell-api",
            environment: &[
                ("EDTECH__ENVIRONMENT", "dev"),
                ("EDTECH__CELL_ID", "cell-001"),
            ],
        },
        SmokeCase {
            package: "cell-worker",
            environment: &[
                ("EDTECH__ENVIRONMENT", "npr"),
                ("EDTECH__CELL_ID", "cell-002"),
            ],
        },
        SmokeCase {
            package: "db-migrator",
            environment: &[
                ("EDTECH__ENVIRONMENT", "prd"),
                ("EDTECH__MIGRATION_SCOPE", "platform"),
            ],
        },
        SmokeCase {
            package: "db-migrator",
            environment: &[
                ("EDTECH__ENVIRONMENT", "prd"),
                ("EDTECH__MIGRATION_SCOPE", "cell"),
                ("EDTECH__CELL_ID", "cell-001"),
            ],
        },
    ];

    for case in cases {
        println!("smoke: {} --check-config", case.package);
        let mut command = Command::new("cargo");
        command
            .args([
                "run",
                "--quiet",
                "--locked",
                "--package",
                case.package,
                "--",
                "--check-config",
            ])
            .current_dir(root)
            .env_remove("EDTECH_CONFIG_FILE");
        for (key, _) in env::vars_os()
            .filter(|(key, _)| key.to_str().is_some_and(|key| key.starts_with("EDTECH__")))
        {
            command.env_remove(key);
        }
        for (key, value) in case.environment {
            command.env(key, value);
        }
        let status = command
            .status()
            .with_context(|| format!("could not start smoke case `{}`", case.package))?;
        if !status.success() {
            bail!(
                "smoke case `{}` failed with exit status {status}",
                case.package
            );
        }
    }

    println!("smoke: all seven configuration cases passed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        AllowedExternalDependency, AllowedWorkspaceEdge, DependencyRules, GraphDependency,
        GraphPackage, RuleClasses, validate_graph,
    };

    fn set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn rules() -> DependencyRules {
        DependencyRules {
            schema_version: 1,
            classes: RuleClasses {
                domain: set(&["tenancy-domain"]),
                application: set(&["platform-application", "cell-application"]),
                platform_binary: set(&["platform-api"]),
                cell_binary: set(&["cell-api"]),
                deployable_binary: set(&["platform-api", "cell-api"]),
                composition_support: BTreeSet::new(),
                test_support: set(&["test-support"]),
                tool: BTreeSet::new(),
            },
            forbidden_package_names: set(&["common", "shared", "core", "utils", "helpers", "misc"]),
            allowed_workspace_edges: vec![
                AllowedWorkspaceEdge {
                    from: String::from("platform-api"),
                    to: String::from("cell-application"),
                    kinds: set(&["normal"]),
                },
                AllowedWorkspaceEdge {
                    from: String::from("platform-application"),
                    to: String::from("cell-application"),
                    kinds: set(&["normal"]),
                },
                AllowedWorkspaceEdge {
                    from: String::from("platform-api"),
                    to: String::from("test-support"),
                    kinds: set(&["normal"]),
                },
            ],
            allowed_external_dependencies: vec![AllowedExternalDependency {
                from: String::from("tenancy-domain"),
                dependency: String::from("uuid"),
                kinds: set(&["normal"]),
            }],
        }
    }

    fn workspace_dependency(name: &str) -> GraphDependency {
        GraphDependency {
            name: name.to_owned(),
            kind: String::from("normal"),
            requirement: String::from("=0.1.0"),
            workspace: true,
            git: false,
            outside_workspace_path: false,
        }
    }

    fn external_dependency(name: &str, requirement: &str) -> GraphDependency {
        GraphDependency {
            name: name.to_owned(),
            kind: String::from("normal"),
            requirement: requirement.to_owned(),
            workspace: false,
            git: false,
            outside_workspace_path: false,
        }
    }

    fn has_violation(violations: &[String], fragment: &str) -> bool {
        violations
            .iter()
            .any(|violation| violation.contains(fragment))
    }

    #[test]
    fn valid_domain_external_dependency_is_accepted() {
        let graph = [GraphPackage {
            name: String::from("tenancy-domain"),
            dependencies: vec![external_dependency("uuid", "=1.24.0")],
        }];
        assert!(validate_graph(&graph, &rules()).is_empty());
    }

    #[test]
    fn domain_importing_sqlx_is_rejected() {
        let graph = [GraphPackage {
            name: String::from("tenancy-domain"),
            dependencies: vec![external_dependency("sqlx", "=0.8.0")],
        }];
        let violations = validate_graph(&graph, &rules());
        assert!(has_violation(&violations, "sqlx"));
        assert!(has_violation(&violations, "not explicitly allowed"));
    }

    #[test]
    fn platform_binary_importing_cell_application_is_rejected() {
        let graph = [GraphPackage {
            name: String::from("platform-api"),
            dependencies: vec![workspace_dependency("cell-application")],
        }];
        let violations = validate_graph(&graph, &rules());
        assert!(has_violation(&violations, "Platform binary"));
    }

    #[test]
    fn application_importing_another_application_is_rejected() {
        let graph = [GraphPackage {
            name: String::from("platform-application"),
            dependencies: vec![workspace_dependency("cell-application")],
        }];
        let violations = validate_graph(&graph, &rules());
        assert!(has_violation(&violations, "application crate"));
    }

    #[test]
    fn deployable_binary_normal_dependency_on_test_support_is_rejected() {
        let graph = [GraphPackage {
            name: String::from("platform-api"),
            dependencies: vec![workspace_dependency("test-support")],
        }];
        let violations = validate_graph(&graph, &rules());
        assert!(has_violation(
            &violations,
            "test-support as a normal dependency"
        ));
    }

    #[test]
    fn non_exact_direct_dependency_is_rejected() {
        let graph = [GraphPackage {
            name: String::from("tenancy-domain"),
            dependencies: vec![external_dependency("uuid", "1.24.0")],
        }];
        let violations = validate_graph(&graph, &rules());
        assert!(has_violation(&violations, "non-exact requirement"));
    }

    #[test]
    fn forbidden_generic_package_name_is_rejected() {
        let graph = [GraphPackage {
            name: String::from("common"),
            dependencies: Vec::new(),
        }];
        let violations = validate_graph(&graph, &rules());
        assert!(has_violation(&violations, "forbidden generic crate name"));
    }
}
