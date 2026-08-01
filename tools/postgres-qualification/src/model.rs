//! Deterministic qualification identities, profiles, results, and evidence schema.

use std::{str::FromStr, time::Duration};

use anyhow::{Result, anyhow, bail};
use serde::Serialize;
use tenancy_domain::TenantId;

pub(crate) const IMAGE_REFERENCE: &str = "postgres:18.4-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Profile {
    Ci,
    Full,
}

impl Profile {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "ci" => Ok(Self::Ci),
            "full" => Ok(Self::Full),
            _ => bail!("profile must be `ci` or `full`"),
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Ci => "ci",
            Self::Full => "full",
        }
    }

    pub(crate) const fn parameters(self) -> ProfileParameters {
        match self {
            Self::Ci => ProfileParameters {
                tenants: 32,
                logical_tables: 6,
                secondary_indexes_per_table: 2,
                rows_per_tenant: 10,
                alternating_switches: 500,
                concurrency: 8,
            },
            Self::Full => ProfileParameters {
                tenants: 500,
                logical_tables: 20,
                secondary_indexes_per_table: 2,
                rows_per_tenant: 50,
                alternating_switches: 10_000,
                concurrency: 32,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ProfileParameters {
    pub(crate) tenants: u32,
    pub(crate) logical_tables: u32,
    pub(crate) secondary_indexes_per_table: u32,
    pub(crate) rows_per_tenant: u32,
    pub(crate) alternating_switches: u32,
    pub(crate) concurrency: u32,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CheckResult {
    pub(crate) name: String,
    pub(crate) passed: bool,
}

#[derive(Default)]
pub(crate) struct CheckBook {
    checks: Vec<CheckResult>,
}

impl CheckBook {
    pub(crate) fn require(&mut self, name: &str, passed: bool) -> Result<()> {
        self.checks.push(CheckResult {
            name: name.to_owned(),
            passed,
        });
        if passed {
            Ok(())
        } else {
            bail!("PostgreSQL qualification check failed: {name}")
        }
    }

    pub(crate) fn into_checks(self) -> Vec<CheckResult> {
        self.checks
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct CandidateMetrics {
    pub(crate) clean_candidate_creation_ms: u64,
    pub(crate) initial_schema_migration_ms: u64,
    pub(crate) incremental_migration_ms: u64,
    pub(crate) tenant_provisioning_ms: u64,
    pub(crate) total_schema_count: u64,
    pub(crate) total_table_count: u64,
    pub(crate) total_index_count: u64,
    pub(crate) relevant_pg_class_rows: u64,
    pub(crate) relevant_pg_attribute_rows: u64,
    pub(crate) database_size_bytes: u64,
    pub(crate) insert_rows_per_second: u64,
    pub(crate) read_rows_per_second: u64,
    pub(crate) tenant_switch_p50_microseconds: u64,
    pub(crate) tenant_switch_p95_microseconds: u64,
    pub(crate) tenant_switch_p99_microseconds: u64,
    pub(crate) prepared_query_alternation_passed: bool,
    pub(crate) concurrent_isolation_passed: bool,
    pub(crate) single_tenant_probe_export_microseconds: u64,
    pub(crate) single_tenant_probe_import_microseconds: u64,
    pub(crate) cleanup_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Versions {
    pub(crate) postgres_server_version_num: u32,
    pub(crate) postgres_image: &'static str,
    pub(crate) rust: String,
    pub(crate) sqlx: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct HostFacts {
    pub(crate) operating_system: &'static str,
    pub(crate) architecture: &'static str,
    pub(crate) available_parallelism: usize,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CorrectnessSummary {
    pub(crate) passed: usize,
    pub(crate) failed: usize,
    pub(crate) checks: Vec<CheckResult>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct QualificationEvidence {
    pub(crate) schema_version: u32,
    pub(crate) checkpoint: &'static str,
    pub(crate) profile: Profile,
    pub(crate) parameters: ProfileParameters,
    pub(crate) versions: Versions,
    pub(crate) host: HostFacts,
    pub(crate) correctness: CorrectnessSummary,
    pub(crate) shared_rls: CandidateMetrics,
    pub(crate) schema_per_tenant: CandidateMetrics,
    pub(crate) selected_model: &'static str,
    pub(crate) timing_limitations: &'static str,
}

impl QualificationEvidence {
    pub(crate) fn new(
        profile: Profile,
        server_version: u32,
        rust: String,
        checks: Vec<CheckResult>,
        shared_rls: CandidateMetrics,
        schema_per_tenant: CandidateMetrics,
    ) -> Self {
        let passed = checks.iter().filter(|check| check.passed).count();
        let failed = checks.len().saturating_sub(passed);
        Self {
            schema_version: 1,
            checkpoint: "02-postgresql-authority-and-tenancy",
            profile,
            parameters: profile.parameters(),
            versions: Versions {
                postgres_server_version_num: server_version,
                postgres_image: IMAGE_REFERENCE,
                rust,
                sqlx: "0.9.0",
            },
            host: HostFacts {
                operating_system: std::env::consts::OS,
                architecture: std::env::consts::ARCH,
                available_parallelism: std::thread::available_parallelism()
                    .map_or(1, std::num::NonZeroUsize::get),
            },
            correctness: CorrectnessSummary {
                passed,
                failed,
                checks,
            },
            shared_rls,
            schema_per_tenant,
            selected_model: "shared_tables_with_tenant_id_and_forced_rls",
            timing_limitations: "Wall-clock measurements are machine-dependent evidence and are not pass thresholds.",
        }
    }
}

pub(crate) fn deterministic_tenant_id(index: u32) -> Result<TenantId> {
    if index == 0 {
        bail!("qualification tenant index must be non-zero");
    }
    let text = format!("01890f47-7cc2-7000-8000-{index:012x}");
    TenantId::from_str(&text).map_err(|_| anyhow!("deterministic tenant identifier is invalid"))
}

pub(crate) fn deterministic_canary_id(index: u32) -> String {
    format!("01890f47-7cc3-7000-8000-{index:012x}")
}

pub(crate) fn tenant_schema_name(tenant_id: TenantId) -> Result<String> {
    let compact = tenant_id.to_string().replace('-', "");
    if compact.len() != 32
        || !compact
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("tenant identifier cannot produce an opaque schema name");
    }
    Ok(format!("t_{compact}"))
}

pub(crate) fn quote_identifier(identifier: &str) -> Result<String> {
    let bytes = identifier.as_bytes();
    let valid = !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_');
    if !valid {
        bail!("SQL identifier is outside the generated identifier grammar");
    }
    Ok(format!("\"{identifier}\""))
}

pub(crate) fn duration_milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn duration_microseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

pub(crate) fn rate_per_second(rows: u64, duration: Duration) -> u64 {
    let micros = duration.as_micros().max(1);
    let scaled = u128::from(rows).saturating_mul(1_000_000) / micros;
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

pub(crate) fn percentile_microseconds(samples: &[Duration], percentile: u32) -> Result<u64> {
    if samples.is_empty() || !(1..=100).contains(&percentile) {
        bail!("percentile requires samples and a value from 1 through 100");
    }
    let mut values = samples.iter().map(Duration::as_micros).collect::<Vec<_>>();
    values.sort_unstable();
    let rank = (values.len().saturating_mul(percentile as usize)).div_ceil(100);
    let index = rank.saturating_sub(1).min(values.len().saturating_sub(1));
    u64::try_from(values[index]).map_err(|_| anyhow!("percentile measurement is too large"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        CandidateMetrics, Profile, QualificationEvidence, deterministic_tenant_id,
        percentile_microseconds, quote_identifier, tenant_schema_name,
    };

    #[test]
    fn deterministic_qualification_ids_are_valid_unique_uuid_v7_values() {
        let first = deterministic_tenant_id(1);
        let second = deterministic_tenant_id(2);
        assert!(first.is_ok());
        assert!(second.is_ok());
        assert_ne!(first.ok(), second.ok());
    }

    #[test]
    fn schema_identifiers_are_opaque_and_reject_external_text() {
        let schema = deterministic_tenant_id(7)
            .ok()
            .and_then(|tenant| tenant_schema_name(tenant).ok());
        assert!(schema.as_deref().is_some_and(|value| {
            value.len() == 34
                && value.starts_with("t_")
                && !value.contains('-')
                && !value.contains("customer")
        }));
        assert_eq!(
            quote_identifier("bench_00").ok().as_deref(),
            Some("\"bench_00\"")
        );
        for invalid in ["", "public, tenant", "MixedCase", "has-hyphen", "x\";DROP"] {
            assert!(quote_identifier(invalid).is_err());
        }
    }

    #[test]
    fn benchmark_percentiles_use_nearest_rank() {
        let samples = (1..=100).map(Duration::from_micros).collect::<Vec<_>>();
        assert_eq!(percentile_microseconds(&samples, 50).ok(), Some(50));
        assert_eq!(percentile_microseconds(&samples, 95).ok(), Some(95));
        assert_eq!(percentile_microseconds(&samples, 99).ok(), Some(99));
        assert!(percentile_microseconds(&[], 50).is_err());
    }

    #[test]
    fn evidence_serialization_has_a_stable_credential_free_shape() {
        let evidence = QualificationEvidence::new(
            Profile::Ci,
            180_004,
            String::from("rustc 1.97.1"),
            Vec::new(),
            CandidateMetrics::default(),
            CandidateMetrics::default(),
        );
        let json = serde_json::to_string(&evidence);
        assert!(json.as_ref().is_ok_and(|value| {
            value.contains("\"schema_version\":1")
                && value.contains("shared_tables_with_tenant_id_and_forced_rls")
                && !value.contains("postgresql:")
                && !value.contains("password")
                && !value.contains("host_port")
                && !value.contains("container_id")
        }));
    }
}
