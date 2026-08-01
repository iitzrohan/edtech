//! Deterministic qualification profiles and credential-free evidence model.

use serde::Serialize;

pub(crate) const IMAGE_REFERENCE: &str = "postgres:18.4-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Profile {
    Ci,
    Full,
}

impl Profile {
    pub(crate) fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "ci" => Ok(Self::Ci),
            "full" => Ok(Self::Full),
            _ => anyhow::bail!("profile must be `ci` or `full`"),
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
                outbound_messages_per_authority: 500,
                total_outbound_messages: 1_000,
                concurrent_claimers_per_authority: 8,
                claim_batch_size: 25,
                inbox_delivery_attempts_per_authority: 500,
                unique_inbox_messages_per_authority: 250,
                duplicate_ratio_percent: 50,
                deliberate_lease_expiry_cases_per_authority: 5,
                cross_authority_command_event_pairs: 100,
                encoded_payload_target_bytes: 256,
            },
            Self::Full => ProfileParameters {
                tenants: 500,
                outbound_messages_per_authority: 20_000,
                total_outbound_messages: 40_000,
                concurrent_claimers_per_authority: 32,
                claim_batch_size: 100,
                inbox_delivery_attempts_per_authority: 20_000,
                unique_inbox_messages_per_authority: 10_000,
                duplicate_ratio_percent: 50,
                deliberate_lease_expiry_cases_per_authority: 100,
                cross_authority_command_event_pairs: 5_000,
                encoded_payload_target_bytes: 256,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ProfileParameters {
    pub(crate) tenants: u32,
    pub(crate) outbound_messages_per_authority: u32,
    pub(crate) total_outbound_messages: u32,
    pub(crate) concurrent_claimers_per_authority: u16,
    pub(crate) claim_batch_size: u16,
    pub(crate) inbox_delivery_attempts_per_authority: u32,
    pub(crate) unique_inbox_messages_per_authority: u32,
    pub(crate) duplicate_ratio_percent: u8,
    pub(crate) deliberate_lease_expiry_cases_per_authority: u16,
    pub(crate) cross_authority_command_event_pairs: u32,
    pub(crate) encoded_payload_target_bytes: u16,
}

impl ProfileParameters {
    pub(crate) fn validate(self, profile: Profile) -> anyhow::Result<()> {
        if self != profile.parameters()
            || self.total_outbound_messages != self.outbound_messages_per_authority * 2
            || self.inbox_delivery_attempts_per_authority
                != self.unique_inbox_messages_per_authority * 2
            || self.duplicate_ratio_percent != 50
        {
            anyhow::bail!("qualification profile parameters were reduced or changed");
        }
        Ok(())
    }
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
    pub(crate) fn require(&mut self, name: &str, passed: bool) -> anyhow::Result<()> {
        self.checks.push(CheckResult {
            name: name.to_owned(),
            passed,
        });
        if passed {
            Ok(())
        } else {
            anyhow::bail!("message-store qualification check failed: {name}")
        }
    }

    pub(crate) fn into_checks(self) -> Vec<CheckResult> {
        self.checks
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct AuthorityMetrics {
    pub(crate) authority: &'static str,
    pub(crate) message_count: u64,
    pub(crate) delivery_row_count: u64,
    pub(crate) inbox_receipt_count: u64,
    pub(crate) database_size_delta_bytes: i64,
    pub(crate) outbox_message_table_bytes: u64,
    pub(crate) outbox_delivery_table_bytes: u64,
    pub(crate) inbox_receipt_table_bytes: u64,
    pub(crate) relevant_index_bytes: u64,
    pub(crate) enqueue_per_second: u64,
    pub(crate) idempotent_duplicate_enqueue_per_second: u64,
    pub(crate) claim_per_second: u64,
    pub(crate) mark_published_per_second: u64,
    pub(crate) reschedule_per_second: u64,
    pub(crate) inbox_insert_per_second: u64,
    pub(crate) duplicate_inbox_per_second: u64,
    pub(crate) claim_latency_p50_microseconds: u64,
    pub(crate) claim_latency_p95_microseconds: u64,
    pub(crate) claim_latency_p99_microseconds: u64,
    pub(crate) inbox_latency_p50_microseconds: u64,
    pub(crate) inbox_latency_p95_microseconds: u64,
    pub(crate) inbox_latency_p99_microseconds: u64,
    pub(crate) maximum_observed_active_lease_overlap: u64,
    pub(crate) message_identity_conflicts_detected: u64,
    pub(crate) expired_leases_reclaimed: u64,
    pub(crate) stale_lease_operations_rejected: u64,
    pub(crate) duplicate_deliveries_suppressed: u64,
    pub(crate) derived_duplicate_effects: u64,
    pub(crate) pending_count_after_completion: u64,
    pub(crate) leased_count_after_completion: u64,
    pub(crate) published_count_after_completion: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct DirectTransferMetrics {
    pub(crate) pairs: u32,
    pub(crate) throughput_per_second: u64,
    pub(crate) command_receipts: u64,
    pub(crate) event_receipts: u64,
    pub(crate) duplicate_deliveries_suppressed: u64,
    pub(crate) derived_duplicate_effects: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct QualificationEvidence {
    pub(crate) schema_version: u32,
    pub(crate) checkpoint: &'static str,
    pub(crate) profile: Profile,
    pub(crate) parameters: ProfileParameters,
    pub(crate) rust_version: String,
    pub(crate) sqlx_version: &'static str,
    pub(crate) postgres_server_version_num: u32,
    pub(crate) postgres_image: &'static str,
    pub(crate) host_os: &'static str,
    pub(crate) cpu_architecture: &'static str,
    pub(crate) available_parallelism: usize,
    pub(crate) contract_fixture_count: u32,
    pub(crate) correctness_passed: usize,
    pub(crate) correctness_failed: usize,
    pub(crate) checks: Vec<CheckResult>,
    pub(crate) platform: AuthorityMetrics,
    pub(crate) cell: AuthorityMetrics,
    pub(crate) direct_transfer: DirectTransferMetrics,
    pub(crate) cleanup_result: &'static str,
    pub(crate) timing_limitations: &'static str,
}

impl QualificationEvidence {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        profile: Profile,
        rust_version: String,
        postgres_server_version_num: u32,
        checks: Vec<CheckResult>,
        platform: AuthorityMetrics,
        cell: AuthorityMetrics,
        direct_transfer: DirectTransferMetrics,
    ) -> Self {
        let correctness_passed = checks.iter().filter(|check| check.passed).count();
        let correctness_failed = checks.len().saturating_sub(correctness_passed);
        Self {
            schema_version: 1,
            checkpoint: "03-message-contract-and-transactional-store",
            profile,
            parameters: profile.parameters(),
            rust_version,
            sqlx_version: "0.9.0",
            postgres_server_version_num,
            postgres_image: IMAGE_REFERENCE,
            host_os: std::env::consts::OS,
            cpu_architecture: std::env::consts::ARCH,
            available_parallelism: std::thread::available_parallelism()
                .map_or(1, std::num::NonZeroUsize::get),
            contract_fixture_count: 2,
            correctness_passed,
            correctness_failed,
            checks,
            platform,
            cell,
            direct_transfer,
            cleanup_result: "disposable authority cleanup is enforced by the invoking xtask",
            timing_limitations: "Timings are machine-dependent observations, not pass thresholds or production capacity claims.",
        }
    }
}

pub(crate) fn percentile(values: &mut [u64], percentile: u8) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    let numerator = values.len().saturating_sub(1) * usize::from(percentile);
    values[numerator.div_ceil(100)]
}

pub(crate) fn throughput(count: u64, elapsed: std::time::Duration) -> u64 {
    let micros = elapsed.as_micros().max(1);
    u64::try_from(u128::from(count) * 1_000_000 / micros).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_are_exact_and_not_silently_reduced() {
        assert!(Profile::Ci.parameters().validate(Profile::Ci).is_ok());
        assert!(Profile::Full.parameters().validate(Profile::Full).is_ok());
        let mut changed = Profile::Full.parameters();
        changed.tenants = 499;
        assert!(changed.validate(Profile::Full).is_err());
    }

    #[test]
    fn percentile_is_deterministic() {
        let mut values = [50, 10, 40, 20, 30];
        assert_eq!(percentile(&mut values, 50), 30);
        assert_eq!(percentile(&mut values, 95), 50);
        assert_eq!(percentile(&mut values, 99), 50);
    }

    #[test]
    fn evidence_serialization_is_stable_and_redacted() {
        let evidence = QualificationEvidence::new(
            Profile::Ci,
            "rustc 1.97.1".to_owned(),
            180_004,
            vec![CheckResult {
                name: "qualification.safe-check".to_owned(),
                passed: true,
            }],
            AuthorityMetrics::default(),
            AuthorityMetrics::default(),
            DirectTransferMetrics::default(),
        );
        let serialized = serde_json::to_string(&evidence);
        assert!(serialized.is_ok());
        let json = serialized.unwrap_or_default();
        assert!(json.contains("\"schema_version\":1"));
        assert!(json.contains("\"correctness_failed\":0"));
        for forbidden in [
            "postgresql://",
            "password",
            "credential_ref",
            "envelope_bytes",
            "\"tenant_id\":",
            "\"message_id\":",
            "payload_sentinel",
        ] {
            assert!(!json.contains(forbidden), "{forbidden}");
        }
    }
}
