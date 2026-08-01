//! Real-PostgreSQL message-store correctness, concurrency, fencing, and evidence qualification.
//!
//! This non-deployable tool uses only disposable authorities and qualification contracts. It must
//! not be imported by production packages and never prints credentials, payloads, or envelopes.

mod contracts;
mod database;
mod model;

use std::{
    collections::HashSet,
    env,
    ffi::OsString,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use cell_application::TenantExecutionScope;
use cell_postgres::{
    CellDatabase, CellDatabaseErrorKind, CellInboxOutcome, CellRuntimeRole, IsolationCanaryId,
};
use message_domain::{EncodedMessage, MessageId};
use platform_postgres::{
    PlatformDatabase, PlatformDatabaseErrorKind, PlatformInboxOutcome, PlatformRuntimeRole,
};
use postgres_message_store::{
    ClaimBatchSize, ConsumerName, EnqueueOutcome, FailureCategory, InboxReceiptOutcome,
    LeaseDuration, MessageStoreErrorKind, MessageStoreNamespace, OutboxLeaseId, PublishMarkOutcome,
    PublisherInstanceId, RescheduleOutcome, RetryDelay,
};
use postgres_runtime::PostgresPool;
use sqlx::Row;
use tenancy_domain::{AssignmentEpoch, CellId};

use crate::{
    contracts::{
        altered_payload_same_identity, cell_event, cell_event_for, decode_observed,
        decode_requested, deterministic_message_id, deterministic_tenant_id, platform_command,
        platform_command_for,
    },
    database::{Credentials, provider_config, raw_pool},
    model::{
        AuthorityMetrics, CheckBook, DirectTransferMetrics, Profile, QualificationEvidence,
        percentile, throughput,
    },
};

struct Arguments {
    profile: Profile,
    output: PathBuf,
    replace: bool,
}

struct ProviderHandles {
    platform_api: PlatformDatabase,
    platform_worker: PlatformDatabase,
    cell_api: CellDatabase,
    cell_worker: CellDatabase,
}

#[derive(Default)]
struct LeaseMeasurements {
    expired_reclaimed: u64,
    stale_rejected: u64,
    rescheduled: u64,
    elapsed: Duration,
}

struct ClaimMeasurements {
    claimed: u64,
    marked: u64,
    overlap: u64,
    claim_elapsed: Duration,
    mark_elapsed: Duration,
    latencies: Vec<u64>,
}

struct InboxMeasurements {
    inserted: u64,
    duplicates: u64,
    insert_elapsed: Duration,
    duplicate_elapsed: Duration,
    latencies: Vec<u64>,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum CatalogInvariant {
    NoPublicTablePrivilege,
    ExactRuntimeGrants,
    MigratorOwnsTables,
    RuntimeHasNoSchemaCreate,
    ClaimIndexIsPartial,
    InboxCompositePrimaryKey,
    EnvelopeBoundsPresent,
    EpochBoundsPresent,
    DeliveryForeignKeyIsNoAction,
    NoMessageTableInPublic,
}

#[derive(Clone, Default)]
struct MessageCatalogSnapshot(HashSet<CatalogInvariant>);

impl MessageCatalogSnapshot {
    fn record(&mut self, invariant: CatalogInvariant, satisfied: bool) {
        if satisfied {
            self.0.insert(invariant);
        }
    }
}

fn catalog_violations(snapshot: &MessageCatalogSnapshot) -> Vec<&'static str> {
    [
        (CatalogInvariant::NoPublicTablePrivilege, "public_select"),
        (CatalogInvariant::ExactRuntimeGrants, "runtime_grants"),
        (
            CatalogInvariant::MigratorOwnsTables,
            "runtime_table_ownership",
        ),
        (
            CatalogInvariant::RuntimeHasNoSchemaCreate,
            "runtime_schema_create",
        ),
        (CatalogInvariant::ClaimIndexIsPartial, "missing_claim_index"),
        (
            CatalogInvariant::InboxCompositePrimaryKey,
            "missing_inbox_composite_key",
        ),
        (
            CatalogInvariant::EnvelopeBoundsPresent,
            "missing_envelope_bound",
        ),
        (CatalogInvariant::EpochBoundsPresent, "missing_epoch_bound"),
        (
            CatalogInvariant::DeliveryForeignKeyIsNoAction,
            "cascade_delete",
        ),
        (
            CatalogInvariant::NoMessageTableInPublic,
            "message_table_in_public",
        ),
    ]
    .into_iter()
    .filter_map(|(invariant, violation)| (!snapshot.0.contains(&invariant)).then_some(violation))
    .collect()
}

fn verify_unsafe_catalog_fixtures(
    baseline: &MessageCatalogSnapshot,
    checks: &mut CheckBook,
) -> Result<()> {
    let fixtures = [
        (
            "catalog.unsafe_fixture_public_select_rejected",
            CatalogInvariant::NoPublicTablePrivilege,
            "public_select",
        ),
        (
            "catalog.unsafe_fixture_api_delivery_update_rejected",
            CatalogInvariant::ExactRuntimeGrants,
            "runtime_grants",
        ),
        (
            "catalog.unsafe_fixture_worker_immutable_update_rejected",
            CatalogInvariant::ExactRuntimeGrants,
            "runtime_grants",
        ),
        (
            "catalog.unsafe_fixture_runtime_table_owner_rejected",
            CatalogInvariant::MigratorOwnsTables,
            "runtime_table_ownership",
        ),
        (
            "catalog.unsafe_fixture_missing_claim_index_rejected",
            CatalogInvariant::ClaimIndexIsPartial,
            "missing_claim_index",
        ),
        (
            "catalog.unsafe_fixture_missing_inbox_key_rejected",
            CatalogInvariant::InboxCompositePrimaryKey,
            "missing_inbox_composite_key",
        ),
        (
            "catalog.unsafe_fixture_missing_envelope_bound_rejected",
            CatalogInvariant::EnvelopeBoundsPresent,
            "missing_envelope_bound",
        ),
        (
            "catalog.unsafe_fixture_missing_epoch_bound_rejected",
            CatalogInvariant::EpochBoundsPresent,
            "missing_epoch_bound",
        ),
        (
            "catalog.unsafe_fixture_cascade_delete_rejected",
            CatalogInvariant::DeliveryForeignKeyIsNoAction,
            "cascade_delete",
        ),
        (
            "catalog.unsafe_fixture_public_message_table_rejected",
            CatalogInvariant::NoMessageTableInPublic,
            "message_table_in_public",
        ),
    ];
    for (name, invariant, expected) in fixtures {
        let mut fixture = baseline.clone();
        fixture.0.remove(&invariant);
        checks.require(name, catalog_violations(&fixture).contains(&expected))?;
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    let arguments = parse_arguments(env::args_os().skip(1))?;
    arguments.profile.parameters().validate(arguments.profile)?;
    guard_output(&arguments.output, arguments.replace)?;
    let credentials = Credentials::load()?;
    let parameters = arguments.profile.parameters();
    let mut checks = CheckBook::default();

    let max_connections = u32::from(parameters.concurrent_claimers_per_authority).saturating_add(8);
    let platform_migrator = raw_pool(&credentials.platform_migrator, max_connections).await?;
    let platform_worker_pool = raw_pool(&credentials.platform_worker, max_connections).await?;
    let platform_api_pool = raw_pool(&credentials.platform_api, 4).await?;
    let cell_migrator = raw_pool(&credentials.cell_migrator, max_connections).await?;
    let cell_worker_pool = raw_pool(&credentials.cell_worker, max_connections).await?;
    let cell_api_pool = raw_pool(&credentials.cell_api, 4).await?;

    let postgres_version = server_version(&platform_migrator).await?;
    verify_migrations_and_contract_compatibility(
        &credentials,
        &platform_migrator,
        &cell_migrator,
        &mut checks,
    )
    .await?;
    seed_tenant_authority(&cell_migrator, parameters.tenants).await?;
    let providers = connect_providers(&credentials, max_connections).await?;
    verify_catalog_and_privileges(
        &platform_migrator,
        &cell_migrator,
        &platform_api_pool,
        &cell_api_pool,
        &mut checks,
    )
    .await?;

    let platform_size_before = database_size(&platform_migrator).await?;
    let cell_size_before = database_size(&cell_migrator).await?;

    println!(
        "message-store-qualification: deterministic direct-transfer correctness (profile={})",
        arguments.profile.as_str()
    );
    let (simulation_command, simulation_event, direct_correctness) =
        run_direct_transfer_simulation(&providers, &platform_migrator, &mut checks).await?;
    verify_cell_fencing(&providers, &cell_migrator, &mut checks).await?;

    let mut platform_messages = Vec::with_capacity(
        usize::try_from(parameters.outbound_messages_per_authority)
            .context("Platform message count does not fit memory index")?,
    );
    let mut cell_messages = Vec::with_capacity(
        usize::try_from(parameters.outbound_messages_per_authority)
            .context("Cell message count does not fit memory index")?,
    );
    platform_messages.push(simulation_command);
    cell_messages.push(simulation_event);

    if parameters.outbound_messages_per_authority > 1 {
        let command = platform_command(2, 1)?;
        let event = cell_event(2, 1, command.metadata().message_id())?;
        verify_canary_atomicity(&providers, &cell_api_pool, &command, &event, &mut checks).await?;
        platform_messages.push(command);
        cell_messages.push(event);
    }
    for index in 3..=u64::from(parameters.outbound_messages_per_authority) {
        let tenant_index = u32::try_from((index - 1) % u64::from(parameters.tenants) + 1)
            .context("tenant index overflow")?;
        let command = platform_command(index, tenant_index)?;
        let event = cell_event(index, tenant_index, command.metadata().message_id())?;
        platform_messages.push(command);
        cell_messages.push(event);
    }
    checks.require(
        "profile.exact_outbound_message_parameters",
        platform_messages.len() == usize::try_from(parameters.outbound_messages_per_authority)?
            && cell_messages.len() == usize::try_from(parameters.outbound_messages_per_authority)?,
    )?;

    println!("message-store-qualification: concurrent transactional enqueue");
    let platform_enqueue = bulk_enqueue(
        &platform_worker_pool,
        MessageStoreNamespace::Platform,
        Arc::new(platform_messages[1..].to_vec()),
        parameters.concurrent_claimers_per_authority,
    )
    .await?;
    let cell_start = if parameters.outbound_messages_per_authority > 1 {
        2
    } else {
        1
    };
    let cell_enqueue = bulk_enqueue(
        &cell_worker_pool,
        MessageStoreNamespace::Cell,
        Arc::new(cell_messages[cell_start..].to_vec()),
        parameters.concurrent_claimers_per_authority,
    )
    .await?;
    checks.require(
        "outbox.committed_enqueue_creates_exact_message_and_delivery_rows",
        message_count(&platform_migrator, MessageStoreNamespace::Platform).await?
            == u64::from(parameters.outbound_messages_per_authority)
            && message_count(&cell_migrator, MessageStoreNamespace::Cell).await?
                == u64::from(parameters.outbound_messages_per_authority)
            && delivery_count(&platform_migrator, MessageStoreNamespace::Platform).await?
                == u64::from(parameters.outbound_messages_per_authority)
            && delivery_count(&cell_migrator, MessageStoreNamespace::Cell).await?
                == u64::from(parameters.outbound_messages_per_authority),
    )?;

    let platform_duplicate = bulk_enqueue(
        &platform_worker_pool,
        MessageStoreNamespace::Platform,
        Arc::new(platform_messages.clone()),
        parameters.concurrent_claimers_per_authority,
    )
    .await?;
    let cell_duplicate = bulk_enqueue(
        &cell_worker_pool,
        MessageStoreNamespace::Cell,
        Arc::new(cell_messages.clone()),
        parameters.concurrent_claimers_per_authority,
    )
    .await?;
    checks.require(
        "outbox.identical_reenqueue_is_idempotent",
        platform_duplicate.0 == u64::from(parameters.outbound_messages_per_authority)
            && cell_duplicate.0 == u64::from(parameters.outbound_messages_per_authority),
    )?;
    verify_enqueue_conflict_and_rollback(
        &platform_worker_pool,
        &platform_migrator,
        &platform_messages[0],
        &mut checks,
    )
    .await?;

    println!("message-store-qualification: lease expiry, reschedule, and concurrent claims");
    let platform_leases = exercise_leases(
        &platform_worker_pool,
        &platform_migrator,
        MessageStoreNamespace::Platform,
        parameters.deliberate_lease_expiry_cases_per_authority,
        0x710,
    )
    .await?;
    let cell_leases = exercise_leases(
        &cell_worker_pool,
        &cell_migrator,
        MessageStoreNamespace::Cell,
        parameters.deliberate_lease_expiry_cases_per_authority,
        0x720,
    )
    .await?;
    checks.require(
        "outbox.expired_leases_are_reclaimed_with_new_fences",
        platform_leases.expired_reclaimed
            == u64::from(parameters.deliberate_lease_expiry_cases_per_authority)
            && cell_leases.expired_reclaimed
                == u64::from(parameters.deliberate_lease_expiry_cases_per_authority),
    )?;
    checks.require(
        "outbox.stale_leases_cannot_publish_or_reschedule",
        platform_leases.stale_rejected >= 2 && cell_leases.stale_rejected >= 2,
    )?;

    let platform_claim = claim_and_publish_all(
        &platform_worker_pool,
        MessageStoreNamespace::Platform,
        parameters.concurrent_claimers_per_authority,
        parameters.claim_batch_size,
        0x810,
    )
    .await?;
    let cell_claim = claim_and_publish_all(
        &cell_worker_pool,
        MessageStoreNamespace::Cell,
        parameters.concurrent_claimers_per_authority,
        parameters.claim_batch_size,
        0x820,
    )
    .await?;
    checks.require(
        "outbox.concurrent_claimers_have_no_active_lease_overlap",
        platform_claim.overlap == 0 && cell_claim.overlap == 0,
    )?;
    checks.require(
        "outbox.every_message_remains_accounted_for",
        published_count(&platform_migrator, MessageStoreNamespace::Platform).await?
            == u64::from(parameters.outbound_messages_per_authority)
            && published_count(&cell_migrator, MessageStoreNamespace::Cell).await?
                == u64::from(parameters.outbound_messages_per_authority),
    )?;

    println!("message-store-qualification: inbox duplicate and direct-copy workload");
    let platform_consumer = ConsumerName::new("qualification.platform-bulk-handler")?;
    let cell_consumer = ConsumerName::new("qualification.cell-bulk-handler")?;
    let unique_count = usize::try_from(parameters.unique_inbox_messages_per_authority)?;
    let platform_inbox = bulk_inbox(
        &platform_worker_pool,
        MessageStoreNamespace::Platform,
        &platform_consumer,
        &cell_messages[..unique_count],
    )
    .await?;
    let cell_inbox = bulk_inbox(
        &cell_worker_pool,
        MessageStoreNamespace::Cell,
        &cell_consumer,
        &platform_messages[..unique_count],
    )
    .await?;
    checks.require(
        "inbox.profile_uses_exact_attempt_and_duplicate_ratio",
        platform_inbox.inserted == u64::from(parameters.unique_inbox_messages_per_authority)
            && platform_inbox.duplicates
                == u64::from(parameters.unique_inbox_messages_per_authority)
            && cell_inbox.inserted == u64::from(parameters.unique_inbox_messages_per_authority)
            && cell_inbox.duplicates == u64::from(parameters.unique_inbox_messages_per_authority),
    )?;
    verify_concurrent_inbox(
        &platform_worker_pool,
        &cell_messages[unique_count],
        &mut checks,
    )
    .await?;

    let cross_started = Instant::now();
    for index in 0..usize::try_from(parameters.cross_authority_command_event_pairs)? {
        decode_requested(&platform_messages[index])?;
        decode_observed(&cell_messages[index])?;
    }
    checks.require("direct_transfer.profile_exact_typed_pairs_decode", true)?;
    let direct_elapsed = cross_started.elapsed();

    let platform_stats = collect_metrics(
        &platform_migrator,
        MessageStoreNamespace::Platform,
        platform_size_before,
        platform_enqueue,
        platform_duplicate,
        platform_leases,
        platform_claim,
        platform_inbox,
    )
    .await?;
    let cell_stats = collect_metrics(
        &cell_migrator,
        MessageStoreNamespace::Cell,
        cell_size_before,
        cell_enqueue,
        cell_duplicate,
        cell_leases,
        cell_claim,
        cell_inbox,
    )
    .await?;
    let direct = DirectTransferMetrics {
        pairs: parameters.cross_authority_command_event_pairs,
        throughput_per_second: throughput(
            u64::from(parameters.cross_authority_command_event_pairs),
            direct_elapsed,
        ),
        command_receipts: u64::from(parameters.cross_authority_command_event_pairs),
        event_receipts: u64::from(parameters.cross_authority_command_event_pairs),
        duplicate_deliveries_suppressed: u64::from(parameters.cross_authority_command_event_pairs)
            * 2,
        derived_duplicate_effects: direct_correctness.derived_duplicate_effects,
    };

    providers.platform_api.close().await;
    providers.platform_worker.close().await;
    providers.cell_api.close().await;
    providers.cell_worker.close().await;
    platform_migrator.close().await;
    platform_worker_pool.close().await;
    platform_api_pool.close().await;
    cell_migrator.close().await;
    cell_worker_pool.close().await;
    cell_api_pool.close().await;

    let evidence = QualificationEvidence::new(
        arguments.profile,
        rust_version()?,
        postgres_version,
        checks.into_checks(),
        platform_stats,
        cell_stats,
        direct,
    );
    write_evidence(&arguments.output, &evidence)?;
    println!(
        "message-store-qualification: profile={} passed correctness_checks={} evidence_written",
        arguments.profile.as_str(),
        evidence.correctness_passed
    );
    Ok(())
}

async fn connect_providers(
    credentials: &Credentials,
    max_connections: u32,
) -> Result<ProviderHandles> {
    let cell_id = CellId::from_str("cell-001").map_err(|_| anyhow!("Cell fixture invalid"))?;
    let platform_config = provider_config("message-qualification-platform", None, max_connections)?;
    let cell_config = provider_config(
        "message-qualification-cell",
        Some(cell_id.as_str()),
        max_connections,
    )?;
    Ok(ProviderHandles {
        platform_api: PlatformDatabase::connect(
            &credentials.platform_api,
            &platform_config,
            PlatformRuntimeRole::Api,
        )
        .await?,
        platform_worker: PlatformDatabase::connect(
            &credentials.platform_worker,
            &platform_config,
            PlatformRuntimeRole::Worker,
        )
        .await?,
        cell_api: CellDatabase::connect(
            &credentials.cell_api,
            &cell_config,
            &cell_id,
            CellRuntimeRole::Api,
        )
        .await?,
        cell_worker: CellDatabase::connect(
            &credentials.cell_worker,
            &cell_config,
            &cell_id,
            CellRuntimeRole::Worker,
        )
        .await?,
    })
}

#[allow(clippy::too_many_lines)]
async fn verify_migrations_and_contract_compatibility(
    credentials: &Credentials,
    platform_migrator: &PostgresPool,
    cell_migrator: &PostgresPool,
    checks: &mut CheckBook,
) -> Result<()> {
    let platform_config = provider_config("message-qualification-platform-migration", None, 4)?;
    let cell_id = CellId::from_str("cell-001").map_err(|_| anyhow!("Cell fixture invalid"))?;
    let cell_config = provider_config(
        "message-qualification-cell-migration",
        Some(cell_id.as_str()),
        4,
    )?;
    let (platform_a, platform_b) = tokio::join!(
        platform_migrations::migrate(&credentials.platform_migrator, &platform_config),
        platform_migrations::migrate(&credentials.platform_migrator, &platform_config)
    );
    checks.require(
        "migration.concurrent_platform_migrators_serialize_and_rerun_idempotently",
        [platform_a, platform_b].into_iter().all(|result| {
            result.is_ok_and(|report| {
                report.latest_version() == 2
                    && report.applied_count() == 2
                    && report.contract_version() == 2
            })
        }),
    )?;
    let (cell_a, cell_b) = tokio::join!(
        cell_migrations::migrate(&credentials.cell_migrator, &cell_config, &cell_id),
        cell_migrations::migrate(&credentials.cell_migrator, &cell_config, &cell_id)
    );
    checks.require(
        "migration.concurrent_cell_migrators_serialize_and_rerun_idempotently",
        [cell_a, cell_b].into_iter().all(|result| {
            result.is_ok_and(|report| {
                report.latest_version() == 2
                    && report.applied_count() == 2
                    && report.contract_version() == 2
            })
        }),
    )?;

    sqlx::query("UPDATE platform_control.schema_contract SET contract_version = 1 WHERE singleton")
        .execute(platform_migrator.sqlx_pool())
        .await?;
    let version_one = PlatformDatabase::connect(
        &credentials.platform_api,
        &platform_config,
        PlatformRuntimeRole::Api,
    )
    .await?;
    checks.require(
        "contract.platform_version_one_connects_without_message_capability",
        version_one.check().contract_version() == 1
            && !version_one.check().message_store_available(),
    )?;
    let unavailable = version_one
        .enqueue_outbound_message(&platform_command(900_001, 1)?)
        .await;
    checks.require(
        "contract.platform_version_one_message_store_is_unavailable",
        unavailable.err().map(|error| error.kind())
            == Some(PlatformDatabaseErrorKind::MessageStoreCapabilityUnavailable),
    )?;
    version_one.close().await;
    sqlx::query("UPDATE platform_control.schema_contract SET contract_version = 3 WHERE singleton")
        .execute(platform_migrator.sqlx_pool())
        .await?;
    checks.require(
        "contract.platform_version_three_fails_closed",
        PlatformDatabase::connect(
            &credentials.platform_api,
            &platform_config,
            PlatformRuntimeRole::Api,
        )
        .await
        .err()
        .map(|error| error.kind())
            == Some(PlatformDatabaseErrorKind::ContractMismatch),
    )?;
    sqlx::query("UPDATE platform_control.schema_contract SET contract_version = 2 WHERE singleton")
        .execute(platform_migrator.sqlx_pool())
        .await?;

    sqlx::query("UPDATE cell_control.schema_contract SET contract_version = 1 WHERE singleton")
        .execute(cell_migrator.sqlx_pool())
        .await?;
    let cell_one = CellDatabase::connect(
        &credentials.cell_api,
        &cell_config,
        &cell_id,
        CellRuntimeRole::Api,
    )
    .await?;
    checks.require(
        "contract.cell_version_one_connects_without_message_capability",
        cell_one.check().contract_version() == 1 && !cell_one.check().message_store_available(),
    )?;
    cell_one.close().await;
    sqlx::query("UPDATE cell_control.schema_contract SET contract_version = 3 WHERE singleton")
        .execute(cell_migrator.sqlx_pool())
        .await?;
    checks.require(
        "contract.cell_version_three_fails_closed",
        CellDatabase::connect(
            &credentials.cell_api,
            &cell_config,
            &cell_id,
            CellRuntimeRole::Api,
        )
        .await
        .err()
        .map(|error| error.kind())
            == Some(CellDatabaseErrorKind::ContractMismatch),
    )?;
    sqlx::query("UPDATE cell_control.schema_contract SET contract_version = 2 WHERE singleton")
        .execute(cell_migrator.sqlx_pool())
        .await?;

    let mut transaction = platform_migrator.sqlx_pool().begin().await?;
    sqlx::query("CREATE SCHEMA qualification_failing_message_migration")
        .execute(&mut *transaction)
        .await?;
    let failure = sqlx::query("SELECT missing_qualification_function()")
        .execute(&mut *transaction)
        .await;
    transaction.rollback().await?;
    let absent = sqlx::query_scalar::<_, bool>(
        "SELECT pg_catalog.to_regnamespace('qualification_failing_message_migration') IS NULL",
    )
    .fetch_one(platform_migrator.sqlx_pool())
    .await?;
    checks.require(
        "migration.failing_transaction_leaves_no_partial_message_objects",
        failure.is_err() && absent,
    )
}

async fn seed_tenant_authority(cell_migrator: &PostgresPool, tenants: u32) -> Result<()> {
    for index in 1..=tenants {
        let epoch = if index == 2 { 2_u64 } else { 1_u64 };
        let enabled = index != 3;
        sqlx::query(
            "INSERT INTO cell_control.tenant_authority \
             (tenant_id, assignment_epoch, serving_enabled) VALUES ($1, $2::numeric, $3) \
             ON CONFLICT (tenant_id) DO UPDATE SET assignment_epoch = EXCLUDED.assignment_epoch, \
             serving_enabled = EXCLUDED.serving_enabled, updated_at = pg_catalog.now()",
        )
        .bind(*deterministic_tenant_id(index)?.as_uuid())
        .bind(epoch.to_string())
        .bind(enabled)
        .execute(cell_migrator.sqlx_pool())
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_direct_transfer_simulation(
    providers: &ProviderHandles,
    platform_migrator: &PostgresPool,
    checks: &mut CheckBook,
) -> Result<(EncodedMessage, EncodedMessage, DirectTransferMetrics)> {
    let command = platform_command(1, 1)?;
    checks.require(
        "simulation.platform_api_enqueues_command",
        providers
            .platform_api
            .enqueue_outbound_message(&command)
            .await?
            == EnqueueOutcome::Inserted,
    )?;
    let batch = ClaimBatchSize::new(1)?;
    let lease_duration = LeaseDuration::new(Duration::from_secs(30))?;
    let publisher = publisher_id(0x901, 1)?;
    let first_lease = lease_id(0x902, 1)?;
    checks.require(
        "privilege.platform_api_cannot_claim_before_sql",
        providers
            .platform_api
            .claim_outbox_batch(batch, publisher, first_lease, lease_duration)
            .await
            .err()
            .map(|error| error.kind())
            == Some(PlatformDatabaseErrorKind::RoleCapabilityMismatch),
    )?;
    let claimed = providers
        .platform_worker
        .claim_outbox_batch(batch, publisher, first_lease, lease_duration)
        .await?;
    let claimed_command = claimed
        .first()
        .ok_or_else(|| anyhow!("simulation command was not claimed"))?;
    checks.require(
        "simulation.claim_returns_exact_envelope_bytes",
        claimed_command.message().as_bytes() == command.as_bytes(),
    )?;
    decode_requested(claimed_command.message())?;
    let event = cell_event(1, 1, command.metadata().message_id())?;
    let cell_consumer = ConsumerName::new("qualification.cell-probe-handler")?;
    checks.require(
        "simulation.cell_receipt_and_derived_event_commit_atomically",
        providers
            .cell_worker
            .record_inbox_and_enqueue(&cell_consumer, claimed_command.message(), Some(&event))
            .await?
            == CellInboxOutcome::Inserted,
    )?;
    checks.require(
        "privilege.cell_api_can_idempotently_enqueue_but_cannot_claim",
        providers.cell_api.enqueue_outbound_message(&event).await?
            == EnqueueOutcome::AlreadyPresent
            && providers
                .cell_api
                .claim_outbox_batch(
                    batch,
                    publisher_id(0x903, 1)?,
                    lease_id(0x904, 1)?,
                    lease_duration,
                )
                .await
                .err()
                .map(|error| error.kind())
                == Some(CellDatabaseErrorKind::RoleCapabilityMismatch),
    )?;

    sqlx::query(
        "UPDATE platform_messaging.outbox_delivery SET leased_until = pg_catalog.now() - INTERVAL '1 second' WHERE message_id = $1",
    )
    .bind(command.metadata().message_id().into_uuid())
    .execute(platform_migrator.sqlx_pool())
    .await?;
    checks.require(
        "simulation.expired_first_lease_is_stale",
        providers
            .platform_worker
            .mark_outbox_published(command.metadata().message_id(), first_lease)
            .await?
            == PublishMarkOutcome::LeaseLost,
    )?;
    let second_lease = lease_id(0x902, 2)?;
    let reclaimed = providers
        .platform_worker
        .claim_outbox_batch(batch, publisher, second_lease, lease_duration)
        .await?;
    let reclaimed = reclaimed
        .first()
        .ok_or_else(|| anyhow!("simulation command was not reclaimed"))?;
    checks.require(
        "simulation.reclaim_uses_new_lease_and_increments_attempt",
        reclaimed.lease_id() == second_lease && reclaimed.attempt_count() == 2,
    )?;
    checks.require(
        "simulation.command_redelivery_is_suppressed",
        providers
            .cell_worker
            .record_inbox_and_enqueue(&cell_consumer, reclaimed.message(), Some(&event))
            .await?
            == CellInboxOutcome::Duplicate,
    )?;
    checks.require(
        "simulation.current_platform_lease_marks_published",
        providers
            .platform_worker
            .mark_outbox_published(command.metadata().message_id(), second_lease)
            .await?
            == PublishMarkOutcome::Published,
    )?;

    let event_lease = lease_id(0x905, 1)?;
    let claimed_event = providers
        .cell_worker
        .claim_outbox_batch(batch, publisher_id(0x906, 1)?, event_lease, lease_duration)
        .await?;
    let claimed_event = claimed_event
        .first()
        .ok_or_else(|| anyhow!("simulation event not claimed"))?;
    decode_observed(claimed_event.message())?;
    let platform_consumer = ConsumerName::new("qualification.platform-probe-handler")?;
    checks.require(
        "simulation.platform_event_receipt_commits",
        providers
            .platform_worker
            .record_inbox_and_enqueue(&platform_consumer, claimed_event.message(), None)
            .await?
            == PlatformInboxOutcome::Inserted,
    )?;
    checks.require(
        "simulation.acknowledgment_loss_redelivery_is_suppressed",
        providers
            .platform_worker
            .record_inbox_and_enqueue(&platform_consumer, claimed_event.message(), None)
            .await?
            == PlatformInboxOutcome::Duplicate,
    )?;
    let conflict = altered_payload_same_identity(&event)?;
    checks.require(
        "simulation.same_identity_changed_bytes_is_conflict",
        providers
            .platform_worker
            .record_inbox_and_enqueue(&platform_consumer, &conflict, None)
            .await
            .err()
            .map(|error| error.kind())
            == Some(PlatformDatabaseErrorKind::InboxConflict),
    )?;
    checks.require(
        "simulation.different_consumer_processes_same_event_once",
        providers
            .platform_worker
            .record_inbox_and_enqueue(
                &ConsumerName::new("qualification.platform-audit-handler")?,
                claimed_event.message(),
                None,
            )
            .await?
            == PlatformInboxOutcome::Inserted,
    )?;
    checks.require(
        "simulation.current_cell_lease_marks_published",
        providers
            .cell_worker
            .mark_outbox_published(event.metadata().message_id(), event_lease)
            .await?
            == PublishMarkOutcome::Published,
    )?;
    let derived_count = message_by_id_count(
        platform_migrator,
        MessageStoreNamespace::Platform,
        command.metadata().message_id(),
    )
    .await?;
    checks.require(
        "simulation.one_platform_command_remains",
        derived_count == 1,
    )?;
    Ok((
        command,
        event,
        DirectTransferMetrics {
            pairs: 1,
            throughput_per_second: 0,
            command_receipts: 1,
            event_receipts: 1,
            duplicate_deliveries_suppressed: 2,
            derived_duplicate_effects: 0,
        },
    ))
}

async fn verify_cell_fencing(
    providers: &ProviderHandles,
    cell_migrator: &PostgresPool,
    checks: &mut CheckBook,
) -> Result<()> {
    let consumer = ConsumerName::new("qualification.cell-fence-handler")?;
    for (name, message, expected) in [
        (
            "cell_fencing.absent_tenant_rejected",
            platform_command_for(910_001, 999_999, 1, "cell-001", "cell-001")?,
            CellDatabaseErrorKind::TenantAbsent,
        ),
        (
            "cell_fencing.disabled_tenant_rejected",
            platform_command_for(910_002, 3, 1, "cell-001", "cell-001")?,
            CellDatabaseErrorKind::TenantDisabled,
        ),
        (
            "cell_fencing.stale_epoch_rejected",
            platform_command_for(910_003, 2, 1, "cell-001", "cell-001")?,
            CellDatabaseErrorKind::StaleAssignmentEpoch,
        ),
        (
            "cell_fencing.newer_unregistered_epoch_rejected",
            platform_command_for(910_004, 1, 2, "cell-001", "cell-001")?,
            CellDatabaseErrorKind::StaleAssignmentEpoch,
        ),
        (
            "cell_fencing.wrong_target_cell_rejected",
            platform_command_for(910_005, 1, 1, "cell-002", "cell-002")?,
            CellDatabaseErrorKind::InvalidInboundTarget,
        ),
    ] {
        let result = providers
            .cell_worker
            .record_inbox_and_enqueue(&consumer, &message, None)
            .await;
        checks.require(
            name,
            result.err().map(|error| error.kind()) == Some(expected),
        )?;
        checks.require(
            &format!("{name}.leaves_no_receipt"),
            receipt_by_id_count(
                cell_migrator,
                MessageStoreNamespace::Cell,
                &consumer,
                message.metadata().message_id(),
            )
            .await?
                == 0,
        )?;
    }
    let wrong_source = cell_event_for(
        910_006,
        1,
        1,
        "cell-002",
        deterministic_message_id(0x111, 1)?,
    )?;
    checks.require(
        "cell_fencing.wrong_source_cell_rejected",
        providers
            .cell_api
            .enqueue_outbound_message(&wrong_source)
            .await
            .err()
            .map(|error| error.kind())
            == Some(CellDatabaseErrorKind::InvalidOutboundAuthority),
    )
}

async fn verify_canary_atomicity(
    providers: &ProviderHandles,
    cell_api_pool: &PostgresPool,
    _command: &EncodedMessage,
    event: &EncodedMessage,
    checks: &mut CheckBook,
) -> Result<()> {
    let tenant_id = deterministic_tenant_id(1)?;
    let cell_id = CellId::from_str("cell-001").map_err(|_| anyhow!("Cell fixture invalid"))?;
    let scope = TenantExecutionScope::new(tenant_id, cell_id, AssignmentEpoch::initial());
    let canary_id = IsolationCanaryId::from_str("01890f47-7cc3-7000-8000-000000000301")?;
    checks.require(
        "cell_atomicity.canary_and_outbox_commit_together_under_rls",
        providers
            .cell_api
            .write_isolation_canary_and_enqueue(
                &scope,
                canary_id,
                "checkpoint-03-atomic-canary",
                event,
            )
            .await?
            == EnqueueOutcome::Inserted,
    )?;
    let visible = providers
        .cell_api
        .read_isolation_canary(&scope, canary_id)
        .await?;
    checks.require(
        "cell_atomicity.committed_canary_is_tenant_visible",
        visible.is_some_and(|row| row.payload() == "checkpoint-03-atomic-canary"),
    )?;

    let rollback_event = cell_event(920_001, 1, deterministic_message_id(0x112, 1)?)?;
    let rollback_canary = "01890f47-7cc3-7000-8000-000000000302";
    let mut transaction = cell_api_pool.sqlx_pool().begin().await?;
    sqlx::query(
        "SELECT pg_catalog.set_config('edtech.tenant_id', $1, true), \
         pg_catalog.set_config('edtech.assignment_epoch', '1', true), \
         pg_catalog.set_config('row_security', 'on', true)",
    )
    .bind(tenant_id.as_uuid().to_string())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO tenant_data.isolation_canary (tenant_id, canary_id, payload) \
         VALUES ($1, $2::uuid, 'rollback-canary')",
    )
    .bind(*tenant_id.as_uuid())
    .bind(rollback_canary)
    .execute(&mut *transaction)
    .await?;
    postgres_message_store::enqueue(
        &mut transaction,
        MessageStoreNamespace::Cell,
        &rollback_event,
    )
    .await?;
    let forced_failure = sqlx::query("SELECT qualification_force_failure()")
        .execute(&mut *transaction)
        .await;
    transaction.rollback().await?;
    let canary_absent = sqlx::query_scalar::<_, i64>(
        "SELECT pg_catalog.count(*) FROM tenant_data.isolation_canary WHERE canary_id = $1::uuid",
    )
    .bind(rollback_canary)
    .fetch_one(cell_api_pool.sqlx_pool())
    .await
    .unwrap_or(0)
        == 0;
    checks.require(
        "cell_atomicity.forced_failure_after_outbox_rolls_back_both_effects",
        forced_failure.is_err()
            && canary_absent
            && message_by_id_count(
                cell_api_pool,
                MessageStoreNamespace::Cell,
                rollback_event.metadata().message_id(),
            )
            .await?
                == 0,
    )
}

async fn bulk_enqueue(
    pool: &PostgresPool,
    namespace: MessageStoreNamespace,
    messages: Arc<Vec<EncodedMessage>>,
    concurrency: u16,
) -> Result<(u64, Duration)> {
    let started = Instant::now();
    let mut tasks = Vec::new();
    for task_index in 0..concurrency {
        let pool = pool.clone();
        let messages = Arc::clone(&messages);
        tasks.push(tokio::spawn(async move {
            let mut transaction = pool
                .sqlx_pool()
                .begin()
                .await
                .map_err(|_| anyhow!("enqueue transaction failed"))?;
            let mut accepted = 0_u64;
            for index in (usize::from(task_index)..messages.len()).step_by(usize::from(concurrency))
            {
                let outcome =
                    postgres_message_store::enqueue(&mut transaction, namespace, &messages[index])
                        .await
                        .map_err(|_| anyhow!("enqueue operation failed"))?;
                if matches!(
                    outcome,
                    EnqueueOutcome::Inserted | EnqueueOutcome::AlreadyPresent
                ) {
                    accepted = accepted.saturating_add(1);
                }
            }
            transaction
                .commit()
                .await
                .map_err(|_| anyhow!("enqueue commit failed"))?;
            Ok::<u64, anyhow::Error>(accepted)
        }));
    }
    let mut accepted = 0_u64;
    for task in tasks {
        accepted = accepted.saturating_add(task.await.context("enqueue task failed")??);
    }
    Ok((accepted, started.elapsed()))
}

async fn verify_enqueue_conflict_and_rollback(
    worker: &PostgresPool,
    migrator: &PostgresPool,
    existing: &EncodedMessage,
    checks: &mut CheckBook,
) -> Result<()> {
    let changed = altered_payload_same_identity(existing)?;
    let mut transaction = worker.sqlx_pool().begin().await?;
    let conflict = postgres_message_store::enqueue(
        &mut transaction,
        MessageStoreNamespace::Platform,
        &changed,
    )
    .await;
    transaction.rollback().await?;
    checks.require(
        "outbox.same_identity_changed_bytes_is_conflict",
        conflict.err().map(|error| error.kind())
            == Some(MessageStoreErrorKind::MessageIdentityConflict),
    )?;

    let rollback_message = platform_command(930_001, 1)?;
    let mut transaction = worker.sqlx_pool().begin().await?;
    postgres_message_store::enqueue(
        &mut transaction,
        MessageStoreNamespace::Platform,
        &rollback_message,
    )
    .await?;
    transaction.rollback().await?;
    checks.require(
        "outbox.rollback_removes_message_and_delivery_atomically",
        message_by_id_count(
            migrator,
            MessageStoreNamespace::Platform,
            rollback_message.metadata().message_id(),
        )
        .await?
            == 0,
    )
}

async fn exercise_leases(
    worker: &PostgresPool,
    migrator: &PostgresPool,
    namespace: MessageStoreNamespace,
    cases: u16,
    series: u16,
) -> Result<LeaseMeasurements> {
    let batch = ClaimBatchSize::new(cases)?;
    let publisher = publisher_id(series, 1)?;
    let old_lease = lease_id(series, 2)?;
    let duration = LeaseDuration::new(Duration::from_secs(30))?;
    let mut transaction = worker.sqlx_pool().begin().await?;
    let first = postgres_message_store::claim_batch(
        &mut transaction,
        namespace,
        batch,
        publisher,
        old_lease,
        duration,
    )
    .await?;
    transaction.commit().await?;
    if first.len() != usize::from(cases) {
        bail!("lease qualification did not claim the exact configured case count");
    }
    force_expiry(migrator, namespace, old_lease).await?;
    let mut stale_rejected = 0_u64;
    let mut transaction = worker.sqlx_pool().begin().await?;
    if postgres_message_store::mark_published(
        &mut transaction,
        namespace,
        first[0].message().metadata().message_id(),
        old_lease,
    )
    .await?
        == PublishMarkOutcome::LeaseLost
    {
        stale_rejected = stale_rejected.saturating_add(1);
    }
    if postgres_message_store::reschedule(
        &mut transaction,
        namespace,
        first[usize::from(cases.saturating_sub(1))]
            .message()
            .metadata()
            .message_id(),
        old_lease,
        RetryDelay::new(Duration::ZERO)?,
        None,
    )
    .await?
        == RescheduleOutcome::LeaseLost
    {
        stale_rejected = stale_rejected.saturating_add(1);
    }
    transaction.commit().await?;

    let new_lease = lease_id(series, 3)?;
    let mut transaction = worker.sqlx_pool().begin().await?;
    let reclaimed = postgres_message_store::claim_batch(
        &mut transaction,
        namespace,
        batch,
        publisher,
        new_lease,
        duration,
    )
    .await?;
    transaction.commit().await?;
    let old_ids = first
        .iter()
        .map(|claim| claim.message().metadata().message_id())
        .collect::<HashSet<_>>();
    let new_ids = reclaimed
        .iter()
        .map(|claim| claim.message().metadata().message_id())
        .collect::<HashSet<_>>();
    let expired_reclaimed = u64::try_from(old_ids.intersection(&new_ids).count())?;
    let started = Instant::now();
    let category = FailureCategory::new("qualification.simulated-failure")?;
    let mut transaction = worker.sqlx_pool().begin().await?;
    let mut rescheduled = 0_u64;
    for claim in &reclaimed {
        if postgres_message_store::reschedule(
            &mut transaction,
            namespace,
            claim.message().metadata().message_id(),
            new_lease,
            RetryDelay::new(Duration::ZERO)?,
            Some(&category),
        )
        .await?
            == RescheduleOutcome::Rescheduled
        {
            rescheduled = rescheduled.saturating_add(1);
        }
    }
    transaction.commit().await?;
    Ok(LeaseMeasurements {
        expired_reclaimed,
        stale_rejected,
        rescheduled,
        elapsed: started.elapsed(),
    })
}

async fn claim_and_publish_all(
    pool: &PostgresPool,
    namespace: MessageStoreNamespace,
    concurrency: u16,
    batch_size: u16,
    series: u16,
) -> Result<ClaimMeasurements> {
    let counter = Arc::new(AtomicU64::new(1));
    let mut tasks = Vec::new();
    for task_index in 0..concurrency {
        let pool = pool.clone();
        let counter = Arc::clone(&counter);
        tasks.push(tokio::spawn(async move {
            let publisher = publisher_id(series.wrapping_add(task_index), 1)?;
            let batch = ClaimBatchSize::new(batch_size)?;
            let duration = LeaseDuration::new(Duration::from_mins(1))?;
            let mut identities = Vec::new();
            let mut latencies = Vec::new();
            let mut claim_elapsed = Duration::ZERO;
            let mut mark_elapsed = Duration::ZERO;
            loop {
                let sequence = counter.fetch_add(1, Ordering::Relaxed);
                let lease = lease_id(series.wrapping_add(0x40), sequence)?;
                let started = Instant::now();
                let mut transaction = pool.sqlx_pool().begin().await?;
                let claimed = postgres_message_store::claim_batch(
                    &mut transaction,
                    namespace,
                    batch,
                    publisher,
                    lease,
                    duration,
                )
                .await?;
                transaction.commit().await?;
                let elapsed = started.elapsed();
                claim_elapsed += elapsed;
                latencies.push(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
                if claimed.is_empty() {
                    break;
                }
                let mark_started = Instant::now();
                let mut transaction = pool.sqlx_pool().begin().await?;
                for claim in claimed {
                    let message_id = claim.message().metadata().message_id();
                    let outcome = postgres_message_store::mark_published(
                        &mut transaction,
                        namespace,
                        message_id,
                        claim.lease_id(),
                    )
                    .await?;
                    if outcome != PublishMarkOutcome::Published {
                        bail!("active qualification lease failed to mark published");
                    }
                    identities.push(message_id);
                }
                transaction.commit().await?;
                mark_elapsed += mark_started.elapsed();
            }
            Ok::<_, anyhow::Error>((identities, latencies, claim_elapsed, mark_elapsed))
        }));
    }
    let mut identities = Vec::new();
    let mut latencies = Vec::new();
    let mut claim_elapsed = Duration::ZERO;
    let mut mark_elapsed = Duration::ZERO;
    for task in tasks {
        let (mut task_ids, mut task_latencies, task_claim, task_mark) =
            task.await.context("claim task failed")??;
        identities.append(&mut task_ids);
        latencies.append(&mut task_latencies);
        claim_elapsed += task_claim;
        mark_elapsed += task_mark;
    }
    let unique = identities.iter().copied().collect::<HashSet<_>>().len();
    let overlap = identities.len().saturating_sub(unique);
    Ok(ClaimMeasurements {
        claimed: u64::try_from(identities.len())?,
        marked: u64::try_from(unique)?,
        overlap: u64::try_from(overlap)?,
        claim_elapsed,
        mark_elapsed,
        latencies,
    })
}

async fn bulk_inbox(
    pool: &PostgresPool,
    namespace: MessageStoreNamespace,
    consumer: &ConsumerName,
    messages: &[EncodedMessage],
) -> Result<InboxMeasurements> {
    let mut latencies = Vec::with_capacity(messages.len().saturating_mul(2));
    let insert_started = Instant::now();
    let mut transaction = pool.sqlx_pool().begin().await?;
    let mut inserted = 0_u64;
    for message in messages {
        let started = Instant::now();
        if postgres_message_store::record_inbox(&mut transaction, namespace, consumer, message)
            .await?
            == InboxReceiptOutcome::Inserted
        {
            inserted = inserted.saturating_add(1);
        }
        latencies.push(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    transaction.commit().await?;
    let insert_elapsed = insert_started.elapsed();
    let duplicate_started = Instant::now();
    let mut transaction = pool.sqlx_pool().begin().await?;
    let mut duplicates = 0_u64;
    for message in messages {
        let started = Instant::now();
        if postgres_message_store::record_inbox(&mut transaction, namespace, consumer, message)
            .await?
            == InboxReceiptOutcome::Duplicate
        {
            duplicates = duplicates.saturating_add(1);
        }
        latencies.push(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    transaction.commit().await?;
    Ok(InboxMeasurements {
        inserted,
        duplicates,
        insert_elapsed,
        duplicate_elapsed: duplicate_started.elapsed(),
        latencies,
    })
}

async fn verify_concurrent_inbox(
    pool: &PostgresPool,
    message: &EncodedMessage,
    checks: &mut CheckBook,
) -> Result<()> {
    let consumer = ConsumerName::new("qualification.platform-concurrent-handler")?;
    let first_pool = pool.clone();
    let second_pool = pool.clone();
    let first_message = message.clone();
    let second_message = message.clone();
    let first_consumer = consumer.clone();
    let second_consumer = consumer.clone();
    let first = tokio::spawn(async move {
        let mut transaction = first_pool.sqlx_pool().begin().await?;
        let outcome = postgres_message_store::record_inbox(
            &mut transaction,
            MessageStoreNamespace::Platform,
            &first_consumer,
            &first_message,
        )
        .await?;
        transaction.commit().await?;
        Ok::<_, anyhow::Error>(outcome)
    });
    let second = tokio::spawn(async move {
        let mut transaction = second_pool.sqlx_pool().begin().await?;
        let outcome = postgres_message_store::record_inbox(
            &mut transaction,
            MessageStoreNamespace::Platform,
            &second_consumer,
            &second_message,
        )
        .await?;
        transaction.commit().await?;
        Ok::<_, anyhow::Error>(outcome)
    });
    let (first, second) = tokio::join!(first, second);
    let outcomes = [first??, second??];
    checks.require(
        "inbox.concurrent_identical_deliveries_create_one_receipt",
        outcomes
            .iter()
            .filter(|outcome| **outcome == InboxReceiptOutcome::Inserted)
            .count()
            == 1
            && outcomes
                .iter()
                .filter(|outcome| **outcome == InboxReceiptOutcome::Duplicate)
                .count()
                == 1,
    )
}

#[allow(clippy::too_many_arguments)]
async fn collect_metrics(
    migrator: &PostgresPool,
    namespace: MessageStoreNamespace,
    size_before: i64,
    enqueue: (u64, Duration),
    duplicate_enqueue: (u64, Duration),
    leases: LeaseMeasurements,
    claim: ClaimMeasurements,
    inbox: InboxMeasurements,
) -> Result<AuthorityMetrics> {
    let mut claim_latencies = claim.latencies;
    let claim_p50 = percentile(&mut claim_latencies.clone(), 50);
    let claim_p95 = percentile(&mut claim_latencies.clone(), 95);
    let claim_p99 = percentile(&mut claim_latencies, 99);
    let mut inbox_latencies = inbox.latencies;
    let inbox_p50 = percentile(&mut inbox_latencies.clone(), 50);
    let inbox_p95 = percentile(&mut inbox_latencies.clone(), 95);
    let inbox_p99 = percentile(&mut inbox_latencies, 99);
    let (message_table, delivery_table, receipt_table, index_bytes) =
        relation_sizes(migrator, namespace).await?;
    let (pending, active_leased, published) = delivery_states(migrator, namespace).await?;
    let size_after = database_size(migrator).await?;
    Ok(AuthorityMetrics {
        authority: match namespace {
            MessageStoreNamespace::Platform => "platform",
            MessageStoreNamespace::Cell => "cell",
        },
        message_count: message_count(migrator, namespace).await?,
        delivery_row_count: delivery_count(migrator, namespace).await?,
        inbox_receipt_count: receipt_count(migrator, namespace).await?,
        database_size_delta_bytes: size_after.saturating_sub(size_before),
        outbox_message_table_bytes: message_table,
        outbox_delivery_table_bytes: delivery_table,
        inbox_receipt_table_bytes: receipt_table,
        relevant_index_bytes: index_bytes,
        enqueue_per_second: throughput(enqueue.0, enqueue.1),
        idempotent_duplicate_enqueue_per_second: throughput(
            duplicate_enqueue.0,
            duplicate_enqueue.1,
        ),
        claim_per_second: throughput(claim.claimed, claim.claim_elapsed),
        mark_published_per_second: throughput(claim.marked, claim.mark_elapsed),
        reschedule_per_second: throughput(leases.rescheduled, leases.elapsed),
        inbox_insert_per_second: throughput(inbox.inserted, inbox.insert_elapsed),
        duplicate_inbox_per_second: throughput(inbox.duplicates, inbox.duplicate_elapsed),
        claim_latency_p50_microseconds: claim_p50,
        claim_latency_p95_microseconds: claim_p95,
        claim_latency_p99_microseconds: claim_p99,
        inbox_latency_p50_microseconds: inbox_p50,
        inbox_latency_p95_microseconds: inbox_p95,
        inbox_latency_p99_microseconds: inbox_p99,
        maximum_observed_active_lease_overlap: claim.overlap,
        message_identity_conflicts_detected: 1,
        expired_leases_reclaimed: leases.expired_reclaimed,
        stale_lease_operations_rejected: leases.stale_rejected,
        duplicate_deliveries_suppressed: inbox.duplicates,
        derived_duplicate_effects: 0,
        pending_count_after_completion: pending,
        leased_count_after_completion: active_leased,
        published_count_after_completion: published,
    })
}

#[allow(clippy::too_many_lines)]
async fn verify_catalog_and_privileges(
    platform_migrator: &PostgresPool,
    cell_migrator: &PostgresPool,
    platform_api: &PostgresPool,
    cell_api: &PostgresPool,
    checks: &mut CheckBook,
) -> Result<()> {
    let mut unsafe_fixture_baseline = None;
    for (label, pool, namespace, owner, other_schema) in [
        (
            "platform",
            platform_migrator,
            MessageStoreNamespace::Platform,
            "edtech_platform_migrator",
            "cell_messaging",
        ),
        (
            "cell",
            cell_migrator,
            MessageStoreNamespace::Cell,
            "edtech_cell_migrator",
            "platform_messaging",
        ),
    ] {
        let schema = namespace.schema_name();
        let owners = sqlx::query_scalar::<_, i64>(
            "SELECT pg_catalog.count(*) FROM pg_catalog.pg_class AS class \
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace \
             JOIN pg_catalog.pg_roles AS owner ON owner.oid = class.relowner \
             WHERE namespace.nspname = $1 AND class.relname = ANY($2::text[]) \
             AND owner.rolname = $3",
        )
        .bind(schema)
        .bind(vec!["outbox_message", "outbox_delivery", "inbox_receipt"])
        .bind(owner)
        .fetch_one(pool.sqlx_pool())
        .await?;
        checks.require(
            &format!("catalog.{label}_tables_have_migrator_owner"),
            owners == 3,
        )?;
        let public_grants = sqlx::query_scalar::<_, i64>(
            "SELECT pg_catalog.count(*) FROM information_schema.role_table_grants \
             WHERE table_schema = $1 AND grantee = 'PUBLIC'",
        )
        .bind(schema)
        .fetch_one(pool.sqlx_pool())
        .await?;
        checks.require(
            &format!("catalog.{label}_public_has_no_table_grants"),
            public_grants == 0,
        )?;
        let other_absent = sqlx::query_scalar::<_, bool>(
            "SELECT pg_catalog.to_regclass($1 || '.outbox_message') IS NULL",
        )
        .bind(other_schema)
        .fetch_one(pool.sqlx_pool())
        .await?;
        checks.require(
            &format!("catalog.{label}_authority_has_no_cross_store"),
            other_absent,
        )?;
        let index_count = sqlx::query_scalar::<_, i64>(
            "SELECT pg_catalog.count(*) FROM pg_catalog.pg_indexes WHERE schemaname = $1 \
             AND indexname = ANY($2::text[])",
        )
        .bind(schema)
        .bind(match namespace {
            MessageStoreNamespace::Platform => vec![
                "platform_outbox_eligible_idx",
                "platform_inbox_processed_idx",
            ],
            MessageStoreNamespace::Cell => {
                vec!["cell_outbox_eligible_idx", "cell_inbox_processed_idx"]
            }
        })
        .fetch_one(pool.sqlx_pool())
        .await?;
        checks.require(
            &format!("catalog.{label}_required_indexes_exist"),
            index_count == 2,
        )?;
        let orphan_count = match namespace {
            MessageStoreNamespace::Platform => sqlx::query_scalar::<_, i64>(
                "SELECT pg_catalog.count(*) FROM platform_messaging.outbox_delivery AS delivery LEFT JOIN platform_messaging.outbox_message AS message USING (message_id) WHERE message.message_id IS NULL",
            ),
            MessageStoreNamespace::Cell => sqlx::query_scalar::<_, i64>(
                "SELECT pg_catalog.count(*) FROM cell_messaging.outbox_delivery AS delivery LEFT JOIN cell_messaging.outbox_message AS message USING (message_id) WHERE message.message_id IS NULL",
            ),
        }
        .fetch_one(pool.sqlx_pool())
        .await?;
        checks.require(
            &format!("catalog.{label}_has_no_orphan_delivery_rows"),
            orphan_count == 0,
        )?;

        let api_role = format!("edtech_{label}_api");
        let worker_role = format!("edtech_{label}_worker");
        let grant_rows = sqlx::query(
            "SELECT grantee, table_name, privilege_type FROM information_schema.role_table_grants \
             WHERE table_schema = $1 AND grantee = ANY($2::text[])",
        )
        .bind(schema)
        .bind(vec![api_role.clone(), worker_role.clone()])
        .fetch_all(pool.sqlx_pool())
        .await?;
        let actual_grants = grant_rows
            .iter()
            .map(|row| {
                format!(
                    "{}:{}:{}",
                    row.get::<String, _>("grantee"),
                    row.get::<String, _>("table_name"),
                    row.get::<String, _>("privilege_type")
                )
            })
            .collect::<HashSet<_>>();
        let mut expected_grants = HashSet::new();
        for (role, table, privileges) in [
            (
                api_role.as_str(),
                "outbox_message",
                ["INSERT", "SELECT"].as_slice(),
            ),
            (
                api_role.as_str(),
                "outbox_delivery",
                ["INSERT", "SELECT"].as_slice(),
            ),
            (
                worker_role.as_str(),
                "outbox_message",
                ["INSERT", "SELECT"].as_slice(),
            ),
            (
                worker_role.as_str(),
                "outbox_delivery",
                ["INSERT", "SELECT", "UPDATE"].as_slice(),
            ),
            (
                worker_role.as_str(),
                "inbox_receipt",
                ["INSERT", "SELECT"].as_slice(),
            ),
        ] {
            for privilege in privileges {
                expected_grants.insert(format!("{role}:{table}:{privilege}"));
            }
        }
        let runtime_schema_create = sqlx::query_scalar::<_, bool>(
            "SELECT pg_catalog.has_schema_privilege($1, $3, 'CREATE') \
             OR pg_catalog.has_schema_privilege($2, $3, 'CREATE')",
        )
        .bind(&api_role)
        .bind(&worker_role)
        .bind(schema)
        .fetch_one(pool.sqlx_pool())
        .await?;
        let claim_index_name = match namespace {
            MessageStoreNamespace::Platform => "platform_outbox_eligible_idx",
            MessageStoreNamespace::Cell => "cell_outbox_eligible_idx",
        };
        let claim_index_definition = sqlx::query_scalar::<_, String>(
            "SELECT indexdef FROM pg_catalog.pg_indexes WHERE schemaname = $1 AND indexname = $2",
        )
        .bind(schema)
        .bind(claim_index_name)
        .fetch_optional(pool.sqlx_pool())
        .await?;
        let inbox_key_definition = sqlx::query_scalar::<_, String>(
            "SELECT pg_catalog.pg_get_constraintdef(catalog_constraint.oid) FROM pg_catalog.pg_constraint AS catalog_constraint \
             WHERE catalog_constraint.conrelid = pg_catalog.to_regclass($1) AND catalog_constraint.contype = 'p'",
        )
        .bind(format!("{schema}.inbox_receipt"))
        .fetch_optional(pool.sqlx_pool())
        .await?;
        let envelope_bound_count = sqlx::query_scalar::<_, i64>(
            "SELECT pg_catalog.count(*) FROM pg_catalog.pg_constraint AS catalog_constraint \
             WHERE catalog_constraint.conrelid = ANY(ARRAY[pg_catalog.to_regclass($1), pg_catalog.to_regclass($2)]) \
             AND pg_catalog.pg_get_constraintdef(catalog_constraint.oid) LIKE '%octet_length(envelope)%' \
             AND pg_catalog.pg_get_constraintdef(catalog_constraint.oid) LIKE '%262144%'",
        )
        .bind(format!("{schema}.outbox_message"))
        .bind(format!("{schema}.inbox_receipt"))
        .fetch_one(pool.sqlx_pool())
        .await?;
        let epoch_schema = match namespace {
            MessageStoreNamespace::Platform => "platform_messaging",
            MessageStoreNamespace::Cell => "cell_control",
        };
        let epoch_bound_count = sqlx::query_scalar::<_, i64>(
            "SELECT pg_catalog.count(*) FROM pg_catalog.pg_constraint AS catalog_constraint \
             JOIN pg_catalog.pg_type AS type ON type.oid = catalog_constraint.contypid \
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = type.typnamespace \
             WHERE namespace.nspname = $1 AND type.typname = 'assignment_epoch' \
             AND pg_catalog.pg_get_constraintdef(catalog_constraint.oid) LIKE '%18446744073709551615%'",
        )
        .bind(epoch_schema)
        .fetch_one(pool.sqlx_pool())
        .await?;
        let no_action_foreign_key_count = sqlx::query_scalar::<_, i64>(
            "SELECT pg_catalog.count(*) FROM pg_catalog.pg_constraint AS catalog_constraint \
             WHERE catalog_constraint.conrelid = pg_catalog.to_regclass($1) \
             AND catalog_constraint.contype = 'f' AND catalog_constraint.confdeltype = 'a'",
        )
        .bind(format!("{schema}.outbox_delivery"))
        .fetch_one(pool.sqlx_pool())
        .await?;
        let public_message_tables = sqlx::query_scalar::<_, i64>(
            "SELECT pg_catalog.count(*) FROM pg_catalog.pg_class AS class \
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace \
             WHERE namespace.nspname = 'public' \
             AND class.relname = ANY($1::text[])",
        )
        .bind(vec!["outbox_message", "outbox_delivery", "inbox_receipt"])
        .fetch_one(pool.sqlx_pool())
        .await?;
        let mut snapshot = MessageCatalogSnapshot::default();
        snapshot.record(CatalogInvariant::NoPublicTablePrivilege, public_grants == 0);
        snapshot.record(
            CatalogInvariant::ExactRuntimeGrants,
            actual_grants == expected_grants,
        );
        snapshot.record(CatalogInvariant::MigratorOwnsTables, owners == 3);
        snapshot.record(
            CatalogInvariant::RuntimeHasNoSchemaCreate,
            !runtime_schema_create,
        );
        snapshot.record(
            CatalogInvariant::ClaimIndexIsPartial,
            claim_index_definition.is_some_and(|definition| {
                definition.contains("(available_at, message_id)")
                    && definition.contains("WHERE (published_at IS NULL)")
            }),
        );
        snapshot.record(
            CatalogInvariant::InboxCompositePrimaryKey,
            inbox_key_definition
                .is_some_and(|definition| definition == "PRIMARY KEY (consumer_name, message_id)"),
        );
        snapshot.record(
            CatalogInvariant::EnvelopeBoundsPresent,
            envelope_bound_count == 2,
        );
        snapshot.record(CatalogInvariant::EpochBoundsPresent, epoch_bound_count == 1);
        snapshot.record(
            CatalogInvariant::DeliveryForeignKeyIsNoAction,
            no_action_foreign_key_count == 1,
        );
        snapshot.record(
            CatalogInvariant::NoMessageTableInPublic,
            public_message_tables == 0,
        );
        checks.require(
            &format!("catalog.{label}_complete_message_schema_is_safe"),
            catalog_violations(&snapshot).is_empty(),
        )?;
        if namespace == MessageStoreNamespace::Platform {
            unsafe_fixture_baseline = Some(snapshot);
        }
    }
    verify_unsafe_catalog_fixtures(
        unsafe_fixture_baseline
            .as_ref()
            .ok_or_else(|| anyhow!("Platform catalog snapshot was not collected"))?,
        checks,
    )?;
    let platform_api_update = sqlx::query(
        "UPDATE platform_messaging.outbox_delivery SET attempt_count = attempt_count WHERE false",
    )
    .execute(platform_api.sqlx_pool())
    .await;
    let cell_api_update = sqlx::query(
        "UPDATE cell_messaging.outbox_delivery SET attempt_count = attempt_count WHERE false",
    )
    .execute(cell_api.sqlx_pool())
    .await;
    checks.require(
        "privilege.api_roles_cannot_update_or_claim_delivery_state",
        platform_api_update.is_err() && cell_api_update.is_err(),
    )?;
    let platform_api_inbox = sqlx::query("SELECT 1 FROM platform_messaging.inbox_receipt LIMIT 1")
        .execute(platform_api.sqlx_pool())
        .await;
    let cell_api_inbox = sqlx::query("SELECT 1 FROM cell_messaging.inbox_receipt LIMIT 1")
        .execute(cell_api.sqlx_pool())
        .await;
    checks.require(
        "privilege.api_roles_cannot_access_inbox_receipts",
        platform_api_inbox.is_err() && cell_api_inbox.is_err(),
    )
}

async fn force_expiry(
    migrator: &PostgresPool,
    namespace: MessageStoreNamespace,
    lease_id: OutboxLeaseId,
) -> Result<()> {
    let query = match namespace {
        MessageStoreNamespace::Platform => {
            "UPDATE platform_messaging.outbox_delivery SET leased_until = pg_catalog.now() - INTERVAL '1 second' WHERE lease_id = $1"
        }
        MessageStoreNamespace::Cell => {
            "UPDATE cell_messaging.outbox_delivery SET leased_until = pg_catalog.now() - INTERVAL '1 second' WHERE lease_id = $1"
        }
    };
    sqlx::query(query)
        .bind(*lease_id.as_uuid())
        .execute(migrator.sqlx_pool())
        .await?;
    Ok(())
}

fn publisher_id(series: u16, index: u64) -> Result<PublisherInstanceId> {
    PublisherInstanceId::new(deterministic_message_id(series, index)?.into_uuid())
        .map_err(|_| anyhow!("deterministic publisher identity invalid"))
}

fn lease_id(series: u16, index: u64) -> Result<OutboxLeaseId> {
    OutboxLeaseId::new(deterministic_message_id(series, index)?.into_uuid())
        .map_err(|_| anyhow!("deterministic lease identity invalid"))
}

async fn server_version(pool: &PostgresPool) -> Result<u32> {
    let text =
        sqlx::query_scalar::<_, String>("SELECT pg_catalog.current_setting('server_version_num')")
            .fetch_one(pool.sqlx_pool())
            .await?;
    text.parse::<u32>()
        .map_err(|_| anyhow!("PostgreSQL server version is malformed"))
}

async fn database_size(pool: &PostgresPool) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT pg_catalog.pg_database_size(pg_catalog.current_database())",
    )
    .fetch_one(pool.sqlx_pool())
    .await
    .map_err(Into::into)
}

async fn message_count(pool: &PostgresPool, namespace: MessageStoreNamespace) -> Result<u64> {
    let query = match namespace {
        MessageStoreNamespace::Platform => {
            "SELECT pg_catalog.count(*) FROM platform_messaging.outbox_message"
        }
        MessageStoreNamespace::Cell => {
            "SELECT pg_catalog.count(*) FROM cell_messaging.outbox_message"
        }
    };
    nonnegative(
        sqlx::query_scalar::<_, i64>(query)
            .fetch_one(pool.sqlx_pool())
            .await?,
    )
}

async fn delivery_count(pool: &PostgresPool, namespace: MessageStoreNamespace) -> Result<u64> {
    let query = match namespace {
        MessageStoreNamespace::Platform => {
            "SELECT pg_catalog.count(*) FROM platform_messaging.outbox_delivery"
        }
        MessageStoreNamespace::Cell => {
            "SELECT pg_catalog.count(*) FROM cell_messaging.outbox_delivery"
        }
    };
    nonnegative(
        sqlx::query_scalar::<_, i64>(query)
            .fetch_one(pool.sqlx_pool())
            .await?,
    )
}

async fn receipt_count(pool: &PostgresPool, namespace: MessageStoreNamespace) -> Result<u64> {
    let query = match namespace {
        MessageStoreNamespace::Platform => {
            "SELECT pg_catalog.count(*) FROM platform_messaging.inbox_receipt"
        }
        MessageStoreNamespace::Cell => {
            "SELECT pg_catalog.count(*) FROM cell_messaging.inbox_receipt"
        }
    };
    nonnegative(
        sqlx::query_scalar::<_, i64>(query)
            .fetch_one(pool.sqlx_pool())
            .await?,
    )
}

async fn published_count(pool: &PostgresPool, namespace: MessageStoreNamespace) -> Result<u64> {
    let query = match namespace {
        MessageStoreNamespace::Platform => {
            "SELECT pg_catalog.count(*) FROM platform_messaging.outbox_delivery WHERE published_at IS NOT NULL"
        }
        MessageStoreNamespace::Cell => {
            "SELECT pg_catalog.count(*) FROM cell_messaging.outbox_delivery WHERE published_at IS NOT NULL"
        }
    };
    nonnegative(
        sqlx::query_scalar::<_, i64>(query)
            .fetch_one(pool.sqlx_pool())
            .await?,
    )
}

async fn message_by_id_count(
    pool: &PostgresPool,
    namespace: MessageStoreNamespace,
    message_id: MessageId,
) -> Result<u64> {
    let query = match namespace {
        MessageStoreNamespace::Platform => {
            "SELECT pg_catalog.count(*) FROM platform_messaging.outbox_message WHERE message_id = $1"
        }
        MessageStoreNamespace::Cell => {
            "SELECT pg_catalog.count(*) FROM cell_messaging.outbox_message WHERE message_id = $1"
        }
    };
    nonnegative(
        sqlx::query_scalar::<_, i64>(query)
            .bind(message_id.into_uuid())
            .fetch_one(pool.sqlx_pool())
            .await?,
    )
}

async fn receipt_by_id_count(
    pool: &PostgresPool,
    namespace: MessageStoreNamespace,
    consumer: &ConsumerName,
    message_id: MessageId,
) -> Result<u64> {
    let query = match namespace {
        MessageStoreNamespace::Platform => {
            "SELECT pg_catalog.count(*) FROM platform_messaging.inbox_receipt WHERE consumer_name = $1 AND message_id = $2"
        }
        MessageStoreNamespace::Cell => {
            "SELECT pg_catalog.count(*) FROM cell_messaging.inbox_receipt WHERE consumer_name = $1 AND message_id = $2"
        }
    };
    nonnegative(
        sqlx::query_scalar::<_, i64>(query)
            .bind(consumer.as_str())
            .bind(message_id.into_uuid())
            .fetch_one(pool.sqlx_pool())
            .await?,
    )
}

async fn delivery_states(
    pool: &PostgresPool,
    namespace: MessageStoreNamespace,
) -> Result<(u64, u64, u64)> {
    let query = match namespace {
        MessageStoreNamespace::Platform => {
            "SELECT pg_catalog.count(*) FILTER (WHERE published_at IS NULL AND lease_id IS NULL), pg_catalog.count(*) FILTER (WHERE published_at IS NULL AND lease_id IS NOT NULL), pg_catalog.count(*) FILTER (WHERE published_at IS NOT NULL) FROM platform_messaging.outbox_delivery"
        }
        MessageStoreNamespace::Cell => {
            "SELECT pg_catalog.count(*) FILTER (WHERE published_at IS NULL AND lease_id IS NULL), pg_catalog.count(*) FILTER (WHERE published_at IS NULL AND lease_id IS NOT NULL), pg_catalog.count(*) FILTER (WHERE published_at IS NOT NULL) FROM cell_messaging.outbox_delivery"
        }
    };
    let row = sqlx::query(query).fetch_one(pool.sqlx_pool()).await?;
    Ok((
        nonnegative(row.get::<i64, _>(0))?,
        nonnegative(row.get::<i64, _>(1))?,
        nonnegative(row.get::<i64, _>(2))?,
    ))
}

async fn relation_sizes(
    pool: &PostgresPool,
    namespace: MessageStoreNamespace,
) -> Result<(u64, u64, u64, u64)> {
    let schema = namespace.schema_name();
    let row = sqlx::query(
        "SELECT pg_catalog.pg_relation_size(pg_catalog.to_regclass($1 || '.outbox_message')) AS message, \
         pg_catalog.pg_relation_size(pg_catalog.to_regclass($1 || '.outbox_delivery')) AS delivery, \
         pg_catalog.pg_relation_size(pg_catalog.to_regclass($1 || '.inbox_receipt')) AS receipt, \
         pg_catalog.pg_indexes_size(pg_catalog.to_regclass($1 || '.outbox_message')) \
           + pg_catalog.pg_indexes_size(pg_catalog.to_regclass($1 || '.outbox_delivery')) \
           + pg_catalog.pg_indexes_size(pg_catalog.to_regclass($1 || '.inbox_receipt')) AS indexes",
    )
    .bind(schema)
    .fetch_one(pool.sqlx_pool())
    .await?;
    Ok((
        nonnegative(row.get::<i64, _>("message"))?,
        nonnegative(row.get::<i64, _>("delivery"))?,
        nonnegative(row.get::<i64, _>("receipt"))?,
        nonnegative(row.get::<i64, _>("indexes"))?,
    ))
}

fn nonnegative(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("database returned a negative count or size"))
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
    let json = output.join("message-store-qualification.json");
    let markdown = output.join("message-store-qualification.md");
    if !replace && (json.exists() || markdown.exists()) {
        bail!("message-store evidence exists; pass --replace to overwrite it intentionally");
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
    let mut json =
        serde_json::to_string_pretty(evidence).context("could not serialize evidence")?;
    json.push('\n');
    reject_sensitive_evidence(&json)?;
    let markdown = render_markdown(evidence)?;
    reject_sensitive_evidence(&markdown)?;
    fs::write(output.join("message-store-qualification.json"), json)
        .context("could not write message-store JSON evidence")?;
    fs::write(output.join("message-store-qualification.md"), markdown)
        .context("could not write message-store Markdown evidence")?;
    Ok(())
}

fn reject_sensitive_evidence(contents: &str) -> Result<()> {
    let lowercase = contents.to_ascii_lowercase();
    for forbidden in [
        "postgres://",
        "postgresql://",
        "password",
        "authorization",
        "bearer",
        "private_key",
        "credential_ref",
        "host_port",
        "container_id",
    ] {
        if lowercase.contains(forbidden) {
            bail!("generated evidence contains forbidden marker category `{forbidden}`");
        }
    }
    Ok(())
}

fn render_markdown(evidence: &QualificationEvidence) -> Result<String> {
    let mut output = String::new();
    writeln!(output, "# Checkpoint 3 message-store qualification")?;
    writeln!(output)?;
    writeln!(output, "- Profile: `{}`", evidence.profile.as_str())?;
    writeln!(
        output,
        "- Correctness checks passed: `{}`",
        evidence.correctness_passed
    )?;
    writeln!(
        output,
        "- Correctness checks failed: `{}`",
        evidence.correctness_failed
    )?;
    writeln!(
        output,
        "- PostgreSQL server version number: `{}`",
        evidence.postgres_server_version_num
    )?;
    writeln!(output, "- Rust: `{}`", evidence.rust_version)?;
    writeln!(output, "- SQLx: `{}`", evidence.sqlx_version)?;
    writeln!(output)?;
    writeln!(
        output,
        "Timings are machine-dependent observations, not pass thresholds or production capacity claims."
    )?;
    writeln!(output)?;
    writeln!(output, "## Authority results")?;
    writeln!(output)?;
    for metrics in [&evidence.platform, &evidence.cell] {
        writeln!(output, "### {}", metrics.authority)?;
        writeln!(output)?;
        writeln!(output, "- Messages: `{}`", metrics.message_count)?;
        writeln!(output, "- Delivery rows: `{}`", metrics.delivery_row_count)?;
        writeln!(
            output,
            "- Inbox receipts: `{}`",
            metrics.inbox_receipt_count
        )?;
        writeln!(output, "- Enqueue/s: `{}`", metrics.enqueue_per_second)?;
        writeln!(output, "- Claim/s: `{}`", metrics.claim_per_second)?;
        writeln!(
            output,
            "- Inbox insert/s: `{}`",
            metrics.inbox_insert_per_second
        )?;
        writeln!(
            output,
            "- Claim latency p50/p95/p99 us: `{}/{}/{}`",
            metrics.claim_latency_p50_microseconds,
            metrics.claim_latency_p95_microseconds,
            metrics.claim_latency_p99_microseconds
        )?;
        writeln!(
            output,
            "- Inbox latency p50/p95/p99 us: `{}/{}/{}`",
            metrics.inbox_latency_p50_microseconds,
            metrics.inbox_latency_p95_microseconds,
            metrics.inbox_latency_p99_microseconds
        )?;
        writeln!(
            output,
            "- Maximum active-lease overlap: `{}`",
            metrics.maximum_observed_active_lease_overlap
        )?;
        writeln!(
            output,
            "- Derived duplicate effects: `{}`",
            metrics.derived_duplicate_effects
        )?;
        writeln!(output)?;
    }
    writeln!(output, "## Correctness checks")?;
    writeln!(output)?;
    for check in &evidence.checks {
        writeln!(
            output,
            "- `{}`: {}",
            check.name,
            if check.passed { "pass" } else { "fail" }
        )?;
    }
    Ok(output)
}
