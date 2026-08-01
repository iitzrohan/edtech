//! Mandatory shared-table forced-RLS correctness checks and profile measurements.

use std::{fmt::Write as _, str::FromStr, time::Instant};

use anyhow::{Context, Result, anyhow};
use cell_application::TenantExecutionScope;
use cell_postgres::{CellDatabase, CellDatabaseErrorKind, CellRuntimeRole, IsolationCanaryId};
use secrecy::SecretString;
use sqlx::{AssertSqlSafe, PgPool, Postgres, Row, Transaction};
use tenancy_domain::{AssignmentEpoch, CellId, TenantId};

use crate::{
    database::{Credentials, provider_config, raw_pool, safe_database_error},
    model::{
        CandidateMetrics, CheckBook, ProfileParameters, deterministic_canary_id,
        deterministic_tenant_id, duration_microseconds, duration_milliseconds,
        percentile_microseconds, quote_identifier, rate_per_second,
    },
};

pub(crate) async fn run(
    credentials: &Credentials,
    parameters: ProfileParameters,
    checks: &mut CheckBook,
) -> Result<CandidateMetrics> {
    let cell_id = CellId::from_str("cell-001").context("fixed Cell identity must be valid")?;
    let config = provider_config("postgres-qualification", Some("cell-001"), 1)?;
    let database = CellDatabase::connect(
        &credentials.cell_api,
        &config,
        &cell_id,
        CellRuntimeRole::Api,
    )
    .await
    .map_err(|_| anyhow!("Cell RLS qualification adapter connection failed"))?;
    let migrator = raw_pool(&credentials.cell_migrator, 4).await?;
    let bootstrap = raw_pool(&credentials.cell_bootstrap, 2).await?;
    let runtime = raw_pool(&credentials.cell_api, 1).await?;
    cleanup_qualification_tenants(&bootstrap).await?;

    let tenant_a = deterministic_tenant_id(1)?;
    let tenant_b = deterministic_tenant_id(2)?;
    seed_authority(&migrator, tenant_a, "1", true).await?;
    seed_authority(&migrator, tenant_b, "1", true).await?;

    mandatory_correctness(
        credentials,
        &database,
        &runtime,
        &migrator,
        &cell_id,
        tenant_a,
        tenant_b,
        checks,
    )
    .await?;
    database.close().await;
    runtime.close().await;

    let metrics = benchmark(credentials, parameters, &migrator, checks).await?;
    cleanup_qualification_tenants(&bootstrap).await?;
    bootstrap.close().await;
    migrator.close().await;
    Ok(metrics)
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn mandatory_correctness(
    credentials: &Credentials,
    database: &CellDatabase,
    runtime: &PgPool,
    migrator: &PgPool,
    cell_id: &CellId,
    tenant_a: TenantId,
    tenant_b: TenantId,
    checks: &mut CheckBook,
) -> Result<()> {
    let epoch_one = AssignmentEpoch::initial();
    let scope_a = TenantExecutionScope::new(tenant_a, cell_id.clone(), epoch_one);
    let scope_b = TenantExecutionScope::new(tenant_b, cell_id.clone(), epoch_one);
    let canary_a = IsolationCanaryId::from_str(&deterministic_canary_id(1))
        .context("fixed canary identity must be valid")?;
    let canary_b = IsolationCanaryId::from_str(&deterministic_canary_id(2))
        .context("fixed canary identity must be valid")?;

    let own_write = database
        .write_isolation_canary(&scope_a, canary_a, "tenant-a")
        .await;
    let own_read = database.read_isolation_canary(&scope_a, canary_a).await;
    checks.require(
        "rls.01_tenant_a_inserts_and_reads_own_canary",
        own_write.is_ok()
            && own_read
                .as_ref()
                .is_ok_and(|row| row.as_ref().is_some_and(|row| row.payload() == "tenant-a")),
    )?;
    checks.require(
        "rls.02_tenant_b_cannot_read_tenant_a",
        database
            .read_isolation_canary(&scope_b, canary_a)
            .await
            .is_ok_and(|row| row.is_none()),
    )?;
    checks.require(
        "rls.03_tenant_b_cannot_update_tenant_a",
        database
            .update_isolation_canary(&scope_b, canary_a, "forbidden")
            .await
            .is_ok_and(|updated| !updated),
    )?;
    checks.require(
        "rls.04_tenant_b_cannot_delete_tenant_a",
        database
            .delete_isolation_canary(&scope_b, canary_a)
            .await
            .is_ok_and(|deleted| !deleted),
    )?;

    let cross_insert = cross_tenant_insert(runtime, tenant_a, tenant_b, canary_b).await?;
    checks.require("rls.05_tenant_a_cannot_insert_tenant_b_row", !cross_insert)?;
    let cross_update = cross_tenant_update(runtime, tenant_a, tenant_b, canary_a).await?;
    checks.require("rls.06_tenant_a_cannot_move_row_to_tenant_b", !cross_update)?;

    let unscoped_count = sqlx::query_scalar::<_, i64>(
        "SELECT pg_catalog.count(*) FROM tenant_data.isolation_canary",
    )
    .fetch_one(runtime)
    .await
    .map_err(safe_database_error("unscoped RLS read"))?;
    checks.require(
        "rls.07_query_without_context_returns_no_rows",
        unscoped_count == 0,
    )?;
    let unscoped_write = sqlx::query(
        "INSERT INTO tenant_data.isolation_canary (tenant_id, canary_id, payload) \
         VALUES ($1, $2::uuid, 'unscoped')",
    )
    .bind(*tenant_a.as_uuid())
    .bind(canary_b.to_string())
    .execute(runtime)
    .await;
    checks.require(
        "rls.08_write_without_context_is_rejected",
        unscoped_write.is_err(),
    )?;

    let absent_tenant = deterministic_tenant_id(900_001)?;
    let absent_scope = TenantExecutionScope::new(absent_tenant, cell_id.clone(), epoch_one);
    checks.require(
        "rls.09_nonexistent_tenant_is_rejected",
        database
            .read_isolation_canary(&absent_scope, canary_a)
            .await
            .err()
            .is_some_and(|error| error.kind() == CellDatabaseErrorKind::TenantAbsent),
    )?;

    let disabled_tenant = deterministic_tenant_id(900_002)?;
    seed_authority(migrator, disabled_tenant, "1", false).await?;
    let disabled_scope = TenantExecutionScope::new(disabled_tenant, cell_id.clone(), epoch_one);
    checks.require(
        "rls.10_disabled_tenant_is_rejected",
        database
            .read_isolation_canary(&disabled_scope, canary_a)
            .await
            .err()
            .is_some_and(|error| error.kind() == CellDatabaseErrorKind::TenantDisabled),
    )?;

    let stale_tenant = deterministic_tenant_id(900_003)?;
    seed_authority(migrator, stale_tenant, "2", true).await?;
    let stale_scope = TenantExecutionScope::new(stale_tenant, cell_id.clone(), epoch_one);
    checks.require(
        "rls.11_stale_assignment_epoch_is_rejected",
        database
            .read_isolation_canary(&stale_scope, canary_a)
            .await
            .err()
            .is_some_and(|error| error.kind() == CellDatabaseErrorKind::StaleAssignmentEpoch),
    )?;

    let newer_tenant = deterministic_tenant_id(900_004)?;
    seed_authority(migrator, newer_tenant, "1", true).await?;
    let epoch_two = AssignmentEpoch::new(2).context("epoch two must be valid")?;
    let newer_scope = TenantExecutionScope::new(newer_tenant, cell_id.clone(), epoch_two);
    checks.require(
        "rls.12_newer_unregistered_epoch_is_rejected",
        database
            .read_isolation_canary(&newer_scope, canary_a)
            .await
            .err()
            .is_some_and(|error| error.kind() == CellDatabaseErrorKind::StaleAssignmentEpoch),
    )?;

    let wrong_cell = CellId::from_str("cell-002").context("fixed Cell identity must be valid")?;
    let wrong_scope = TenantExecutionScope::new(tenant_a, wrong_cell, epoch_one);
    checks.require(
        "rls.13_wrong_cell_is_rejected_before_data_access",
        database
            .read_isolation_canary(&wrong_scope, canary_a)
            .await
            .err()
            .is_some_and(|error| error.kind() == CellDatabaseErrorKind::WrongCell),
    )?;

    database
        .write_isolation_canary(&scope_b, canary_b, "tenant-b")
        .await
        .map_err(|_| anyhow!("tenant B canary setup failed"))?;
    let after_commit = database.read_isolation_canary(&scope_b, canary_a).await;
    checks.require(
        "rls.14_commit_then_connection_reuse_leaks_no_rows",
        after_commit.is_ok_and(|row| row.is_none()),
    )?;
    let rollback_leak = rollback_then_switch(runtime, tenant_a, tenant_b).await?;
    checks.require(
        "rls.15_rollback_then_connection_reuse_leaks_no_context",
        !rollback_leak,
    )?;

    let alternating = alternate_tenants(runtime, tenant_a, tenant_b, 1_000).await?;
    checks.require("rls.16_alternate_1000_times_on_one_connection", alternating)?;
    checks.require(
        "rls.17_sqlx_prepared_query_reused_across_tenants",
        alternating,
    )?;

    let concurrent = concurrent_canaries(credentials, cell_id, migrator).await?;
    checks.require("rls.18_concurrent_32_tenants_have_no_leakage", concurrent)?;

    let row_security_bypass = attempt_row_security_off(runtime, tenant_a).await?;
    checks.require(
        "rls.19_row_security_off_does_not_bypass",
        !row_security_bypass,
    )?;
    let alter = sqlx::query("ALTER TABLE tenant_data.isolation_canary DISABLE ROW LEVEL SECURITY")
        .execute(runtime)
        .await;
    checks.require("rls.20_runtime_cannot_disable_rls", alter.is_err())?;
    let drop_policy =
        sqlx::query("DROP POLICY isolation_canary_tenant_policy ON tenant_data.isolation_canary")
            .execute(runtime)
            .await;
    checks.require("rls.21_runtime_cannot_drop_policy", drop_policy.is_err())?;
    let create_policy = sqlx::query(
        "CREATE POLICY qualification_forbidden ON tenant_data.isolation_canary USING (true)",
    )
    .execute(runtime)
    .await;
    checks.require(
        "rls.22_runtime_cannot_create_policy",
        create_policy.is_err(),
    )?;
    let set_role = sqlx::query("SET ROLE edtech_cell_migrator")
        .execute(runtime)
        .await;
    checks.require(
        "rls.23_runtime_cannot_set_role_to_migrator",
        set_role.is_err(),
    )?;

    verify_catalog_rls(runtime, checks).await?;
    verify_local_setting_cleanup(runtime, tenant_a, checks).await?;
    verify_large_epochs(database, migrator, cell_id, canary_a, checks).await?;

    let invalid_credential = SecretString::from(String::from(
        "postgresql://qualification-secret-sentinel-is-never-rendered",
    ));
    let bad_config = provider_config("postgres-qualification", None, 1)?;
    let safe_failure = platform_postgres::check_database(
        &invalid_credential,
        &bad_config,
        platform_postgres::PlatformRuntimeRole::Api,
    )
    .await;
    let rendered = safe_failure
        .err()
        .map(|error| format!("{error:?} {error}"))
        .unwrap_or_default();
    checks.require(
        "rls.35_secret_values_absent_from_failure_output",
        !rendered.contains("qualification-secret-sentinel"),
    )?;
    Ok(())
}

async fn seed_authority(
    migrator: &PgPool,
    tenant_id: TenantId,
    epoch: &str,
    enabled: bool,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO cell_control.tenant_authority \
             (tenant_id, assignment_epoch, serving_enabled) \
         VALUES ($1, $2::numeric, $3) \
         ON CONFLICT (tenant_id) DO UPDATE SET \
             assignment_epoch = EXCLUDED.assignment_epoch, \
             serving_enabled = EXCLUDED.serving_enabled, \
             updated_at = pg_catalog.now()",
    )
    .bind(*tenant_id.as_uuid())
    .bind(epoch)
    .bind(enabled)
    .execute(migrator)
    .await
    .map_err(safe_database_error("tenant authority seed"))?;
    Ok(())
}

async fn cleanup_qualification_tenants(bootstrap: &PgPool) -> Result<()> {
    sqlx::query(
        "DELETE FROM tenant_data.isolation_canary \
         WHERE tenant_id::text LIKE '01890f47-7cc2-7000-8000-%'",
    )
    .execute(bootstrap)
    .await
    .map_err(safe_database_error("qualification canary cleanup"))?;
    sqlx::query(
        "DELETE FROM cell_control.tenant_authority \
         WHERE tenant_id::text LIKE '01890f47-7cc2-7000-8000-%'",
    )
    .execute(bootstrap)
    .await
    .map_err(safe_database_error("qualification authority cleanup"))?;
    Ok(())
}

async fn set_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    epoch: &str,
) -> Result<()> {
    sqlx::query(
        "SELECT pg_catalog.set_config('edtech.tenant_id', $1, true), \
                pg_catalog.set_config('edtech.assignment_epoch', $2, true), \
                pg_catalog.set_config('row_security', 'on', true)",
    )
    .bind(tenant_id.to_string())
    .bind(epoch)
    .execute(&mut **transaction)
    .await
    .map_err(safe_database_error("tenant transaction context"))?;
    Ok(())
}

async fn cross_tenant_insert(
    pool: &PgPool,
    scope_tenant: TenantId,
    row_tenant: TenantId,
    canary: IsolationCanaryId,
) -> Result<bool> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(safe_database_error("cross-tenant insert begin"))?;
    set_scope(&mut transaction, scope_tenant, "1").await?;
    let result = sqlx::query(
        "INSERT INTO tenant_data.isolation_canary (tenant_id, canary_id, payload) \
         VALUES ($1, $2::uuid, 'forbidden')",
    )
    .bind(*row_tenant.as_uuid())
    .bind(canary.to_string())
    .execute(&mut *transaction)
    .await;
    let succeeded = result.is_ok();
    let _rollback_result = transaction.rollback().await;
    Ok(succeeded)
}

async fn cross_tenant_update(
    pool: &PgPool,
    scope_tenant: TenantId,
    row_tenant: TenantId,
    canary: IsolationCanaryId,
) -> Result<bool> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(safe_database_error("cross-tenant update begin"))?;
    set_scope(&mut transaction, scope_tenant, "1").await?;
    let result = sqlx::query(
        "UPDATE tenant_data.isolation_canary SET tenant_id = $1 \
         WHERE tenant_id = $2 AND canary_id = $3::uuid",
    )
    .bind(*row_tenant.as_uuid())
    .bind(*scope_tenant.as_uuid())
    .bind(canary.to_string())
    .execute(&mut *transaction)
    .await;
    let succeeded = result.is_ok();
    let _rollback_result = transaction.rollback().await;
    Ok(succeeded)
}

async fn rollback_then_switch(
    pool: &PgPool,
    tenant_a: TenantId,
    tenant_b: TenantId,
) -> Result<bool> {
    let mut first = pool
        .begin()
        .await
        .map_err(safe_database_error("rollback isolation begin"))?;
    set_scope(&mut first, tenant_a, "1").await?;
    first
        .rollback()
        .await
        .map_err(safe_database_error("rollback isolation rollback"))?;
    let mut second = pool
        .begin()
        .await
        .map_err(safe_database_error("rollback isolation reuse"))?;
    set_scope(&mut second, tenant_b, "1").await?;
    let leaked = sqlx::query_scalar::<_, i64>(
        "SELECT pg_catalog.count(*) FROM tenant_data.isolation_canary \
         WHERE tenant_id <> $1",
    )
    .bind(*tenant_b.as_uuid())
    .fetch_one(&mut *second)
    .await
    .map_err(safe_database_error("rollback isolation read"))?
        != 0;
    second
        .commit()
        .await
        .map_err(safe_database_error("rollback isolation commit"))?;
    Ok(leaked)
}

async fn alternate_tenants(
    pool: &PgPool,
    tenant_a: TenantId,
    tenant_b: TenantId,
    switches: u32,
) -> Result<bool> {
    for index in 0..switches {
        let tenant = if index % 2 == 0 { tenant_a } else { tenant_b };
        let mut transaction = pool
            .begin()
            .await
            .map_err(safe_database_error("alternating tenant begin"))?;
        set_scope(&mut transaction, tenant, "1").await?;
        let foreign_rows = sqlx::query_scalar::<_, i64>(
            "SELECT pg_catalog.count(*) FROM tenant_data.isolation_canary \
             WHERE tenant_id <> $1",
        )
        .bind(*tenant.as_uuid())
        .persistent(true)
        .fetch_one(&mut *transaction)
        .await
        .map_err(safe_database_error("alternating prepared query"))?;
        if foreign_rows != 0 {
            let _rollback_result = transaction.rollback().await;
            return Ok(false);
        }
        transaction
            .commit()
            .await
            .map_err(safe_database_error("alternating tenant commit"))?;
    }
    Ok(true)
}

async fn concurrent_canaries(
    credentials: &Credentials,
    cell_id: &CellId,
    migrator: &PgPool,
) -> Result<bool> {
    let config = provider_config("postgres-qualification", Some("cell-001"), 32)?;
    let database = CellDatabase::connect(
        &credentials.cell_api,
        &config,
        cell_id,
        CellRuntimeRole::Api,
    )
    .await
    .map_err(|_| anyhow!("concurrent Cell adapter connection failed"))?;
    for index in 1..=32 {
        seed_authority(migrator, deterministic_tenant_id(index)?, "1", true).await?;
    }

    let mut tasks = Vec::new();
    for index in 1..=32 {
        let database = database.clone();
        let cell_id = cell_id.clone();
        tasks.push(tokio::spawn(async move {
            let tenant = deterministic_tenant_id(index)?;
            let scope = TenantExecutionScope::new(tenant, cell_id, AssignmentEpoch::initial());
            let canary = IsolationCanaryId::from_str(&deterministic_canary_id(10_000 + index))
                .map_err(|_| anyhow!("concurrent canary identity is invalid"))?;
            database
                .write_isolation_canary(&scope, canary, "concurrent")
                .await
                .map_err(|_| anyhow!("concurrent canary write failed"))?;
            let row = database
                .read_isolation_canary(&scope, canary)
                .await
                .map_err(|_| anyhow!("concurrent canary read failed"))?;
            Ok::<bool, anyhow::Error>(row.is_some_and(|row| row.payload() == "concurrent"))
        }));
    }
    let mut passed = true;
    for task in tasks {
        let result = task
            .await
            .map_err(|_| anyhow!("concurrent tenant task failed"))??;
        passed &= result;
    }
    database.close().await;
    Ok(passed)
}

async fn attempt_row_security_off(pool: &PgPool, tenant: TenantId) -> Result<bool> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(safe_database_error("row_security off begin"))?;
    set_scope(&mut transaction, tenant, "1").await?;
    let setting = sqlx::query("SET LOCAL row_security = off")
        .execute(&mut *transaction)
        .await;
    let read = if setting.is_ok() {
        sqlx::query_scalar::<_, i64>("SELECT pg_catalog.count(*) FROM tenant_data.isolation_canary")
            .fetch_one(&mut *transaction)
            .await
            .ok()
    } else {
        None
    };
    let _rollback_result = transaction.rollback().await;
    Ok(read.is_some_and(|count| count > 0))
}

async fn verify_catalog_rls(pool: &PgPool, checks: &mut CheckBook) -> Result<()> {
    let row = sqlx::query(
        "SELECT c.relrowsecurity, c.relforcerowsecurity, owner.rolname AS owner_name, \
                runtime.rolbypassrls, \
                pg_catalog.has_table_privilege('public', c.oid, \
                    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AS public_privilege \
         FROM pg_catalog.pg_class AS c \
         JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
         JOIN pg_catalog.pg_roles AS owner ON owner.oid = c.relowner \
         JOIN pg_catalog.pg_roles AS runtime ON runtime.rolname = CURRENT_USER \
         WHERE n.nspname = 'tenant_data' AND c.relname = 'isolation_canary'",
    )
    .fetch_one(pool)
    .await
    .map_err(safe_database_error("RLS catalog inspection"))?;
    checks.require(
        "rls.24_runtime_role_is_not_table_owner",
        row.get::<String, _>("owner_name") != "edtech_cell_api",
    )?;
    checks.require(
        "rls.25_runtime_role_has_no_bypassrls",
        !row.get::<bool, _>("rolbypassrls"),
    )?;
    checks.require(
        "rls.26_table_has_rls_enabled",
        row.get::<bool, _>("relrowsecurity"),
    )?;
    checks.require(
        "rls.27_table_has_rls_forced",
        row.get::<bool, _>("relforcerowsecurity"),
    )?;
    let policy = sqlx::query_as::<_, (bool, bool)>(
        "SELECT polqual IS NOT NULL, polwithcheck IS NOT NULL \
         FROM pg_catalog.pg_policy AS policy \
         JOIN pg_catalog.pg_class AS table_meta ON table_meta.oid = policy.polrelid \
         JOIN pg_catalog.pg_namespace AS schema_meta ON schema_meta.oid = table_meta.relnamespace \
         WHERE schema_meta.nspname = 'tenant_data' \
           AND table_meta.relname = 'isolation_canary' \
           AND policy.polname = 'isolation_canary_tenant_policy'",
    )
    .fetch_one(pool)
    .await
    .map_err(safe_database_error("RLS policy expression inspection"))?;
    checks.require(
        "rls.28_policy_has_using_and_with_check",
        policy.0 && policy.1,
    )?;
    checks.require(
        "rls.29_public_has_no_table_privilege",
        !row.get::<bool, _>("public_privilege"),
    )
}

async fn verify_local_setting_cleanup(
    pool: &PgPool,
    tenant: TenantId,
    checks: &mut CheckBook,
) -> Result<()> {
    let mut committed = pool
        .begin()
        .await
        .map_err(safe_database_error("local setting commit begin"))?;
    set_scope(&mut committed, tenant, "1").await?;
    committed
        .commit()
        .await
        .map_err(safe_database_error("local setting commit"))?;
    let after_commit = sqlx::query_scalar::<_, Option<String>>(
        "SELECT pg_catalog.current_setting('edtech.tenant_id', true)",
    )
    .fetch_one(pool)
    .await
    .map_err(safe_database_error("local setting post-commit inspection"))?;
    checks.require(
        "rls.30_transaction_local_setting_disappears_after_commit",
        after_commit.as_deref().is_none_or(str::is_empty),
    )?;

    let mut rolled_back = pool
        .begin()
        .await
        .map_err(safe_database_error("local setting rollback begin"))?;
    set_scope(&mut rolled_back, tenant, "1").await?;
    rolled_back
        .rollback()
        .await
        .map_err(safe_database_error("local setting rollback"))?;
    let after_rollback = sqlx::query_scalar::<_, Option<String>>(
        "SELECT pg_catalog.current_setting('edtech.tenant_id', true)",
    )
    .fetch_one(pool)
    .await
    .map_err(safe_database_error(
        "local setting post-rollback inspection",
    ))?;
    checks.require(
        "rls.31_transaction_local_setting_disappears_after_rollback",
        after_rollback.as_deref().is_none_or(str::is_empty),
    )
}

async fn verify_large_epochs(
    database: &CellDatabase,
    migrator: &PgPool,
    cell_id: &CellId,
    canary: IsolationCanaryId,
    checks: &mut CheckBook,
) -> Result<()> {
    let max_tenant = deterministic_tenant_id(900_005)?;
    seed_authority(migrator, max_tenant, "18446744073709551615", true).await?;
    let max_epoch = AssignmentEpoch::new(u64::MAX).context("maximum epoch must be valid")?;
    let max_scope = TenantExecutionScope::new(max_tenant, cell_id.clone(), max_epoch);
    let max_operation = database
        .write_isolation_canary(&max_scope, canary, "max-epoch")
        .await;
    let max_stored = sqlx::query_scalar::<_, String>(
        "SELECT assignment_epoch::text FROM cell_control.tenant_authority WHERE tenant_id = $1",
    )
    .bind(*max_tenant.as_uuid())
    .fetch_one(migrator)
    .await
    .map_err(safe_database_error("maximum epoch storage inspection"))?;
    checks.require(
        "rls.32_u64_max_epoch_stores_authorizes_and_compares",
        max_operation.is_ok() && max_stored == "18446744073709551615",
    )?;

    let above_signed_tenant = deterministic_tenant_id(900_006)?;
    seed_authority(migrator, above_signed_tenant, "9223372036854775808", true).await?;
    let above_signed_epoch = AssignmentEpoch::new(9_223_372_036_854_775_808)
        .context("above-signed epoch must be valid")?;
    let above_signed_scope =
        TenantExecutionScope::new(above_signed_tenant, cell_id.clone(), above_signed_epoch);
    checks.require(
        "rls.33_epoch_above_signed_max_is_lossless",
        database
            .read_isolation_canary(&above_signed_scope, canary)
            .await
            .is_ok(),
    )?;

    let zero_tenant = deterministic_tenant_id(900_007)?;
    let zero_insert = sqlx::query(
        "INSERT INTO cell_control.tenant_authority \
             (tenant_id, assignment_epoch, serving_enabled) VALUES ($1, 0, true)",
    )
    .bind(*zero_tenant.as_uuid())
    .execute(migrator)
    .await;
    checks.require("rls.34_zero_epoch_cannot_be_stored", zero_insert.is_err())
}

#[allow(clippy::too_many_lines)]
async fn benchmark(
    credentials: &Credentials,
    parameters: ProfileParameters,
    migrator: &PgPool,
    checks: &mut CheckBook,
) -> Result<CandidateMetrics> {
    let runtime = raw_pool(&credentials.cell_api, parameters.concurrency.max(1)).await?;
    let mut metrics = CandidateMetrics::default();
    let clean_started = Instant::now();
    sqlx::query("DROP SCHEMA IF EXISTS qualification_rls CASCADE")
        .execute(migrator)
        .await
        .map_err(safe_database_error("shared RLS candidate clean"))?;
    sqlx::query("CREATE SCHEMA qualification_rls AUTHORIZATION edtech_cell_migrator")
        .execute(migrator)
        .await
        .map_err(safe_database_error("shared RLS candidate creation"))?;
    sqlx::raw_sql(
        "REVOKE ALL ON SCHEMA qualification_rls FROM PUBLIC; \
         GRANT USAGE ON SCHEMA qualification_rls TO edtech_cell_api, edtech_cell_worker",
    )
    .execute(migrator)
    .await
    .map_err(safe_database_error(
        "shared RLS candidate schema privileges",
    ))?;
    metrics.clean_candidate_creation_ms = duration_milliseconds(clean_started.elapsed());

    let migration_started = Instant::now();
    let ddl = shared_table_ddl(parameters.logical_tables)?;
    sqlx::raw_sql(AssertSqlSafe(ddl))
        .execute(migrator)
        .await
        .map_err(safe_database_error(
            "shared RLS candidate initial migration",
        ))?;
    metrics.initial_schema_migration_ms = duration_milliseconds(migration_started.elapsed());

    let provisioning_started = Instant::now();
    for index in 1..=parameters.tenants {
        seed_authority(migrator, deterministic_tenant_id(index)?, "1", true).await?;
    }
    metrics.tenant_provisioning_ms = duration_milliseconds(provisioning_started.elapsed());

    let insert_started = Instant::now();
    for index in 1..=parameters.tenants {
        let tenant = deterministic_tenant_id(index)?;
        let mut transaction = runtime
            .begin()
            .await
            .map_err(safe_database_error("shared benchmark insert begin"))?;
        set_scope(&mut transaction, tenant, "1").await?;
        let rows = i32::try_from(parameters.rows_per_tenant)
            .map_err(|_| anyhow!("shared benchmark row count is too large"))?;
        sqlx::query(
            "INSERT INTO qualification_rls.bench_00 \
                 (tenant_id, row_id, payload, auxiliary_a, auxiliary_b) \
             SELECT $1, series, 'benchmark', series, series \
             FROM pg_catalog.generate_series(1, $2) AS series",
        )
        .bind(*tenant.as_uuid())
        .bind(rows)
        .execute(&mut *transaction)
        .await
        .map_err(safe_database_error("shared benchmark insert"))?;
        transaction
            .commit()
            .await
            .map_err(safe_database_error("shared benchmark insert commit"))?;
    }
    let inserted_rows = u64::from(parameters.tenants) * u64::from(parameters.rows_per_tenant);
    metrics.insert_rows_per_second = rate_per_second(inserted_rows, insert_started.elapsed());

    let read_started = Instant::now();
    for index in 1..=parameters.tenants {
        let tenant = deterministic_tenant_id(index)?;
        let mut transaction = runtime
            .begin()
            .await
            .map_err(safe_database_error("shared benchmark read begin"))?;
        set_scope(&mut transaction, tenant, "1").await?;
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT pg_catalog.count(*) FROM qualification_rls.bench_00",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(safe_database_error("shared benchmark read"))?;
        transaction
            .commit()
            .await
            .map_err(safe_database_error("shared benchmark read commit"))?;
        if u64::try_from(count).ok() != Some(u64::from(parameters.rows_per_tenant)) {
            return Err(anyhow!("shared benchmark read isolation failed"));
        }
    }
    metrics.read_rows_per_second = rate_per_second(inserted_rows, read_started.elapsed());

    let switch_capacity = usize::try_from(parameters.alternating_switches)
        .map_err(|_| anyhow!("shared benchmark switch count is too large"))?;
    let mut switch_samples = Vec::with_capacity(switch_capacity);
    for index in 0..parameters.alternating_switches {
        let tenant_index = (index % parameters.tenants) + 1;
        let tenant = deterministic_tenant_id(tenant_index)?;
        let started = Instant::now();
        let mut transaction = runtime
            .begin()
            .await
            .map_err(safe_database_error("shared benchmark switch begin"))?;
        set_scope(&mut transaction, tenant, "1").await?;
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT pg_catalog.count(*) FROM qualification_rls.bench_00",
        )
        .persistent(true)
        .fetch_one(&mut *transaction)
        .await
        .map_err(safe_database_error("shared benchmark prepared switch"))?;
        transaction
            .commit()
            .await
            .map_err(safe_database_error("shared benchmark switch commit"))?;
        if u64::try_from(count).ok() != Some(u64::from(parameters.rows_per_tenant)) {
            return Err(anyhow!("shared benchmark tenant switch leaked rows"));
        }
        switch_samples.push(started.elapsed());
    }
    metrics.tenant_switch_p50_microseconds = percentile_microseconds(&switch_samples, 50)?;
    metrics.tenant_switch_p95_microseconds = percentile_microseconds(&switch_samples, 95)?;
    metrics.tenant_switch_p99_microseconds = percentile_microseconds(&switch_samples, 99)?;
    metrics.prepared_query_alternation_passed = true;

    metrics.concurrent_isolation_passed = concurrent_profile_reads(&runtime, parameters).await?;
    checks.require(
        "rls.profile_concurrent_isolation",
        metrics.concurrent_isolation_passed,
    )?;

    let probe_tenant = deterministic_tenant_id(1)?;
    let export_started = Instant::now();
    let mut export_transaction = runtime
        .begin()
        .await
        .map_err(safe_database_error("shared probe export begin"))?;
    set_scope(&mut export_transaction, probe_tenant, "1").await?;
    let exported = sqlx::query_as::<_, (i32, String)>(
        "SELECT row_id, payload FROM qualification_rls.bench_00 ORDER BY row_id",
    )
    .fetch_all(&mut *export_transaction)
    .await
    .map_err(safe_database_error("shared probe export"))?;
    export_transaction
        .commit()
        .await
        .map_err(safe_database_error("shared probe export commit"))?;
    metrics.single_tenant_probe_export_microseconds =
        duration_microseconds(export_started.elapsed());

    let import_started = Instant::now();
    let mut import_transaction = runtime
        .begin()
        .await
        .map_err(safe_database_error("shared probe import begin"))?;
    set_scope(&mut import_transaction, probe_tenant, "1").await?;
    for (row_id, payload) in &exported {
        sqlx::query(
            "INSERT INTO qualification_rls.bench_00 \
                 (tenant_id, row_id, payload, auxiliary_a, auxiliary_b) \
             VALUES ($1, $2 + 100000, $3, $2, $2)",
        )
        .bind(*probe_tenant.as_uuid())
        .bind(*row_id)
        .bind(payload)
        .execute(&mut *import_transaction)
        .await
        .map_err(safe_database_error("shared probe import"))?;
    }
    import_transaction
        .commit()
        .await
        .map_err(safe_database_error("shared probe import commit"))?;
    metrics.single_tenant_probe_import_microseconds =
        duration_microseconds(import_started.elapsed());

    let incremental_started = Instant::now();
    let incremental = shared_incremental_ddl(parameters.logical_tables)?;
    sqlx::raw_sql(AssertSqlSafe(incremental))
        .execute(migrator)
        .await
        .map_err(safe_database_error("shared RLS incremental migration"))?;
    metrics.incremental_migration_ms = duration_milliseconds(incremental_started.elapsed());
    populate_catalog_metrics(migrator, &mut metrics).await?;

    let cleanup_started = Instant::now();
    sqlx::query("DROP SCHEMA qualification_rls CASCADE")
        .execute(migrator)
        .await
        .map_err(safe_database_error("shared RLS candidate cleanup"))?;
    metrics.cleanup_ms = duration_milliseconds(cleanup_started.elapsed());
    runtime.close().await;
    Ok(metrics)
}

fn shared_table_ddl(table_count: u32) -> Result<String> {
    let mut ddl = String::new();
    for index in 0..table_count {
        let name = format!("bench_{index:02}");
        let identifier = quote_identifier(&name)?;
        write!(
            &mut ddl,
            "CREATE TABLE qualification_rls.{identifier} (\
             tenant_id UUID NOT NULL, row_id INTEGER NOT NULL, payload TEXT NOT NULL, \
             auxiliary_a INTEGER NOT NULL, auxiliary_b INTEGER NOT NULL, \
             PRIMARY KEY (tenant_id, row_id)); \
             ALTER TABLE qualification_rls.{identifier} ENABLE ROW LEVEL SECURITY; \
             ALTER TABLE qualification_rls.{identifier} FORCE ROW LEVEL SECURITY; \
             CREATE POLICY {name}_tenant_policy ON qualification_rls.{identifier} \
             FOR ALL TO edtech_cell_api, edtech_cell_worker \
             USING (edtech_internal.tenant_is_authorized(tenant_id)) \
             WITH CHECK (edtech_internal.tenant_is_authorized(tenant_id)); \
             CREATE INDEX {name}_aux_a_idx ON qualification_rls.{identifier} \
             (tenant_id, auxiliary_a); \
             CREATE INDEX {name}_aux_b_idx ON qualification_rls.{identifier} \
             (tenant_id, auxiliary_b); \
             REVOKE ALL ON qualification_rls.{identifier} FROM PUBLIC; \
             GRANT SELECT, INSERT, UPDATE, DELETE ON qualification_rls.{identifier} \
             TO edtech_cell_api, edtech_cell_worker;"
        )
        .map_err(|_| anyhow!("shared RLS DDL rendering failed"))?;
    }
    Ok(ddl)
}

fn shared_incremental_ddl(table_count: u32) -> Result<String> {
    let mut ddl = String::new();
    for index in 0..table_count {
        let name = format!("bench_{index:02}");
        let identifier = quote_identifier(&name)?;
        write!(
            &mut ddl,
            "ALTER TABLE qualification_rls.{identifier} ADD COLUMN incremental_probe TEXT;"
        )
        .map_err(|_| anyhow!("shared incremental DDL rendering failed"))?;
    }
    ddl.push_str(
        "CREATE INDEX bench_00_incremental_idx ON qualification_rls.bench_00 \
         (tenant_id, row_id, incremental_probe);",
    );
    Ok(ddl)
}

async fn concurrent_profile_reads(pool: &PgPool, parameters: ProfileParameters) -> Result<bool> {
    let mut tasks = Vec::new();
    for worker in 0..parameters.concurrency {
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move {
            let tenant_index = (worker % parameters.tenants) + 1;
            let tenant = deterministic_tenant_id(tenant_index)?;
            let mut transaction = pool
                .begin()
                .await
                .map_err(safe_database_error("shared concurrent profile begin"))?;
            set_scope(&mut transaction, tenant, "1").await?;
            let count = sqlx::query_scalar::<_, i64>(
                "SELECT pg_catalog.count(*) FROM qualification_rls.bench_00 \
                 WHERE tenant_id <> $1",
            )
            .bind(*tenant.as_uuid())
            .fetch_one(&mut *transaction)
            .await
            .map_err(safe_database_error("shared concurrent profile read"))?;
            transaction
                .commit()
                .await
                .map_err(safe_database_error("shared concurrent profile commit"))?;
            Ok::<bool, anyhow::Error>(count == 0)
        }));
    }
    let mut passed = true;
    for task in tasks {
        passed &= task
            .await
            .map_err(|_| anyhow!("shared concurrent profile task failed"))??;
    }
    Ok(passed)
}

async fn populate_catalog_metrics(pool: &PgPool, metrics: &mut CandidateMetrics) -> Result<()> {
    let counts = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT \
             (SELECT pg_catalog.count(*) FROM pg_catalog.pg_namespace \
              WHERE nspname = 'qualification_rls'), \
             (SELECT pg_catalog.count(*) FROM pg_catalog.pg_class AS c \
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
              WHERE n.nspname = 'qualification_rls' AND c.relkind = 'r'), \
             (SELECT pg_catalog.count(*) FROM pg_catalog.pg_class AS c \
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
              WHERE n.nspname = 'qualification_rls' AND c.relkind = 'i'), \
             (SELECT pg_catalog.count(*) FROM pg_catalog.pg_class AS c \
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
              WHERE n.nspname = 'qualification_rls'), \
             (SELECT pg_catalog.count(*) FROM pg_catalog.pg_attribute AS a \
              JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid \
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
              WHERE n.nspname = 'qualification_rls' AND a.attnum > 0 AND NOT a.attisdropped)",
    )
    .fetch_one(pool)
    .await
    .map_err(safe_database_error("shared candidate catalog metrics"))?;
    metrics.total_schema_count = nonnegative(counts.0)?;
    metrics.total_table_count = nonnegative(counts.1)?;
    metrics.total_index_count = nonnegative(counts.2)?;
    metrics.relevant_pg_class_rows = nonnegative(counts.3)?;
    metrics.relevant_pg_attribute_rows = nonnegative(counts.4)?;
    let size = sqlx::query_scalar::<_, i64>(
        "SELECT pg_catalog.pg_database_size(pg_catalog.current_database())",
    )
    .fetch_one(pool)
    .await
    .map_err(safe_database_error("shared candidate database size"))?;
    metrics.database_size_bytes = nonnegative(size)?;
    Ok(())
}

fn nonnegative(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("catalog metric was negative"))
}
