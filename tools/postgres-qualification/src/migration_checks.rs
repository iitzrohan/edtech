//! Real authority, role-separation, and migration-behavior qualification.

use std::str::FromStr;

use anyhow::{Context, Result};
use cell_migrations::CellMigrationErrorKind;
use cell_postgres::{CellDatabaseErrorKind, CellRuntimeRole};
use platform_migrations::PlatformMigrationErrorKind;
use platform_postgres::PlatformRuntimeRole;
use postgres_runtime::ProviderErrorKind;
use sqlx::{Executor, PgPool};
use tenancy_domain::CellId;

use crate::{
    database::{Credentials, provider_config, raw_pool, safe_database_error},
    model::CheckBook,
};

#[allow(clippy::too_many_lines)]
pub(crate) async fn run(credentials: &Credentials, checks: &mut CheckBook) -> Result<u32> {
    let platform_config = provider_config("postgres-qualification", None, 4)?;
    let cell_config = provider_config("postgres-qualification", Some("cell-001"), 4)?;
    let cell_id = CellId::from_str("cell-001").context("fixed Cell identity must be valid")?;
    let wrong_cell_id =
        CellId::from_str("cell-002").context("fixed Cell identity must be valid")?;

    let platform_repeat =
        platform_migrations::migrate(&credentials.platform_migrator, &platform_config).await;
    checks.require(
        "migration.platform.clean_and_repeat_idempotent",
        platform_repeat
            .as_ref()
            .is_ok_and(|report| report.applied_count() == 1 && report.latest_version() == 1),
    )?;
    let cell_repeat =
        cell_migrations::migrate(&credentials.cell_migrator, &cell_config, &cell_id).await;
    checks.require(
        "migration.cell.clean_and_repeat_idempotent",
        cell_repeat
            .as_ref()
            .is_ok_and(|report| report.applied_count() == 1 && report.latest_version() == 1),
    )?;

    let (platform_first, platform_second) = tokio::join!(
        platform_migrations::migrate(&credentials.platform_migrator, &platform_config),
        platform_migrations::migrate(&credentials.platform_migrator, &platform_config),
    );
    checks.require(
        "migration.platform.concurrent_migrators_serialize",
        platform_first.is_ok() && platform_second.is_ok(),
    )?;
    let (cell_first, cell_second) = tokio::join!(
        cell_migrations::migrate(&credentials.cell_migrator, &cell_config, &cell_id),
        cell_migrations::migrate(&credentials.cell_migrator, &cell_config, &cell_id),
    );
    checks.require(
        "migration.cell.concurrent_migrators_serialize",
        cell_first.is_ok() && cell_second.is_ok(),
    )?;

    let wrong_platform =
        platform_migrations::migrate(&credentials.cell_migrator, &platform_config).await;
    checks.require(
        "migration.platform_rejects_cell_authority_before_ddl",
        wrong_platform
            .err()
            .is_some_and(|error| error.kind() == PlatformMigrationErrorKind::AuthorityMismatch),
    )?;
    let wrong_cell =
        cell_migrations::migrate(&credentials.platform_migrator, &cell_config, &cell_id).await;
    checks.require(
        "migration.cell_rejects_platform_authority_before_ddl",
        wrong_cell
            .err()
            .is_some_and(|error| error.kind() == CellMigrationErrorKind::AuthorityMismatch),
    )?;
    let wrong_identity =
        cell_migrations::migrate(&credentials.cell_migrator, &cell_config, &wrong_cell_id).await;
    checks.require(
        "migration.cell_rejects_wrong_cell_identity",
        wrong_identity
            .err()
            .is_some_and(|error| error.kind() == CellMigrationErrorKind::AuthorityMismatch),
    )?;

    let platform_runtime_migration =
        platform_migrations::migrate(&credentials.platform_api, &platform_config).await;
    checks.require(
        "migration.platform_runtime_credential_cannot_migrate",
        platform_runtime_migration
            .err()
            .is_some_and(|error| error.kind() == PlatformMigrationErrorKind::PrivilegeMismatch),
    )?;
    let cell_runtime_migration =
        cell_migrations::migrate(&credentials.cell_api, &cell_config, &cell_id).await;
    checks.require(
        "migration.cell_runtime_credential_cannot_migrate",
        cell_runtime_migration
            .err()
            .is_some_and(|error| error.kind() == CellMigrationErrorKind::PrivilegeMismatch),
    )?;

    let platform_migrator_runtime = platform_postgres::check_database(
        &credentials.platform_migrator,
        &platform_config,
        PlatformRuntimeRole::Api,
    )
    .await;
    checks.require(
        "authority.platform_migrator_rejected_by_runtime_adapter",
        platform_migrator_runtime
            .err()
            .is_some_and(|error| error.kind() == ProviderErrorKind::PrivilegeMismatch),
    )?;
    let cell_migrator_runtime = cell_postgres::check_database(
        &credentials.cell_migrator,
        &cell_config,
        &cell_id,
        CellRuntimeRole::Api,
    )
    .await;
    checks.require(
        "authority.cell_migrator_rejected_by_runtime_adapter",
        cell_migrator_runtime
            .err()
            .is_some_and(|error| error.kind() == CellDatabaseErrorKind::PrivilegeMismatch),
    )?;

    let platform_api_check = platform_postgres::check_database(
        &credentials.platform_api,
        &platform_config,
        PlatformRuntimeRole::Api,
    )
    .await;
    let platform_worker_check = platform_postgres::check_database(
        &credentials.platform_worker,
        &platform_config,
        PlatformRuntimeRole::Worker,
    )
    .await;
    checks.require(
        "authority.platform_runtime_roles_ready",
        platform_api_check.is_ok() && platform_worker_check.is_ok(),
    )?;
    let cell_api_check = cell_postgres::check_database(
        &credentials.cell_api,
        &cell_config,
        &cell_id,
        CellRuntimeRole::Api,
    )
    .await;
    let cell_worker_check = cell_postgres::check_database(
        &credentials.cell_worker,
        &cell_config,
        &cell_id,
        CellRuntimeRole::Worker,
    )
    .await;
    checks.require(
        "authority.cell_runtime_roles_ready",
        cell_api_check.is_ok() && cell_worker_check.is_ok(),
    )?;

    let platform_bootstrap = raw_pool(&credentials.platform_bootstrap, 2).await?;
    let platform_migrator = raw_pool(&credentials.platform_migrator, 2).await?;
    let platform_api = raw_pool(&credentials.platform_api, 2).await?;
    let cell_bootstrap = raw_pool(&credentials.cell_bootstrap, 2).await?;
    let cell_migrator = raw_pool(&credentials.cell_migrator, 2).await?;
    let cell_api = raw_pool(&credentials.cell_api, 2).await?;

    verify_wrong_authority_left_no_objects(&platform_bootstrap, &cell_bootstrap, checks).await?;
    verify_marker_immutability(
        &platform_migrator,
        &platform_api,
        &cell_migrator,
        &cell_api,
        checks,
    )
    .await?;
    verify_runtime_migration_privileges(&platform_api, &cell_api, checks).await?;
    verify_histories_and_public_schema(&platform_bootstrap, &cell_bootstrap, checks).await?;
    verify_role_separation(&platform_bootstrap, &cell_bootstrap, checks).await?;
    verify_transactional_failure(&cell_migrator, checks).await?;

    platform_bootstrap.close().await;
    platform_migrator.close().await;
    platform_api.close().await;
    cell_bootstrap.close().await;
    cell_migrator.close().await;
    cell_api.close().await;

    platform_api_check
        .map(platform_postgres::PlatformDatabaseCheck::server_version)
        .map_err(|_| anyhow::anyhow!("Platform server version qualification failed"))
}

async fn verify_wrong_authority_left_no_objects(
    platform_bootstrap: &PgPool,
    cell_bootstrap: &PgPool,
    checks: &mut CheckBook,
) -> Result<()> {
    let cell_object_on_platform = sqlx::query_scalar::<_, Option<String>>(
        "SELECT pg_catalog.to_regclass('cell_control.schema_contract')::text",
    )
    .fetch_one(platform_bootstrap)
    .await
    .map_err(safe_database_error(
        "wrong-authority Platform object inspection",
    ))?;
    let platform_object_on_cell = sqlx::query_scalar::<_, Option<String>>(
        "SELECT pg_catalog.to_regclass('platform_control.schema_contract')::text",
    )
    .fetch_one(cell_bootstrap)
    .await
    .map_err(safe_database_error(
        "wrong-authority Cell object inspection",
    ))?;
    checks.require(
        "migration.wrong_authority_attempts_leave_no_application_objects",
        cell_object_on_platform.is_none() && platform_object_on_cell.is_none(),
    )
}

async fn verify_marker_immutability(
    platform_migrator: &PgPool,
    platform_api: &PgPool,
    cell_migrator: &PgPool,
    cell_api: &PgPool,
    checks: &mut CheckBook,
) -> Result<()> {
    let statement =
        "UPDATE edtech_bootstrap.authority_identity SET initialized_at = pg_catalog.now()";
    let results = [
        sqlx::query(statement).execute(platform_migrator).await,
        sqlx::query(statement).execute(platform_api).await,
        sqlx::query(statement).execute(cell_migrator).await,
        sqlx::query(statement).execute(cell_api).await,
    ];
    checks.require(
        "authority.marker_is_immutable_to_migration_and_runtime_roles",
        results.iter().all(Result::is_err),
    )
}

async fn verify_runtime_migration_privileges(
    platform_api: &PgPool,
    cell_api: &PgPool,
    checks: &mut CheckBook,
) -> Result<()> {
    let platform_history = sqlx::query("SELECT version FROM edtech_migrations._sqlx_migrations")
        .fetch_optional(platform_api)
        .await;
    let cell_history = sqlx::query("SELECT version FROM edtech_migrations._sqlx_migrations")
        .fetch_optional(cell_api)
        .await;
    checks.require(
        "migration.runtime_roles_cannot_read_history",
        platform_history.is_err() && cell_history.is_err(),
    )?;

    let platform_create = sqlx::query("CREATE TABLE edtech_migrations.runtime_forbidden(id INT)")
        .execute(platform_api)
        .await;
    let cell_create = sqlx::query("CREATE TABLE edtech_migrations.runtime_forbidden(id INT)")
        .execute(cell_api)
        .await;
    checks.require(
        "migration.runtime_roles_cannot_create_in_history_schema",
        platform_create.is_err() && cell_create.is_err(),
    )?;

    let platform_contract =
        sqlx::query("UPDATE platform_control.schema_contract SET updated_at = pg_catalog.now()")
            .execute(platform_api)
            .await;
    let cell_contract =
        sqlx::query("UPDATE cell_control.schema_contract SET updated_at = pg_catalog.now()")
            .execute(cell_api)
            .await;
    checks.require(
        "migration.runtime_roles_cannot_modify_schema_contract",
        platform_contract.is_err() && cell_contract.is_err(),
    )
}

async fn verify_histories_and_public_schema(
    platform_bootstrap: &PgPool,
    cell_bootstrap: &PgPool,
    checks: &mut CheckBook,
) -> Result<()> {
    for (label, pool, expected_contract) in [
        (
            "platform",
            platform_bootstrap,
            "platform_control.schema_contract",
        ),
        ("cell", cell_bootstrap, "cell_control.schema_contract"),
    ] {
        let history_schemas = sqlx::query_scalar::<_, String>(
            "SELECT n.nspname FROM pg_catalog.pg_class AS c \
             JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
             WHERE c.relname = '_sqlx_migrations'",
        )
        .fetch_all(pool)
        .await
        .map_err(safe_database_error("migration history location inspection"))?;
        checks.require(
            &format!("migration.{label}_history_only_in_owned_schema"),
            history_schemas == [String::from("edtech_migrations")],
        )?;

        let contract =
            sqlx::query_scalar::<_, Option<String>>("SELECT pg_catalog.to_regclass($1)::text")
                .bind(expected_contract)
                .fetch_one(pool)
                .await
                .map_err(safe_database_error("schema contract visibility inspection"))?;
        checks.require(
            &format!("migration.{label}_schema_contract_visible"),
            contract.as_deref() == Some(expected_contract),
        )?;

        let public_tables = sqlx::query_scalar::<_, i64>(
            "SELECT pg_catalog.count(*) FROM pg_catalog.pg_class AS c \
             JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm')",
        )
        .fetch_one(pool)
        .await
        .map_err(safe_database_error("public schema inspection"))?;
        checks.require(
            &format!("migration.{label}_has_no_public_application_objects"),
            public_tables == 0,
        )?;
    }
    Ok(())
}

async fn verify_role_separation(
    platform_bootstrap: &PgPool,
    cell_bootstrap: &PgPool,
    checks: &mut CheckBook,
) -> Result<()> {
    let platform_has_cell_roles = sqlx::query_scalar::<_, i64>(
        "SELECT pg_catalog.count(*) FROM pg_catalog.pg_roles WHERE rolname LIKE 'edtech_cell_%'",
    )
    .fetch_one(platform_bootstrap)
    .await
    .map_err(safe_database_error("Platform role-separation inspection"))?;
    let cell_has_platform_roles = sqlx::query_scalar::<_, i64>(
        "SELECT pg_catalog.count(*) FROM pg_catalog.pg_roles \
         WHERE rolname LIKE 'edtech_platform_%'",
    )
    .fetch_one(cell_bootstrap)
    .await
    .map_err(safe_database_error("Cell role-separation inspection"))?;
    checks.require(
        "authority.role_sets_are_physically_separate",
        platform_has_cell_roles == 0 && cell_has_platform_roles == 0,
    )
}

async fn verify_transactional_failure(pool: &PgPool, checks: &mut CheckBook) -> Result<()> {
    sqlx::query("DROP SCHEMA IF EXISTS qualification_transactional CASCADE")
        .execute(pool)
        .await
        .map_err(safe_database_error("transactional fixture cleanup"))?;
    let mut transaction = pool
        .begin()
        .await
        .map_err(safe_database_error("transactional migration begin"))?;
    transaction
        .execute("CREATE SCHEMA qualification_transactional")
        .await
        .map_err(safe_database_error(
            "transactional migration schema creation",
        ))?;
    transaction
        .execute("CREATE TABLE qualification_transactional.partial_object(id INTEGER)")
        .await
        .map_err(safe_database_error(
            "transactional migration table creation",
        ))?;
    let deliberate_failure = transaction.execute("SELECT 1 / 0").await;
    let rollback = transaction.rollback().await;
    let remaining = sqlx::query_scalar::<_, Option<String>>(
        "SELECT pg_catalog.to_regclass('qualification_transactional.partial_object')::text",
    )
    .fetch_one(pool)
    .await
    .map_err(safe_database_error(
        "transactional migration rollback inspection",
    ))?;
    checks.require(
        "migration.failed_transaction_leaves_no_partial_object",
        deliberate_failure.is_err() && rollback.is_ok() && remaining.is_none(),
    )
}
