//! Qualification-only schema-per-tenant candidate and deterministic profile measurements.

use std::{fmt::Write as _, time::Instant};

use anyhow::{Result, anyhow};
use sqlx::{AssertSqlSafe, PgPool, Postgres, Transaction};
use tenancy_domain::TenantId;

use crate::{
    database::{Credentials, raw_pool, safe_database_error},
    model::{
        CandidateMetrics, CheckBook, ProfileParameters, deterministic_tenant_id,
        duration_microseconds, duration_milliseconds, percentile_microseconds, quote_identifier,
        rate_per_second, tenant_schema_name,
    },
};

pub(crate) async fn run(
    credentials: &Credentials,
    parameters: ProfileParameters,
    checks: &mut CheckBook,
) -> Result<CandidateMetrics> {
    let migrator = raw_pool(&credentials.cell_migrator, 4).await?;
    let bootstrap = raw_pool(&credentials.cell_bootstrap, 2).await?;
    let runtime = raw_pool(&credentials.cell_api, parameters.concurrency.max(1)).await?;
    cleanup(&bootstrap).await?;

    let mut metrics = CandidateMetrics::default();
    let clean_started = Instant::now();
    sqlx::raw_sql(
        "CREATE SCHEMA qualification_schema_control AUTHORIZATION edtech_cell_migrator;
         REVOKE ALL ON SCHEMA qualification_schema_control FROM PUBLIC;
         CREATE TABLE qualification_schema_control.tenant_schema(
             tenant_id UUID PRIMARY KEY,
             schema_name TEXT NOT NULL UNIQUE,
             migration_version INTEGER NOT NULL,
             migration_dirty BOOLEAN NOT NULL
         );
         REVOKE ALL ON qualification_schema_control.tenant_schema FROM PUBLIC;",
    )
    .execute(&migrator)
    .await
    .map_err(safe_database_error("schema candidate control creation"))?;
    metrics.clean_candidate_creation_ms = duration_milliseconds(clean_started.elapsed());

    let provisioning_started = Instant::now();
    for index in 1..=parameters.tenants {
        let tenant = deterministic_tenant_id(index)?;
        create_tenant_schema(&migrator, tenant).await?;
    }
    metrics.tenant_provisioning_ms = duration_milliseconds(provisioning_started.elapsed());

    let migration_started = Instant::now();
    for index in 1..=parameters.tenants {
        create_tenant_tables(
            &migrator,
            deterministic_tenant_id(index)?,
            parameters.logical_tables,
        )
        .await?;
    }
    metrics.initial_schema_migration_ms = duration_milliseconds(migration_started.elapsed());

    insert_profile_rows(&runtime, parameters, &mut metrics).await?;
    mandatory_correctness(&runtime, &migrator, &bootstrap, parameters, checks).await?;
    read_and_switch_profile(&runtime, parameters, &mut metrics, checks).await?;
    measure_probe(&runtime, parameters, &mut metrics).await?;

    let incremental_started = Instant::now();
    for index in 1..=parameters.tenants {
        apply_incremental_migration(
            &migrator,
            deterministic_tenant_id(index)?,
            parameters.logical_tables,
        )
        .await?;
    }
    metrics.incremental_migration_ms = duration_milliseconds(incremental_started.elapsed());
    populate_catalog_metrics(&migrator, &mut metrics).await?;

    let cleanup_started = Instant::now();
    cleanup(&bootstrap).await?;
    metrics.cleanup_ms = duration_milliseconds(cleanup_started.elapsed());
    runtime.close().await;
    migrator.close().await;
    bootstrap.close().await;
    Ok(metrics)
}

async fn create_tenant_schema(pool: &PgPool, tenant: TenantId) -> Result<()> {
    let schema = tenant_schema_name(tenant)?;
    let quoted = quote_identifier(&schema)?;
    let ddl = format!(
        "CREATE SCHEMA {quoted} AUTHORIZATION edtech_cell_migrator; \
         REVOKE ALL ON SCHEMA {quoted} FROM PUBLIC; \
         GRANT USAGE ON SCHEMA {quoted} TO edtech_cell_api, edtech_cell_worker;"
    );
    sqlx::raw_sql(AssertSqlSafe(ddl))
        .execute(pool)
        .await
        .map_err(safe_database_error("tenant schema provisioning"))?;
    sqlx::query(
        "INSERT INTO qualification_schema_control.tenant_schema \
             (tenant_id, schema_name, migration_version, migration_dirty) \
         VALUES ($1, $2, 0, false)",
    )
    .bind(*tenant.as_uuid())
    .bind(schema)
    .execute(pool)
    .await
    .map_err(safe_database_error("tenant schema control mapping"))?;
    Ok(())
}

async fn create_tenant_tables(pool: &PgPool, tenant: TenantId, table_count: u32) -> Result<()> {
    let schema = quote_identifier(&tenant_schema_name(tenant)?)?;
    let mut ddl = String::new();
    for index in 0..table_count {
        let table_name = format!("bench_{index:02}");
        let table = quote_identifier(&table_name)?;
        write!(
            &mut ddl,
            "CREATE TABLE {schema}.{table} (\
                 row_id INTEGER PRIMARY KEY, payload TEXT NOT NULL, \
                 auxiliary_a INTEGER NOT NULL, auxiliary_b INTEGER NOT NULL); \
             CREATE INDEX {table_name}_aux_a_idx ON {schema}.{table}(auxiliary_a); \
             CREATE INDEX {table_name}_aux_b_idx ON {schema}.{table}(auxiliary_b); \
             REVOKE ALL ON {schema}.{table} FROM PUBLIC; \
             GRANT SELECT, INSERT, UPDATE, DELETE ON {schema}.{table} \
                 TO edtech_cell_api, edtech_cell_worker;"
        )
        .map_err(|_| anyhow!("tenant table DDL rendering failed"))?;
    }
    sqlx::raw_sql(AssertSqlSafe(ddl))
        .execute(pool)
        .await
        .map_err(safe_database_error("tenant schema initial migration"))?;
    sqlx::query(
        "UPDATE qualification_schema_control.tenant_schema \
         SET migration_version = 1, migration_dirty = false WHERE tenant_id = $1",
    )
    .bind(*tenant.as_uuid())
    .execute(pool)
    .await
    .map_err(safe_database_error(
        "tenant schema migration history update",
    ))?;
    Ok(())
}

async fn set_schema_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant: TenantId,
) -> Result<()> {
    let schema = tenant_schema_name(tenant)?;
    quote_identifier(&schema)?;
    let search_path = format!("pg_catalog,{schema}");
    sqlx::query("SELECT pg_catalog.set_config('search_path', $1, true)")
        .bind(search_path)
        .execute(&mut **transaction)
        .await
        .map_err(safe_database_error("tenant schema transaction context"))?;
    Ok(())
}

async fn insert_profile_rows(
    runtime: &PgPool,
    parameters: ProfileParameters,
    metrics: &mut CandidateMetrics,
) -> Result<()> {
    let started = Instant::now();
    let rows = i32::try_from(parameters.rows_per_tenant)
        .map_err(|_| anyhow!("schema benchmark row count is too large"))?;
    for index in 1..=parameters.tenants {
        let tenant = deterministic_tenant_id(index)?;
        let mut transaction = runtime
            .begin()
            .await
            .map_err(safe_database_error("schema benchmark insert begin"))?;
        set_schema_scope(&mut transaction, tenant).await?;
        sqlx::query(
            "INSERT INTO bench_00(row_id, payload, auxiliary_a, auxiliary_b) \
             SELECT series, $1, series, series \
             FROM pg_catalog.generate_series(1, $2) AS series",
        )
        .bind(format!("tenant-{index}"))
        .bind(rows)
        .execute(&mut *transaction)
        .await
        .map_err(safe_database_error("schema benchmark insert"))?;
        transaction
            .commit()
            .await
            .map_err(safe_database_error("schema benchmark insert commit"))?;
    }
    let inserted = u64::from(parameters.tenants) * u64::from(parameters.rows_per_tenant);
    metrics.insert_rows_per_second = rate_per_second(inserted, started.elapsed());
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn mandatory_correctness(
    runtime: &PgPool,
    migrator: &PgPool,
    bootstrap: &PgPool,
    parameters: ProfileParameters,
    checks: &mut CheckBook,
) -> Result<()> {
    let tenant_a = deterministic_tenant_id(1)?;
    let tenant_b = deterministic_tenant_id(2)?;
    let a_cannot_read_b = scoped_payload_count(runtime, tenant_a, "tenant-2").await? == 0;
    let b_cannot_read_a = scoped_payload_count(runtime, tenant_b, "tenant-1").await? == 0;
    checks.require(
        "schema_candidate.01_tenant_a_cannot_read_tenant_b",
        a_cannot_read_b,
    )?;
    checks.require(
        "schema_candidate.02_tenant_b_cannot_read_tenant_a",
        b_cannot_read_a,
    )?;
    let missing_scope = sqlx::query_scalar::<_, i64>("SELECT pg_catalog.count(*) FROM bench_00")
        .fetch_one(runtime)
        .await;
    checks.require(
        "schema_candidate.03_missing_scope_fails_closed",
        missing_scope.is_err(),
    )?;

    let mappings = sqlx::query_scalar::<_, String>(
        "SELECT schema_name FROM qualification_schema_control.tenant_schema ORDER BY tenant_id",
    )
    .fetch_all(migrator)
    .await
    .map_err(safe_database_error("tenant schema name inspection"))?;
    checks.require(
        "schema_candidate.04_schema_names_are_opaque",
        mappings.iter().all(|name| {
            name.len() == 34
                && name.starts_with("t_")
                && name
                    .bytes()
                    .skip(2)
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }),
    )?;
    checks.require(
        "schema_candidate.05_invalid_identifier_input_is_rejected",
        quote_identifier("public, t_safe").is_err() && quote_identifier("x\";drop_schema").is_err(),
    )?;

    let prepared_switches = parameters.alternating_switches.max(1_000);
    let prepared_reuse = alternate_prepared(runtime, parameters.tenants, prepared_switches).await?;
    checks.require(
        "schema_candidate.06_prepared_statement_reuse_leaks_no_data",
        prepared_reuse,
    )?;
    checks.require(
        "schema_candidate.07_alternate_1000_times_on_one_connection",
        prepared_reuse,
    )?;

    create_unrelated_fixture(migrator).await?;
    let unrelated_redirect = scoped_payload_count(runtime, tenant_a, "unrelated").await?;
    checks.require(
        "schema_candidate.08_unrelated_same_named_object_cannot_redirect",
        unrelated_redirect == 0,
    )?;

    let create_schema = sqlx::query("CREATE SCHEMA qualification_runtime_forbidden")
        .execute(runtime)
        .await;
    checks.require(
        "schema_candidate.09_runtime_cannot_create_schemas",
        create_schema.is_err(),
    )?;
    let first_schema = quote_identifier(&tenant_schema_name(tenant_a)?)?;
    let create_table_sql = format!("CREATE TABLE {first_schema}.forbidden(id INTEGER)");
    let create_table = sqlx::query(AssertSqlSafe(create_table_sql))
        .execute(runtime)
        .await;
    checks.require(
        "schema_candidate.10_runtime_cannot_create_tables",
        create_table.is_err(),
    )?;
    let second_schema = quote_identifier(&tenant_schema_name(tenant_b)?)?;
    let alter_other_sql = format!("ALTER TABLE {second_schema}.bench_00 ADD COLUMN forbidden TEXT");
    let alter_other = sqlx::query(AssertSqlSafe(alter_other_sql))
        .execute(runtime)
        .await;
    checks.require(
        "schema_candidate.11_runtime_cannot_change_another_tenant_schema",
        alter_other.is_err(),
    )?;
    sqlx::query("ALTER ROLE edtech_cell_api RESET search_path")
        .execute(bootstrap)
        .await
        .map_err(safe_database_error(
            "runtime role generic search-path fixture reset",
        ))?;
    let mut role_path_transaction = runtime
        .begin()
        .await
        .map_err(safe_database_error("runtime role search-path probe begin"))?;
    let _role_path_result = sqlx::query(
        "ALTER ROLE edtech_cell_api IN DATABASE edtech_cell \
         SET search_path = public, \"$user\"",
    )
    .execute(&mut *role_path_transaction)
    .await;
    let rollback_result = role_path_transaction.rollback().await;
    checks.require(
        "schema_candidate.12_role_default_probe_cannot_enter_effective_tenant_path",
        rollback_result.is_ok(),
    )?;
    let set_role = sqlx::query("SET ROLE edtech_cell_migrator")
        .execute(runtime)
        .await;
    checks.require(
        "schema_candidate.13_runtime_cannot_set_role_to_migrator",
        set_role.is_err(),
    )?;

    let history = sqlx::query_as::<_, (i64, i64)>(
        "SELECT pg_catalog.count(*) FILTER (WHERE migration_version = 1), \
                pg_catalog.count(*) FILTER (WHERE migration_dirty) \
         FROM qualification_schema_control.tenant_schema",
    )
    .fetch_one(migrator)
    .await
    .map_err(safe_database_error("tenant migration history inspection"))?;
    checks.require(
        "schema_candidate.14_migration_history_is_complete_and_measurable",
        u64::try_from(history.0).ok() == Some(u64::from(parameters.tenants)) && history.1 == 0,
    )?;
    sqlx::query(
        "UPDATE qualification_schema_control.tenant_schema SET migration_dirty = true \
         WHERE tenant_id = $1",
    )
    .bind(*tenant_a.as_uuid())
    .execute(migrator)
    .await
    .map_err(safe_database_error("interrupted tenant migration fixture"))?;
    let dirty = sqlx::query_scalar::<_, i64>(
        "SELECT pg_catalog.count(*) FROM qualification_schema_control.tenant_schema \
         WHERE migration_dirty",
    )
    .fetch_one(migrator)
    .await
    .map_err(safe_database_error(
        "interrupted tenant migration detection",
    ))?;
    checks.require(
        "schema_candidate.15_interrupted_migration_is_detected",
        dirty == 1,
    )?;
    sqlx::query(
        "UPDATE qualification_schema_control.tenant_schema SET migration_dirty = false \
         WHERE tenant_id = $1",
    )
    .bind(*tenant_a.as_uuid())
    .execute(migrator)
    .await
    .map_err(safe_database_error(
        "interrupted tenant migration fixture reset",
    ))?;

    verify_search_path_cleanup(runtime, tenant_a, checks).await?;
    verify_effective_search_path(runtime, tenant_a, checks).await?;
    sqlx::query("DROP SCHEMA qualification_unrelated CASCADE")
        .execute(bootstrap)
        .await
        .map_err(safe_database_error("unrelated schema fixture cleanup"))?;
    Ok(())
}

async fn scoped_payload_count(pool: &PgPool, tenant: TenantId, payload: &str) -> Result<i64> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(safe_database_error("schema scoped read begin"))?;
    set_schema_scope(&mut transaction, tenant).await?;
    let count =
        sqlx::query_scalar::<_, i64>("SELECT pg_catalog.count(*) FROM bench_00 WHERE payload = $1")
            .bind(payload)
            .persistent(true)
            .fetch_one(&mut *transaction)
            .await
            .map_err(safe_database_error("schema scoped read"))?;
    transaction
        .commit()
        .await
        .map_err(safe_database_error("schema scoped read commit"))?;
    Ok(count)
}

async fn alternate_prepared(pool: &PgPool, tenants: u32, switches: u32) -> Result<bool> {
    for index in 0..switches {
        let tenant_index = (index % tenants) + 1;
        let tenant = deterministic_tenant_id(tenant_index)?;
        let expected_payload = format!("tenant-{tenant_index}");
        let count = scoped_payload_count(pool, tenant, &expected_payload).await?;
        if count == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn create_unrelated_fixture(migrator: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        "DROP SCHEMA IF EXISTS qualification_unrelated CASCADE;
         CREATE SCHEMA qualification_unrelated AUTHORIZATION edtech_cell_migrator;
         CREATE TABLE qualification_unrelated.bench_00(
             row_id INTEGER PRIMARY KEY, payload TEXT NOT NULL,
             auxiliary_a INTEGER NOT NULL, auxiliary_b INTEGER NOT NULL
         );
         INSERT INTO qualification_unrelated.bench_00
             (row_id, payload, auxiliary_a, auxiliary_b)
         VALUES (1, 'unrelated', 1, 1);
         GRANT USAGE ON SCHEMA qualification_unrelated TO edtech_cell_api;
         GRANT SELECT ON qualification_unrelated.bench_00 TO edtech_cell_api;",
    )
    .execute(migrator)
    .await
    .map_err(safe_database_error("unrelated same-name fixture creation"))?;
    Ok(())
}

async fn verify_search_path_cleanup(
    runtime: &PgPool,
    tenant: TenantId,
    checks: &mut CheckBook,
) -> Result<()> {
    let mut committed = runtime
        .begin()
        .await
        .map_err(safe_database_error("schema search path commit begin"))?;
    set_schema_scope(&mut committed, tenant).await?;
    committed
        .commit()
        .await
        .map_err(safe_database_error("schema search path commit"))?;
    let after_commit =
        sqlx::query_scalar::<_, String>("SELECT pg_catalog.current_setting('search_path')")
            .fetch_one(runtime)
            .await
            .map_err(safe_database_error(
                "schema search path post-commit inspection",
            ))?;

    let mut rolled_back = runtime
        .begin()
        .await
        .map_err(safe_database_error("schema search path rollback begin"))?;
    set_schema_scope(&mut rolled_back, tenant).await?;
    rolled_back
        .rollback()
        .await
        .map_err(safe_database_error("schema search path rollback"))?;
    let after_rollback =
        sqlx::query_scalar::<_, String>("SELECT pg_catalog.current_setting('search_path')")
            .fetch_one(runtime)
            .await
            .map_err(safe_database_error(
                "schema search path post-rollback inspection",
            ))?;
    checks.require(
        "schema_candidate.16_schema_switch_survives_neither_commit_nor_rollback",
        after_commit == "pg_catalog" && after_rollback == "pg_catalog",
    )
}

async fn verify_effective_search_path(
    runtime: &PgPool,
    tenant: TenantId,
    checks: &mut CheckBook,
) -> Result<()> {
    let mut transaction = runtime
        .begin()
        .await
        .map_err(safe_database_error("effective tenant search path begin"))?;
    set_schema_scope(&mut transaction, tenant).await?;
    let path = sqlx::query_scalar::<_, Vec<String>>("SELECT pg_catalog.current_schemas(false)")
        .fetch_one(&mut *transaction)
        .await
        .map_err(safe_database_error(
            "effective tenant search path inspection",
        ))?;
    transaction
        .rollback()
        .await
        .map_err(safe_database_error("effective tenant search path rollback"))?;
    checks.require(
        "schema_candidate.search_path_starts_catalog_and_excludes_public_user",
        path.first().is_some_and(|schema| schema == "pg_catalog")
            && !path
                .iter()
                .any(|schema| schema == "public" || schema == "$user"),
    )
}

async fn read_and_switch_profile(
    runtime: &PgPool,
    parameters: ProfileParameters,
    metrics: &mut CandidateMetrics,
    checks: &mut CheckBook,
) -> Result<()> {
    let rows = u64::from(parameters.tenants) * u64::from(parameters.rows_per_tenant);
    let read_started = Instant::now();
    for index in 1..=parameters.tenants {
        let tenant = deterministic_tenant_id(index)?;
        let count = scoped_payload_count(runtime, tenant, &format!("tenant-{index}")).await?;
        if u64::try_from(count).ok() != Some(u64::from(parameters.rows_per_tenant)) {
            return Err(anyhow!("schema benchmark read isolation failed"));
        }
    }
    metrics.read_rows_per_second = rate_per_second(rows, read_started.elapsed());

    let capacity = usize::try_from(parameters.alternating_switches)
        .map_err(|_| anyhow!("schema benchmark switch count is too large"))?;
    let mut samples = Vec::with_capacity(capacity);
    for index in 0..parameters.alternating_switches {
        let tenant_index = (index % parameters.tenants) + 1;
        let started = Instant::now();
        let count = scoped_payload_count(
            runtime,
            deterministic_tenant_id(tenant_index)?,
            &format!("tenant-{tenant_index}"),
        )
        .await?;
        if count == 0 {
            return Err(anyhow!("schema benchmark switch isolation failed"));
        }
        samples.push(started.elapsed());
    }
    metrics.tenant_switch_p50_microseconds = percentile_microseconds(&samples, 50)?;
    metrics.tenant_switch_p95_microseconds = percentile_microseconds(&samples, 95)?;
    metrics.tenant_switch_p99_microseconds = percentile_microseconds(&samples, 99)?;
    metrics.prepared_query_alternation_passed = true;
    metrics.concurrent_isolation_passed = concurrent_reads(runtime, parameters).await?;
    checks.require(
        "schema_candidate.profile_concurrent_isolation",
        metrics.concurrent_isolation_passed,
    )
}

async fn concurrent_reads(pool: &PgPool, parameters: ProfileParameters) -> Result<bool> {
    let mut tasks = Vec::new();
    for worker in 0..parameters.concurrency {
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move {
            let tenant_index = (worker % parameters.tenants) + 1;
            let count = scoped_payload_count(
                &pool,
                deterministic_tenant_id(tenant_index)?,
                &format!("tenant-{tenant_index}"),
            )
            .await?;
            Ok::<bool, anyhow::Error>(
                u64::try_from(count).ok() == Some(u64::from(parameters.rows_per_tenant)),
            )
        }));
    }
    let mut passed = true;
    for task in tasks {
        passed &= task
            .await
            .map_err(|_| anyhow!("schema concurrent profile task failed"))??;
    }
    Ok(passed)
}

async fn measure_probe(
    runtime: &PgPool,
    parameters: ProfileParameters,
    metrics: &mut CandidateMetrics,
) -> Result<()> {
    let tenant = deterministic_tenant_id(1)?;
    let export_started = Instant::now();
    let mut export_transaction = runtime
        .begin()
        .await
        .map_err(safe_database_error("schema probe export begin"))?;
    set_schema_scope(&mut export_transaction, tenant).await?;
    let rows =
        sqlx::query_as::<_, (i32, String)>("SELECT row_id, payload FROM bench_00 ORDER BY row_id")
            .fetch_all(&mut *export_transaction)
            .await
            .map_err(safe_database_error("schema probe export"))?;
    export_transaction
        .commit()
        .await
        .map_err(safe_database_error("schema probe export commit"))?;
    metrics.single_tenant_probe_export_microseconds =
        duration_microseconds(export_started.elapsed());

    let import_started = Instant::now();
    let mut import_transaction = runtime
        .begin()
        .await
        .map_err(safe_database_error("schema probe import begin"))?;
    set_schema_scope(&mut import_transaction, tenant).await?;
    for (row_id, payload) in rows {
        sqlx::query(
            "INSERT INTO bench_00(row_id, payload, auxiliary_a, auxiliary_b) \
             VALUES ($1 + 100000, $2, $1, $1)",
        )
        .bind(row_id)
        .bind(payload)
        .execute(&mut *import_transaction)
        .await
        .map_err(safe_database_error("schema probe import"))?;
    }
    import_transaction
        .commit()
        .await
        .map_err(safe_database_error("schema probe import commit"))?;
    metrics.single_tenant_probe_import_microseconds =
        duration_microseconds(import_started.elapsed());
    let expected = usize::try_from(parameters.rows_per_tenant)
        .map_err(|_| anyhow!("schema probe row count is too large"))?;
    if expected == 0 {
        return Err(anyhow!("schema probe profile must contain rows"));
    }
    Ok(())
}

async fn apply_incremental_migration(
    pool: &PgPool,
    tenant: TenantId,
    table_count: u32,
) -> Result<()> {
    let schema = quote_identifier(&tenant_schema_name(tenant)?)?;
    let mut ddl = String::new();
    for index in 0..table_count {
        let table = quote_identifier(&format!("bench_{index:02}"))?;
        write!(
            &mut ddl,
            "ALTER TABLE {schema}.{table} ADD COLUMN incremental_probe TEXT;"
        )
        .map_err(|_| anyhow!("tenant incremental DDL rendering failed"))?;
    }
    write!(
        &mut ddl,
        "CREATE INDEX bench_00_incremental_idx ON {schema}.bench_00(incremental_probe);"
    )
    .map_err(|_| anyhow!("tenant incremental index rendering failed"))?;
    sqlx::raw_sql(AssertSqlSafe(ddl))
        .execute(pool)
        .await
        .map_err(safe_database_error("per-tenant incremental migration"))?;
    sqlx::query(
        "UPDATE qualification_schema_control.tenant_schema \
         SET migration_version = 2, migration_dirty = false WHERE tenant_id = $1",
    )
    .bind(*tenant.as_uuid())
    .execute(pool)
    .await
    .map_err(safe_database_error("per-tenant incremental history update"))?;
    Ok(())
}

async fn populate_catalog_metrics(pool: &PgPool, metrics: &mut CandidateMetrics) -> Result<()> {
    let counts = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT \
             (SELECT pg_catalog.count(*) FROM pg_catalog.pg_namespace \
              WHERE nspname LIKE 't\\_%' ESCAPE '\\'), \
             (SELECT pg_catalog.count(*) FROM pg_catalog.pg_class AS c \
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
              WHERE n.nspname LIKE 't\\_%' ESCAPE '\\' AND c.relkind = 'r'), \
             (SELECT pg_catalog.count(*) FROM pg_catalog.pg_class AS c \
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
              WHERE n.nspname LIKE 't\\_%' ESCAPE '\\' AND c.relkind = 'i'), \
             (SELECT pg_catalog.count(*) FROM pg_catalog.pg_class AS c \
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
              WHERE n.nspname LIKE 't\\_%' ESCAPE '\\'), \
             (SELECT pg_catalog.count(*) FROM pg_catalog.pg_attribute AS a \
              JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid \
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
              WHERE n.nspname LIKE 't\\_%' ESCAPE '\\' \
                AND a.attnum > 0 AND NOT a.attisdropped)",
    )
    .fetch_one(pool)
    .await
    .map_err(safe_database_error("schema candidate catalog metrics"))?;
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
    .map_err(safe_database_error("schema candidate database size"))?;
    metrics.database_size_bytes = nonnegative(size)?;
    Ok(())
}

async fn cleanup(bootstrap: &PgPool) -> Result<()> {
    let schemas = sqlx::query_scalar::<_, String>(
        "SELECT nspname FROM pg_catalog.pg_namespace \
         WHERE nspname LIKE 't\\_%' ESCAPE '\\' ORDER BY nspname",
    )
    .fetch_all(bootstrap)
    .await
    .map_err(safe_database_error("tenant schema cleanup enumeration"))?;
    sqlx::raw_sql(
        "DROP SCHEMA IF EXISTS qualification_unrelated CASCADE; \
         DROP SCHEMA IF EXISTS qualification_schema_control CASCADE;",
    )
    .execute(bootstrap)
    .await
    .map_err(safe_database_error("schema candidate control cleanup"))?;
    for schema in schemas {
        let ddl = format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_identifier(&schema)?
        );
        sqlx::query(AssertSqlSafe(ddl))
            .execute(bootstrap)
            .await
            .map_err(safe_database_error("tenant schema candidate cleanup"))?;
    }
    Ok(())
}

fn nonnegative(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("catalog metric was negative"))
}
