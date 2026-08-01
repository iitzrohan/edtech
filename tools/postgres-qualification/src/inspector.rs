//! Catalog-driven tenant-table invariant inspection and unsafe-schema fixtures.

use anyhow::Result;
use sqlx::{PgPool, Row};

use crate::{database::safe_database_error, model::CheckBook};

const RUNTIME_ROLES: &[&str] = &["edtech_cell_api", "edtech_cell_worker"];

pub(crate) async fn inspect_selected_schema(
    migrator: &PgPool,
    bootstrap: &PgPool,
    checks: &mut CheckBook,
) -> Result<()> {
    let violations = inspect_schema(migrator, "tenant_data").await?;
    checks.require(
        "schema_inspector.selected_tenant_data_passes",
        violations.is_empty(),
    )?;

    create_unsafe_fixtures(migrator, bootstrap).await?;
    let fixture_violations = inspect_schema(bootstrap, "qualification_unsafe").await?;
    let expected_tables = [
        "missing_tenant",
        "wrong_tenant_type",
        "nullable_tenant",
        "rls_disabled",
        "rls_unforced",
        "no_policy",
        "policy_missing_check",
        "public_privilege",
        "global_unique",
        "bad_foreign_key",
        "runtime_owned",
    ];
    checks.require(
        "schema_inspector.synthetic_major_rule_violations_are_rejected",
        expected_tables.iter().all(|table| {
            fixture_violations
                .iter()
                .any(|violation| violation.contains(table))
        }),
    )?;
    cleanup_unsafe_fixtures(bootstrap).await
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn inspect_schema(pool: &PgPool, schema: &str) -> Result<Vec<String>> {
    let tables = sqlx::query(
        "SELECT c.oid::bigint AS table_oid, c.relname, c.relrowsecurity, \
                c.relforcerowsecurity, owner.rolname AS owner_name \
         FROM pg_catalog.pg_class AS c \
         JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
         JOIN pg_catalog.pg_roles AS owner ON owner.oid = c.relowner \
         WHERE n.nspname = $1 AND c.relkind = 'r' ORDER BY c.relname",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .map_err(safe_database_error("tenant schema table enumeration"))?;
    let mut violations = Vec::new();

    let runtime_schema_create = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS ( \
             SELECT 1 FROM pg_catalog.pg_namespace AS n \
             CROSS JOIN pg_catalog.unnest($2::text[]) AS runtime_role \
             WHERE n.nspname = $1 \
               AND pg_catalog.has_schema_privilege(runtime_role, n.oid, 'CREATE') \
         )",
    )
    .bind(schema)
    .bind(RUNTIME_ROLES)
    .fetch_one(pool)
    .await
    .map_err(safe_database_error(
        "tenant schema CREATE privilege inspection",
    ))?;
    if runtime_schema_create {
        violations.push(format!(
            "schema `{schema}` grants CREATE to a Cell runtime role"
        ));
    }

    for table in tables {
        let table_oid = table.get::<i64, _>("table_oid");
        let table_name = table.get::<String, _>("relname");
        let qualified = format!("{schema}.{table_name}");
        let tenant_column = sqlx::query(
            "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type, a.attnotnull \
             FROM pg_catalog.pg_attribute AS a \
             WHERE a.attrelid = $1::oid AND a.attname = 'tenant_id' \
               AND a.attnum > 0 AND NOT a.attisdropped",
        )
        .bind(table_oid)
        .fetch_optional(pool)
        .await
        .map_err(safe_database_error("tenant_id column inspection"))?;
        match tenant_column {
            None => violations.push(format!("table `{qualified}` has no tenant_id column")),
            Some(column) => {
                if column.get::<String, _>("data_type") != "uuid" {
                    violations.push(format!("table `{qualified}` tenant_id is not UUID"));
                }
                if !column.get::<bool, _>("attnotnull") {
                    violations.push(format!("table `{qualified}` tenant_id is nullable"));
                }
            }
        }

        if !table.get::<bool, _>("relrowsecurity") {
            violations.push(format!("table `{qualified}` does not enable RLS"));
        }
        if !table.get::<bool, _>("relforcerowsecurity") {
            violations.push(format!("table `{qualified}` does not force RLS"));
        }

        let policy = sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT pg_catalog.count(*), \
                    pg_catalog.count(*) FILTER (WHERE polqual IS NOT NULL), \
                    pg_catalog.count(*) FILTER (WHERE polwithcheck IS NOT NULL) \
             FROM pg_catalog.pg_policy WHERE polrelid = $1::oid",
        )
        .bind(table_oid)
        .fetch_one(pool)
        .await
        .map_err(safe_database_error("tenant policy inspection"))?;
        if policy.0 == 0 {
            violations.push(format!("table `{qualified}` has no applicable policy"));
        }
        if policy.1 == 0 {
            violations.push(format!(
                "table `{qualified}` has no policy USING expression"
            ));
        }
        if policy.2 == 0 {
            violations.push(format!(
                "table `{qualified}` has no policy WITH CHECK expression"
            ));
        }
        let public_policy = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_policy \
             WHERE polrelid = $1::oid AND 0::oid = ANY(polroles))",
        )
        .bind(table_oid)
        .fetch_one(pool)
        .await
        .map_err(safe_database_error("tenant PUBLIC policy inspection"))?;
        if public_policy {
            violations.push(format!("table `{qualified}` has a policy for PUBLIC"));
        }

        let owner = table.get::<String, _>("owner_name");
        if RUNTIME_ROLES.contains(&owner.as_str()) {
            violations.push(format!(
                "table `{qualified}` is owned by runtime role `{owner}`"
            ));
        }
        let runtime_can_set_owner = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS ( \
                 SELECT 1 FROM pg_catalog.pg_roles AS owner_role \
                 CROSS JOIN pg_catalog.unnest($2::text[]) AS runtime_role \
                 WHERE owner_role.rolname = $1 \
                   AND pg_catalog.pg_has_role(runtime_role, owner_role.oid, 'SET') \
             )",
        )
        .bind(&owner)
        .bind(RUNTIME_ROLES)
        .fetch_one(pool)
        .await
        .map_err(safe_database_error(
            "tenant table ownership escalation inspection",
        ))?;
        if runtime_can_set_owner {
            violations.push(format!(
                "table `{qualified}` owner is reachable through SET ROLE"
            ));
        }

        let public_privilege = sqlx::query_scalar::<_, bool>(
            "SELECT pg_catalog.has_table_privilege( \
                 'public', $1::oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER' \
             )",
        )
        .bind(table_oid)
        .fetch_one(pool)
        .await
        .map_err(safe_database_error(
            "tenant PUBLIC table privilege inspection",
        ))?;
        if public_privilege {
            violations.push(format!("table `{qualified}` grants a privilege to PUBLIC"));
        }

        let unsafe_indexes = sqlx::query_scalar::<_, String>(
            "SELECT index_class.relname \
             FROM pg_catalog.pg_index AS index_meta \
             JOIN pg_catalog.pg_class AS index_class ON index_class.oid = index_meta.indexrelid \
             WHERE index_meta.indrelid = $1::oid \
               AND (index_meta.indisprimary OR index_meta.indisunique) \
               AND NOT EXISTS ( \
                   SELECT 1 \
                   FROM pg_catalog.unnest(index_meta.indkey::smallint[]) AS key(attnum) \
                   JOIN pg_catalog.pg_attribute AS attribute \
                     ON attribute.attrelid = index_meta.indrelid \
                    AND attribute.attnum = key.attnum \
                   WHERE attribute.attname = 'tenant_id' \
               )",
        )
        .bind(table_oid)
        .fetch_all(pool)
        .await
        .map_err(safe_database_error("tenant unique-index inspection"))?;
        for index in unsafe_indexes {
            violations.push(format!(
                "index `{schema}.{index}` on table `{qualified}` omits tenant_id"
            ));
        }

        let unsafe_outbound = sqlx::query_scalar::<_, String>(
            "SELECT constraint_meta.conname \
             FROM pg_catalog.pg_constraint AS constraint_meta \
             WHERE constraint_meta.conrelid = $1::oid AND constraint_meta.contype = 'f' \
               AND NOT EXISTS ( \
                   SELECT 1 \
                   FROM pg_catalog.unnest(constraint_meta.conkey) AS key(attnum) \
                   JOIN pg_catalog.pg_attribute AS attribute \
                     ON attribute.attrelid = constraint_meta.conrelid \
                    AND attribute.attnum = key.attnum \
                   WHERE attribute.attname = 'tenant_id' \
               )",
        )
        .bind(table_oid)
        .fetch_all(pool)
        .await
        .map_err(safe_database_error(
            "tenant outbound foreign-key inspection",
        ))?;
        for constraint in unsafe_outbound {
            violations.push(format!(
                "constraint `{schema}.{constraint}` on table `{qualified}` omits referencing tenant_id"
            ));
        }

        let unsafe_inbound = sqlx::query_scalar::<_, String>(
            "SELECT constraint_meta.conname \
             FROM pg_catalog.pg_constraint AS constraint_meta \
             WHERE constraint_meta.confrelid = $1::oid AND constraint_meta.contype = 'f' \
               AND NOT EXISTS ( \
                   SELECT 1 \
                   FROM pg_catalog.unnest(constraint_meta.confkey) AS key(attnum) \
                   JOIN pg_catalog.pg_attribute AS attribute \
                     ON attribute.attrelid = constraint_meta.confrelid \
                    AND attribute.attnum = key.attnum \
                   WHERE attribute.attname = 'tenant_id' \
               )",
        )
        .bind(table_oid)
        .fetch_all(pool)
        .await
        .map_err(safe_database_error("tenant inbound foreign-key inspection"))?;
        for constraint in unsafe_inbound {
            violations.push(format!(
                "constraint `{schema}.{constraint}` targeting table `{qualified}` omits referenced tenant_id"
            ));
        }
    }

    Ok(violations)
}

async fn create_unsafe_fixtures(migrator: &PgPool, bootstrap: &PgPool) -> Result<()> {
    cleanup_unsafe_fixtures(bootstrap).await?;
    sqlx::raw_sql(
        "CREATE SCHEMA qualification_unsafe AUTHORIZATION edtech_cell_migrator;

         CREATE TABLE qualification_unsafe.missing_tenant(id UUID PRIMARY KEY);
         ALTER TABLE qualification_unsafe.missing_tenant ENABLE ROW LEVEL SECURITY;
         ALTER TABLE qualification_unsafe.missing_tenant FORCE ROW LEVEL SECURITY;
         CREATE POLICY missing_tenant_policy ON qualification_unsafe.missing_tenant
             TO edtech_cell_api USING (true) WITH CHECK (true);

         CREATE TABLE qualification_unsafe.wrong_tenant_type(
             tenant_id TEXT NOT NULL PRIMARY KEY
         );
         ALTER TABLE qualification_unsafe.wrong_tenant_type ENABLE ROW LEVEL SECURITY;
         ALTER TABLE qualification_unsafe.wrong_tenant_type FORCE ROW LEVEL SECURITY;
         CREATE POLICY wrong_type_policy ON qualification_unsafe.wrong_tenant_type
             TO edtech_cell_api USING (true) WITH CHECK (true);

         CREATE TABLE qualification_unsafe.nullable_tenant(tenant_id UUID, local_id UUID);
         ALTER TABLE qualification_unsafe.nullable_tenant ENABLE ROW LEVEL SECURITY;
         ALTER TABLE qualification_unsafe.nullable_tenant FORCE ROW LEVEL SECURITY;
         CREATE POLICY nullable_policy ON qualification_unsafe.nullable_tenant
             TO edtech_cell_api USING (true) WITH CHECK (true);

         CREATE TABLE qualification_unsafe.rls_disabled(tenant_id UUID NOT NULL PRIMARY KEY);
         CREATE POLICY disabled_policy ON qualification_unsafe.rls_disabled
             TO edtech_cell_api USING (true) WITH CHECK (true);

         CREATE TABLE qualification_unsafe.rls_unforced(tenant_id UUID NOT NULL PRIMARY KEY);
         ALTER TABLE qualification_unsafe.rls_unforced ENABLE ROW LEVEL SECURITY;
         CREATE POLICY unforced_policy ON qualification_unsafe.rls_unforced
             TO edtech_cell_api USING (true) WITH CHECK (true);

         CREATE TABLE qualification_unsafe.no_policy(tenant_id UUID NOT NULL PRIMARY KEY);
         ALTER TABLE qualification_unsafe.no_policy ENABLE ROW LEVEL SECURITY;
         ALTER TABLE qualification_unsafe.no_policy FORCE ROW LEVEL SECURITY;

         CREATE TABLE qualification_unsafe.policy_missing_check(
             tenant_id UUID NOT NULL PRIMARY KEY
         );
         ALTER TABLE qualification_unsafe.policy_missing_check ENABLE ROW LEVEL SECURITY;
         ALTER TABLE qualification_unsafe.policy_missing_check FORCE ROW LEVEL SECURITY;
         CREATE POLICY missing_check_policy ON qualification_unsafe.policy_missing_check
             FOR SELECT TO edtech_cell_api USING (true);

         CREATE TABLE qualification_unsafe.public_privilege(
             tenant_id UUID NOT NULL PRIMARY KEY
         );
         ALTER TABLE qualification_unsafe.public_privilege ENABLE ROW LEVEL SECURITY;
         ALTER TABLE qualification_unsafe.public_privilege FORCE ROW LEVEL SECURITY;
         CREATE POLICY public_privilege_policy ON qualification_unsafe.public_privilege
             TO edtech_cell_api USING (true) WITH CHECK (true);
         GRANT SELECT ON qualification_unsafe.public_privilege TO PUBLIC;

         CREATE TABLE qualification_unsafe.global_unique(
             tenant_id UUID NOT NULL,
             local_id UUID NOT NULL,
             PRIMARY KEY (tenant_id, local_id),
             UNIQUE (local_id)
         );
         ALTER TABLE qualification_unsafe.global_unique ENABLE ROW LEVEL SECURITY;
         ALTER TABLE qualification_unsafe.global_unique FORCE ROW LEVEL SECURITY;
         CREATE POLICY global_unique_policy ON qualification_unsafe.global_unique
             TO edtech_cell_api USING (true) WITH CHECK (true);

         CREATE TABLE qualification_unsafe.bad_foreign_key(
             tenant_id UUID NOT NULL,
             parent_id UUID NOT NULL,
             PRIMARY KEY (tenant_id, parent_id),
             CONSTRAINT bad_foreign_key_constraint FOREIGN KEY (parent_id)
                 REFERENCES qualification_unsafe.global_unique(local_id)
         );
         ALTER TABLE qualification_unsafe.bad_foreign_key ENABLE ROW LEVEL SECURITY;
         ALTER TABLE qualification_unsafe.bad_foreign_key FORCE ROW LEVEL SECURITY;
         CREATE POLICY bad_foreign_key_policy ON qualification_unsafe.bad_foreign_key
             TO edtech_cell_api USING (true) WITH CHECK (true);

         CREATE TABLE qualification_unsafe.runtime_owned(
             tenant_id UUID NOT NULL PRIMARY KEY
         );
         ALTER TABLE qualification_unsafe.runtime_owned ENABLE ROW LEVEL SECURITY;
         ALTER TABLE qualification_unsafe.runtime_owned FORCE ROW LEVEL SECURITY;
         CREATE POLICY runtime_owned_policy ON qualification_unsafe.runtime_owned
             TO edtech_cell_api USING (true) WITH CHECK (true);",
    )
    .execute(migrator)
    .await
    .map_err(safe_database_error("unsafe tenant-schema fixture creation"))?;
    sqlx::query("ALTER TABLE qualification_unsafe.runtime_owned OWNER TO edtech_cell_api")
        .execute(bootstrap)
        .await
        .map_err(safe_database_error("unsafe runtime-owner fixture creation"))?;
    Ok(())
}

async fn cleanup_unsafe_fixtures(bootstrap: &PgPool) -> Result<()> {
    sqlx::query("DROP SCHEMA IF EXISTS qualification_unsafe CASCADE")
        .execute(bootstrap)
        .await
        .map_err(safe_database_error("unsafe tenant-schema fixture cleanup"))?;
    Ok(())
}
