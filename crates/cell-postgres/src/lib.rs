//! Runtime `PostgreSQL` adapter for one logical Cell database authority.
//!
//! This crate owns authority/contract verification, tenant-scoped transaction mechanics, and
//! narrowly named isolation-canary operations for the selected shared-table RLS model. It must not
//! contain DDL, migrations, Platform dependencies, configuration loading, or expose raw `SQLx`
//! pools, connections, rows, transactions, or arbitrary query execution.

use std::{fmt, str::FromStr};

use cell_application::TenantExecutionScope;
use message_domain::{EncodedMessage, MessageAuthority, MessageKind, MessageScope, MessageTarget};
use postgres_message_store::{
    ClaimBatchSize, ClaimedMessage, ConsumerName, EnqueueOutcome, FailureCategory,
    InboxReceiptOutcome, LeaseDuration, MessageStoreError, MessageStoreNamespace, OutboxLeaseId,
    PublishMarkOutcome, PublisherInstanceId, RescheduleOutcome, RetryDelay,
};
use postgres_runtime::{
    DatabaseCredential, PostgresConnectionConfig, PostgresPool, ProviderError, ProviderErrorKind,
    connect, verify_runtime_role, verify_server_version,
};
use sqlx::{Postgres, Row, Transaction};
use tenancy_domain::{AssignmentEpoch, CellId};
use uuid::Uuid;

const MIGRATION_ROLE: &str = "edtech_cell_migrator";
const MIN_SUPPORTED_CONTRACT_VERSION: u32 = 1;
const MAX_SUPPORTED_CONTRACT_VERSION: u32 = 2;
const MAX_CANARY_PAYLOAD_CHARACTERS: usize = 4_096;
const CELL_SCHEMAS: &[&str] = &[
    "cell_control",
    "cell_messaging",
    "edtech_bootstrap",
    "edtech_internal",
    "edtech_migrations",
    "tenant_data",
];

/// The separately scoped Cell runtime role expected on a connection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CellRuntimeRole {
    /// Cell API runtime role.
    Api,
    /// Cell worker runtime role.
    Worker,
}

impl CellRuntimeRole {
    const fn database_role(self) -> &'static str {
        match self {
            Self::Api => "edtech_cell_api",
            Self::Worker => "edtech_cell_worker",
        }
    }
}

/// A validated `UUIDv7` identity for one synthetic isolation-canary row.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IsolationCanaryId(Uuid);

impl IsolationCanaryId {
    /// Constructs a canary identity after validating `UUID` version 7.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidIsolationCanaryId`] for any other UUID version.
    pub fn new(value: Uuid) -> Result<Self, InvalidIsolationCanaryId> {
        if value.get_version_num() == 7 {
            Ok(Self(value))
        } else {
            Err(InvalidIsolationCanaryId)
        }
    }

    const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl FromStr for IsolationCanaryId {
    type Err = InvalidIsolationCanaryId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = Uuid::parse_str(value).map_err(|_| InvalidIsolationCanaryId)?;
        Self::new(value)
    }
}

impl fmt::Display for IsolationCanaryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for IsolationCanaryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("IsolationCanaryId")
            .field(&self.0)
            .finish()
    }
}

/// A canary identifier is malformed or not `UUIDv7`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidIsolationCanaryId;

impl fmt::Display for InvalidIsolationCanaryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("isolation canary identifier must be UUID version 7")
    }
}

impl std::error::Error for InvalidIsolationCanaryId {}

/// A visible canary row returned through a tenant-authorized operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IsolationCanary {
    canary_id: IsolationCanaryId,
    payload: String,
}

impl IsolationCanary {
    /// Returns the synthetic row identity.
    #[must_use]
    pub const fn canary_id(&self) -> IsolationCanaryId {
        self.canary_id
    }

    /// Returns the bounded synthetic payload.
    #[must_use]
    pub fn payload(&self) -> &str {
        &self.payload
    }
}

/// Safe information proven when a Cell database becomes ready.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CellDatabaseCheck {
    server_version: u32,
    contract_version: u32,
    cell_id: CellId,
}

impl CellDatabaseCheck {
    /// Returns `server_version_num`.
    #[must_use]
    pub const fn server_version(&self) -> u32 {
        self.server_version
    }

    /// Returns the supported Cell schema-contract version.
    #[must_use]
    pub const fn contract_version(&self) -> u32 {
        self.contract_version
    }

    /// Returns the verified stable logical Cell identity.
    #[must_use]
    pub const fn cell_id(&self) -> &CellId {
        &self.cell_id
    }

    /// Reports whether schema-contract version 2 message-store operations are available.
    #[must_use]
    pub const fn message_store_available(&self) -> bool {
        self.contract_version >= 2
    }
}

/// Stable safe Cell database error categories.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CellDatabaseErrorKind {
    /// Credential, connection, TLS, timeout, server, or generic database failure.
    Provider,
    /// The bootstrap authority marker does not identify the configured Cell.
    AuthorityMismatch,
    /// The connected role violates the expected runtime profile.
    PrivilegeMismatch,
    /// The Cell schema contract is absent or incompatible.
    ContractMismatch,
    /// Message-store operations require schema-contract version 2.
    MessageStoreCapabilityUnavailable,
    /// The connected runtime role cannot perform the operation.
    RoleCapabilityMismatch,
    /// An outbound message is not sourced and scoped by this Cell.
    InvalidOutboundAuthority,
    /// An inbound message does not target or scope to this Cell.
    InvalidInboundTarget,
    /// Reusable message-store mechanics failed.
    MessageStoreFailure,
    /// An inbox identity conflicts with immutable stored content.
    InboxConflict,
    /// The execution scope names another logical Cell.
    WrongCell,
    /// The tenant has no local authority record.
    TenantAbsent,
    /// Tenant serving is disabled locally.
    TenantDisabled,
    /// The supplied assignment epoch is not the current epoch.
    StaleAssignmentEpoch,
    /// A canary identifier is malformed.
    InvalidCanaryId,
    /// A canary payload is empty or exceeds its bound.
    InvalidPayload,
    /// A tenant operation failed and was rolled back.
    Operation,
    /// An assignment epoch could not be represented losslessly.
    InvalidAssignmentEpoch,
}

impl CellDatabaseErrorKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Provider => "provider_failure",
            Self::AuthorityMismatch => "authority_mismatch",
            Self::PrivilegeMismatch => "privilege_mismatch",
            Self::ContractMismatch => "schema_contract_mismatch",
            Self::MessageStoreCapabilityUnavailable => "message_store_capability_unavailable",
            Self::RoleCapabilityMismatch => "role_capability_mismatch",
            Self::InvalidOutboundAuthority => "invalid_outbound_authority",
            Self::InvalidInboundTarget => "invalid_inbound_target",
            Self::MessageStoreFailure => "message_store_failure",
            Self::InboxConflict => "inbox_conflict",
            Self::WrongCell => "wrong_cell",
            Self::TenantAbsent => "tenant_absent",
            Self::TenantDisabled => "tenant_disabled",
            Self::StaleAssignmentEpoch => "stale_assignment_epoch",
            Self::InvalidCanaryId => "invalid_canary_id",
            Self::InvalidPayload => "invalid_payload",
            Self::Operation => "tenant_operation_failure",
            Self::InvalidAssignmentEpoch => "invalid_assignment_epoch",
        }
    }
}

/// A sanitized Cell runtime database failure.
pub struct CellDatabaseError {
    kind: CellDatabaseErrorKind,
}

impl CellDatabaseError {
    const fn new(kind: CellDatabaseErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable safe category.
    #[must_use]
    pub const fn kind(&self) -> CellDatabaseErrorKind {
        self.kind
    }
}

impl fmt::Display for CellDatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "cell postgres error: {}", self.kind.as_str())
    }
}

impl fmt::Debug for CellDatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CellDatabaseError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl std::error::Error for CellDatabaseError {}

impl From<ProviderError> for CellDatabaseError {
    fn from(error: ProviderError) -> Self {
        let kind = match error.kind() {
            ProviderErrorKind::AuthorityMismatch => CellDatabaseErrorKind::AuthorityMismatch,
            ProviderErrorKind::PrivilegeMismatch => CellDatabaseErrorKind::PrivilegeMismatch,
            ProviderErrorKind::SchemaContractMismatch => CellDatabaseErrorKind::ContractMismatch,
            _ => CellDatabaseErrorKind::Provider,
        };
        Self::new(kind)
    }
}

impl From<MessageStoreError> for CellDatabaseError {
    fn from(_error: MessageStoreError) -> Self {
        Self::new(CellDatabaseErrorKind::MessageStoreFailure)
    }
}

/// Result of atomically recording Cell inbound work and optional derived output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CellInboxOutcome {
    /// The named handler committed its first local receipt and optional output.
    Inserted,
    /// An exact redelivery was suppressed and produced no second output.
    Duplicate,
}

/// Opaque, verified Cell runtime database handle.
#[derive(Clone)]
pub struct CellDatabase {
    pool: PostgresPool,
    cell_id: CellId,
    check: CellDatabaseCheck,
    role: CellRuntimeRole,
}

impl fmt::Debug for CellDatabase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CellDatabase")
            .field("cell_id", &self.cell_id)
            .field("check", &self.check)
            .finish_non_exhaustive()
    }
}

struct TenantTransaction<'a> {
    transaction: Transaction<'a, Postgres>,
    tenant_id: Uuid,
}

impl CellDatabase {
    /// Connects and fails closed unless authority, Cell, server, role, and contract all match.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`CellDatabaseError`] without connection or credential details.
    pub async fn connect(
        credential: &impl DatabaseCredential,
        config: &PostgresConnectionConfig,
        cell_id: &CellId,
        role: CellRuntimeRole,
    ) -> Result<Self, CellDatabaseError> {
        let pool = connect(credential, config)
            .await
            .map_err(CellDatabaseError::from)?;
        match verify_ready(&pool, cell_id, role).await {
            Ok(check) => Ok(Self {
                pool,
                cell_id: cell_id.clone(),
                check,
                role,
            }),
            Err(error) => {
                pool.close().await;
                Err(error)
            }
        }
    }

    /// Returns safe readiness facts established at connection time.
    #[must_use]
    pub const fn check(&self) -> &CellDatabaseCheck {
        &self.check
    }

    /// Enqueues one Cell-sourced message, validating any tenant fence in the same transaction.
    ///
    /// # Errors
    ///
    /// Rejects contract 1, wrong Cell authority/scope, stale tenant fences, and store failures.
    pub async fn enqueue_outbound_message(
        &self,
        message: &EncodedMessage,
    ) -> Result<EnqueueOutcome, CellDatabaseError> {
        self.require_message_store()?;
        self.validate_cell_outbound(message)?;
        match message.metadata().scope() {
            MessageScope::Tenant {
                tenant_id,
                cell_id,
                assignment_epoch,
            } => {
                let scope =
                    TenantExecutionScope::new(*tenant_id, cell_id.clone(), *assignment_epoch);
                let mut tenant = self.begin_tenant(&scope).await?;
                let outcome = postgres_message_store::enqueue(
                    &mut tenant.transaction,
                    MessageStoreNamespace::Cell,
                    message,
                )
                .await
                .map_err(CellDatabaseError::from)?;
                tenant
                    .transaction
                    .commit()
                    .await
                    .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::Operation))?;
                Ok(outcome)
            }
            MessageScope::Cell(_) => {
                let mut transaction = self
                    .pool
                    .sqlx_pool()
                    .begin()
                    .await
                    .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::Operation))?;
                let outcome = postgres_message_store::enqueue(
                    &mut transaction,
                    MessageStoreNamespace::Cell,
                    message,
                )
                .await
                .map_err(CellDatabaseError::from)?;
                transaction
                    .commit()
                    .await
                    .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::Operation))?;
                Ok(outcome)
            }
            MessageScope::Platform => Err(CellDatabaseError::new(
                CellDatabaseErrorKind::InvalidOutboundAuthority,
            )),
        }
    }

    /// Atomically writes the operational isolation canary and a matching tenant outbox message.
    ///
    /// # Errors
    ///
    /// Rejects invalid payload, wrong/mismatched scope, tenant authority, RLS, or store failures;
    /// both effects commit or roll back together.
    pub async fn write_isolation_canary_and_enqueue(
        &self,
        scope: &TenantExecutionScope,
        canary_id: IsolationCanaryId,
        payload: &str,
        message: &EncodedMessage,
    ) -> Result<EnqueueOutcome, CellDatabaseError> {
        self.require_message_store()?;
        validate_payload(payload)?;
        self.validate_cell_outbound(message)?;
        if !message_scope_matches_execution(message.metadata().scope(), scope) {
            return Err(CellDatabaseError::new(
                CellDatabaseErrorKind::InvalidOutboundAuthority,
            ));
        }
        let mut tenant = self.begin_tenant(scope).await?;
        sqlx::query(
            "INSERT INTO tenant_data.isolation_canary (tenant_id, canary_id, payload) \
             VALUES ($1, $2, $3)",
        )
        .bind(tenant.tenant_id)
        .bind(*canary_id.as_uuid())
        .bind(payload)
        .execute(&mut *tenant.transaction)
        .await
        .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::Operation))?;
        let outcome = postgres_message_store::enqueue(
            &mut tenant.transaction,
            MessageStoreNamespace::Cell,
            message,
        )
        .await
        .map_err(CellDatabaseError::from)?;
        tenant
            .transaction
            .commit()
            .await
            .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::Operation))?;
        Ok(outcome)
    }

    /// Claims an eligible Cell outbox batch for the worker role.
    ///
    /// # Errors
    ///
    /// Rejects API roles before SQL and returns safe store failures.
    pub async fn claim_outbox_batch(
        &self,
        batch_size: ClaimBatchSize,
        publisher: PublisherInstanceId,
        lease_id: OutboxLeaseId,
        lease_duration: LeaseDuration,
    ) -> Result<Vec<ClaimedMessage>, CellDatabaseError> {
        self.require_worker()?;
        self.require_message_store()?;
        let mut transaction = self
            .pool
            .sqlx_pool()
            .begin()
            .await
            .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::Operation))?;
        let claimed = postgres_message_store::claim_batch(
            &mut transaction,
            MessageStoreNamespace::Cell,
            batch_size,
            publisher,
            lease_id,
            lease_duration,
        )
        .await
        .map_err(CellDatabaseError::from)?;
        transaction
            .commit()
            .await
            .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::Operation))?;
        Ok(claimed)
    }

    /// Marks future transport acceptance under an active Cell outbox lease.
    ///
    /// # Errors
    ///
    /// Rejects API roles, contract 1, and safe store failures.
    pub async fn mark_outbox_published(
        &self,
        message_id: message_domain::MessageId,
        lease_id: OutboxLeaseId,
    ) -> Result<PublishMarkOutcome, CellDatabaseError> {
        self.require_worker()?;
        self.require_message_store()?;
        let mut transaction = self
            .pool
            .sqlx_pool()
            .begin()
            .await
            .map_err(operation_error)?;
        let outcome = postgres_message_store::mark_published(
            &mut transaction,
            MessageStoreNamespace::Cell,
            message_id,
            lease_id,
        )
        .await
        .map_err(CellDatabaseError::from)?;
        transaction.commit().await.map_err(operation_error)?;
        Ok(outcome)
    }

    /// Reschedules a Cell outbox row under an active lease.
    ///
    /// # Errors
    ///
    /// Rejects API roles, contract 1, and safe store failures.
    pub async fn reschedule_outbox_message(
        &self,
        message_id: message_domain::MessageId,
        lease_id: OutboxLeaseId,
        retry_delay: RetryDelay,
        failure_category: Option<&FailureCategory>,
    ) -> Result<RescheduleOutcome, CellDatabaseError> {
        self.require_worker()?;
        self.require_message_store()?;
        let mut transaction = self
            .pool
            .sqlx_pool()
            .begin()
            .await
            .map_err(operation_error)?;
        let outcome = postgres_message_store::reschedule(
            &mut transaction,
            MessageStoreNamespace::Cell,
            message_id,
            lease_id,
            retry_delay,
            failure_category,
        )
        .await
        .map_err(CellDatabaseError::from)?;
        transaction.commit().await.map_err(operation_error)?;
        Ok(outcome)
    }

    /// Atomically records one worker inbox receipt and optional Cell-sourced output.
    ///
    /// # Errors
    ///
    /// Rejects wrong targets/scopes, tenant fences, roles, conflicts, and store failures.
    pub async fn record_inbox_and_enqueue(
        &self,
        consumer: &ConsumerName,
        inbound: &EncodedMessage,
        derived: Option<&EncodedMessage>,
    ) -> Result<CellInboxOutcome, CellDatabaseError> {
        self.require_worker()?;
        self.require_message_store()?;
        self.validate_cell_inbound(inbound)?;
        if let Some(message) = derived {
            self.validate_cell_outbound(message)?;
        }
        let mut transaction = self
            .pool
            .sqlx_pool()
            .begin()
            .await
            .map_err(operation_error)?;
        if let MessageScope::Tenant {
            tenant_id,
            cell_id,
            assignment_epoch,
        } = inbound.metadata().scope()
        {
            let scope = TenantExecutionScope::new(*tenant_id, cell_id.clone(), *assignment_epoch);
            self.validate_tenant_in_transaction(&mut transaction, &scope)
                .await?;
        }
        if let Some(message) = derived
            && let MessageScope::Tenant {
                tenant_id,
                cell_id,
                assignment_epoch,
            } = message.metadata().scope()
        {
            let scope = TenantExecutionScope::new(*tenant_id, cell_id.clone(), *assignment_epoch);
            self.validate_tenant_in_transaction(&mut transaction, &scope)
                .await?;
        }
        let receipt = postgres_message_store::record_inbox(
            &mut transaction,
            MessageStoreNamespace::Cell,
            consumer,
            inbound,
        )
        .await
        .map_err(CellDatabaseError::from)?;
        let outcome = match receipt {
            InboxReceiptOutcome::Inserted => {
                if let Some(message) = derived {
                    postgres_message_store::enqueue(
                        &mut transaction,
                        MessageStoreNamespace::Cell,
                        message,
                    )
                    .await
                    .map_err(CellDatabaseError::from)?;
                }
                CellInboxOutcome::Inserted
            }
            InboxReceiptOutcome::Duplicate => CellInboxOutcome::Duplicate,
            InboxReceiptOutcome::Conflict => {
                let _rollback_result = transaction.rollback().await;
                return Err(CellDatabaseError::new(CellDatabaseErrorKind::InboxConflict));
            }
        };
        transaction.commit().await.map_err(operation_error)?;
        Ok(outcome)
    }

    fn require_message_store(&self) -> Result<(), CellDatabaseError> {
        if self.check.message_store_available() {
            Ok(())
        } else {
            Err(CellDatabaseError::new(
                CellDatabaseErrorKind::MessageStoreCapabilityUnavailable,
            ))
        }
    }

    fn require_worker(&self) -> Result<(), CellDatabaseError> {
        if self.role == CellRuntimeRole::Worker {
            Ok(())
        } else {
            Err(CellDatabaseError::new(
                CellDatabaseErrorKind::RoleCapabilityMismatch,
            ))
        }
    }

    fn validate_cell_outbound(&self, message: &EncodedMessage) -> Result<(), CellDatabaseError> {
        let source_matches = matches!(
            message.metadata().source(),
            MessageAuthority::Cell(cell_id) if cell_id == &self.cell_id
        );
        let scope_matches = message
            .metadata()
            .scope()
            .cell_id()
            .is_some_and(|cell_id| cell_id == &self.cell_id);
        if source_matches && scope_matches {
            Ok(())
        } else {
            Err(CellDatabaseError::new(
                CellDatabaseErrorKind::InvalidOutboundAuthority,
            ))
        }
    }

    fn validate_cell_inbound(&self, message: &EncodedMessage) -> Result<(), CellDatabaseError> {
        let scope_matches = message
            .metadata()
            .scope()
            .cell_id()
            .is_some_and(|cell_id| cell_id == &self.cell_id);
        let target_matches = match message.metadata().descriptor().kind() {
            MessageKind::Command => matches!(
                message.metadata().target(),
                Some(MessageTarget::Cell(cell_id)) if cell_id == &self.cell_id
            ),
            MessageKind::Event => message.metadata().target().is_none(),
        };
        if scope_matches && target_matches {
            Ok(())
        } else {
            Err(CellDatabaseError::new(
                CellDatabaseErrorKind::InvalidInboundTarget,
            ))
        }
    }

    /// Writes one isolation-canary row within a validated tenant scope.
    ///
    /// # Errors
    ///
    /// Fails closed and rolls back for invalid scope, payload, RLS, or operation results.
    pub async fn write_isolation_canary(
        &self,
        scope: &TenantExecutionScope,
        canary_id: IsolationCanaryId,
        payload: &str,
    ) -> Result<(), CellDatabaseError> {
        validate_payload(payload)?;
        let mut tenant = self.begin_tenant(scope).await?;
        let result = sqlx::query(
            "INSERT INTO tenant_data.isolation_canary (tenant_id, canary_id, payload) \
             VALUES ($1, $2, $3)",
        )
        .bind(tenant.tenant_id)
        .bind(*canary_id.as_uuid())
        .bind(payload)
        .execute(&mut *tenant.transaction)
        .await;
        finish_unit(tenant.transaction, result.map(|_| ())).await
    }

    /// Reads one isolation-canary row within a validated tenant scope.
    ///
    /// # Errors
    ///
    /// Fails closed and rolls back for invalid scope or operation results.
    pub async fn read_isolation_canary(
        &self,
        scope: &TenantExecutionScope,
        canary_id: IsolationCanaryId,
    ) -> Result<Option<IsolationCanary>, CellDatabaseError> {
        let mut tenant = self.begin_tenant(scope).await?;
        let result = sqlx::query(
            "SELECT canary_id, payload FROM tenant_data.isolation_canary \
             WHERE tenant_id = $1 AND canary_id = $2",
        )
        .bind(tenant.tenant_id)
        .bind(*canary_id.as_uuid())
        .fetch_optional(&mut *tenant.transaction)
        .await;
        if let Ok(row) = result {
            let value = row.map(|row| IsolationCanary {
                canary_id: IsolationCanaryId(row.get::<Uuid, _>("canary_id")),
                payload: row.get::<String, _>("payload"),
            });
            tenant
                .transaction
                .commit()
                .await
                .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::Operation))?;
            Ok(value)
        } else {
            let _rollback_result = tenant.transaction.rollback().await;
            Err(CellDatabaseError::new(CellDatabaseErrorKind::Operation))
        }
    }

    /// Updates one isolation-canary payload within a validated tenant scope.
    ///
    /// # Errors
    ///
    /// Fails closed and rolls back for invalid scope, payload, RLS, or operation results.
    pub async fn update_isolation_canary(
        &self,
        scope: &TenantExecutionScope,
        canary_id: IsolationCanaryId,
        payload: &str,
    ) -> Result<bool, CellDatabaseError> {
        validate_payload(payload)?;
        let mut tenant = self.begin_tenant(scope).await?;
        let result = sqlx::query(
            "UPDATE tenant_data.isolation_canary SET payload = $3, updated_at = pg_catalog.now() \
             WHERE tenant_id = $1 AND canary_id = $2",
        )
        .bind(tenant.tenant_id)
        .bind(*canary_id.as_uuid())
        .bind(payload)
        .execute(&mut *tenant.transaction)
        .await
        .map(|result| result.rows_affected() == 1);
        finish_value(tenant.transaction, result).await
    }

    /// Deletes one isolation-canary row within a validated tenant scope.
    ///
    /// # Errors
    ///
    /// Fails closed and rolls back for invalid scope, RLS, or operation results.
    pub async fn delete_isolation_canary(
        &self,
        scope: &TenantExecutionScope,
        canary_id: IsolationCanaryId,
    ) -> Result<bool, CellDatabaseError> {
        let mut tenant = self.begin_tenant(scope).await?;
        let result = sqlx::query(
            "DELETE FROM tenant_data.isolation_canary WHERE tenant_id = $1 AND canary_id = $2",
        )
        .bind(tenant.tenant_id)
        .bind(*canary_id.as_uuid())
        .execute(&mut *tenant.transaction)
        .await
        .map(|result| result.rows_affected() == 1);
        finish_value(tenant.transaction, result).await
    }

    /// Closes all Cell runtime connections.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    async fn begin_tenant(
        &self,
        scope: &TenantExecutionScope,
    ) -> Result<TenantTransaction<'_>, CellDatabaseError> {
        if scope.cell_id() != &self.cell_id {
            return Err(CellDatabaseError::new(CellDatabaseErrorKind::WrongCell));
        }
        let tenant_id = *scope.tenant_id().as_uuid();
        let mut transaction = self
            .pool
            .sqlx_pool()
            .begin()
            .await
            .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::Operation))?;
        self.validate_tenant_in_transaction(&mut transaction, scope)
            .await?;
        Ok(TenantTransaction {
            transaction,
            tenant_id,
        })
    }

    async fn validate_tenant_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        scope: &TenantExecutionScope,
    ) -> Result<(), CellDatabaseError> {
        if scope.cell_id() != &self.cell_id {
            return Err(CellDatabaseError::new(CellDatabaseErrorKind::WrongCell));
        }
        let tenant_text = scope.tenant_id().as_uuid().to_string();
        let epoch_text = assignment_epoch_to_database_text(scope.assignment_epoch());
        let setup = sqlx::query(
            "SELECT pg_catalog.set_config('edtech.tenant_id', $1, true), \
             pg_catalog.set_config('edtech.assignment_epoch', $2, true), \
             pg_catalog.set_config('row_security', 'on', true)",
        )
        .bind(tenant_text)
        .bind(epoch_text)
        .execute(&mut **transaction)
        .await;
        if setup.is_err() {
            return Err(CellDatabaseError::new(CellDatabaseErrorKind::Operation));
        }

        let status =
            sqlx::query_scalar::<_, String>("SELECT edtech_internal.tenant_scope_status()")
                .fetch_one(&mut **transaction)
                .await;
        match status.as_deref() {
            Ok("active") => Ok(()),
            Ok("absent") => Err(CellDatabaseError::new(CellDatabaseErrorKind::TenantAbsent)),
            Ok("disabled") => Err(CellDatabaseError::new(
                CellDatabaseErrorKind::TenantDisabled,
            )),
            Ok("stale") => Err(CellDatabaseError::new(
                CellDatabaseErrorKind::StaleAssignmentEpoch,
            )),
            _ => Err(CellDatabaseError::new(CellDatabaseErrorKind::Operation)),
        }
    }
}

/// Performs the full one-shot Cell runtime database check and closes its pool.
///
/// # Errors
///
/// Returns a sanitized [`CellDatabaseError`] without connection or credential details.
pub async fn check_database(
    credential: &impl DatabaseCredential,
    config: &PostgresConnectionConfig,
    cell_id: &CellId,
    role: CellRuntimeRole,
) -> Result<CellDatabaseCheck, CellDatabaseError> {
    let database = CellDatabase::connect(credential, config, cell_id, role).await?;
    let check = database.check().clone();
    database.close().await;
    Ok(check)
}

async fn finish_unit(
    transaction: Transaction<'_, Postgres>,
    result: Result<(), sqlx::Error>,
) -> Result<(), CellDatabaseError> {
    finish_value(transaction, result).await
}

async fn finish_value<T>(
    transaction: Transaction<'_, Postgres>,
    result: Result<T, sqlx::Error>,
) -> Result<T, CellDatabaseError> {
    if let Ok(value) = result {
        transaction
            .commit()
            .await
            .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::Operation))?;
        Ok(value)
    } else {
        let _rollback_result = transaction.rollback().await;
        Err(CellDatabaseError::new(CellDatabaseErrorKind::Operation))
    }
}

fn validate_payload(payload: &str) -> Result<(), CellDatabaseError> {
    let character_count = payload.chars().count();
    if character_count == 0 || character_count > MAX_CANARY_PAYLOAD_CHARACTERS {
        return Err(CellDatabaseError::new(
            CellDatabaseErrorKind::InvalidPayload,
        ));
    }
    Ok(())
}

fn operation_error(_error: sqlx::Error) -> CellDatabaseError {
    CellDatabaseError::new(CellDatabaseErrorKind::MessageStoreFailure)
}

fn message_scope_matches_execution(
    message_scope: &MessageScope,
    execution_scope: &TenantExecutionScope,
) -> bool {
    matches!(
        message_scope,
        MessageScope::Tenant {
            tenant_id,
            cell_id,
            assignment_epoch,
        } if *tenant_id == execution_scope.tenant_id()
            && cell_id == execution_scope.cell_id()
            && *assignment_epoch == execution_scope.assignment_epoch()
    )
}

async fn verify_ready(
    pool: &PostgresPool,
    cell_id: &CellId,
    role: CellRuntimeRole,
) -> Result<CellDatabaseCheck, CellDatabaseError> {
    let server_version = verify_server_version(pool)
        .await
        .map_err(CellDatabaseError::from)?;
    verify_cell_marker(pool, cell_id).await?;
    verify_runtime_role(pool, role.database_role(), MIGRATION_ROLE, CELL_SCHEMAS)
        .await
        .map_err(CellDatabaseError::from)?;
    let contract_version = verify_contract(pool).await?;
    Ok(CellDatabaseCheck {
        server_version,
        contract_version,
        cell_id: cell_id.clone(),
    })
}

async fn verify_cell_marker(
    pool: &PostgresPool,
    cell_id: &CellId,
) -> Result<(), CellDatabaseError> {
    let row = sqlx::query(
        "SELECT authority_kind, cell_id FROM edtech_bootstrap.authority_identity WHERE singleton",
    )
    .fetch_optional(pool.sqlx_pool())
    .await
    .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::AuthorityMismatch))?;
    let matches = row.is_some_and(|row| {
        row.get::<String, _>("authority_kind") == "cell"
            && row.get::<Option<String>, _>("cell_id").as_deref() == Some(cell_id.as_str())
    });
    if !matches {
        return Err(CellDatabaseError::new(
            CellDatabaseErrorKind::AuthorityMismatch,
        ));
    }
    Ok(())
}

async fn verify_contract(pool: &PostgresPool) -> Result<u32, CellDatabaseError> {
    let row = sqlx::query(
        "SELECT contract_name, contract_version FROM cell_control.schema_contract WHERE singleton",
    )
    .fetch_optional(pool.sqlx_pool())
    .await
    .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::ContractMismatch))?;
    let Some(row) = row else {
        return Err(CellDatabaseError::new(
            CellDatabaseErrorKind::ContractMismatch,
        ));
    };
    validate_contract(
        &row.get::<String, _>("contract_name"),
        row.get::<i32, _>("contract_version"),
    )
}

fn validate_contract(name: &str, version: i32) -> Result<u32, CellDatabaseError> {
    if name != "cell" {
        return Err(CellDatabaseError::new(
            CellDatabaseErrorKind::ContractMismatch,
        ));
    }
    let version = u32::try_from(version)
        .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::ContractMismatch))?;
    if !(MIN_SUPPORTED_CONTRACT_VERSION..=MAX_SUPPORTED_CONTRACT_VERSION).contains(&version) {
        return Err(CellDatabaseError::new(
            CellDatabaseErrorKind::ContractMismatch,
        ));
    }
    Ok(version)
}

/// Converts the complete non-zero `u64` assignment epoch to exact decimal text.
#[must_use]
pub fn assignment_epoch_to_database_text(epoch: AssignmentEpoch) -> String {
    epoch.get().to_string()
}

/// Parses exact decimal text into a non-zero `u64` assignment epoch.
///
/// # Errors
///
/// Returns a safe error for zero, negative, malformed, or overflowing values.
pub fn assignment_epoch_from_database_text(
    value: &str,
) -> Result<AssignmentEpoch, CellDatabaseError> {
    let value = value
        .parse::<u64>()
        .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::InvalidAssignmentEpoch))?;
    AssignmentEpoch::new(value)
        .map_err(|_| CellDatabaseError::new(CellDatabaseErrorKind::InvalidAssignmentEpoch))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tenancy_domain::AssignmentEpoch;

    use super::{
        CellDatabaseErrorKind, IsolationCanaryId, assignment_epoch_from_database_text,
        assignment_epoch_to_database_text, validate_contract,
    };

    #[test]
    fn assignment_epoch_database_text_is_lossless_across_u64() {
        for value in [
            1,
            9_223_372_036_854_775_807,
            9_223_372_036_854_775_808,
            u64::MAX,
        ] {
            let epoch = AssignmentEpoch::new(value);
            if let Ok(epoch) = epoch {
                let text = assignment_epoch_to_database_text(epoch);
                assert_eq!(
                    assignment_epoch_from_database_text(&text)
                        .ok()
                        .map(AssignmentEpoch::get),
                    Some(value)
                );
            } else {
                panic!("non-zero epoch fixture must remain valid");
            }
        }
        for value in ["0", "18446744073709551616", "-1", "not-a-number"] {
            assert_eq!(
                assignment_epoch_from_database_text(value)
                    .err()
                    .map(|error| error.kind()),
                Some(CellDatabaseErrorKind::InvalidAssignmentEpoch)
            );
        }
    }

    #[test]
    fn cell_contract_compatibility_fails_closed() {
        assert_eq!(validate_contract("cell", 1).ok(), Some(1));
        assert_eq!(validate_contract("cell", 2).ok(), Some(2));
        for (name, version) in [("platform", 1), ("cell", 0), ("cell", 3)] {
            assert_eq!(
                validate_contract(name, version)
                    .err()
                    .map(|error| error.kind()),
                Some(CellDatabaseErrorKind::ContractMismatch)
            );
        }
    }

    #[test]
    fn canary_identifiers_require_uuid_v7() {
        assert!(IsolationCanaryId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c0101").is_ok());
        assert!(IsolationCanaryId::from_str("550e8400-e29b-41d4-a716-446655440000").is_err());
    }
}
