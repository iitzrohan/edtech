//! Runtime `PostgreSQL` adapter for the Platform database authority.
//!
//! This crate verifies the Platform bootstrap marker, qualified server, runtime role, and schema
//! contract behind an opaque handle. It must not contain DDL, migrations, Cell dependencies,
//! application workflows, configuration loading, or public `SQLx` types.

use message_domain::{EncodedMessage, MessageAuthority, MessageKind, MessageTarget};
use postgres_message_store::{
    ClaimBatchSize, ClaimedMessage, ConsumerName, EnqueueOutcome, FailureCategory,
    InboxReceiptOutcome, LeaseDuration, MessageStoreError, MessageStoreErrorKind,
    MessageStoreNamespace, OutboxLeaseId, PublishMarkOutcome, PublisherInstanceId,
    RescheduleOutcome, RetryDelay,
};
use postgres_runtime::{
    DatabaseCredential, PostgresConnectionConfig, PostgresPool, ProviderError, ProviderErrorKind,
    connect, verify_runtime_role, verify_server_version,
};
use sqlx::Row;

const MIGRATION_ROLE: &str = "edtech_platform_migrator";
const MIN_SUPPORTED_CONTRACT_VERSION: u32 = 1;
const MAX_SUPPORTED_CONTRACT_VERSION: u32 = 2;
const PLATFORM_SCHEMAS: &[&str] = &[
    "edtech_bootstrap",
    "edtech_migrations",
    "platform_control",
    "platform_messaging",
];

/// The separately scoped Platform runtime role expected on a connection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PlatformRuntimeRole {
    /// Platform API runtime role.
    Api,
    /// Platform worker runtime role.
    Worker,
}

impl PlatformRuntimeRole {
    const fn database_role(self) -> &'static str {
        match self {
            Self::Api => "edtech_platform_api",
            Self::Worker => "edtech_platform_worker",
        }
    }
}

/// Safe information proven when a Platform database becomes ready.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlatformDatabaseCheck {
    server_version: u32,
    contract_version: u32,
}

impl PlatformDatabaseCheck {
    /// Returns `server_version_num`.
    #[must_use]
    pub const fn server_version(self) -> u32 {
        self.server_version
    }

    /// Returns the supported Platform schema-contract version.
    #[must_use]
    pub const fn contract_version(self) -> u32 {
        self.contract_version
    }

    /// Reports whether schema-contract version 2 message-store operations are available.
    #[must_use]
    pub const fn message_store_available(self) -> bool {
        self.contract_version >= 2
    }
}

/// Stable safe Platform database error categories.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PlatformDatabaseErrorKind {
    /// Credential, connection, TLS, timeout, server, or generic database failure.
    Provider,
    /// The bootstrap marker is not the Platform authority.
    AuthorityMismatch,
    /// The runtime role violates its least-privilege profile.
    PrivilegeMismatch,
    /// The Platform schema contract is absent or incompatible.
    ContractMismatch,
    /// Message-store operations require schema-contract version 2.
    MessageStoreCapabilityUnavailable,
    /// The connected runtime role cannot perform the operation.
    RoleCapabilityMismatch,
    /// An outbound message is not sourced by Platform.
    InvalidOutboundAuthority,
    /// An inbound command does not target Platform.
    InvalidInboundTarget,
    /// Reusable message-store mechanics failed.
    MessageStoreFailure,
    /// One message identity names different immutable content.
    MessageIdentityConflict,
    /// Immutable stored message state is corrupt or cannot be represented.
    StoreCorruption,
    /// An inbox identity conflicts with immutable stored content.
    InboxConflict,
}

impl PlatformDatabaseErrorKind {
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
            Self::MessageIdentityConflict => "message_identity_conflict",
            Self::StoreCorruption => "store_corruption",
            Self::InboxConflict => "inbox_conflict",
        }
    }
}

/// A sanitized Platform runtime database failure.
pub struct PlatformDatabaseError {
    kind: PlatformDatabaseErrorKind,
}

impl PlatformDatabaseError {
    const fn new(kind: PlatformDatabaseErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable safe category.
    #[must_use]
    pub const fn kind(&self) -> PlatformDatabaseErrorKind {
        self.kind
    }
}

impl std::fmt::Display for PlatformDatabaseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "platform postgres error: {}", self.kind.as_str())
    }
}

impl std::fmt::Debug for PlatformDatabaseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlatformDatabaseError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl std::error::Error for PlatformDatabaseError {}

impl From<ProviderError> for PlatformDatabaseError {
    fn from(error: ProviderError) -> Self {
        let kind = match error.kind() {
            ProviderErrorKind::AuthorityMismatch => PlatformDatabaseErrorKind::AuthorityMismatch,
            ProviderErrorKind::PrivilegeMismatch => PlatformDatabaseErrorKind::PrivilegeMismatch,
            ProviderErrorKind::SchemaContractMismatch => {
                PlatformDatabaseErrorKind::ContractMismatch
            }
            _ => PlatformDatabaseErrorKind::Provider,
        };
        drop(error);
        Self::new(kind)
    }
}

impl From<MessageStoreError> for PlatformDatabaseError {
    fn from(error: MessageStoreError) -> Self {
        let kind = match error.kind() {
            MessageStoreErrorKind::ProviderFailure => {
                PlatformDatabaseErrorKind::MessageStoreFailure
            }
            MessageStoreErrorKind::MessageIdentityConflict => {
                PlatformDatabaseErrorKind::MessageIdentityConflict
            }
            MessageStoreErrorKind::StoreCorruption | MessageStoreErrorKind::InvalidStoredValue => {
                PlatformDatabaseErrorKind::StoreCorruption
            }
        };
        Self::new(kind)
    }
}

/// Result of atomically recording inbound work and optional derived output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlatformInboxOutcome {
    /// The named handler committed its first local receipt and optional output.
    Inserted,
    /// An exact redelivery was suppressed and produced no second output.
    Duplicate,
}

/// Opaque, verified Platform runtime database handle.
pub struct PlatformDatabase {
    pool: PostgresPool,
    check: PlatformDatabaseCheck,
    role: PlatformRuntimeRole,
}

impl std::fmt::Debug for PlatformDatabase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlatformDatabase")
            .field("check", &self.check)
            .finish_non_exhaustive()
    }
}

impl PlatformDatabase {
    /// Connects and fails closed unless authority, server, role, and contract all match.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`ProviderError`] without connection or credential details.
    pub async fn connect(
        credential: &impl DatabaseCredential,
        config: &PostgresConnectionConfig,
        role: PlatformRuntimeRole,
    ) -> Result<Self, PlatformDatabaseError> {
        let pool = connect(credential, config).await?;
        match verify_ready(&pool, role).await {
            Ok(check) => Ok(Self { pool, check, role }),
            Err(error) => {
                pool.close().await;
                Err(error)
            }
        }
    }

    /// Returns the safe readiness facts established at connection time.
    #[must_use]
    pub const fn check(&self) -> PlatformDatabaseCheck {
        self.check
    }

    /// Enqueues one Platform-sourced message in a local transaction.
    ///
    /// # Errors
    ///
    /// Rejects contract 1, non-Platform sources, and store/commit failures.
    pub async fn enqueue_outbound_message(
        &self,
        message: &EncodedMessage,
    ) -> Result<EnqueueOutcome, PlatformDatabaseError> {
        self.require_message_store()?;
        validate_platform_source(message)?;
        let mut transaction = self
            .pool
            .sqlx_pool()
            .begin()
            .await
            .map_err(operation_error)?;
        let outcome = postgres_message_store::enqueue(
            &mut transaction,
            MessageStoreNamespace::Platform,
            message,
        )
        .await
        .map_err(PlatformDatabaseError::from)?;
        transaction.commit().await.map_err(operation_error)?;
        Ok(outcome)
    }

    /// Claims an eligible Platform outbox batch for a worker role.
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
    ) -> Result<Vec<ClaimedMessage>, PlatformDatabaseError> {
        self.require_worker()?;
        self.require_message_store()?;
        let mut transaction = self
            .pool
            .sqlx_pool()
            .begin()
            .await
            .map_err(operation_error)?;
        let claimed = postgres_message_store::claim_batch(
            &mut transaction,
            MessageStoreNamespace::Platform,
            batch_size,
            publisher,
            lease_id,
            lease_duration,
        )
        .await
        .map_err(PlatformDatabaseError::from)?;
        transaction.commit().await.map_err(operation_error)?;
        Ok(claimed)
    }

    /// Marks future transport acceptance under an active Platform outbox lease.
    ///
    /// # Errors
    ///
    /// Rejects API roles, contract 1, and safe store failures.
    pub async fn mark_outbox_published(
        &self,
        message_id: message_domain::MessageId,
        lease_id: OutboxLeaseId,
    ) -> Result<PublishMarkOutcome, PlatformDatabaseError> {
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
            MessageStoreNamespace::Platform,
            message_id,
            lease_id,
        )
        .await
        .map_err(PlatformDatabaseError::from)?;
        transaction.commit().await.map_err(operation_error)?;
        Ok(outcome)
    }

    /// Reschedules a Platform outbox row under an active lease.
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
    ) -> Result<RescheduleOutcome, PlatformDatabaseError> {
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
            MessageStoreNamespace::Platform,
            message_id,
            lease_id,
            retry_delay,
            failure_category,
        )
        .await
        .map_err(PlatformDatabaseError::from)?;
        transaction.commit().await.map_err(operation_error)?;
        Ok(outcome)
    }

    /// Atomically records one worker inbox receipt and optional Platform-sourced output.
    ///
    /// # Errors
    ///
    /// Rejects wrong targets, roles, derived authority, conflicts, and store/commit failures.
    pub async fn record_inbox_and_enqueue(
        &self,
        consumer: &ConsumerName,
        inbound: &EncodedMessage,
        derived: Option<&EncodedMessage>,
    ) -> Result<PlatformInboxOutcome, PlatformDatabaseError> {
        self.require_worker()?;
        self.require_message_store()?;
        validate_platform_inbound(inbound)?;
        if let Some(message) = derived {
            validate_platform_source(message)?;
        }
        let mut transaction = self
            .pool
            .sqlx_pool()
            .begin()
            .await
            .map_err(operation_error)?;
        let receipt = postgres_message_store::record_inbox(
            &mut transaction,
            MessageStoreNamespace::Platform,
            consumer,
            inbound,
        )
        .await
        .map_err(PlatformDatabaseError::from)?;
        let outcome = match receipt {
            InboxReceiptOutcome::Inserted => {
                if let Some(message) = derived {
                    postgres_message_store::enqueue(
                        &mut transaction,
                        MessageStoreNamespace::Platform,
                        message,
                    )
                    .await
                    .map_err(PlatformDatabaseError::from)?;
                }
                PlatformInboxOutcome::Inserted
            }
            InboxReceiptOutcome::Duplicate => PlatformInboxOutcome::Duplicate,
            InboxReceiptOutcome::Conflict => {
                let _rollback_result = transaction.rollback().await;
                return Err(PlatformDatabaseError::new(
                    PlatformDatabaseErrorKind::InboxConflict,
                ));
            }
        };
        transaction.commit().await.map_err(operation_error)?;
        Ok(outcome)
    }

    fn require_message_store(&self) -> Result<(), PlatformDatabaseError> {
        if self.check.message_store_available() {
            Ok(())
        } else {
            Err(PlatformDatabaseError::new(
                PlatformDatabaseErrorKind::MessageStoreCapabilityUnavailable,
            ))
        }
    }

    fn require_worker(&self) -> Result<(), PlatformDatabaseError> {
        if self.role == PlatformRuntimeRole::Worker {
            Ok(())
        } else {
            Err(PlatformDatabaseError::new(
                PlatformDatabaseErrorKind::RoleCapabilityMismatch,
            ))
        }
    }

    /// Closes all Platform runtime connections.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

/// Performs the full one-shot Platform runtime database check and closes its pool.
///
/// # Errors
///
/// Returns a sanitized [`ProviderError`] without connection or credential details.
pub async fn check_database(
    credential: &impl DatabaseCredential,
    config: &PostgresConnectionConfig,
    role: PlatformRuntimeRole,
) -> Result<PlatformDatabaseCheck, PlatformDatabaseError> {
    let database = PlatformDatabase::connect(credential, config, role).await?;
    let check = database.check();
    database.close().await;
    Ok(check)
}

async fn verify_ready(
    pool: &PostgresPool,
    role: PlatformRuntimeRole,
) -> Result<PlatformDatabaseCheck, PlatformDatabaseError> {
    let server_version = verify_server_version(pool)
        .await
        .map_err(PlatformDatabaseError::from)?;
    verify_platform_marker(pool).await?;
    verify_runtime_role(pool, role.database_role(), MIGRATION_ROLE, PLATFORM_SCHEMAS)
        .await
        .map_err(PlatformDatabaseError::from)?;
    let contract_version = verify_contract(pool).await?;
    Ok(PlatformDatabaseCheck {
        server_version,
        contract_version,
    })
}

async fn verify_platform_marker(pool: &PostgresPool) -> Result<(), PlatformDatabaseError> {
    let row = sqlx::query(
        "SELECT authority_kind, cell_id FROM edtech_bootstrap.authority_identity \
         WHERE singleton",
    )
    .fetch_optional(pool.sqlx_pool())
    .await
    .map_err(ProviderError::from_sqlx)
    .map_err(PlatformDatabaseError::from)?;
    let matches = row.is_some_and(|row| {
        row.get::<String, _>("authority_kind") == "platform"
            && row.get::<Option<String>, _>("cell_id").is_none()
    });
    if !matches {
        return Err(PlatformDatabaseError::new(
            PlatformDatabaseErrorKind::AuthorityMismatch,
        ));
    }
    Ok(())
}

async fn verify_contract(pool: &PostgresPool) -> Result<u32, PlatformDatabaseError> {
    let row = sqlx::query(
        "SELECT contract_name, contract_version FROM platform_control.schema_contract \
         WHERE singleton",
    )
    .fetch_optional(pool.sqlx_pool())
    .await
    .map_err(|_| PlatformDatabaseError::new(PlatformDatabaseErrorKind::ContractMismatch))?;
    let Some(row) = row else {
        return Err(PlatformDatabaseError::new(
            PlatformDatabaseErrorKind::ContractMismatch,
        ));
    };
    validate_contract(
        &row.get::<String, _>("contract_name"),
        row.get::<i32, _>("contract_version"),
    )
}

fn validate_contract(name: &str, version: i32) -> Result<u32, PlatformDatabaseError> {
    if name != "platform" {
        return Err(PlatformDatabaseError::new(
            PlatformDatabaseErrorKind::ContractMismatch,
        ));
    }
    let version = u32::try_from(version)
        .map_err(|_| PlatformDatabaseError::new(PlatformDatabaseErrorKind::ContractMismatch))?;
    if !(MIN_SUPPORTED_CONTRACT_VERSION..=MAX_SUPPORTED_CONTRACT_VERSION).contains(&version) {
        return Err(PlatformDatabaseError::new(
            PlatformDatabaseErrorKind::ContractMismatch,
        ));
    }
    Ok(version)
}

fn operation_error(_error: sqlx::Error) -> PlatformDatabaseError {
    PlatformDatabaseError::new(PlatformDatabaseErrorKind::MessageStoreFailure)
}

fn validate_platform_source(message: &EncodedMessage) -> Result<(), PlatformDatabaseError> {
    if matches!(message.metadata().source(), MessageAuthority::Platform) {
        Ok(())
    } else {
        Err(PlatformDatabaseError::new(
            PlatformDatabaseErrorKind::InvalidOutboundAuthority,
        ))
    }
}

fn validate_platform_inbound(message: &EncodedMessage) -> Result<(), PlatformDatabaseError> {
    match message.metadata().descriptor().kind() {
        MessageKind::Command
            if matches!(message.metadata().target(), Some(MessageTarget::Platform)) =>
        {
            Ok(())
        }
        MessageKind::Event if message.metadata().target().is_none() => Ok(()),
        MessageKind::Command | MessageKind::Event => Err(PlatformDatabaseError::new(
            PlatformDatabaseErrorKind::InvalidInboundTarget,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{PlatformDatabaseErrorKind, validate_contract};

    #[test]
    fn platform_contract_compatibility_fails_closed() {
        assert_eq!(validate_contract("platform", 1).ok(), Some(1));
        assert_eq!(validate_contract("platform", 2).ok(), Some(2));
        for (name, version) in [("cell", 1), ("platform", 0), ("platform", 3)] {
            assert_eq!(
                validate_contract(name, version)
                    .err()
                    .map(|error| error.kind()),
                Some(PlatformDatabaseErrorKind::ContractMismatch)
            );
        }
    }
}
