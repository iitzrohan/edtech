//! Fixed-namespace `PostgreSQL` mechanics for transactional outboxes and inbox receipts.
//!
//! This provider crate owns reusable SQL mechanics only. It must not own DDL, migrations,
//! application workflows, configuration, secrets, retry loops, publisher/consumer tasks, dynamic
//! schema names, or transport-provider concepts.

use std::{fmt, str::FromStr, time::Duration};

use message_domain::{
    ContractDescriptor, CorrelationId, EmittedAt, EncodedMessage, MessageAuthority, MessageId,
    MessageKind, MessageMetadata, MessageName, MessageSchemaVersion, MessageScope, MessageTarget,
};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use time::OffsetDateTime;
use uuid::Uuid;

/// A fixed compile-time message-store namespace.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MessageStoreNamespace {
    /// The Platform authority store in `platform_messaging`.
    Platform,
    /// The Cell authority store in `cell_messaging`.
    Cell,
}

impl MessageStoreNamespace {
    /// Returns the fixed schema name for diagnostics and inspection.
    #[must_use]
    pub const fn schema_name(self) -> &'static str {
        match self {
            Self::Platform => "platform_messaging",
            Self::Cell => "cell_messaging",
        }
    }
}

/// A stable logical inbox handler name.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsumerName(String);

/// A safe message-store category name such as a transport failure category.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FailureCategory(String);

/// A bounded-name validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidSafeName;

impl fmt::Display for InvalidSafeName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("safe message-store name violates its bounded grammar")
    }
}

impl std::error::Error for InvalidSafeName {}

fn validate_safe_name(value: &str) -> Result<(), InvalidSafeName> {
    let bytes = value.as_bytes();
    if !(3..=96).contains(&bytes.len())
        || !value.is_ascii()
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b".-".contains(byte))
        || !bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        || !bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        || bytes.windows(2).any(|window| window == b"--")
        || value.split('.').any(str::is_empty)
    {
        return Err(InvalidSafeName);
    }
    Ok(())
}

impl ConsumerName {
    /// Validates a stable logical handler name.
    ///
    /// # Errors
    ///
    /// Rejects values outside the safe 3-to-96-byte segmented grammar.
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidSafeName> {
        let value = value.into();
        validate_safe_name(&value)?;
        Ok(Self(value))
    }

    /// Returns the stable logical name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ConsumerName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ConsumerName")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for ConsumerName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ConsumerName {
    type Err = InvalidSafeName;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl FailureCategory {
    /// Validates a content-free failure category.
    ///
    /// # Errors
    ///
    /// Rejects values outside the safe 3-to-96-byte segmented grammar.
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidSafeName> {
        let value = value.into();
        validate_safe_name(&value)?;
        Ok(Self(value))
    }

    /// Returns the safe category text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for FailureCategory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("FailureCategory")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for FailureCategory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

macro_rules! provider_uuid_v7 {
    ($(#[$metadata:meta])* $name:ident, $error:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Uuid);

        #[doc = concat!("A supplied `", stringify!($name), "` is not UUID version 7.")]
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub struct $error;

        impl $name {
            #[doc = concat!("Constructs a validated `", stringify!($name), "`.")]
            ///
            /// # Errors
            ///
            /// Rejects UUID versions other than 7.
            pub fn new(value: Uuid) -> Result<Self, $error> {
                if value.get_version_num() == 7 {
                    Ok(Self(value))
                } else {
                    Err($error)
                }
            }

            /// Borrows the validated UUID.
            #[must_use]
            pub const fn as_uuid(&self) -> &Uuid {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = $error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map_err(|_| $error).and_then(Self::new)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.debug_tuple(stringify!($name)).field(&self.0).finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl fmt::Display for $error {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), " must use UUID version 7"))
            }
        }

        impl std::error::Error for $error {}
    };
}

provider_uuid_v7!(
    /// Identifies one publisher process instance for diagnostic leasing.
    PublisherInstanceId,
    InvalidPublisherInstanceId
);
provider_uuid_v7!(
    /// Fences one outbox claim against stale workers.
    OutboxLeaseId,
    InvalidOutboxLeaseId
);

/// A claim batch size bounded to 1 through 500.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ClaimBatchSize(u16);

/// A claim-batch bound violation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidClaimBatchSize;

impl ClaimBatchSize {
    /// Constructs a bounded claim batch size.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above 500.
    pub fn new(value: u16) -> Result<Self, InvalidClaimBatchSize> {
        if (1..=500).contains(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidClaimBatchSize)
        }
    }

    /// Returns the bounded size.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// A lease duration bounded to whole seconds from 1 through 300.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LeaseDuration(u16);

/// A lease-duration bound violation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidLeaseDuration;

impl LeaseDuration {
    /// Constructs a bounded whole-second lease duration.
    ///
    /// # Errors
    ///
    /// Rejects sub-second values and values outside 1 second through 5 minutes.
    pub fn new(value: Duration) -> Result<Self, InvalidLeaseDuration> {
        let seconds = value.as_secs();
        if value.subsec_nanos() != 0 || !(1..=300).contains(&seconds) {
            return Err(InvalidLeaseDuration);
        }
        u16::try_from(seconds)
            .map(Self)
            .map_err(|_| InvalidLeaseDuration)
    }

    /// Returns the bounded whole seconds.
    #[must_use]
    pub const fn seconds(self) -> u16 {
        self.0
    }
}

/// A retry delay bounded to whole seconds from zero through 24 hours.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RetryDelay(u32);

/// A retry-delay bound violation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidRetryDelay;

impl RetryDelay {
    /// Constructs a bounded whole-second retry delay.
    ///
    /// # Errors
    ///
    /// Rejects fractional seconds and values above 24 hours.
    pub fn new(value: Duration) -> Result<Self, InvalidRetryDelay> {
        let seconds = value.as_secs();
        if value.subsec_nanos() != 0 || seconds > 86_400 {
            return Err(InvalidRetryDelay);
        }
        u32::try_from(seconds)
            .map(Self)
            .map_err(|_| InvalidRetryDelay)
    }

    /// Returns the bounded whole seconds.
    #[must_use]
    pub const fn seconds(self) -> u32 {
        self.0
    }
}

macro_rules! display_bound_error {
    ($name:ident, $message:literal) => {
        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str($message)
            }
        }
        impl std::error::Error for $name {}
    };
}

display_bound_error!(
    InvalidClaimBatchSize,
    "claim batch size must be 1 through 500"
);
display_bound_error!(
    InvalidLeaseDuration,
    "lease duration must be 1 through 300 whole seconds"
);
display_bound_error!(
    InvalidRetryDelay,
    "retry delay must be 0 through 86400 whole seconds"
);

/// Result of an idempotent outbox enqueue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueOutcome {
    /// The immutable message and delivery state were inserted.
    Inserted,
    /// Identical immutable state already existed.
    AlreadyPresent,
}

/// Result of an inbox receipt attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InboxReceiptOutcome {
    /// This logical consumer inserted its first receipt.
    Inserted,
    /// The exact immutable receipt already existed.
    Duplicate,
    /// The identity existed with different immutable bytes or metadata.
    Conflict,
}

/// Result of fencing an outbox publication marker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishMarkOutcome {
    /// The matching active lease marked transport acceptance.
    Published,
    /// The message was already marked published.
    AlreadyPublished,
    /// The supplied lease was wrong, stale, expired, or replaced.
    LeaseLost,
}

/// Result of fencing an outbox reschedule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RescheduleOutcome {
    /// The matching active lease set a new database-time availability.
    Rescheduled,
    /// The message was already marked published.
    AlreadyPublished,
    /// The supplied lease was wrong, stale, expired, or replaced.
    LeaseLost,
}

/// One immutable message returned under an active lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedMessage {
    message: EncodedMessage,
    attempt_count: u64,
    lease_id: OutboxLeaseId,
    leased_until: OffsetDateTime,
}

impl ClaimedMessage {
    /// Returns the exact stored message.
    #[must_use]
    pub const fn message(&self) -> &EncodedMessage {
        &self.message
    }
    /// Returns the incremented attempt count.
    #[must_use]
    pub const fn attempt_count(&self) -> u64 {
        self.attempt_count
    }
    /// Returns the active stale-worker fence.
    #[must_use]
    pub const fn lease_id(&self) -> OutboxLeaseId {
        self.lease_id
    }
    /// Returns the database-time lease expiry.
    #[must_use]
    pub const fn leased_until(&self) -> OffsetDateTime {
        self.leased_until
    }
}

/// Stable content-free provider error categories.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MessageStoreErrorKind {
    /// A `PostgreSQL` operation failed.
    ProviderFailure,
    /// One message identity names different immutable content.
    MessageIdentityConflict,
    /// Immutable message and delivery state are inconsistent or malformed.
    StoreCorruption,
    /// A stored domain value failed closed conversion.
    InvalidStoredValue,
}

/// A sanitized `PostgreSQL` message-store failure.
pub struct MessageStoreError {
    kind: MessageStoreErrorKind,
}

impl MessageStoreError {
    const fn new(kind: MessageStoreErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable safe category.
    #[must_use]
    pub const fn kind(&self) -> MessageStoreErrorKind {
        self.kind
    }
}

impl fmt::Display for MessageStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let category = match self.kind {
            MessageStoreErrorKind::ProviderFailure => "provider_failure",
            MessageStoreErrorKind::MessageIdentityConflict => "message_identity_conflict",
            MessageStoreErrorKind::StoreCorruption => "store_corruption",
            MessageStoreErrorKind::InvalidStoredValue => "invalid_stored_value",
        };
        write!(formatter, "postgres message store error: {category}")
    }
}

impl fmt::Debug for MessageStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MessageStoreError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl std::error::Error for MessageStoreError {}

struct DatabaseMetadata {
    message_id: Uuid,
    message_kind: &'static str,
    message_name: String,
    message_schema_version: i32,
    emitted_at: OffsetDateTime,
    source_kind: &'static str,
    source_cell_id: Option<String>,
    scope_kind: &'static str,
    scope_cell_id: Option<String>,
    tenant_id: Option<Uuid>,
    assignment_epoch: Option<String>,
    target_kind: Option<&'static str>,
    target_cell_id: Option<String>,
    correlation_id: Uuid,
    causation_id: Option<Uuid>,
}

impl DatabaseMetadata {
    fn from_message(message: &EncodedMessage) -> Self {
        let metadata = message.metadata();
        let (source_kind, source_cell_id) = match metadata.source() {
            MessageAuthority::Platform => ("platform", None),
            MessageAuthority::Cell(cell_id) => ("cell", Some(cell_id.as_str().to_owned())),
        };
        let (scope_kind, scope_cell_id, tenant_id, assignment_epoch) = match metadata.scope() {
            MessageScope::Platform => ("platform", None, None, None),
            MessageScope::Cell(cell_id) => ("cell", Some(cell_id.as_str().to_owned()), None, None),
            MessageScope::Tenant {
                tenant_id,
                cell_id,
                assignment_epoch,
            } => (
                "tenant",
                Some(cell_id.as_str().to_owned()),
                Some(*tenant_id.as_uuid()),
                Some(assignment_epoch.to_string()),
            ),
        };
        let (target_kind, target_cell_id) = match metadata.target() {
            None => (None, None),
            Some(MessageTarget::Platform) => (Some("platform"), None),
            Some(MessageTarget::Cell(cell_id)) => (Some("cell"), Some(cell_id.as_str().to_owned())),
        };
        Self {
            message_id: metadata.message_id().into_uuid(),
            message_kind: kind_text(metadata.descriptor().kind()),
            message_name: metadata.descriptor().name().as_str().to_owned(),
            message_schema_version: metadata.descriptor().schema_version().get().cast_signed(),
            emitted_at: metadata.emitted_at().as_offset_date_time(),
            source_kind,
            source_cell_id,
            scope_kind,
            scope_cell_id,
            tenant_id,
            assignment_epoch,
            target_kind,
            target_cell_id,
            correlation_id: metadata.correlation_id().into_uuid(),
            causation_id: metadata.causation_id().map(MessageId::into_uuid),
        }
    }
}

const fn kind_text(kind: MessageKind) -> &'static str {
    match kind {
        MessageKind::Command => "command",
        MessageKind::Event => "event",
    }
}

const PLATFORM_INSERT_MESSAGE: &str = "INSERT INTO platform_messaging.outbox_message (message_id, envelope_version, message_kind, message_name, message_schema_version, emitted_at, source_kind, source_cell_id, scope_kind, scope_cell_id, tenant_id, assignment_epoch, target_kind, target_cell_id, correlation_id, causation_id, content_type, envelope) VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::numeric, $12, $13, $14, $15, $16, $17) ON CONFLICT (message_id) DO NOTHING";
const CELL_INSERT_MESSAGE: &str = "INSERT INTO cell_messaging.outbox_message (message_id, envelope_version, message_kind, message_name, message_schema_version, emitted_at, source_kind, source_cell_id, scope_kind, scope_cell_id, tenant_id, assignment_epoch, target_kind, target_cell_id, correlation_id, causation_id, content_type, envelope) VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::numeric, $12, $13, $14, $15, $16, $17) ON CONFLICT (message_id) DO NOTHING";
const PLATFORM_SELECT_MESSAGE: &str = "SELECT message_id, message_kind, message_name, message_schema_version, emitted_at, source_kind, source_cell_id, scope_kind, scope_cell_id, tenant_id, assignment_epoch::text AS assignment_epoch, target_kind, target_cell_id, correlation_id, causation_id, envelope FROM platform_messaging.outbox_message WHERE message_id = $1";
const CELL_SELECT_MESSAGE: &str = "SELECT message_id, message_kind, message_name, message_schema_version, emitted_at, source_kind, source_cell_id, scope_kind, scope_cell_id, tenant_id, assignment_epoch::text AS assignment_epoch, target_kind, target_cell_id, correlation_id, causation_id, envelope FROM cell_messaging.outbox_message WHERE message_id = $1";
const PLATFORM_INSERT_DELIVERY: &str = "INSERT INTO platform_messaging.outbox_delivery (message_id, available_at) VALUES ($1, pg_catalog.now())";
const CELL_INSERT_DELIVERY: &str = "INSERT INTO cell_messaging.outbox_delivery (message_id, available_at) VALUES ($1, pg_catalog.now())";
const PLATFORM_DELIVERY_EXISTS: &str =
    "SELECT EXISTS (SELECT 1 FROM platform_messaging.outbox_delivery WHERE message_id = $1)";
const CELL_DELIVERY_EXISTS: &str =
    "SELECT EXISTS (SELECT 1 FROM cell_messaging.outbox_delivery WHERE message_id = $1)";

/// Inserts or compares one immutable outbox message inside the caller's local transaction.
///
/// # Errors
///
/// Returns a safe conflict if the identity exists with different immutable state, corruption when
/// delivery state is missing, or a provider category on SQL failure.
pub async fn enqueue(
    transaction: &mut Transaction<'_, Postgres>,
    namespace: MessageStoreNamespace,
    message: &EncodedMessage,
) -> Result<EnqueueOutcome, MessageStoreError> {
    let metadata = DatabaseMetadata::from_message(message);
    let insert_sql = match namespace {
        MessageStoreNamespace::Platform => PLATFORM_INSERT_MESSAGE,
        MessageStoreNamespace::Cell => CELL_INSERT_MESSAGE,
    };
    let result = sqlx::query(insert_sql)
        .bind(metadata.message_id)
        .bind(metadata.message_kind)
        .bind(&metadata.message_name)
        .bind(metadata.message_schema_version)
        .bind(metadata.emitted_at)
        .bind(metadata.source_kind)
        .bind(metadata.source_cell_id.as_deref())
        .bind(metadata.scope_kind)
        .bind(metadata.scope_cell_id.as_deref())
        .bind(metadata.tenant_id)
        .bind(metadata.assignment_epoch.as_deref())
        .bind(metadata.target_kind)
        .bind(metadata.target_cell_id.as_deref())
        .bind(metadata.correlation_id)
        .bind(metadata.causation_id)
        .bind(message.content_type())
        .bind(message.as_bytes())
        .execute(&mut **transaction)
        .await
        .map_err(|_| MessageStoreError::new(MessageStoreErrorKind::ProviderFailure))?;

    if result.rows_affected() == 1 {
        let insert_delivery = match namespace {
            MessageStoreNamespace::Platform => PLATFORM_INSERT_DELIVERY,
            MessageStoreNamespace::Cell => CELL_INSERT_DELIVERY,
        };
        sqlx::query(insert_delivery)
            .bind(metadata.message_id)
            .execute(&mut **transaction)
            .await
            .map_err(|_| MessageStoreError::new(MessageStoreErrorKind::ProviderFailure))?;
        return Ok(EnqueueOutcome::Inserted);
    }

    let select_message = match namespace {
        MessageStoreNamespace::Platform => PLATFORM_SELECT_MESSAGE,
        MessageStoreNamespace::Cell => CELL_SELECT_MESSAGE,
    };
    let existing = sqlx::query(select_message)
        .bind(metadata.message_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|_| MessageStoreError::new(MessageStoreErrorKind::ProviderFailure))?
        .ok_or_else(|| MessageStoreError::new(MessageStoreErrorKind::StoreCorruption))?;
    let existing = message_from_row(&existing)?;
    if &existing != message {
        return Err(MessageStoreError::new(
            MessageStoreErrorKind::MessageIdentityConflict,
        ));
    }
    let delivery_exists = match namespace {
        MessageStoreNamespace::Platform => PLATFORM_DELIVERY_EXISTS,
        MessageStoreNamespace::Cell => CELL_DELIVERY_EXISTS,
    };
    let delivery_exists = sqlx::query_scalar::<_, bool>(delivery_exists)
        .bind(metadata.message_id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|_| MessageStoreError::new(MessageStoreErrorKind::ProviderFailure))?;
    if !delivery_exists {
        return Err(MessageStoreError::new(
            MessageStoreErrorKind::StoreCorruption,
        ));
    }
    Ok(EnqueueOutcome::AlreadyPresent)
}

const PLATFORM_CLAIM: &str = "WITH candidates AS (SELECT delivery.message_id FROM platform_messaging.outbox_delivery AS delivery JOIN platform_messaging.outbox_message AS message USING (message_id) WHERE delivery.published_at IS NULL AND delivery.available_at <= pg_catalog.now() AND (delivery.leased_until IS NULL OR delivery.leased_until <= pg_catalog.now()) ORDER BY delivery.available_at, message.created_at, delivery.message_id FOR UPDATE OF delivery SKIP LOCKED LIMIT $1), claimed AS (UPDATE platform_messaging.outbox_delivery AS delivery SET publisher_instance_id = $2, lease_id = $3, leased_until = pg_catalog.now() + pg_catalog.make_interval(secs => $4), last_attempt_at = pg_catalog.now(), attempt_count = delivery.attempt_count + 1 FROM candidates WHERE delivery.message_id = candidates.message_id RETURNING delivery.message_id, delivery.attempt_count, delivery.lease_id, delivery.leased_until) SELECT message.message_id, message.message_kind, message.message_name, message.message_schema_version, message.emitted_at, message.source_kind, message.source_cell_id, message.scope_kind, message.scope_cell_id, message.tenant_id, message.assignment_epoch::text AS assignment_epoch, message.target_kind, message.target_cell_id, message.correlation_id, message.causation_id, message.envelope, claimed.attempt_count, claimed.lease_id, claimed.leased_until FROM claimed JOIN platform_messaging.outbox_message AS message USING (message_id)";
const CELL_CLAIM: &str = "WITH candidates AS (SELECT delivery.message_id FROM cell_messaging.outbox_delivery AS delivery JOIN cell_messaging.outbox_message AS message USING (message_id) WHERE delivery.published_at IS NULL AND delivery.available_at <= pg_catalog.now() AND (delivery.leased_until IS NULL OR delivery.leased_until <= pg_catalog.now()) ORDER BY delivery.available_at, message.created_at, delivery.message_id FOR UPDATE OF delivery SKIP LOCKED LIMIT $1), claimed AS (UPDATE cell_messaging.outbox_delivery AS delivery SET publisher_instance_id = $2, lease_id = $3, leased_until = pg_catalog.now() + pg_catalog.make_interval(secs => $4), last_attempt_at = pg_catalog.now(), attempt_count = delivery.attempt_count + 1 FROM candidates WHERE delivery.message_id = candidates.message_id RETURNING delivery.message_id, delivery.attempt_count, delivery.lease_id, delivery.leased_until) SELECT message.message_id, message.message_kind, message.message_name, message.message_schema_version, message.emitted_at, message.source_kind, message.source_cell_id, message.scope_kind, message.scope_cell_id, message.tenant_id, message.assignment_epoch::text AS assignment_epoch, message.target_kind, message.target_cell_id, message.correlation_id, message.causation_id, message.envelope, claimed.attempt_count, claimed.lease_id, claimed.leased_until FROM claimed JOIN cell_messaging.outbox_message AS message USING (message_id)";

/// Claims an eligible batch using database time, row locks, `SKIP LOCKED`, and a caller-supplied
/// lease fence.
///
/// # Errors
///
/// Returns a safe provider or stored-value failure.
pub async fn claim_batch(
    transaction: &mut Transaction<'_, Postgres>,
    namespace: MessageStoreNamespace,
    batch_size: ClaimBatchSize,
    publisher: PublisherInstanceId,
    lease_id: OutboxLeaseId,
    lease_duration: LeaseDuration,
) -> Result<Vec<ClaimedMessage>, MessageStoreError> {
    let sql = match namespace {
        MessageStoreNamespace::Platform => PLATFORM_CLAIM,
        MessageStoreNamespace::Cell => CELL_CLAIM,
    };
    let rows = sqlx::query(sql)
        .bind(i32::from(batch_size.get()))
        .bind(*publisher.as_uuid())
        .bind(*lease_id.as_uuid())
        .bind(f64::from(lease_duration.seconds()))
        .fetch_all(&mut **transaction)
        .await
        .map_err(|_| MessageStoreError::new(MessageStoreErrorKind::ProviderFailure))?;
    rows.iter().map(claimed_from_row).collect()
}

const PLATFORM_MARK_PUBLISHED: &str = "UPDATE platform_messaging.outbox_delivery SET published_at = pg_catalog.now(), publisher_instance_id = NULL, lease_id = NULL, leased_until = NULL WHERE message_id = $1 AND published_at IS NULL AND lease_id = $2 AND leased_until > pg_catalog.now()";
const CELL_MARK_PUBLISHED: &str = "UPDATE cell_messaging.outbox_delivery SET published_at = pg_catalog.now(), publisher_instance_id = NULL, lease_id = NULL, leased_until = NULL WHERE message_id = $1 AND published_at IS NULL AND lease_id = $2 AND leased_until > pg_catalog.now()";
const PLATFORM_PUBLISHED_STATE: &str =
    "SELECT published_at IS NOT NULL FROM platform_messaging.outbox_delivery WHERE message_id = $1";
const CELL_PUBLISHED_STATE: &str =
    "SELECT published_at IS NOT NULL FROM cell_messaging.outbox_delivery WHERE message_id = $1";

/// Marks only a matching active lease as accepted by a future transport.
///
/// # Errors
///
/// Returns a safe provider or corruption failure; stale leases are typed outcomes.
pub async fn mark_published(
    transaction: &mut Transaction<'_, Postgres>,
    namespace: MessageStoreNamespace,
    message_id: MessageId,
    lease_id: OutboxLeaseId,
) -> Result<PublishMarkOutcome, MessageStoreError> {
    let sql = match namespace {
        MessageStoreNamespace::Platform => PLATFORM_MARK_PUBLISHED,
        MessageStoreNamespace::Cell => CELL_MARK_PUBLISHED,
    };
    let updated = sqlx::query(sql)
        .bind(message_id.into_uuid())
        .bind(*lease_id.as_uuid())
        .execute(&mut **transaction)
        .await
        .map_err(|_| MessageStoreError::new(MessageStoreErrorKind::ProviderFailure))?;
    if updated.rows_affected() == 1 {
        return Ok(PublishMarkOutcome::Published);
    }
    published_or_lost(transaction, namespace, message_id)
        .await
        .map(|published| {
            if published {
                PublishMarkOutcome::AlreadyPublished
            } else {
                PublishMarkOutcome::LeaseLost
            }
        })
}

async fn published_or_lost(
    transaction: &mut Transaction<'_, Postgres>,
    namespace: MessageStoreNamespace,
    message_id: MessageId,
) -> Result<bool, MessageStoreError> {
    let sql = match namespace {
        MessageStoreNamespace::Platform => PLATFORM_PUBLISHED_STATE,
        MessageStoreNamespace::Cell => CELL_PUBLISHED_STATE,
    };
    sqlx::query_scalar::<_, bool>(sql)
        .bind(message_id.into_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|_| MessageStoreError::new(MessageStoreErrorKind::ProviderFailure))?
        .ok_or_else(|| MessageStoreError::new(MessageStoreErrorKind::StoreCorruption))
}

const PLATFORM_RESCHEDULE: &str = "UPDATE platform_messaging.outbox_delivery SET available_at = pg_catalog.now() + pg_catalog.make_interval(secs => $3), publisher_instance_id = NULL, lease_id = NULL, leased_until = NULL, last_failure_category = $4 WHERE message_id = $1 AND published_at IS NULL AND lease_id = $2 AND leased_until > pg_catalog.now()";
const CELL_RESCHEDULE: &str = "UPDATE cell_messaging.outbox_delivery SET available_at = pg_catalog.now() + pg_catalog.make_interval(secs => $3), publisher_instance_id = NULL, lease_id = NULL, leased_until = NULL, last_failure_category = $4 WHERE message_id = $1 AND published_at IS NULL AND lease_id = $2 AND leased_until > pg_catalog.now()";

/// Reschedules only a matching active lease using database time and a safe optional category.
///
/// # Errors
///
/// Returns a safe provider or corruption failure; stale leases are typed outcomes.
pub async fn reschedule(
    transaction: &mut Transaction<'_, Postgres>,
    namespace: MessageStoreNamespace,
    message_id: MessageId,
    lease_id: OutboxLeaseId,
    retry_delay: RetryDelay,
    failure_category: Option<&FailureCategory>,
) -> Result<RescheduleOutcome, MessageStoreError> {
    let sql = match namespace {
        MessageStoreNamespace::Platform => PLATFORM_RESCHEDULE,
        MessageStoreNamespace::Cell => CELL_RESCHEDULE,
    };
    let updated = sqlx::query(sql)
        .bind(message_id.into_uuid())
        .bind(*lease_id.as_uuid())
        .bind(f64::from(retry_delay.seconds()))
        .bind(failure_category.map(FailureCategory::as_str))
        .execute(&mut **transaction)
        .await
        .map_err(|_| MessageStoreError::new(MessageStoreErrorKind::ProviderFailure))?;
    if updated.rows_affected() == 1 {
        return Ok(RescheduleOutcome::Rescheduled);
    }
    published_or_lost(transaction, namespace, message_id)
        .await
        .map(|published| {
            if published {
                RescheduleOutcome::AlreadyPublished
            } else {
                RescheduleOutcome::LeaseLost
            }
        })
}

const PLATFORM_INSERT_RECEIPT: &str = "INSERT INTO platform_messaging.inbox_receipt (consumer_name, message_id, message_name, message_schema_version, message_kind, envelope) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (consumer_name, message_id) DO NOTHING";
const CELL_INSERT_RECEIPT: &str = "INSERT INTO cell_messaging.inbox_receipt (consumer_name, message_id, message_name, message_schema_version, message_kind, envelope) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (consumer_name, message_id) DO NOTHING";
const PLATFORM_SELECT_RECEIPT: &str = "SELECT message_name, message_schema_version, message_kind, envelope FROM platform_messaging.inbox_receipt WHERE consumer_name = $1 AND message_id = $2";
const CELL_SELECT_RECEIPT: &str = "SELECT message_name, message_schema_version, message_kind, envelope FROM cell_messaging.inbox_receipt WHERE consumer_name = $1 AND message_id = $2";

/// Inserts or compares one immutable inbox receipt inside the caller's local transaction.
///
/// # Errors
///
/// Returns a safe provider or corruption category. A differing identity returns the typed
/// [`InboxReceiptOutcome::Conflict`] outcome and never overwrites the receipt.
pub async fn record_inbox(
    transaction: &mut Transaction<'_, Postgres>,
    namespace: MessageStoreNamespace,
    consumer: &ConsumerName,
    message: &EncodedMessage,
) -> Result<InboxReceiptOutcome, MessageStoreError> {
    let metadata = message.metadata();
    let sql = match namespace {
        MessageStoreNamespace::Platform => PLATFORM_INSERT_RECEIPT,
        MessageStoreNamespace::Cell => CELL_INSERT_RECEIPT,
    };
    let result = sqlx::query(sql)
        .bind(consumer.as_str())
        .bind(metadata.message_id().into_uuid())
        .bind(metadata.descriptor().name().as_str())
        .bind(metadata.descriptor().schema_version().get().cast_signed())
        .bind(kind_text(metadata.descriptor().kind()))
        .bind(message.as_bytes())
        .execute(&mut **transaction)
        .await
        .map_err(|_| MessageStoreError::new(MessageStoreErrorKind::ProviderFailure))?;
    if result.rows_affected() == 1 {
        return Ok(InboxReceiptOutcome::Inserted);
    }
    let select = match namespace {
        MessageStoreNamespace::Platform => PLATFORM_SELECT_RECEIPT,
        MessageStoreNamespace::Cell => CELL_SELECT_RECEIPT,
    };
    let row = sqlx::query(select)
        .bind(consumer.as_str())
        .bind(metadata.message_id().into_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|_| MessageStoreError::new(MessageStoreErrorKind::ProviderFailure))?
        .ok_or_else(|| MessageStoreError::new(MessageStoreErrorKind::StoreCorruption))?;
    let identical = row.get::<String, _>("message_name") == metadata.descriptor().name().as_str()
        && row.get::<i32, _>("message_schema_version")
            == metadata.descriptor().schema_version().get().cast_signed()
        && row.get::<String, _>("message_kind") == kind_text(metadata.descriptor().kind())
        && row.get::<Vec<u8>, _>("envelope") == message.as_bytes();
    Ok(if identical {
        InboxReceiptOutcome::Duplicate
    } else {
        InboxReceiptOutcome::Conflict
    })
}

fn claimed_from_row(row: &PgRow) -> Result<ClaimedMessage, MessageStoreError> {
    let attempt_count = u64::try_from(row.get::<i64, _>("attempt_count"))
        .map_err(|_| MessageStoreError::new(MessageStoreErrorKind::InvalidStoredValue))?;
    let lease_id = OutboxLeaseId::new(row.get::<Uuid, _>("lease_id"))
        .map_err(|_| MessageStoreError::new(MessageStoreErrorKind::InvalidStoredValue))?;
    Ok(ClaimedMessage {
        message: message_from_row(row)?,
        attempt_count,
        lease_id,
        leased_until: row.get::<OffsetDateTime, _>("leased_until"),
    })
}

#[allow(clippy::too_many_lines)]
fn message_from_row(row: &PgRow) -> Result<EncodedMessage, MessageStoreError> {
    let invalid = || MessageStoreError::new(MessageStoreErrorKind::InvalidStoredValue);
    let message_id = MessageId::new(row.get::<Uuid, _>("message_id")).map_err(|_| invalid())?;
    let kind = match row.get::<String, _>("message_kind").as_str() {
        "command" => MessageKind::Command,
        "event" => MessageKind::Event,
        _ => return Err(invalid()),
    };
    let name =
        MessageName::from_str(&row.get::<String, _>("message_name")).map_err(|_| invalid())?;
    let stored_version = row.get::<i32, _>("message_schema_version");
    let version = u32::try_from(stored_version)
        .ok()
        .and_then(|value| MessageSchemaVersion::new(value).ok())
        .ok_or_else(invalid)?;
    let emitted_at =
        EmittedAt::new(row.get::<OffsetDateTime, _>("emitted_at")).map_err(|_| invalid())?;
    let source_kind = row.get::<String, _>("source_kind");
    let source_cell = row.get::<Option<String>, _>("source_cell_id");
    let source = match (source_kind.as_str(), source_cell) {
        ("platform", None) => MessageAuthority::Platform,
        ("cell", Some(value)) => MessageAuthority::cell(value).map_err(|_| invalid())?,
        _ => return Err(invalid()),
    };
    let scope_kind = row.get::<String, _>("scope_kind");
    let scope_cell = row.get::<Option<String>, _>("scope_cell_id");
    let tenant = row.get::<Option<Uuid>, _>("tenant_id");
    let epoch = row.get::<Option<String>, _>("assignment_epoch");
    let scope = match (scope_kind.as_str(), scope_cell, tenant, epoch) {
        ("platform", None, None, None) => MessageScope::Platform,
        ("cell", Some(cell), None, None) => MessageScope::cell(cell).map_err(|_| invalid())?,
        ("tenant", Some(cell), Some(tenant), Some(epoch)) => {
            MessageScope::tenant(tenant, cell, assignment_epoch_from_database_text(&epoch)?)
                .map_err(|_| invalid())?
        }
        _ => return Err(invalid()),
    };
    let target_kind = row.get::<Option<String>, _>("target_kind");
    let target_cell = row.get::<Option<String>, _>("target_cell_id");
    let target = match (target_kind.as_deref(), target_cell) {
        (None, None) => None,
        (Some("platform"), None) => Some(MessageTarget::Platform),
        (Some("cell"), Some(cell)) => Some(MessageTarget::cell(cell).map_err(|_| invalid())?),
        _ => return Err(invalid()),
    };
    let correlation =
        CorrelationId::new(row.get::<Uuid, _>("correlation_id")).map_err(|_| invalid())?;
    let causation = row
        .get::<Option<Uuid>, _>("causation_id")
        .map(MessageId::new)
        .transpose()
        .map_err(|_| invalid())?;
    let metadata = MessageMetadata::new(
        message_id,
        ContractDescriptor::new(kind, name, version),
        emitted_at,
        source,
        scope,
        target,
        correlation,
        causation,
    )
    .map_err(|_| invalid())?;
    EncodedMessage::new(metadata, row.get::<Vec<u8>, _>("envelope")).map_err(|_| invalid())
}

/// Parses an exact decimal `PostgreSQL` assignment epoch without signed conversion.
///
/// # Errors
///
/// Rejects zero, negative, malformed, or overflowing values.
pub fn assignment_epoch_from_database_text(value: &str) -> Result<u64, MessageStoreError> {
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value != 0)
        .ok_or_else(|| MessageStoreError::new(MessageStoreErrorKind::InvalidStoredValue))
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use super::*;

    #[test]
    fn namespace_mapping_is_fixed() {
        assert_eq!(
            MessageStoreNamespace::Platform.schema_name(),
            "platform_messaging"
        );
        assert_eq!(MessageStoreNamespace::Cell.schema_name(), "cell_messaging");
    }

    #[test]
    fn consumer_and_failure_names_use_safe_bounded_grammar() {
        for value in [
            "platform.provisioning-result-handler",
            "cell.provision-command-handler",
            "qualification.platform-handler",
            "transport.timeout",
        ] {
            assert!(ConsumerName::from_str(value).is_ok(), "{value}");
            assert!(FailureCategory::new(value).is_ok(), "{value}");
        }
        for value in [
            "ab",
            "UPPER.case",
            "bad_name",
            "bad/name",
            ".bad",
            "bad.",
            "bad..name",
            "bad--name",
        ] {
            assert!(ConsumerName::from_str(value).is_err(), "{value}");
            assert!(FailureCategory::new(value).is_err(), "{value}");
        }
    }

    #[test]
    fn provider_identifiers_require_uuid_v7() {
        assert!(PublisherInstanceId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c3001").is_ok());
        assert!(OutboxLeaseId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c3002").is_ok());
        assert!(PublisherInstanceId::from_str("550e8400-e29b-41d4-a716-446655440000").is_err());
    }

    #[test]
    fn operational_inputs_are_strictly_bounded() {
        assert!(ClaimBatchSize::new(1).is_ok());
        assert!(ClaimBatchSize::new(500).is_ok());
        assert!(ClaimBatchSize::new(0).is_err());
        assert!(ClaimBatchSize::new(501).is_err());
        assert!(LeaseDuration::new(Duration::from_secs(1)).is_ok());
        assert!(LeaseDuration::new(Duration::from_mins(5)).is_ok());
        assert!(LeaseDuration::new(Duration::ZERO).is_err());
        assert!(LeaseDuration::new(Duration::from_secs(301)).is_err());
        assert!(LeaseDuration::new(Duration::from_millis(1_500)).is_err());
        assert!(RetryDelay::new(Duration::ZERO).is_ok());
        assert!(RetryDelay::new(Duration::from_hours(24)).is_ok());
        assert!(RetryDelay::new(Duration::from_secs(86_401)).is_err());
    }

    #[test]
    fn assignment_epoch_text_conversion_preserves_the_complete_u64_range() {
        for value in [1, i64::MAX as u64, (i64::MAX as u64) + 1, u64::MAX] {
            assert_eq!(
                assignment_epoch_from_database_text(&value.to_string()).ok(),
                Some(value)
            );
        }
        for value in ["0", "-1", "18446744073709551616", "invalid"] {
            assert!(assignment_epoch_from_database_text(value).is_err());
        }
    }

    #[test]
    fn errors_are_safe_categories() {
        let error = MessageStoreError::new(MessageStoreErrorKind::MessageIdentityConflict);
        assert_eq!(
            error.to_string(),
            "postgres message store error: message_identity_conflict"
        );
        assert!(!format!("{error:?}").contains("payload"));
    }

    #[test]
    fn typed_outcomes_keep_success_duplicate_and_fence_states_distinct() {
        assert_ne!(EnqueueOutcome::Inserted, EnqueueOutcome::AlreadyPresent);
        assert_ne!(
            InboxReceiptOutcome::Inserted,
            InboxReceiptOutcome::Duplicate
        );
        assert_ne!(
            InboxReceiptOutcome::Duplicate,
            InboxReceiptOutcome::Conflict
        );
        assert_ne!(
            PublishMarkOutcome::Published,
            PublishMarkOutcome::AlreadyPublished
        );
        assert_ne!(
            PublishMarkOutcome::AlreadyPublished,
            PublishMarkOutcome::LeaseLost
        );
        assert_ne!(
            RescheduleOutcome::Rescheduled,
            RescheduleOutcome::AlreadyPublished
        );
        assert_ne!(
            RescheduleOutcome::AlreadyPublished,
            RescheduleOutcome::LeaseLost
        );
    }
}
