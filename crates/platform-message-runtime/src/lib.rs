//! Platform outbox publication and fixed inbound transport-probe consumer loops.
//!
//! This crate owns only Platform message-runtime orchestration. It has no Cell provider, topology
//! mutation, process configuration, direct SQL, or product-domain dependency.

use std::{fmt, sync::Arc, time::Duration};

use futures_util::{StreamExt, stream};
use message_codec_json::{decode_envelope, decode_typed, encode};
use message_domain::{
    EncodedMessage, MessageAuthority, MessageKind, MessageMetadata, MessageScope, MessageTarget,
};
use nats_jetstream::{
    ConsumerBinding, InboundDelivery, JetStreamRuntime, TransportErrorKind, TransportStream,
    TransportSubject, platform_command_binding, platform_event_binding,
};
use platform_postgres::{
    PlatformDatabase, PlatformDatabaseError, PlatformDatabaseErrorKind, PlatformInboxOutcome,
};
use postgres_message_store::{
    ClaimBatchSize, ClaimedMessage, ConsumerName, FailureCategory, LeaseDuration, OutboxLeaseId,
    PublishMarkOutcome, PublisherInstanceId, RescheduleOutcome, RetryDelay,
};
use process_lifecycle::TaskFailure;
use runtime_identity::{RuntimeIdentitySource, emitted_at_now, next_message_id};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use transport_probe_contracts::{
    TransportCellProbeObservedV1, TransportPlatformProbeObservedV1,
    TransportPlatformProbeRequestedV1, transport_cell_probe_observed_descriptor,
    transport_platform_probe_observed_descriptor, transport_platform_probe_requested_descriptor,
};

/// Stable Platform inbox identity for the Cell-observed event handler.
pub const CELL_PROBE_OBSERVED_HANDLER: &str = "platform.transport-cell-probe-observed-v1";
/// Stable Platform inbox identity for the Platform-probe command handler.
pub const PLATFORM_PROBE_REQUESTED_HANDLER: &str = "platform.transport-platform-probe-requested-v1";

/// Provider-neutral outbox publisher bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublisherSettings {
    poll_interval: Duration,
    claim_batch_size: ClaimBatchSize,
    lease_duration: LeaseDuration,
    concurrency: u16,
    retry_base: Duration,
    retry_max: Duration,
}

impl PublisherSettings {
    /// Constructs validated settings for one Platform publisher task.
    ///
    /// # Errors
    ///
    /// Rejects invalid provider bounds, zero timing, lease/ack safety gaps, or retry inversion.
    pub fn new(
        poll_interval: Duration,
        claim_batch_size: u16,
        lease_duration: Duration,
        concurrency: u16,
        retry_base: Duration,
        retry_max: Duration,
    ) -> Result<Self, PlatformMessageRuntimeError> {
        if poll_interval.is_zero()
            || !(1..=128).contains(&concurrency)
            || retry_base.is_zero()
            || retry_max < retry_base
            || retry_max > Duration::from_mins(5)
        {
            return Err(PlatformMessageRuntimeError::new(
                PlatformMessageRuntimeErrorKind::Configuration,
            ));
        }
        let claim_batch_size = ClaimBatchSize::new(claim_batch_size).map_err(|_| {
            PlatformMessageRuntimeError::new(PlatformMessageRuntimeErrorKind::Configuration)
        })?;
        let lease_duration = LeaseDuration::new(lease_duration).map_err(|_| {
            PlatformMessageRuntimeError::new(PlatformMessageRuntimeErrorKind::Configuration)
        })?;
        Ok(Self {
            poll_interval,
            claim_batch_size,
            lease_duration,
            concurrency,
            retry_base,
            retry_max,
        })
    }
}

/// Provider-neutral durable-consumer bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerSettings {
    fetch_batch_size: u16,
    fetch_expires: Duration,
    handler_timeout: Duration,
    nak_delay: Duration,
    max_in_flight: u16,
}

impl ConsumerSettings {
    /// Constructs bounded consumer settings compatible with the fixed 30-second `AckWait`.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive fetch, concurrency, timeout, or NAK values.
    pub fn new(
        fetch_batch_size: u16,
        fetch_expires: Duration,
        handler_timeout: Duration,
        nak_delay: Duration,
        max_in_flight: u16,
    ) -> Result<Self, PlatformMessageRuntimeError> {
        if !(1..=500).contains(&fetch_batch_size)
            || fetch_expires.is_zero()
            || fetch_expires > Duration::from_secs(5)
            || handler_timeout.is_zero()
            || handler_timeout >= Duration::from_secs(30)
            || nak_delay.is_zero()
            || nak_delay > Duration::from_mins(5)
            || !(1..=1_024).contains(&max_in_flight)
        {
            return Err(PlatformMessageRuntimeError::new(
                PlatformMessageRuntimeErrorKind::Configuration,
            ));
        }
        Ok(Self {
            fetch_batch_size,
            fetch_expires,
            handler_timeout,
            nak_delay,
            max_in_flight,
        })
    }
}

/// Creates the single publisher identity used for one Platform worker process start.
///
/// # Errors
///
/// Returns a safe identity category if `UUIDv7` generation fails.
pub fn create_publisher_instance_id(
    identity: &(impl RuntimeIdentitySource + ?Sized),
) -> Result<PublisherInstanceId, PlatformMessageRuntimeError> {
    PublisherInstanceId::new(
        identity.generate_uuid_v7().map_err(|_| {
            PlatformMessageRuntimeError::new(PlatformMessageRuntimeErrorKind::Identity)
        })?,
    )
    .map_err(|_| PlatformMessageRuntimeError::new(PlatformMessageRuntimeErrorKind::Identity))
}

/// Safe Platform message-runtime failure categories.
#[derive(Clone, Copy, Debug, Eq, Error, Hash, PartialEq)]
pub enum PlatformMessageRuntimeErrorKind {
    /// A provider-neutral runtime setting or static handler identity is invalid.
    #[error("runtime.configuration")]
    Configuration,
    /// Runtime UUID/time generation failed.
    #[error("runtime.identity")]
    Identity,
    /// A canonical envelope or derived encoding failed.
    #[error("message.codec")]
    Codec,
    /// Required static handler registration is invalid.
    #[error("runtime.handler-registry")]
    HandlerRegistry,
    /// A message identity conflicts with immutable database content.
    #[error("message.identity-conflict")]
    MessageIdentityConflict,
    /// Immutable database message state is corrupt.
    #[error("message.store-corruption")]
    StoreCorruption,
    /// A database operation failed after broker acceptance or outside a safely retryable delivery.
    #[error("database.transient")]
    Database,
    /// A fatal transport failure occurred.
    #[error("transport.fatal")]
    Transport,
}

/// Content-free Platform runtime error.
pub struct PlatformMessageRuntimeError {
    kind: PlatformMessageRuntimeErrorKind,
    transport_kind: Option<TransportErrorKind>,
}

impl PlatformMessageRuntimeError {
    const fn new(kind: PlatformMessageRuntimeErrorKind) -> Self {
        Self {
            kind,
            transport_kind: None,
        }
    }

    const fn transport(kind: TransportErrorKind) -> Self {
        Self {
            kind: PlatformMessageRuntimeErrorKind::Transport,
            transport_kind: Some(kind),
        }
    }

    /// Returns the stable safe category.
    #[must_use]
    pub const fn kind(&self) -> PlatformMessageRuntimeErrorKind {
        self.kind
    }

    /// Returns the transport category when transport caused the failure.
    #[must_use]
    pub const fn transport_kind(&self) -> Option<TransportErrorKind> {
        self.transport_kind
    }

    fn task_failure(&self) -> TaskFailure {
        self.transport_kind.map_or_else(
            || TaskFailure::new(self.kind.to_string()),
            |kind| TaskFailure::new(kind.safe_category()),
        )
    }
}

impl fmt::Display for PlatformMessageRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(kind) = self.transport_kind {
            formatter.write_str(kind.safe_category())
        } else {
            self.kind.fmt(formatter)
        }
    }
}

impl fmt::Debug for PlatformMessageRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlatformMessageRuntimeError")
            .field("kind", &self.kind)
            .field("transport_kind", &self.transport_kind)
            .finish()
    }
}

impl std::error::Error for PlatformMessageRuntimeError {}

/// Runs the Platform outbox publisher until cancellation or a fatal invariant failure.
///
/// # Errors
///
/// Returns a task failure when identity generation, storage integrity, or the transport fails
/// permanently.
pub async fn run_outbox_publisher(
    database: Arc<PlatformDatabase>,
    transport: JetStreamRuntime,
    publisher: PublisherInstanceId,
    identity: Arc<dyn RuntimeIdentitySource>,
    settings: PublisherSettings,
    cancellation: CancellationToken,
) -> Result<(), TaskFailure> {
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let lease_id = OutboxLeaseId::new(
            identity
                .generate_uuid_v7()
                .map_err(|_| runtime_failure(PlatformMessageRuntimeErrorKind::Identity))?,
        )
        .map_err(|_| runtime_failure(PlatformMessageRuntimeErrorKind::Identity))?;
        let claimed = match database
            .claim_outbox_batch(
                settings.claim_batch_size,
                publisher,
                lease_id,
                settings.lease_duration,
            )
            .await
        {
            Ok(claimed) => claimed,
            Err(error) if database_error_is_integrity(&error) => {
                return Err(database_runtime_error(&error).task_failure());
            }
            Err(_) => {
                wait_or_cancel(settings.retry_base, &cancellation).await;
                continue;
            }
        };

        if claimed.is_empty() {
            wait_or_cancel(settings.poll_interval, &cancellation).await;
            continue;
        }

        let mut results = stream::iter(claimed)
            .map(|claimed| {
                publish_one(
                    Arc::clone(&database),
                    transport.clone(),
                    Arc::clone(&identity),
                    settings.clone(),
                    claimed,
                )
            })
            .buffer_unordered(usize::from(settings.concurrency));
        while let Some(result) = results.next().await {
            result.map_err(|error| error.task_failure())?;
        }
    }
}

async fn publish_one(
    database: Arc<PlatformDatabase>,
    transport: JetStreamRuntime,
    identity: Arc<dyn RuntimeIdentitySource>,
    settings: PublisherSettings,
    claimed: ClaimedMessage,
) -> Result<(), PlatformMessageRuntimeError> {
    match transport.publish_exact(claimed.message()).await {
        Ok(_acceptance) => match database
            .mark_outbox_published(
                claimed.message().metadata().message_id(),
                claimed.lease_id(),
            )
            .await
        {
            Ok(PublishMarkOutcome::Published | PublishMarkOutcome::AlreadyPublished) => Ok(()),
            Ok(PublishMarkOutcome::LeaseLost) => {
                tracing::warn!(
                    safe_category = "outbox.lease-lost",
                    "publication fence lost"
                );
                Ok(())
            }
            Err(error) => Err(database_runtime_error(&error)),
        },
        Err(error) if error.kind().is_transient() => {
            let failure = FailureCategory::new(error.kind().safe_category()).map_err(|_| {
                PlatformMessageRuntimeError::new(PlatformMessageRuntimeErrorKind::HandlerRegistry)
            })?;
            let retry = plan_retry_delay(
                claimed.attempt_count(),
                settings.retry_base,
                settings.retry_max,
                identity.as_ref(),
            )?;
            match database
                .reschedule_outbox_message(
                    claimed.message().metadata().message_id(),
                    claimed.lease_id(),
                    retry,
                    Some(&failure),
                )
                .await
            {
                Ok(RescheduleOutcome::Rescheduled | RescheduleOutcome::AlreadyPublished) => Ok(()),
                Ok(RescheduleOutcome::LeaseLost) => {
                    tracing::warn!(safe_category = "outbox.lease-lost", "retry fence lost");
                    Ok(())
                }
                Err(error) => Err(database_runtime_error(&error)),
            }
        }
        Err(error) => Err(PlatformMessageRuntimeError::transport(error.kind())),
    }
}

/// Runs the exact Platform command durable until cancellation or fatal failure.
///
/// # Errors
///
/// Returns a task failure when the durable cannot be bound or a permanent handler failure occurs.
pub async fn run_command_consumer(
    database: Arc<PlatformDatabase>,
    transport: JetStreamRuntime,
    identity: Arc<dyn RuntimeIdentitySource>,
    settings: ConsumerSettings,
    cancellation: CancellationToken,
) -> Result<(), TaskFailure> {
    run_consumer(
        database,
        transport,
        identity,
        settings,
        cancellation,
        platform_command_binding(),
        PlatformHandler::Command,
    )
    .await
}

/// Runs the exact Platform event durable until cancellation or fatal failure.
///
/// # Errors
///
/// Returns a task failure when the durable cannot be bound or a permanent handler failure occurs.
pub async fn run_event_consumer(
    database: Arc<PlatformDatabase>,
    transport: JetStreamRuntime,
    identity: Arc<dyn RuntimeIdentitySource>,
    settings: ConsumerSettings,
    cancellation: CancellationToken,
) -> Result<(), TaskFailure> {
    run_consumer(
        database,
        transport,
        identity,
        settings,
        cancellation,
        platform_event_binding(),
        PlatformHandler::Event,
    )
    .await
}

#[derive(Clone, Copy)]
enum PlatformHandler {
    Command,
    Event,
}

async fn run_consumer(
    database: Arc<PlatformDatabase>,
    transport: JetStreamRuntime,
    identity: Arc<dyn RuntimeIdentitySource>,
    settings: ConsumerSettings,
    cancellation: CancellationToken,
    binding: ConsumerBinding,
    handler: PlatformHandler,
) -> Result<(), TaskFailure> {
    let consumer = transport
        .bind_consumer(&binding)
        .await
        .map_err(|error| PlatformMessageRuntimeError::transport(error.kind()).task_failure())?;
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let fetched = tokio::select! {
            () = cancellation.cancelled() => return Ok(()),
            result = consumer.fetch(settings.fetch_batch_size, settings.fetch_expires) => result,
        };
        let deliveries = match fetched {
            Ok(deliveries) => deliveries,
            Err(error) if error.kind().is_transient() => continue,
            Err(error) => {
                return Err(PlatformMessageRuntimeError::transport(error.kind()).task_failure());
            }
        };
        let mut results = stream::iter(deliveries)
            .map(|delivery| {
                process_delivery(
                    Arc::clone(&database),
                    Arc::clone(&identity),
                    settings.clone(),
                    handler,
                    delivery,
                )
            })
            .buffer_unordered(usize::from(settings.max_in_flight));
        while let Some(result) = results.next().await {
            result.map_err(|error| error.task_failure())?;
        }
    }
}

async fn process_delivery(
    database: Arc<PlatformDatabase>,
    identity: Arc<dyn RuntimeIdentitySource>,
    settings: ConsumerSettings,
    handler: PlatformHandler,
    delivery: InboundDelivery,
) -> Result<(), PlatformMessageRuntimeError> {
    if let Ok(result) = tokio::time::timeout(
        settings.handler_timeout,
        process_delivery_inner(database, identity, settings.nak_delay, handler, delivery),
    )
    .await
    {
        result
    } else {
        tracing::warn!(
            safe_category = "database.transient",
            "delivery handler timed out"
        );
        Ok(())
    }
}

async fn process_delivery_inner(
    database: Arc<PlatformDatabase>,
    identity: Arc<dyn RuntimeIdentitySource>,
    nak_delay: Duration,
    handler: PlatformHandler,
    delivery: InboundDelivery,
) -> Result<(), PlatformMessageRuntimeError> {
    let Ok(message) = decode_envelope(delivery.payload().as_bytes()) else {
        return reject_delivery(delivery, nak_delay, "delivery.malformed-envelope").await;
    };
    if delivery.validate_headers(&message).is_err() {
        return reject_delivery(delivery, nak_delay, "delivery.header-mismatch").await;
    }
    let Ok(expected_subject) = TransportSubject::derive(message.metadata()) else {
        return reject_delivery(delivery, nak_delay, "delivery.subject-mismatch").await;
    };
    if delivery.actual_subject() != expected_subject.as_str()
        || delivery.stream() != stream_for_kind(message.metadata().descriptor().kind())
    {
        return reject_delivery(delivery, nak_delay, "delivery.subject-mismatch").await;
    }

    let handler_result = match handler {
        PlatformHandler::Command => {
            handle_platform_command(database.as_ref(), identity.as_ref(), &message).await
        }
        PlatformHandler::Event => handle_platform_event(database.as_ref(), &message).await,
    };
    match handler_result {
        Ok(PlatformInboxOutcome::Inserted | PlatformInboxOutcome::Duplicate) => {
            let acknowledgment = delivery.into_acknowledgment();
            if let Err(error) = acknowledgment.double_ack().await {
                tracing::warn!(
                    safe_category = error.kind().safe_category(),
                    "post-commit acknowledgment deferred to redelivery"
                );
            }
            Ok(())
        }
        Err(HandlerError::Database(error)) if database_error_is_integrity(&error) => {
            Err(database_runtime_error(&error))
        }
        Err(HandlerError::Database(_)) => {
            reject_delivery(delivery, nak_delay, "database.transient").await
        }
        Err(HandlerError::Rejected) => {
            reject_delivery(delivery, nak_delay, "delivery.unsupported-contract").await
        }
        Err(HandlerError::Runtime(error)) => Err(error),
    }
}

async fn reject_delivery(
    delivery: InboundDelivery,
    delay: Duration,
    safe_category: &'static str,
) -> Result<(), PlatformMessageRuntimeError> {
    tracing::warn!(safe_category, "delivery rejected without local receipt");
    if let Err(error) = delivery.into_acknowledgment().nak_with_delay(delay).await {
        tracing::warn!(
            safe_category = error.kind().safe_category(),
            "delivery NAK deferred to broker redelivery"
        );
    }
    Ok(())
}

async fn handle_platform_event(
    database: &PlatformDatabase,
    message: &EncodedMessage,
) -> Result<PlatformInboxOutcome, HandlerError> {
    if !valid_cell_to_platform_event(message.metadata()) {
        return Err(HandlerError::Rejected);
    }
    let descriptor = transport_cell_probe_observed_descriptor().map_err(|_| {
        HandlerError::Runtime(PlatformMessageRuntimeError::new(
            PlatformMessageRuntimeErrorKind::HandlerRegistry,
        ))
    })?;
    decode_typed::<TransportCellProbeObservedV1>(message, &descriptor)
        .map_err(|_| HandlerError::Rejected)?;
    let consumer = ConsumerName::new(CELL_PROBE_OBSERVED_HANDLER).map_err(|_| {
        HandlerError::Runtime(PlatformMessageRuntimeError::new(
            PlatformMessageRuntimeErrorKind::HandlerRegistry,
        ))
    })?;
    database
        .record_inbox_and_enqueue(&consumer, message, None)
        .await
        .map_err(HandlerError::Database)
}

async fn handle_platform_command(
    database: &PlatformDatabase,
    identity: &dyn RuntimeIdentitySource,
    message: &EncodedMessage,
) -> Result<PlatformInboxOutcome, HandlerError> {
    if !valid_cell_to_platform_command(message.metadata()) {
        return Err(HandlerError::Rejected);
    }
    let descriptor = transport_platform_probe_requested_descriptor().map_err(|_| {
        HandlerError::Runtime(PlatformMessageRuntimeError::new(
            PlatformMessageRuntimeErrorKind::HandlerRegistry,
        ))
    })?;
    let decoded = decode_typed::<TransportPlatformProbeRequestedV1>(message, &descriptor)
        .map_err(|_| HandlerError::Rejected)?;
    let derived_metadata = MessageMetadata::new(
        next_message_id(identity).map_err(|_| {
            HandlerError::Runtime(PlatformMessageRuntimeError::new(
                PlatformMessageRuntimeErrorKind::Identity,
            ))
        })?,
        transport_platform_probe_observed_descriptor().map_err(|_| {
            HandlerError::Runtime(PlatformMessageRuntimeError::new(
                PlatformMessageRuntimeErrorKind::HandlerRegistry,
            ))
        })?,
        emitted_at_now(identity).map_err(|_| {
            HandlerError::Runtime(PlatformMessageRuntimeError::new(
                PlatformMessageRuntimeErrorKind::Identity,
            ))
        })?,
        MessageAuthority::Platform,
        message.metadata().scope().clone(),
        None,
        message.metadata().correlation_id(),
        Some(message.metadata().message_id()),
    )
    .map_err(|_| {
        HandlerError::Runtime(PlatformMessageRuntimeError::new(
            PlatformMessageRuntimeErrorKind::Codec,
        ))
    })?;
    let payload = TransportPlatformProbeObservedV1::new(decoded.payload().operation_id(), true);
    let derived = encode(&derived_metadata, &payload).map_err(|_| {
        HandlerError::Runtime(PlatformMessageRuntimeError::new(
            PlatformMessageRuntimeErrorKind::Codec,
        ))
    })?;
    let consumer = ConsumerName::new(PLATFORM_PROBE_REQUESTED_HANDLER).map_err(|_| {
        HandlerError::Runtime(PlatformMessageRuntimeError::new(
            PlatformMessageRuntimeErrorKind::HandlerRegistry,
        ))
    })?;
    database
        .record_inbox_and_enqueue(&consumer, message, Some(&derived))
        .await
        .map_err(HandlerError::Database)
}

enum HandlerError {
    Rejected,
    Database(PlatformDatabaseError),
    Runtime(PlatformMessageRuntimeError),
}

fn valid_cell_to_platform_event(metadata: &MessageMetadata) -> bool {
    matches!(metadata.source(), MessageAuthority::Cell(_))
        && matches!(metadata.scope(), MessageScope::Tenant { .. })
        && metadata.target().is_none()
}

fn valid_cell_to_platform_command(metadata: &MessageMetadata) -> bool {
    matches!(metadata.source(), MessageAuthority::Cell(_))
        && matches!(metadata.scope(), MessageScope::Tenant { .. })
        && metadata.target() == Some(&MessageTarget::Platform)
}

const fn stream_for_kind(kind: MessageKind) -> TransportStream {
    match kind {
        MessageKind::Command => TransportStream::Commands,
        MessageKind::Event => TransportStream::Events,
    }
}

fn database_error_is_integrity(error: &PlatformDatabaseError) -> bool {
    matches!(
        error.kind(),
        PlatformDatabaseErrorKind::InboxConflict
            | PlatformDatabaseErrorKind::MessageIdentityConflict
            | PlatformDatabaseErrorKind::StoreCorruption
    )
}

fn database_runtime_error(error: &PlatformDatabaseError) -> PlatformMessageRuntimeError {
    match error.kind() {
        PlatformDatabaseErrorKind::InboxConflict
        | PlatformDatabaseErrorKind::MessageIdentityConflict => PlatformMessageRuntimeError::new(
            PlatformMessageRuntimeErrorKind::MessageIdentityConflict,
        ),
        PlatformDatabaseErrorKind::StoreCorruption => {
            PlatformMessageRuntimeError::new(PlatformMessageRuntimeErrorKind::StoreCorruption)
        }
        _ => PlatformMessageRuntimeError::new(PlatformMessageRuntimeErrorKind::Database),
    }
}

fn runtime_failure(kind: PlatformMessageRuntimeErrorKind) -> TaskFailure {
    TaskFailure::new(kind.to_string())
}

async fn wait_or_cancel(duration: Duration, cancellation: &CancellationToken) {
    tokio::select! {
        () = cancellation.cancelled() => {}
        () = tokio::time::sleep(duration) => {}
    }
}

fn plan_retry_delay(
    attempt_count: u64,
    base: Duration,
    maximum: Duration,
    identity: &dyn RuntimeIdentitySource,
) -> Result<RetryDelay, PlatformMessageRuntimeError> {
    let base_ms = u64::try_from(base.as_millis()).unwrap_or(u64::MAX);
    let maximum_ms = u64::try_from(maximum.as_millis()).unwrap_or(u64::MAX);
    if base_ms == 0 || maximum_ms < base_ms {
        return Err(PlatformMessageRuntimeError::new(
            PlatformMessageRuntimeErrorKind::Configuration,
        ));
    }
    let exponent = u32::try_from(attempt_count.saturating_sub(1).min(63)).unwrap_or(63);
    let cap = base_ms
        .saturating_mul(1_u64.checked_shl(exponent).unwrap_or(u64::MAX))
        .min(maximum_ms);
    let mut entropy = [0_u8; 8];
    let jittered = if identity.fill_entropy(&mut entropy).is_ok() {
        let span = cap.saturating_sub(base_ms).saturating_add(1);
        base_ms.saturating_add(u64::from_le_bytes(entropy) % span)
    } else {
        base_ms
    };
    let seconds = jittered.saturating_add(999) / 1_000;
    RetryDelay::new(Duration::from_secs(seconds)).map_err(|_| {
        PlatformMessageRuntimeError::new(PlatformMessageRuntimeErrorKind::Configuration)
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use message_domain::{
        ContractDescriptor, CorrelationId, EmittedAt, MessageId, MessageName, MessageSchemaVersion,
    };
    use runtime_identity::DeterministicRuntimeIdentity;
    use tenancy_domain::{AssignmentEpoch, CellId, TenantId};
    use time::OffsetDateTime;
    use uuid::Uuid;

    use super::*;

    fn uuid(value: &str) -> Uuid {
        Uuid::from_str(value).unwrap_or_else(|error| panic!("static UUID: {error}"))
    }

    #[test]
    fn retry_planning_is_bounded_and_entropy_failure_has_a_stable_fallback() {
        let with_entropy =
            DeterministicRuntimeIdentity::new(std::iter::empty(), std::iter::empty(), [u8::MAX; 8]);
        let retry = plan_retry_delay(
            u64::MAX,
            Duration::from_millis(250),
            Duration::from_secs(30),
            &with_entropy,
        );
        assert!(retry.is_ok_and(|value| (1..=30).contains(&value.seconds())));

        let no_entropy = DeterministicRuntimeIdentity::new(
            std::iter::empty(),
            std::iter::empty(),
            std::iter::empty(),
        );
        assert_eq!(
            plan_retry_delay(
                10,
                Duration::from_millis(250),
                Duration::from_secs(30),
                &no_entropy,
            )
            .ok()
            .map(RetryDelay::seconds),
            Some(1)
        );
    }

    #[test]
    fn route_fences_accept_only_cell_tenant_messages_to_platform() {
        let cell =
            CellId::from_str("cell-001").unwrap_or_else(|error| panic!("static cell: {error}"));
        let tenant = TenantId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c1003")
            .unwrap_or_else(|error| panic!("static tenant: {error:?}"));
        let scope = MessageScope::Tenant {
            tenant_id: tenant,
            cell_id: cell.clone(),
            assignment_epoch: AssignmentEpoch::new(1)
                .unwrap_or_else(|error| panic!("static epoch: {error:?}")),
        };
        let descriptor = ContractDescriptor::new(
            MessageKind::Command,
            MessageName::from_str("edtech.transport.platform-probe.requested")
                .unwrap_or_else(|error| panic!("static name: {error}")),
            MessageSchemaVersion::new(1).unwrap_or_else(|error| panic!("static version: {error}")),
        );
        let metadata = MessageMetadata::new(
            MessageId::new(uuid("01890f47-7cc2-7a1b-8d5d-7f6ebc9c1001"))
                .unwrap_or_else(|error| panic!("static message: {error}")),
            descriptor,
            EmittedAt::new(OffsetDateTime::UNIX_EPOCH)
                .unwrap_or_else(|error| panic!("static emitted: {error}")),
            MessageAuthority::Cell(cell),
            scope,
            Some(MessageTarget::Platform),
            CorrelationId::new(uuid("01890f47-7cc2-7a1b-8d5d-7f6ebc9c1002"))
                .unwrap_or_else(|error| panic!("static correlation: {error}")),
            None,
        )
        .unwrap_or_else(|error| panic!("static metadata: {error}"));
        assert!(valid_cell_to_platform_command(&metadata));
        assert!(!valid_cell_to_platform_event(&metadata));
    }
}
