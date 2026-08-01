//! Runtime-only `UUIDv7`, UTC time, and entropy generation at composition boundaries.
//!
//! This crate must not own domain decisions, persistence, configuration, transport, or logging.

use std::{collections::VecDeque, fmt, sync::Mutex};

use message_domain::{CorrelationId, EmittedAt, MessageId};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

/// Safe runtime generation failure categories.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RuntimeIdentityError {
    /// The configured identity source could not produce another UUID.
    #[error("runtime identity generation is unavailable")]
    IdentityUnavailable,
    /// The configured clock could not produce a usable UTC timestamp.
    #[error("runtime UTC time generation is unavailable")]
    TimeUnavailable,
    /// Runtime entropy could not be obtained.
    #[error("runtime entropy generation is unavailable")]
    EntropyUnavailable,
    /// A generated primitive was rejected by an existing domain constructor.
    #[error("runtime identity source produced an invalid domain value")]
    InvalidDomainValue,
}

/// Injectable runtime UUID, clock, and entropy source.
///
/// Production composition uses [`SystemRuntimeIdentity`]. Deterministic tests use
/// [`DeterministicRuntimeIdentity`] and never observe wall-clock time or ambient randomness.
pub trait RuntimeIdentitySource: Send + Sync {
    /// Produces one UUID version 7 primitive.
    ///
    /// # Errors
    ///
    /// Returns a safe category when the source is unavailable or produces another UUID version.
    fn generate_uuid_v7(&self) -> Result<Uuid, RuntimeIdentityError>;

    /// Produces one UTC wall-clock timestamp primitive.
    ///
    /// # Errors
    ///
    /// Returns a safe category when the source is unavailable.
    fn utc_now(&self) -> Result<OffsetDateTime, RuntimeIdentityError>;

    /// Fills a caller-owned bounded buffer with runtime entropy.
    ///
    /// # Errors
    ///
    /// Returns a safe category when the operating-system entropy provider is unavailable.
    fn fill_entropy(&self, destination: &mut [u8]) -> Result<(), RuntimeIdentityError>;
}

/// Production runtime `UUIDv7`, UTC clock, and operating-system entropy source.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemRuntimeIdentity;

impl RuntimeIdentitySource for SystemRuntimeIdentity {
    fn generate_uuid_v7(&self) -> Result<Uuid, RuntimeIdentityError> {
        let value = Uuid::now_v7();
        if value.get_version_num() == 7 {
            Ok(value)
        } else {
            Err(RuntimeIdentityError::IdentityUnavailable)
        }
    }

    fn utc_now(&self) -> Result<OffsetDateTime, RuntimeIdentityError> {
        Ok(OffsetDateTime::now_utc())
    }

    fn fill_entropy(&self, destination: &mut [u8]) -> Result<(), RuntimeIdentityError> {
        getrandom::fill(destination).map_err(|_| RuntimeIdentityError::EntropyUnavailable)
    }
}

/// Converts the next runtime UUID into a validated transport [`MessageId`].
///
/// # Errors
///
/// Returns a safe source or domain validation category.
pub fn next_message_id(
    source: &(impl RuntimeIdentitySource + ?Sized),
) -> Result<MessageId, RuntimeIdentityError> {
    MessageId::new(source.generate_uuid_v7()?).map_err(|_| RuntimeIdentityError::InvalidDomainValue)
}

/// Converts the next runtime UUID into a validated workflow [`CorrelationId`].
///
/// # Errors
///
/// Returns a safe source or domain validation category.
pub fn next_correlation_id(
    source: &(impl RuntimeIdentitySource + ?Sized),
) -> Result<CorrelationId, RuntimeIdentityError> {
    CorrelationId::new(source.generate_uuid_v7()?)
        .map_err(|_| RuntimeIdentityError::InvalidDomainValue)
}

/// Converts the next runtime clock reading into a canonical [`EmittedAt`].
///
/// # Errors
///
/// Returns a safe source or domain validation category.
pub fn emitted_at_now(
    source: &(impl RuntimeIdentitySource + ?Sized),
) -> Result<EmittedAt, RuntimeIdentityError> {
    EmittedAt::new(source.utc_now()?).map_err(|_| RuntimeIdentityError::InvalidDomainValue)
}

/// Deterministic source backed by finite caller-supplied queues.
pub struct DeterministicRuntimeIdentity {
    uuids: Mutex<VecDeque<Uuid>>,
    timestamps: Mutex<VecDeque<OffsetDateTime>>,
    entropy: Mutex<VecDeque<u8>>,
}

impl DeterministicRuntimeIdentity {
    /// Constructs a deterministic finite source without ambient state.
    #[must_use]
    pub fn new(
        uuids: impl IntoIterator<Item = Uuid>,
        timestamps: impl IntoIterator<Item = OffsetDateTime>,
        entropy: impl IntoIterator<Item = u8>,
    ) -> Self {
        Self {
            uuids: Mutex::new(uuids.into_iter().collect()),
            timestamps: Mutex::new(timestamps.into_iter().collect()),
            entropy: Mutex::new(entropy.into_iter().collect()),
        }
    }
}

impl fmt::Debug for DeterministicRuntimeIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeterministicRuntimeIdentity")
            .finish_non_exhaustive()
    }
}

impl RuntimeIdentitySource for DeterministicRuntimeIdentity {
    fn generate_uuid_v7(&self) -> Result<Uuid, RuntimeIdentityError> {
        let mut values = self
            .uuids
            .lock()
            .map_err(|_| RuntimeIdentityError::IdentityUnavailable)?;
        let value = values
            .pop_front()
            .ok_or(RuntimeIdentityError::IdentityUnavailable)?;
        if value.get_version_num() == 7 {
            Ok(value)
        } else {
            Err(RuntimeIdentityError::InvalidDomainValue)
        }
    }

    fn utc_now(&self) -> Result<OffsetDateTime, RuntimeIdentityError> {
        self.timestamps
            .lock()
            .map_err(|_| RuntimeIdentityError::TimeUnavailable)?
            .pop_front()
            .ok_or(RuntimeIdentityError::TimeUnavailable)
    }

    fn fill_entropy(&self, destination: &mut [u8]) -> Result<(), RuntimeIdentityError> {
        let mut values = self
            .entropy
            .lock()
            .map_err(|_| RuntimeIdentityError::EntropyUnavailable)?;
        if values.len() < destination.len() {
            return Err(RuntimeIdentityError::EntropyUnavailable);
        }
        for byte in destination {
            *byte = values
                .pop_front()
                .ok_or(RuntimeIdentityError::EntropyUnavailable)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use time::{Duration, OffsetDateTime};

    use super::*;

    const FIRST: &str = "01890f47-7cc2-7a1b-8d5d-7f6ebc9c1001";
    const SECOND: &str = "01890f47-7cc2-7a1b-8d5d-7f6ebc9c1002";

    fn uuid(text: &str) -> Uuid {
        Uuid::from_str(text).unwrap_or_else(|error| panic!("static UUID fixture: {error}"))
    }

    #[test]
    fn production_source_generates_uuid_v7_and_valid_time() {
        let source = SystemRuntimeIdentity;
        let generated = source.generate_uuid_v7();
        assert_eq!(generated.ok().map(|value| value.get_version_num()), Some(7));
        assert!(emitted_at_now(&source).is_ok());
    }

    #[test]
    fn deterministic_source_produces_exact_values_without_ambient_state() {
        let instant = OffsetDateTime::UNIX_EPOCH + Duration::seconds(10);
        let source = DeterministicRuntimeIdentity::new(
            [uuid(FIRST), uuid(SECOND)],
            [instant],
            [3_u8, 7, 11],
        );
        assert_eq!(
            next_message_id(&source).ok().map(|value| value.to_string()),
            Some(String::from(FIRST))
        );
        assert_eq!(
            next_correlation_id(&source)
                .ok()
                .map(|value| value.to_string()),
            Some(String::from(SECOND))
        );
        assert_eq!(
            emitted_at_now(&source)
                .ok()
                .map(EmittedAt::unix_timestamp_micros),
            Some(10_000_000)
        );
        let mut bytes = [0_u8; 3];
        assert!(source.fill_entropy(&mut bytes).is_ok());
        assert_eq!(bytes, [3, 7, 11]);
    }

    #[test]
    fn deterministic_exhaustion_and_invalid_values_fail_safely() {
        let invalid = Uuid::from_str("550e8400-e29b-41d4-a716-446655440000")
            .unwrap_or_else(|error| panic!("static UUID fixture: {error}"));
        let source =
            DeterministicRuntimeIdentity::new([invalid], std::iter::empty(), std::iter::empty());
        assert_eq!(
            source.generate_uuid_v7(),
            Err(RuntimeIdentityError::InvalidDomainValue)
        );
        assert_eq!(
            source.generate_uuid_v7(),
            Err(RuntimeIdentityError::IdentityUnavailable)
        );
        assert_eq!(source.utc_now(), Err(RuntimeIdentityError::TimeUnavailable));
        assert_eq!(
            source.fill_entropy(&mut [0_u8; 1]),
            Err(RuntimeIdentityError::EntropyUnavailable)
        );
    }
}
