//! Minimal identity and fencing primitives for future provisioning workflows.
//!
//! This crate deliberately contains no provisioning state machine, persistence, transport,
//! runtime framework, or provider integration.

use std::{fmt, num::NonZeroU64, str::FromStr};

use thiserror::Error;
use uuid::Uuid;

/// A stable `UUIDv7` identity for one provisioning operation.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProvisioningOperationId(Uuid);

/// A validation failure for [`ProvisioningOperationId`].
#[derive(Debug, Error)]
pub enum ProvisioningOperationIdError {
    /// The supplied text is not a UUID.
    #[error("provisioning operation identifier is not a valid UUID")]
    InvalidUuid(#[from] uuid::Error),
    /// The UUID is not version 7.
    #[error("provisioning operation identifier must use UUID version 7")]
    WrongVersion,
}

impl ProvisioningOperationId {
    /// Constructs an operation identifier from a `UUIDv7` value.
    ///
    /// # Errors
    ///
    /// Returns [`ProvisioningOperationIdError::WrongVersion`] for another UUID version.
    pub fn new(value: Uuid) -> Result<Self, ProvisioningOperationIdError> {
        if value.get_version_num() == 7 {
            Ok(Self(value))
        } else {
            Err(ProvisioningOperationIdError::WrongVersion)
        }
    }
}

impl FromStr for ProvisioningOperationId {
    type Err = ProvisioningOperationIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(Uuid::parse_str(value)?)
    }
}

impl fmt::Display for ProvisioningOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for ProvisioningOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ProvisioningOperationId")
            .field(&self.0)
            .finish()
    }
}

/// A monotonically increasing fence for repeated provisioning work.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProvisioningGeneration(NonZeroU64);

/// A validation failure while constructing a [`ProvisioningGeneration`].
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("provisioning generation must be non-zero")]
pub struct InvalidProvisioningGeneration;

/// An overflow while advancing a [`ProvisioningGeneration`].
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("provisioning generation cannot advance beyond u64::MAX")]
pub struct ProvisioningGenerationOverflow;

impl ProvisioningGeneration {
    /// Returns generation 1 for explicitly initiated provisioning work.
    #[must_use]
    pub const fn initial() -> Self {
        Self(NonZeroU64::MIN)
    }

    /// Constructs a non-zero generation.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidProvisioningGeneration`] when `value` is zero.
    pub fn new(value: u64) -> Result<Self, InvalidProvisioningGeneration> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(InvalidProvisioningGeneration)
    }

    /// Returns the numeric generation.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Advances this generation without wrapping.
    ///
    /// # Errors
    ///
    /// Returns [`ProvisioningGenerationOverflow`] at `u64::MAX`.
    pub fn checked_advance(self) -> Result<Self, ProvisioningGenerationOverflow> {
        self.get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or(ProvisioningGenerationOverflow)
    }
}

impl fmt::Display for ProvisioningGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{
        ProvisioningGeneration, ProvisioningGenerationOverflow, ProvisioningOperationId,
        ProvisioningOperationIdError,
    };

    #[test]
    fn operation_id_requires_uuid_v7() {
        assert!(ProvisioningOperationId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c0002").is_ok());
        assert!(matches!(
            ProvisioningOperationId::from_str("550e8400-e29b-41d4-a716-446655440000"),
            Err(ProvisioningOperationIdError::WrongVersion)
        ));
    }

    #[test]
    fn generation_is_non_zero_and_checked() {
        assert_eq!(ProvisioningGeneration::initial().get(), 1);
        assert!(ProvisioningGeneration::new(0).is_err());
        let advanced = ProvisioningGeneration::new(7)
            .ok()
            .and_then(|generation| generation.checked_advance().ok());
        assert_eq!(advanced.map(ProvisioningGeneration::get), Some(8));
        let overflow = ProvisioningGeneration::new(u64::MAX)
            .ok()
            .and_then(|generation| generation.checked_advance().err());
        assert_eq!(overflow, Some(ProvisioningGenerationOverflow));
    }
}
