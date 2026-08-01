//! Foundational tenant-authority identifiers and placement-version primitives.
//!
//! This crate owns topology-neutral identity and assignment concepts. It must not know about
//! configuration, persistence, transport, web frameworks, deployment coordinates, or product
//! features.

use std::{fmt, num::NonZeroU64, str::FromStr};

use thiserror::Error;
use uuid::Uuid;

/// A validation failure for a `UUID`-version-7 domain identifier.
#[derive(Debug, Error)]
pub enum UuidV7Error {
    /// The supplied text is not a UUID.
    #[error("identifier is not a valid UUID")]
    InvalidUuid(#[from] uuid::Error),
    /// The UUID is valid but is not version 7.
    #[error("identifier must use UUID version 7")]
    WrongVersion,
}

macro_rules! uuid_v7_identifier {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Uuid);

        impl $name {
            /// Constructs the identifier after validating that `value` is `UUID` version 7.
            ///
            /// # Errors
            ///
            /// Returns [`UuidV7Error::WrongVersion`] for any other UUID version.
            pub fn new(value: Uuid) -> Result<Self, UuidV7Error> {
                if value.get_version_num() == 7 {
                    Ok(Self(value))
                } else {
                    Err(UuidV7Error::WrongVersion)
                }
            }
        }

        impl FromStr for $name {
            type Err = UuidV7Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(Uuid::parse_str(value)?)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0)
                    .finish()
            }
        }
    };
}

uuid_v7_identifier!(
    /// Stable identity of an organization owned by the Platform authority.
    OrganizationId
);

uuid_v7_identifier!(
    /// Stable identity of a tenant, independent of its Cell placement.
    TenantId
);

/// A stable logical Cell identifier that carries no deployment coordinates.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CellId(String);

/// A validation failure for [`CellId`].
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CellIdError {
    /// The identifier is outside the inclusive 3-to-63-byte bound.
    #[error("cell_id must be between 3 and 63 ASCII characters")]
    InvalidLength,
    /// A character is not a lowercase ASCII letter, digit, or hyphen.
    #[error("cell_id contains a forbidden character")]
    InvalidCharacter,
    /// The first or last character is not an ASCII letter or digit.
    #[error("cell_id must begin and end with an ASCII letter or digit")]
    InvalidBoundary,
    /// Consecutive hyphens form an empty segment.
    #[error("cell_id must not contain empty segments")]
    EmptySegment,
}

impl CellId {
    /// Validates and constructs a topology-neutral Cell identifier.
    ///
    /// # Errors
    ///
    /// Returns a [`CellIdError`] when the value violates the length or grammar constraints.
    pub fn new(value: impl Into<String>) -> Result<Self, CellIdError> {
        let value = value.into();
        let bytes = value.as_bytes();

        if !(3..=63).contains(&bytes.len()) {
            return Err(CellIdError::InvalidLength);
        }
        if !bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
        {
            return Err(CellIdError::InvalidCharacter);
        }
        if !bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
            || !bytes
                .last()
                .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        {
            return Err(CellIdError::InvalidBoundary);
        }
        if bytes.windows(2).any(|window| window == b"--") {
            return Err(CellIdError::EmptySegment);
        }

        Ok(Self(value))
    }

    /// Returns the validated logical identifier as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for CellId {
    type Err = CellIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl fmt::Display for CellId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for CellId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("CellId").field(&self.0).finish()
    }
}

/// A monotonically increasing fence for a tenant-to-Cell assignment.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AssignmentEpoch(NonZeroU64);

/// A validation failure while constructing an [`AssignmentEpoch`].
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("assignment epoch must be non-zero")]
pub struct InvalidAssignmentEpoch;

/// An overflow while advancing an [`AssignmentEpoch`].
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("assignment epoch cannot advance beyond u64::MAX")]
pub struct AssignmentEpochOverflow;

impl AssignmentEpoch {
    /// Returns epoch 1 for an explicitly initiated first assignment.
    #[must_use]
    pub const fn initial() -> Self {
        Self(NonZeroU64::MIN)
    }

    /// Constructs an epoch from a non-zero integer.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidAssignmentEpoch`] when `value` is zero.
    pub fn new(value: u64) -> Result<Self, InvalidAssignmentEpoch> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(InvalidAssignmentEpoch)
    }

    /// Returns the numeric epoch.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Advances this epoch without wrapping.
    ///
    /// # Errors
    ///
    /// Returns [`AssignmentEpochOverflow`] when this epoch is already `u64::MAX`.
    pub fn checked_advance(self) -> Result<Self, AssignmentEpochOverflow> {
        self.get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or(AssignmentEpochOverflow)
    }
}

impl fmt::Display for AssignmentEpoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use proptest::prelude::*;

    use super::{
        AssignmentEpoch, AssignmentEpochOverflow, CellId, CellIdError, OrganizationId, TenantId,
        UuidV7Error,
    };

    const VALID_UUID_V7: &str = "01890f47-7cc2-7a1b-8d5d-7f6ebc9c0001";

    #[test]
    fn uuid_identifiers_accept_only_version_seven() {
        assert!(OrganizationId::from_str(VALID_UUID_V7).is_ok());
        assert!(TenantId::from_str(VALID_UUID_V7).is_ok());
        assert!(matches!(
            TenantId::from_str("550e8400-e29b-41d4-a716-446655440000"),
            Err(UuidV7Error::WrongVersion)
        ));
        assert!(matches!(
            OrganizationId::from_str("not-a-uuid"),
            Err(UuidV7Error::InvalidUuid(_))
        ));
    }

    #[test]
    fn cell_id_accepts_valid_examples_and_bounds() {
        assert!(CellId::from_str("cell-001").is_ok());
        assert!(CellId::from_str("a1b").is_ok());
        assert!(CellId::from_str(&format!("a{}z", "1".repeat(61))).is_ok());
    }

    #[test]
    fn cell_id_rejects_forbidden_shapes() {
        let cases = [
            (String::new(), CellIdError::InvalidLength),
            (String::from("ab"), CellIdError::InvalidLength),
            ("a".repeat(64), CellIdError::InvalidLength),
            (String::from("Cell-001"), CellIdError::InvalidCharacter),
            (String::from("cell_001"), CellIdError::InvalidCharacter),
            (String::from("cell/001"), CellIdError::InvalidCharacter),
            (String::from("cell.001"), CellIdError::InvalidCharacter),
            (String::from("cell 001"), CellIdError::InvalidCharacter),
            (String::from("https://cell"), CellIdError::InvalidCharacter),
            (String::from("-cell"), CellIdError::InvalidBoundary),
            (String::from("cell-"), CellIdError::InvalidBoundary),
            (String::from("cell--001"), CellIdError::EmptySegment),
        ];

        for (value, expected) in cases {
            assert_eq!(CellId::from_str(&value), Err(expected));
        }
    }

    #[test]
    fn assignment_epoch_is_non_zero_checked_and_monotonic() {
        assert_eq!(AssignmentEpoch::initial().get(), 1);
        assert!(AssignmentEpoch::new(0).is_err());
        let advanced = AssignmentEpoch::new(41)
            .ok()
            .and_then(|epoch| epoch.checked_advance().ok());
        assert_eq!(advanced.map(AssignmentEpoch::get), Some(42));

        let overflow = AssignmentEpoch::new(u64::MAX)
            .ok()
            .and_then(|epoch| epoch.checked_advance().err());
        assert_eq!(overflow, Some(AssignmentEpochOverflow));
    }

    proptest! {
        #[test]
        fn every_accepted_cell_id_obeys_the_complete_grammar(candidate in ".{0,80}") {
            if let Ok(cell_id) = CellId::from_str(&candidate) {
                let value = cell_id.as_str();
                prop_assert!((3..=63).contains(&value.len()));
                prop_assert!(value.is_ascii());
                let characters_are_valid = value.bytes().all(|byte| {
                    byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                });
                prop_assert!(characters_are_valid);
                prop_assert!(value.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric));
                prop_assert!(value.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric));
                prop_assert!(!value.contains("--"));
            }
        }

        #[test]
        fn generated_non_empty_segments_form_valid_cell_ids(
            segments in prop::collection::vec("[a-z0-9]{1,8}", 1..7)
        ) {
            let candidate = segments.join("-");
            prop_assume!((3..=63).contains(&candidate.len()));
            prop_assert!(CellId::from_str(&candidate).is_ok());
        }
    }
}
