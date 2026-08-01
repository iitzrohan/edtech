//! Minimal provider-neutral audit event, action, actor, and outcome primitives.
//!
//! This crate does not define audit persistence, transport, arbitrary metadata, or provider
//! schemas.

use std::{fmt, str::FromStr};

use auth_context::{ActorKind, PrincipalId};
use thiserror::Error;
use uuid::Uuid;

const AUDIT_ACTION_MIN_LENGTH: usize = 3;
const AUDIT_ACTION_MAX_LENGTH: usize = 64;

/// A stable `UUIDv7` identity for an audit event.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AuditEventId(Uuid);

/// A validation failure for [`AuditEventId`].
#[derive(Debug, Error)]
pub enum AuditEventIdError {
    /// The supplied text is not a UUID.
    #[error("audit event identifier is not a valid UUID")]
    InvalidUuid(#[from] uuid::Error),
    /// The UUID is not version 7.
    #[error("audit event identifier must use UUID version 7")]
    WrongVersion,
}

impl AuditEventId {
    /// Constructs an audit event identity from a `UUIDv7` value.
    ///
    /// # Errors
    ///
    /// Returns [`AuditEventIdError::WrongVersion`] for another UUID version.
    pub fn new(value: Uuid) -> Result<Self, AuditEventIdError> {
        if value.get_version_num() == 7 {
            Ok(Self(value))
        } else {
            Err(AuditEventIdError::WrongVersion)
        }
    }
}

impl FromStr for AuditEventId {
    type Err = AuditEventIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(Uuid::parse_str(value)?)
    }
}

impl fmt::Display for AuditEventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for AuditEventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("AuditEventId")
            .field(&self.0)
            .finish()
    }
}

/// The provider-neutral outcome of an audited action.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AuditOutcome {
    /// The action completed successfully.
    Succeeded,
    /// Authorization denied the action.
    Denied,
    /// The action failed after it was allowed to begin.
    Failed,
}

/// A bounded lowercase `namespace.action` audit classification.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AuditAction(String);

/// A validation failure for [`AuditAction`].
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AuditActionError {
    /// The action is outside the inclusive 3-to-64-byte bound.
    #[error("audit action must be between 3 and 64 ASCII characters")]
    InvalidLength,
    /// The action does not contain exactly two non-empty dot-separated segments.
    #[error("audit action must have namespace.action format")]
    InvalidShape,
    /// A segment contains something other than lowercase ASCII, a digit, or a hyphen.
    #[error("audit action contains a forbidden character")]
    InvalidCharacter,
}

impl AuditAction {
    /// Validates and constructs a bounded audit action.
    ///
    /// # Errors
    ///
    /// Returns an [`AuditActionError`] when the value is unbounded or malformed.
    pub fn new(value: impl Into<String>) -> Result<Self, AuditActionError> {
        let value = value.into();
        if !(AUDIT_ACTION_MIN_LENGTH..=AUDIT_ACTION_MAX_LENGTH).contains(&value.len()) {
            return Err(AuditActionError::InvalidLength);
        }

        let mut segments = value.split('.');
        let namespace = segments.next();
        let action = segments.next();
        if segments.next().is_some()
            || namespace.is_none_or(str::is_empty)
            || action.is_none_or(str::is_empty)
        {
            return Err(AuditActionError::InvalidShape);
        }

        if !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'.'
        }) {
            return Err(AuditActionError::InvalidCharacter);
        }

        Ok(Self(value))
    }

    /// Returns the validated action classification.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for AuditAction {
    type Err = AuditActionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl fmt::Display for AuditAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The authenticated principal and broad actor kind recorded by an audit event.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AuditActor {
    principal_id: PrincipalId,
    actor_kind: ActorKind,
}

impl AuditActor {
    /// Constructs an audit actor from framework-neutral authentication primitives.
    #[must_use]
    pub const fn new(principal_id: PrincipalId, actor_kind: ActorKind) -> Self {
        Self {
            principal_id,
            actor_kind,
        }
    }

    /// Returns the actor's stable principal identity.
    #[must_use]
    pub const fn principal_id(self) -> PrincipalId {
        self.principal_id
    }

    /// Returns the broad actor kind.
    #[must_use]
    pub const fn actor_kind(self) -> ActorKind {
        self.actor_kind
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{AuditAction, AuditActionError, AuditEventId, AuditEventIdError};

    #[test]
    fn audit_event_id_requires_uuid_v7() {
        assert!(AuditEventId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c0005").is_ok());
        assert!(matches!(
            AuditEventId::from_str("550e8400-e29b-41d4-a716-446655440000"),
            Err(AuditEventIdError::WrongVersion)
        ));
    }

    #[test]
    fn audit_action_enforces_bounded_namespace_action_grammar() {
        assert_eq!(
            AuditAction::from_str("tenant.create").map(|action| action.as_str().to_owned()),
            Ok(String::from("tenant.create"))
        );
        assert_eq!(
            AuditAction::from_str("Tenant.create"),
            Err(AuditActionError::InvalidCharacter)
        );
        assert_eq!(
            AuditAction::from_str("tenant create"),
            Err(AuditActionError::InvalidShape)
        );
        assert_eq!(
            AuditAction::from_str("tenant..create"),
            Err(AuditActionError::InvalidShape)
        );
        assert_eq!(
            AuditAction::from_str("tenant.create.extra"),
            Err(AuditActionError::InvalidShape)
        );
        assert_eq!(
            AuditAction::from_str(&"a".repeat(65)),
            Err(AuditActionError::InvalidLength)
        );
    }
}
