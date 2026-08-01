//! Framework-neutral authenticated actor and request-scope context.
//!
//! This crate carries validated identity and authority scope only. It must not contain JWT
//! claims, HTTP concepts, roles, permissions, extractors, or identity-provider SDK types.

use std::{fmt, str::FromStr};

use tenancy_domain::{AssignmentEpoch, CellId, TenantId};
use thiserror::Error;
use uuid::Uuid;

/// A validation failure for an auth-context `UUIDv7` identifier.
#[derive(Debug, Error)]
pub enum AuthIdentifierError {
    /// The supplied text is not a UUID.
    #[error("authentication identifier is not a valid UUID")]
    InvalidUuid(#[from] uuid::Error),
    /// The UUID is not version 7.
    #[error("authentication identifier must use UUID version 7")]
    WrongVersion,
}

macro_rules! auth_uuid_v7_identifier {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Uuid);

        impl $name {
            /// Constructs this identity from a `UUIDv7` value.
            ///
            /// # Errors
            ///
            /// Returns [`AuthIdentifierError::WrongVersion`] for another UUID version.
            pub fn new(value: Uuid) -> Result<Self, AuthIdentifierError> {
                if value.get_version_num() == 7 {
                    Ok(Self(value))
                } else {
                    Err(AuthIdentifierError::WrongVersion)
                }
            }
        }

        impl FromStr for $name {
            type Err = AuthIdentifierError;

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

auth_uuid_v7_identifier!(
    /// Stable identity of the authenticated principal.
    PrincipalId
);

auth_uuid_v7_identifier!(
    /// Stable identity used to correlate one request.
    RequestId
);

/// The broad, provider-neutral kind of authenticated actor.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ActorKind {
    /// A person acting interactively or through a client.
    Human,
    /// A separately authenticated workload.
    Workload,
    /// A trusted internal system action.
    System,
}

/// The authority scope in which a request is allowed to execute.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RequestScope {
    /// A request governed by Platform authority.
    Platform,
    /// A request governed by one tenant's currently assigned Cell authority.
    Tenant {
        /// The tenant being served.
        tenant_id: TenantId,
        /// The logical Cell expected to serve the tenant.
        cell_id: CellId,
        /// The assignment fence observed for this request.
        assignment_epoch: AssignmentEpoch,
    },
}

/// Authenticated, request-correlated context passed into application boundaries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthContext {
    principal_id: PrincipalId,
    request_id: RequestId,
    actor_kind: ActorKind,
    scope: RequestScope,
}

impl AuthContext {
    /// Constructs a framework-neutral authenticated request context.
    #[must_use]
    pub const fn new(
        principal_id: PrincipalId,
        request_id: RequestId,
        actor_kind: ActorKind,
        scope: RequestScope,
    ) -> Self {
        Self {
            principal_id,
            request_id,
            actor_kind,
            scope,
        }
    }

    /// Returns the authenticated principal identity.
    #[must_use]
    pub const fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    /// Returns the request correlation identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the broad actor kind.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Returns the request's authority scope.
    #[must_use]
    pub const fn scope(&self) -> &RequestScope {
        &self.scope
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{
        ActorKind, AuthContext, AuthIdentifierError, PrincipalId, RequestId, RequestScope,
    };

    #[test]
    fn identifiers_require_uuid_v7() {
        let principal = PrincipalId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c0003");
        let request = RequestId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c0004");
        assert!(principal.is_ok());
        assert!(request.is_ok());
        assert!(matches!(
            PrincipalId::from_str("550e8400-e29b-41d4-a716-446655440000"),
            Err(AuthIdentifierError::WrongVersion)
        ));
    }

    #[test]
    fn context_keeps_platform_scope_framework_neutral() {
        let principal = PrincipalId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c0003");
        let request = RequestId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c0004");
        if let (Ok(principal), Ok(request)) = (principal, request) {
            let context = AuthContext::new(
                principal,
                request,
                ActorKind::Workload,
                RequestScope::Platform,
            );
            assert_eq!(context.actor_kind(), ActorKind::Workload);
            assert_eq!(context.scope(), &RequestScope::Platform);
        } else {
            panic!("fixed UUIDv7 fixtures must remain valid");
        }
    }
}
