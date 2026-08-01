//! Four operational transport-probe contracts used only to qualify cross-authority delivery.
//!
//! This crate must not own product behavior, persistence, runtimes, configuration, or transport.

use std::{fmt, str::FromStr};

use message_domain::{ContractDescriptor, MessageKind, MessageName, MessageSchemaVersion};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;
use uuid::Uuid;

/// Platform-to-Cell operational command name.
pub const TRANSPORT_CELL_PROBE_REQUESTED_NAME: &str = "edtech.transport.cell-probe.requested";
/// Cell-to-Platform operational event name.
pub const TRANSPORT_CELL_PROBE_OBSERVED_NAME: &str = "edtech.transport.cell-probe.observed";
/// Cell-to-Platform operational command name.
pub const TRANSPORT_PLATFORM_PROBE_REQUESTED_NAME: &str =
    "edtech.transport.platform-probe.requested";
/// Platform-to-Cell operational event name.
pub const TRANSPORT_PLATFORM_PROBE_OBSERVED_NAME: &str = "edtech.transport.platform-probe.observed";

const CONTRACT_VERSION: u32 = 1;
const MAX_PROBE_SCALARS: usize = 256;

/// A transport-probe payload validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TransportProbeContractError {
    /// The operation identity is not UUID version 7.
    #[error("transport probe operation identifier must use UUID version 7")]
    InvalidOperationId,
    /// Probe text is empty or exceeds the Unicode-scalar bound.
    #[error("transport probe value must contain 1 through 256 Unicode scalar values")]
    InvalidProbeValue,
    /// A static contract descriptor could not be constructed.
    #[error("transport probe descriptor is invalid")]
    InvalidDescriptor,
}

/// One validated operational probe workflow identity.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransportProbeOperationId(Uuid);

impl TransportProbeOperationId {
    /// Validates a UUID version 7 operation identity.
    ///
    /// # Errors
    ///
    /// Rejects every other UUID version.
    pub fn new(value: Uuid) -> Result<Self, TransportProbeContractError> {
        if value.get_version_num() == 7 {
            Ok(Self(value))
        } else {
            Err(TransportProbeContractError::InvalidOperationId)
        }
    }

    /// Returns the validated UUID primitive.
    #[must_use]
    pub const fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for TransportProbeOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for TransportProbeOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("TransportProbeOperationId")
            .field(&self.0)
            .finish()
    }
}

impl FromStr for TransportProbeOperationId {
    type Err = TransportProbeContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value)
            .map_err(|_| TransportProbeContractError::InvalidOperationId)
            .and_then(Self::new)
    }
}

impl Serialize for TransportProbeOperationId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TransportProbeOperationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

/// Bounded operational probe text that must never be logged.
#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct TransportProbeValue(String);

impl TransportProbeValue {
    /// Validates a probe value containing 1 through 256 Unicode scalar values.
    ///
    /// # Errors
    ///
    /// Rejects empty or oversized values.
    pub fn new(value: impl Into<String>) -> Result<Self, TransportProbeContractError> {
        let value = value.into();
        if (1..=MAX_PROBE_SCALARS).contains(&value.chars().count()) {
            Ok(Self(value))
        } else {
            Err(TransportProbeContractError::InvalidProbeValue)
        }
    }

    /// Borrows the validated value for canonical encoding and handler logic.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for TransportProbeValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TransportProbeValue([REDACTED])")
    }
}

impl<'de> Deserialize<'de> for TransportProbeValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

macro_rules! requested_payload {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            operation_id: TransportProbeOperationId,
            probe_value: TransportProbeValue,
        }

        impl $name {
            /// Constructs a validated operational request payload.
            #[must_use]
            pub const fn new(
                operation_id: TransportProbeOperationId,
                probe_value: TransportProbeValue,
            ) -> Self {
                Self { operation_id, probe_value }
            }

            /// Returns the operation identity.
            #[must_use]
            pub const fn operation_id(&self) -> TransportProbeOperationId {
                self.operation_id
            }

            /// Borrows the bounded probe value.
            #[must_use]
            pub const fn probe_value(&self) -> &TransportProbeValue {
                &self.probe_value
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("operation_id", &self.operation_id)
                    .field("probe_value", &"[REDACTED]")
                    .finish()
            }
        }
    };
}

macro_rules! observed_payload {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            operation_id: TransportProbeOperationId,
            accepted: bool,
        }

        impl $name {
            /// Constructs a validated operational observed-event payload.
            #[must_use]
            pub const fn new(operation_id: TransportProbeOperationId, accepted: bool) -> Self {
                Self { operation_id, accepted }
            }

            /// Returns the operation identity.
            #[must_use]
            pub const fn operation_id(&self) -> TransportProbeOperationId {
                self.operation_id
            }

            /// Reports whether the destination accepted the probe.
            #[must_use]
            pub const fn accepted(&self) -> bool {
                self.accepted
            }
        }
    };
}

requested_payload!(
    /// Platform-to-Cell operational command payload.
    TransportCellProbeRequestedV1
);
observed_payload!(
    /// Cell-to-Platform operational event payload.
    TransportCellProbeObservedV1
);
requested_payload!(
    /// Cell-to-Platform operational command payload.
    TransportPlatformProbeRequestedV1
);
observed_payload!(
    /// Platform-to-Cell operational event payload.
    TransportPlatformProbeObservedV1
);

/// Returns the Platform-to-Cell command descriptor.
///
/// # Errors
///
/// Fails closed if the compiled static descriptor violates message-domain rules.
pub fn transport_cell_probe_requested_descriptor()
-> Result<ContractDescriptor, TransportProbeContractError> {
    descriptor(MessageKind::Command, TRANSPORT_CELL_PROBE_REQUESTED_NAME)
}

/// Returns the Cell-to-Platform event descriptor.
///
/// # Errors
///
/// Fails closed if the compiled static descriptor violates message-domain rules.
pub fn transport_cell_probe_observed_descriptor()
-> Result<ContractDescriptor, TransportProbeContractError> {
    descriptor(MessageKind::Event, TRANSPORT_CELL_PROBE_OBSERVED_NAME)
}

/// Returns the Cell-to-Platform command descriptor.
///
/// # Errors
///
/// Fails closed if the compiled static descriptor violates message-domain rules.
pub fn transport_platform_probe_requested_descriptor()
-> Result<ContractDescriptor, TransportProbeContractError> {
    descriptor(
        MessageKind::Command,
        TRANSPORT_PLATFORM_PROBE_REQUESTED_NAME,
    )
}

/// Returns the Platform-to-Cell event descriptor.
///
/// # Errors
///
/// Fails closed if the compiled static descriptor violates message-domain rules.
pub fn transport_platform_probe_observed_descriptor()
-> Result<ContractDescriptor, TransportProbeContractError> {
    descriptor(MessageKind::Event, TRANSPORT_PLATFORM_PROBE_OBSERVED_NAME)
}

fn descriptor(
    kind: MessageKind,
    name: &'static str,
) -> Result<ContractDescriptor, TransportProbeContractError> {
    let name =
        MessageName::from_str(name).map_err(|_| TransportProbeContractError::InvalidDescriptor)?;
    let version = MessageSchemaVersion::new(CONTRACT_VERSION)
        .map_err(|_| TransportProbeContractError::InvalidDescriptor)?;
    Ok(ContractDescriptor::new(kind, name, version))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use message_codec_json::{decode_envelope, decode_typed, encode};
    use message_domain::{
        ContractDescriptor, CorrelationId, EmittedAt, MessageAuthority, MessageId, MessageMetadata,
        MessageScope, MessageTarget,
    };
    use serde::{Serialize, de::DeserializeOwned};
    use tenancy_domain::{AssignmentEpoch, CellId, TenantId};
    use time::{Duration, OffsetDateTime};

    use super::*;

    const UUID_V7: &str = "01890f47-7cc2-7a1b-8d5d-7f6ebc9c1101";

    #[test]
    fn payload_validation_is_strict_and_debug_redacts_probe_text() {
        assert!(TransportProbeOperationId::from_str(UUID_V7).is_ok());
        assert!(
            TransportProbeOperationId::from_str("550e8400-e29b-41d4-a716-446655440000").is_err()
        );
        assert!(TransportProbeValue::new("").is_err());
        assert!(TransportProbeValue::new("x".repeat(256)).is_ok());
        assert!(TransportProbeValue::new("x".repeat(257)).is_err());

        let json = format!(r#"{{"operation_id":"{UUID_V7}","probe_value":"ok","extra":1}}"#);
        assert!(serde_json::from_str::<TransportCellProbeRequestedV1>(&json).is_err());
        let payload = TransportCellProbeRequestedV1::new(
            TransportProbeOperationId::from_str(UUID_V7)
                .unwrap_or_else(|error| panic!("fixture operation: {error}")),
            TransportProbeValue::new("unique-payload-sentinel")
                .unwrap_or_else(|error| panic!("fixture value: {error}")),
        );
        assert!(!format!("{payload:?}").contains("unique-payload-sentinel"));
    }

    #[test]
    fn descriptors_are_unique_and_exact() {
        let descriptors = [
            transport_cell_probe_requested_descriptor(),
            transport_cell_probe_observed_descriptor(),
            transport_platform_probe_requested_descriptor(),
            transport_platform_probe_observed_descriptor(),
        ];
        let names = descriptors
            .iter()
            .filter_map(|result| result.as_ref().ok())
            .map(|value| value.name().as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names.len(), 4);
        assert!(descriptors.into_iter().all(|result| result.is_ok()));
    }

    #[test]
    fn typed_round_trip_preserves_a_full_range_assignment_epoch() {
        let message_id = MessageId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c1102")
            .unwrap_or_else(|error| panic!("fixture message: {error}"));
        let correlation = CorrelationId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c1103")
            .unwrap_or_else(|error| panic!("fixture correlation: {error}"));
        let cell =
            CellId::from_str("cell-001").unwrap_or_else(|error| panic!("fixture cell: {error}"));
        let tenant = TenantId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c1104")
            .unwrap_or_else(|error| panic!("fixture tenant: {error:?}"));
        let metadata = MessageMetadata::new(
            message_id,
            transport_cell_probe_requested_descriptor()
                .unwrap_or_else(|error| panic!("fixture descriptor: {error}")),
            EmittedAt::new(OffsetDateTime::UNIX_EPOCH + Duration::seconds(10))
                .unwrap_or_else(|error| panic!("fixture timestamp: {error}")),
            MessageAuthority::Platform,
            MessageScope::Tenant {
                tenant_id: tenant,
                cell_id: cell.clone(),
                assignment_epoch: AssignmentEpoch::new(u64::MAX)
                    .unwrap_or_else(|error| panic!("fixture epoch: {error}")),
            },
            Some(MessageTarget::Cell(cell)),
            correlation,
            None,
        )
        .unwrap_or_else(|error| panic!("fixture metadata: {error}"));
        let payload = TransportCellProbeRequestedV1::new(
            TransportProbeOperationId::from_str(UUID_V7)
                .unwrap_or_else(|error| panic!("fixture operation: {error}")),
            TransportProbeValue::new("probe")
                .unwrap_or_else(|error| panic!("fixture value: {error}")),
        );
        let encoded =
            encode(&metadata, &payload).unwrap_or_else(|error| panic!("fixture encode: {error}"));
        let decoded = decode_envelope(encoded.as_bytes())
            .unwrap_or_else(|error| panic!("fixture envelope: {error}"));
        let typed = decode_typed::<TransportCellProbeRequestedV1>(
            &decoded,
            &transport_cell_probe_requested_descriptor()
                .unwrap_or_else(|error| panic!("fixture descriptor: {error}")),
        );
        assert_eq!(
            typed.ok().map(|value| value.payload().clone()),
            Some(payload)
        );
    }

    fn assert_fixture_round_trip<T>(fixture: &[u8], descriptor: &ContractDescriptor)
    where
        T: DeserializeOwned + Serialize,
    {
        assert!(fixture.ends_with(b"\n"));
        let canonical = &fixture[..fixture.len().saturating_sub(1)];
        let envelope =
            decode_envelope(canonical).unwrap_or_else(|error| panic!("fixture envelope: {error}"));
        let typed = decode_typed::<T>(&envelope, descriptor)
            .unwrap_or_else(|error| panic!("fixture payload: {error}"));
        let encoded = encode(typed.metadata(), typed.payload())
            .unwrap_or_else(|error| panic!("fixture re-encode: {error}"));
        assert_eq!(encoded.as_bytes(), canonical);
    }

    #[test]
    fn all_four_canonical_transport_fixtures_round_trip_byte_for_byte() {
        assert_fixture_round_trip::<TransportCellProbeRequestedV1>(
            include_bytes!(
                "../../../docs/contracts/fixtures/transport-cell-probe-requested-v1.json"
            ),
            &transport_cell_probe_requested_descriptor()
                .unwrap_or_else(|error| panic!("cell request descriptor: {error}")),
        );
        assert_fixture_round_trip::<TransportCellProbeObservedV1>(
            include_bytes!(
                "../../../docs/contracts/fixtures/transport-cell-probe-observed-v1.json"
            ),
            &transport_cell_probe_observed_descriptor()
                .unwrap_or_else(|error| panic!("cell observed descriptor: {error}")),
        );
        assert_fixture_round_trip::<TransportPlatformProbeRequestedV1>(
            include_bytes!(
                "../../../docs/contracts/fixtures/transport-platform-probe-requested-v1.json"
            ),
            &transport_platform_probe_requested_descriptor()
                .unwrap_or_else(|error| panic!("platform request descriptor: {error}")),
        );
        assert_fixture_round_trip::<TransportPlatformProbeObservedV1>(
            include_bytes!(
                "../../../docs/contracts/fixtures/transport-platform-probe-observed-v1.json"
            ),
            &transport_platform_probe_observed_descriptor()
                .unwrap_or_else(|error| panic!("platform observed descriptor: {error}")),
        );
    }
}
