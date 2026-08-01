//! Qualification-only typed contracts and deterministic message generation.

use std::{fmt, str::FromStr};

use anyhow::{Result, anyhow, bail};
use message_domain::{
    ContractDescriptor, CorrelationId, EmittedAt, EncodedMessage, MessageAuthority, MessageId,
    MessageKind, MessageMetadata, MessageName, MessageSchemaVersion, MessageScope, MessageTarget,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use tenancy_domain::{AssignmentEpoch, CellId, TenantId};
use time::{Duration, OffsetDateTime};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct QualificationOperationId(MessageId);

impl QualificationOperationId {
    fn deterministic(series: u16, index: u64) -> Result<Self> {
        deterministic_message_id(series, index).map(Self)
    }
}

impl Serialize for QualificationOperationId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for QualificationOperationId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        MessageId::from_str(&text)
            .map(Self)
            .map_err(|_| de::Error::custom("operation_id must be UUIDv7"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProbeValue(String);

impl ProbeValue {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if !(1..=256).contains(&value.chars().count()) {
            bail!("probe_value must contain 1 through 256 Unicode scalar values");
        }
        Ok(Self(value))
    }
}

impl Serialize for ProbeValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ProbeValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QualificationProbeRequestedV1 {
    operation_id: QualificationOperationId,
    probe_value: ProbeValue,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QualificationProbeObservedV1 {
    operation_id: QualificationOperationId,
    accepted: bool,
}

pub(crate) fn requested_descriptor() -> Result<ContractDescriptor> {
    descriptor(MessageKind::Command, "edtech.qualification.probe.requested")
}

pub(crate) fn observed_descriptor() -> Result<ContractDescriptor> {
    descriptor(MessageKind::Event, "edtech.qualification.probe.observed")
}

fn descriptor(kind: MessageKind, name: &str) -> Result<ContractDescriptor> {
    Ok(ContractDescriptor::new(
        kind,
        MessageName::from_str(name)
            .map_err(|_| anyhow!("qualification descriptor name invalid"))?,
        MessageSchemaVersion::new(1)
            .map_err(|_| anyhow!("qualification descriptor version invalid"))?,
    ))
}

pub(crate) fn deterministic_tenant_id(index: u32) -> Result<TenantId> {
    if index == 0 {
        bail!("qualification tenant index must be non-zero");
    }
    TenantId::from_str(&format!("01890f47-7cc2-7000-8001-{index:012x}"))
        .map_err(|_| anyhow!("deterministic tenant identity invalid"))
}

pub(crate) fn deterministic_message_id(series: u16, index: u64) -> Result<MessageId> {
    let text = format!(
        "01890f47-7cc2-7{:03x}-8{:03x}-{:012x}",
        series & 0x0fff,
        series.wrapping_mul(17) & 0x0fff,
        index & 0x0000_ffff_ffff_ffff
    );
    MessageId::from_str(&text).map_err(|_| anyhow!("deterministic message identity invalid"))
}

fn deterministic_correlation_id(series: u16, index: u64) -> Result<CorrelationId> {
    let id = deterministic_message_id(series, index)?;
    CorrelationId::new(id.into_uuid())
        .map_err(|_| anyhow!("deterministic correlation identity invalid"))
}

fn deterministic_emitted_at(index: u64) -> Result<EmittedAt> {
    let micros = i64::try_from(index % 1_000_000)
        .map_err(|_| anyhow!("deterministic timestamp index invalid"))?;
    EmittedAt::new(
        OffsetDateTime::UNIX_EPOCH
            + Duration::seconds(1_700_000_000)
            + Duration::microseconds(micros),
    )
    .map_err(|_| anyhow!("deterministic timestamp invalid"))
}

pub(crate) fn platform_command(index: u64, tenant_index: u32) -> Result<EncodedMessage> {
    platform_command_for(index, tenant_index, 1, "cell-001", "cell-001")
}

pub(crate) fn platform_command_for(
    index: u64,
    tenant_index: u32,
    epoch: u64,
    scope_cell: &str,
    target_cell: &str,
) -> Result<EncodedMessage> {
    let scope_cell =
        CellId::from_str(scope_cell).map_err(|_| anyhow!("scope Cell fixture invalid"))?;
    let target_cell =
        CellId::from_str(target_cell).map_err(|_| anyhow!("target Cell fixture invalid"))?;
    let metadata = MessageMetadata::new(
        deterministic_message_id(0x101, index)?,
        requested_descriptor()?,
        deterministic_emitted_at(index)?,
        MessageAuthority::Platform,
        MessageScope::Tenant {
            tenant_id: deterministic_tenant_id(tenant_index)?,
            cell_id: scope_cell,
            assignment_epoch: AssignmentEpoch::new(epoch)
                .map_err(|_| anyhow!("qualification assignment epoch invalid"))?,
        },
        Some(MessageTarget::Cell(target_cell)),
        deterministic_correlation_id(0x301, index)?,
        None,
    )
    .map_err(|_| anyhow!("qualification Platform metadata invalid"))?;
    let payload = QualificationProbeRequestedV1 {
        operation_id: QualificationOperationId::deterministic(0x501, index)?,
        probe_value: ProbeValue::new(format!("probe-{index:012x}-{}", "x".repeat(220)))?,
    };
    message_codec_json::encode(&metadata, &payload)
        .map_err(|_| anyhow!("qualification Platform encoding failed"))
}

pub(crate) fn cell_event(
    index: u64,
    tenant_index: u32,
    causation_id: MessageId,
) -> Result<EncodedMessage> {
    cell_event_for(index, tenant_index, 1, "cell-001", causation_id)
}

pub(crate) fn cell_event_for(
    index: u64,
    tenant_index: u32,
    epoch: u64,
    cell_id: &str,
    causation_id: MessageId,
) -> Result<EncodedMessage> {
    let cell = CellId::from_str(cell_id).map_err(|_| anyhow!("Cell fixture invalid"))?;
    let metadata = MessageMetadata::new(
        deterministic_message_id(0x201, index)?,
        observed_descriptor()?,
        deterministic_emitted_at(index.saturating_add(1))?,
        MessageAuthority::Cell(cell.clone()),
        MessageScope::Tenant {
            tenant_id: deterministic_tenant_id(tenant_index)?,
            cell_id: cell,
            assignment_epoch: AssignmentEpoch::new(epoch)
                .map_err(|_| anyhow!("qualification assignment epoch invalid"))?,
        },
        None,
        deterministic_correlation_id(0x301, index)?,
        Some(causation_id),
    )
    .map_err(|_| anyhow!("qualification Cell metadata invalid"))?;
    let payload = QualificationProbeObservedV1 {
        operation_id: QualificationOperationId::deterministic(0x501, index)?,
        accepted: true,
    };
    message_codec_json::encode(&metadata, &payload)
        .map_err(|_| anyhow!("qualification Cell encoding failed"))
}

pub(crate) fn altered_payload_same_identity(message: &EncodedMessage) -> Result<EncodedMessage> {
    match message.metadata().descriptor().kind() {
        MessageKind::Command => {
            let payload = QualificationProbeRequestedV1 {
                operation_id: QualificationOperationId::deterministic(0x601, 1)?,
                probe_value: ProbeValue::new("changed-but-valid")?,
            };
            message_codec_json::encode(message.metadata(), &payload)
                .map_err(|_| anyhow!("altered command encoding failed"))
        }
        MessageKind::Event => {
            let payload = QualificationProbeObservedV1 {
                operation_id: QualificationOperationId::deterministic(0x601, 2)?,
                accepted: false,
            };
            message_codec_json::encode(message.metadata(), &payload)
                .map_err(|_| anyhow!("altered event encoding failed"))
        }
    }
}

pub(crate) fn decode_requested(message: &EncodedMessage) -> Result<()> {
    message_codec_json::decode_typed::<QualificationProbeRequestedV1>(
        message,
        &requested_descriptor()?,
    )
    .map(|_| ())
    .map_err(|_| anyhow!("typed requested contract decode failed"))
}

pub(crate) fn decode_observed(message: &EncodedMessage) -> Result<()> {
    message_codec_json::decode_typed::<QualificationProbeObservedV1>(
        message,
        &observed_descriptor()?,
    )
    .map(|_| ())
    .map_err(|_| anyhow!("typed observed contract decode failed"))
}

impl fmt::Display for QualificationOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_identity_and_timestamp_generation_is_stable() {
        assert_eq!(
            deterministic_message_id(0x101, 42)
                .map(|value| value.to_string())
                .ok()
                .as_deref(),
            Some("01890f47-7cc2-7101-8111-00000000002a")
        );
        let first = platform_command(42, 1)
            .unwrap_or_else(|error| panic!("first deterministic message: {error}"));
        let second = platform_command(42, 1)
            .unwrap_or_else(|error| panic!("second deterministic message: {error}"));
        assert_eq!(first, second);
    }

    #[test]
    fn qualification_payload_bounds_are_enforced() {
        assert!(ProbeValue::new("").is_err());
        assert!(ProbeValue::new("x".repeat(256)).is_ok());
        assert!(ProbeValue::new("x".repeat(257)).is_err());
    }
}
