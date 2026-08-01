//! Canonical JSON envelope version 1 encoding and strict typed decoding.
//!
//! This crate owns wire representation only. It must not know about databases, async runtimes,
//! logging, configuration, secrets, application workflows, migrations, or message transports.

use std::{fmt, str::FromStr};

use message_domain::{
    ContractDescriptor, CorrelationId, EmittedAt, EncodedMessage, MAX_ENCODED_MESSAGE_BYTES,
    MessageAuthority, MessageId, MessageKind, MessageMetadata, MessageName, MessageSchemaVersion,
    MessageScope, MessageTarget,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tenancy_domain::{AssignmentEpoch, CellId, TenantId};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339, macros::format_description};

const ENVELOPE_VERSION: u32 = 1;
const CANONICAL_TIMESTAMP: &[time::format_description::BorrowedFormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]Z");

/// Stable, content-free codec error categories.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CodecError {
    /// The input exceeded the envelope byte bound before parsing.
    #[error("message codec error: envelope_too_large")]
    EnvelopeTooLarge,
    /// JSON structure, required fields, duplicate fields, or unknown fields are invalid.
    #[error("message codec error: invalid_envelope")]
    InvalidEnvelope,
    /// The envelope version is unsupported.
    #[error("message codec error: unsupported envelope_version")]
    UnsupportedEnvelopeVersion,
    /// A named metadata field is invalid.
    #[error("message codec error: invalid {0}")]
    InvalidField(&'static str),
    /// The payload root is not a JSON object.
    #[error("message codec error: invalid payload root")]
    InvalidPayloadRoot,
    /// The envelope descriptor is not the exact descriptor requested by the consumer.
    #[error("message codec error: contract mismatch")]
    ContractMismatch,
    /// Typed payload serialization or deserialization failed.
    #[error("message codec error: invalid typed payload")]
    InvalidTypedPayload,
    /// Bounded encoded-message construction failed.
    #[error("message codec error: encoded message bounds")]
    EncodedMessageBounds,
}

/// A typed decoded payload paired with its validated metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedMessage<T> {
    metadata: MessageMetadata,
    payload: T,
}

impl<T> DecodedMessage<T> {
    /// Returns the validated metadata.
    #[must_use]
    pub const fn metadata(&self) -> &MessageMetadata {
        &self.metadata
    }

    /// Returns the typed payload.
    #[must_use]
    pub const fn payload(&self) -> &T {
        &self.payload
    }

    /// Separates the validated metadata and typed payload.
    #[must_use]
    pub fn into_parts(self) -> (MessageMetadata, T) {
        (self.metadata, self.payload)
    }
}

#[derive(Serialize)]
struct EncodeEnvelope<'a, T> {
    envelope_version: u32,
    message_id: String,
    message_kind: &'static str,
    message_name: &'a str,
    message_schema_version: u32,
    emitted_at: String,
    source: EncodeSource<'a>,
    scope: EncodeScope<'a>,
    target: Option<EncodeTarget<'a>>,
    correlation_id: String,
    causation_id: Option<String>,
    payload: &'a T,
}

#[derive(Serialize)]
struct EncodeSource<'a> {
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cell_id: Option<&'a str>,
}

#[derive(Serialize)]
struct EncodeScope<'a> {
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cell_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    assignment_epoch: Option<String>,
}

#[derive(Serialize)]
struct EncodeTarget<'a> {
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cell_id: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecodeEnvelope {
    envelope_version: u32,
    message_id: String,
    message_kind: String,
    message_name: String,
    message_schema_version: u32,
    emitted_at: String,
    source: DecodeSource,
    scope: DecodeScope,
    target: Value,
    correlation_id: String,
    causation_id: Value,
    payload: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecodeSource {
    kind: String,
    cell_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecodeScope {
    kind: String,
    cell_id: Option<String>,
    tenant_id: Option<String>,
    assignment_epoch: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecodeTarget {
    kind: String,
    cell_id: Option<String>,
}

/// Encodes a typed object payload using canonical envelope version 1 field order.
///
/// # Errors
///
/// Returns a safe error for a non-object payload, serialization failure, invalid timestamp, or
/// encoded envelope exceeding the fixed byte bound.
pub fn encode<T: Serialize>(
    metadata: &MessageMetadata,
    payload: &T,
) -> Result<EncodedMessage, CodecError> {
    let payload_shape =
        serde_json::to_value(payload).map_err(|_| CodecError::InvalidTypedPayload)?;
    if !payload_shape.is_object() {
        return Err(CodecError::InvalidPayloadRoot);
    }
    let envelope = EncodeEnvelope {
        envelope_version: ENVELOPE_VERSION,
        message_id: metadata.message_id().to_string(),
        message_kind: encode_kind(metadata.descriptor().kind()),
        message_name: metadata.descriptor().name().as_str(),
        message_schema_version: metadata.descriptor().schema_version().get(),
        emitted_at: format_emitted_at(metadata.emitted_at())?,
        source: encode_source(metadata.source()),
        scope: encode_scope(metadata.scope()),
        target: metadata.target().map(encode_target),
        correlation_id: metadata.correlation_id().to_string(),
        causation_id: metadata.causation_id().map(|value| value.to_string()),
        payload,
    };
    let bytes = serde_json::to_vec(&envelope).map_err(|_| CodecError::InvalidTypedPayload)?;
    if bytes.len() > MAX_ENCODED_MESSAGE_BYTES {
        return Err(CodecError::EnvelopeTooLarge);
    }
    EncodedMessage::new(metadata.clone(), bytes).map_err(|_| CodecError::EncodedMessageBounds)
}

/// Strictly validates canonical envelope bytes and returns a bounded encoded message.
///
/// # Errors
///
/// Fails closed for oversized, malformed, unsupported, or cross-field-invalid envelopes.
pub fn decode_envelope(bytes: &[u8]) -> Result<EncodedMessage, CodecError> {
    if bytes.len() > MAX_ENCODED_MESSAGE_BYTES {
        return Err(CodecError::EnvelopeTooLarge);
    }
    let (metadata, _) = parse(bytes)?;
    EncodedMessage::new(metadata, bytes.to_vec()).map_err(|_| CodecError::EncodedMessageBounds)
}

/// Strictly validates envelope metadata without exposing an untyped payload.
///
/// # Errors
///
/// Fails closed for an invalid envelope.
pub fn decode_metadata(bytes: &[u8]) -> Result<MessageMetadata, CodecError> {
    if bytes.len() > MAX_ENCODED_MESSAGE_BYTES {
        return Err(CodecError::EnvelopeTooLarge);
    }
    parse(bytes).map(|(metadata, _)| metadata)
}

/// Decodes a typed payload only when its descriptor exactly matches the expected contract.
///
/// # Errors
///
/// Returns [`CodecError::ContractMismatch`] before payload decoding for the wrong contract and a
/// safe payload category for typed deserialization failures.
pub fn decode_typed<T: DeserializeOwned>(
    encoded: &EncodedMessage,
    expected: &ContractDescriptor,
) -> Result<DecodedMessage<T>, CodecError> {
    let (metadata, payload) = parse(encoded.as_bytes())?;
    if &metadata != encoded.metadata() {
        return Err(CodecError::InvalidField("metadata"));
    }
    if metadata.descriptor() != expected {
        return Err(CodecError::ContractMismatch);
    }
    let payload = serde_json::from_value(payload).map_err(|_| CodecError::InvalidTypedPayload)?;
    Ok(DecodedMessage { metadata, payload })
}

fn parse(bytes: &[u8]) -> Result<(MessageMetadata, Value), CodecError> {
    let envelope: DecodeEnvelope =
        serde_json::from_slice(bytes).map_err(|_| CodecError::InvalidEnvelope)?;
    if envelope.envelope_version != ENVELOPE_VERSION {
        return Err(CodecError::UnsupportedEnvelopeVersion);
    }
    if !envelope.payload.is_object() {
        return Err(CodecError::InvalidPayloadRoot);
    }
    let message_id = MessageId::from_str(&envelope.message_id)
        .map_err(|_| CodecError::InvalidField("message_id"))?;
    let kind = decode_kind(&envelope.message_kind)?;
    let name = MessageName::from_str(&envelope.message_name)
        .map_err(|_| CodecError::InvalidField("message_name"))?;
    let schema_version = MessageSchemaVersion::new(envelope.message_schema_version)
        .map_err(|_| CodecError::InvalidField("message_schema_version"))?;
    let emitted_at = parse_emitted_at(&envelope.emitted_at)?;
    let source = decode_source(envelope.source)?;
    let scope = decode_scope(envelope.scope)?;
    let target = decode_optional_target(envelope.target)?;
    let correlation_id = CorrelationId::from_str(&envelope.correlation_id)
        .map_err(|_| CodecError::InvalidField("correlation_id"))?;
    let causation_id = decode_optional_message_id(&envelope.causation_id)?;
    let descriptor = ContractDescriptor::new(kind, name, schema_version);
    let metadata = MessageMetadata::new(
        message_id,
        descriptor,
        emitted_at,
        source,
        scope,
        target,
        correlation_id,
        causation_id,
    )
    .map_err(|_| CodecError::InvalidField("metadata"))?;
    Ok((metadata, envelope.payload))
}

const fn encode_kind(kind: MessageKind) -> &'static str {
    match kind {
        MessageKind::Command => "command",
        MessageKind::Event => "event",
    }
}

fn decode_kind(value: &str) -> Result<MessageKind, CodecError> {
    match value {
        "command" => Ok(MessageKind::Command),
        "event" => Ok(MessageKind::Event),
        _ => Err(CodecError::InvalidField("message_kind")),
    }
}

fn encode_source(source: &MessageAuthority) -> EncodeSource<'_> {
    match source {
        MessageAuthority::Platform => EncodeSource {
            kind: "platform",
            cell_id: None,
        },
        MessageAuthority::Cell(cell_id) => EncodeSource {
            kind: "cell",
            cell_id: Some(cell_id.as_str()),
        },
    }
}

fn decode_source(source: DecodeSource) -> Result<MessageAuthority, CodecError> {
    match (source.kind.as_str(), source.cell_id) {
        ("platform", None) => Ok(MessageAuthority::Platform),
        ("cell", Some(cell_id)) => CellId::from_str(&cell_id)
            .map(MessageAuthority::Cell)
            .map_err(|_| CodecError::InvalidField("source.cell_id")),
        _ => Err(CodecError::InvalidField("source")),
    }
}

fn encode_scope(scope: &MessageScope) -> EncodeScope<'_> {
    match scope {
        MessageScope::Platform => EncodeScope {
            kind: "platform",
            cell_id: None,
            tenant_id: None,
            assignment_epoch: None,
        },
        MessageScope::Cell(cell_id) => EncodeScope {
            kind: "cell",
            cell_id: Some(cell_id.as_str()),
            tenant_id: None,
            assignment_epoch: None,
        },
        MessageScope::Tenant {
            tenant_id,
            cell_id,
            assignment_epoch,
        } => EncodeScope {
            kind: "tenant",
            cell_id: Some(cell_id.as_str()),
            tenant_id: Some(tenant_id.to_string()),
            assignment_epoch: Some(assignment_epoch.to_string()),
        },
    }
}

fn decode_scope(scope: DecodeScope) -> Result<MessageScope, CodecError> {
    match (
        scope.kind.as_str(),
        scope.cell_id,
        scope.tenant_id,
        scope.assignment_epoch,
    ) {
        ("platform", None, None, None) => Ok(MessageScope::Platform),
        ("cell", Some(cell_id), None, None) => CellId::from_str(&cell_id)
            .map(MessageScope::Cell)
            .map_err(|_| CodecError::InvalidField("scope.cell_id")),
        ("tenant", Some(cell_id), Some(tenant_id), Some(epoch)) => {
            let cell_id = CellId::from_str(&cell_id)
                .map_err(|_| CodecError::InvalidField("scope.cell_id"))?;
            let tenant_id = TenantId::from_str(&tenant_id)
                .map_err(|_| CodecError::InvalidField("scope.tenant_id"))?;
            let epoch = epoch
                .parse::<u64>()
                .ok()
                .and_then(|value| AssignmentEpoch::new(value).ok())
                .ok_or(CodecError::InvalidField("scope.assignment_epoch"))?;
            Ok(MessageScope::Tenant {
                tenant_id,
                cell_id,
                assignment_epoch: epoch,
            })
        }
        _ => Err(CodecError::InvalidField("scope")),
    }
}

fn encode_target(target: &MessageTarget) -> EncodeTarget<'_> {
    match target {
        MessageTarget::Platform => EncodeTarget {
            kind: "platform",
            cell_id: None,
        },
        MessageTarget::Cell(cell_id) => EncodeTarget {
            kind: "cell",
            cell_id: Some(cell_id.as_str()),
        },
    }
}

fn decode_optional_target(value: Value) -> Result<Option<MessageTarget>, CodecError> {
    if value.is_null() {
        return Ok(None);
    }
    let target: DecodeTarget =
        serde_json::from_value(value).map_err(|_| CodecError::InvalidField("target"))?;
    match (target.kind.as_str(), target.cell_id) {
        ("platform", None) => Ok(Some(MessageTarget::Platform)),
        ("cell", Some(cell_id)) => CellId::from_str(&cell_id)
            .map(MessageTarget::Cell)
            .map(Some)
            .map_err(|_| CodecError::InvalidField("target.cell_id")),
        _ => Err(CodecError::InvalidField("target")),
    }
}

fn decode_optional_message_id(value: &Value) -> Result<Option<MessageId>, CodecError> {
    if value.is_null() {
        return Ok(None);
    }
    let text = value
        .as_str()
        .ok_or(CodecError::InvalidField("causation_id"))?;
    MessageId::from_str(text)
        .map(Some)
        .map_err(|_| CodecError::InvalidField("causation_id"))
}

fn format_emitted_at(value: EmittedAt) -> Result<String, CodecError> {
    value
        .as_offset_date_time()
        .format(CANONICAL_TIMESTAMP)
        .map_err(|_| CodecError::InvalidField("emitted_at"))
}

fn parse_emitted_at(value: &str) -> Result<EmittedAt, CodecError> {
    let bytes = value.as_bytes();
    if bytes.len() != 27
        || bytes.get(19) != Some(&b'.')
        || bytes.get(26) != Some(&b'Z')
        || !bytes[20..26].iter().all(u8::is_ascii_digit)
    {
        return Err(CodecError::InvalidField("emitted_at"));
    }
    let timestamp = OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|_| CodecError::InvalidField("emitted_at"))?;
    let emitted = EmittedAt::new(timestamp).map_err(|_| CodecError::InvalidField("emitted_at"))?;
    if format_emitted_at(emitted)?.as_str() != value {
        return Err(CodecError::InvalidField("emitted_at"));
    }
    Ok(emitted)
}

impl<T: fmt::Debug> fmt::Display for DecodedMessage<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "decoded message {} (payload=[REDACTED])",
            self.metadata.message_id()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use message_domain::{MESSAGE_CONTENT_TYPE, MessageMetadataError};
    use serde::{Deserialize, Serialize};
    use time::{Duration, OffsetDateTime};

    use super::*;

    const MESSAGE_ID: &str = "01890f47-7cc2-7a1b-8d5d-7f6ebc9c2001";
    const CORRELATION_ID: &str = "01890f47-7cc2-7a1b-8d5d-7f6ebc9c2002";
    const TENANT_ID: &str = "01890f47-7cc2-7a1b-8d5d-7f6ebc9c2003";

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[serde(deny_unknown_fields)]
    struct ProbeRequested {
        operation_id: String,
        probe_value: String,
    }

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[serde(deny_unknown_fields)]
    struct ProbeObserved {
        operation_id: String,
        accepted: bool,
    }

    fn command_metadata(epoch: u64) -> Result<MessageMetadata, MessageMetadataError> {
        let cell =
            CellId::from_str("cell-001").unwrap_or_else(|error| panic!("fixture cell: {error}"));
        MessageMetadata::new(
            MessageId::from_str(MESSAGE_ID).unwrap_or_else(|error| panic!("fixture id: {error}")),
            ContractDescriptor::new(
                MessageKind::Command,
                MessageName::from_str("edtech.qualification.probe.requested")
                    .unwrap_or_else(|error| panic!("fixture name: {error}")),
                MessageSchemaVersion::new(1)
                    .unwrap_or_else(|error| panic!("fixture version: {error}")),
            ),
            EmittedAt::new(
                OffsetDateTime::UNIX_EPOCH
                    + Duration::seconds(1_700_000_000)
                    + Duration::nanoseconds(123_456_789),
            )
            .unwrap_or_else(|error| panic!("fixture time: {error}")),
            MessageAuthority::Platform,
            MessageScope::Tenant {
                tenant_id: TenantId::from_str(TENANT_ID)
                    .unwrap_or_else(|error| panic!("fixture tenant: {error:?}")),
                cell_id: cell.clone(),
                assignment_epoch: AssignmentEpoch::new(epoch)
                    .unwrap_or_else(|error| panic!("fixture epoch: {error}")),
            },
            Some(MessageTarget::Cell(cell)),
            CorrelationId::from_str(CORRELATION_ID)
                .unwrap_or_else(|error| panic!("fixture id: {error}")),
            None,
        )
    }

    fn payload() -> ProbeRequested {
        ProbeRequested {
            operation_id: String::from("01890f47-7cc2-7a1b-8d5d-7f6ebc9c2004"),
            probe_value: String::from("qualification-probe"),
        }
    }

    #[test]
    fn command_encoding_is_canonical_and_preserves_full_epoch() {
        let metadata =
            command_metadata(u64::MAX).unwrap_or_else(|error| panic!("metadata: {error}"));
        let encoded =
            encode(&metadata, &payload()).unwrap_or_else(|error| panic!("encode: {error}"));
        let text = std::str::from_utf8(encoded.as_bytes())
            .unwrap_or_else(|error| panic!("canonical UTF-8: {error}"));
        assert!(text.starts_with(
            "{\"envelope_version\":1,\"message_id\":\"01890f47-7cc2-7a1b-8d5d-7f6ebc9c2001\",\"message_kind\":\"command\",\"message_name\":"
        ));
        assert!(text.contains("\"emitted_at\":\"2023-11-14T22:13:20.123456Z\""));
        assert!(text.contains("\"assignment_epoch\":\"18446744073709551615\""));
        assert_eq!(encoded.content_type(), MESSAGE_CONTENT_TYPE);
        let decoded = decode_typed::<ProbeRequested>(&encoded, metadata.descriptor())
            .unwrap_or_else(|error| panic!("decode: {error}"));
        assert_eq!(decoded.payload(), &payload());
        assert_eq!(
            decoded
                .metadata()
                .scope()
                .tenant_fence()
                .map(|(_, _, epoch)| epoch.get()),
            Some(u64::MAX)
        );
    }

    #[test]
    fn strict_decode_rejects_shape_version_and_contract_errors() {
        let metadata = command_metadata(1).unwrap_or_else(|error| panic!("metadata: {error}"));
        let encoded =
            encode(&metadata, &payload()).unwrap_or_else(|error| panic!("encode: {error}"));
        let text = std::str::from_utf8(encoded.as_bytes())
            .unwrap_or_else(|error| panic!("UTF-8: {error}"));
        let unknown = text.replacen('{', "{\"unknown\":true,", 1);
        assert_eq!(
            decode_envelope(unknown.as_bytes()),
            Err(CodecError::InvalidEnvelope)
        );
        let duplicate = text.replacen('{', "{\"envelope_version\":1,", 1);
        assert_eq!(
            decode_envelope(duplicate.as_bytes()),
            Err(CodecError::InvalidEnvelope)
        );
        let unsupported = text.replacen("\"envelope_version\":1", "\"envelope_version\":2", 1);
        assert_eq!(
            decode_envelope(unsupported.as_bytes()),
            Err(CodecError::UnsupportedEnvelopeVersion)
        );
        for invalid_payload in ["null", "[]", "\"text\"", "1"] {
            let changed = text.replace(
                "{\"operation_id\":\"01890f47-7cc2-7a1b-8d5d-7f6ebc9c2004\",\"probe_value\":\"qualification-probe\"}",
                invalid_payload,
            );
            assert_eq!(
                decode_envelope(changed.as_bytes()),
                Err(CodecError::InvalidPayloadRoot)
            );
        }
        let wrong = ContractDescriptor::new(
            MessageKind::Event,
            MessageName::from_str("edtech.qualification.probe.observed")
                .unwrap_or_else(|error| panic!("name: {error}")),
            MessageSchemaVersion::new(1).unwrap_or_else(|error| panic!("version: {error}")),
        );
        assert_eq!(
            decode_typed::<ProbeRequested>(&encoded, &wrong),
            Err(CodecError::ContractMismatch)
        );
    }

    #[test]
    fn payload_unknown_fields_and_oversized_input_fail_closed() {
        let metadata = command_metadata(1).unwrap_or_else(|error| panic!("metadata: {error}"));
        let encoded =
            encode(&metadata, &payload()).unwrap_or_else(|error| panic!("encode: {error}"));
        let changed = std::str::from_utf8(encoded.as_bytes())
            .unwrap_or_else(|error| panic!("UTF-8: {error}"))
            .replace(
                "\"probe_value\":\"qualification-probe\"",
                "\"probe_value\":\"qualification-probe\",\"unknown\":true",
            );
        let changed = decode_envelope(changed.as_bytes())
            .unwrap_or_else(|error| panic!("envelope remains structurally valid: {error}"));
        assert_eq!(
            decode_typed::<ProbeRequested>(&changed, metadata.descriptor()),
            Err(CodecError::InvalidTypedPayload)
        );
        assert_eq!(
            decode_envelope(&vec![b'x'; MAX_ENCODED_MESSAGE_BYTES + 1]),
            Err(CodecError::EnvelopeTooLarge)
        );
    }

    #[test]
    fn codec_errors_and_debug_do_not_echo_payloads() {
        let sentinel = "private-secret-sentinel";
        let error = decode_envelope(sentinel.as_bytes())
            .err()
            .unwrap_or_else(|| panic!("invalid envelope must fail"));
        assert!(!error.to_string().contains(sentinel));
        assert!(!format!("{error:?}").contains(sentinel));
        let scalar = encode(
            &command_metadata(1)
                .unwrap_or_else(|metadata_error| panic!("metadata: {metadata_error}")),
            &sentinel,
        );
        assert_eq!(scalar, Err(CodecError::InvalidPayloadRoot));
    }

    #[test]
    fn checked_in_fixtures_round_trip_as_exact_canonical_bytes() {
        let fixture_directory =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/contracts/fixtures");
        let command_bytes = std::fs::read(fixture_directory.join("qualification-command-v1.json"))
            .unwrap_or_else(|error| panic!("read command fixture: {error}"));
        let event_bytes = std::fs::read(fixture_directory.join("qualification-event-v1.json"))
            .unwrap_or_else(|error| panic!("read event fixture: {error}"));
        assert!(command_bytes.ends_with(b"\n"));
        assert!(event_bytes.ends_with(b"\n"));
        assert!(!command_bytes[..command_bytes.len() - 1].ends_with(b"\n"));
        assert!(!event_bytes[..event_bytes.len() - 1].ends_with(b"\n"));

        let command_canonical = &command_bytes[..command_bytes.len() - 1];
        let command = decode_envelope(command_canonical)
            .unwrap_or_else(|error| panic!("decode command fixture: {error}"));
        let command_typed =
            decode_typed::<ProbeRequested>(&command, command.metadata().descriptor())
                .unwrap_or_else(|error| panic!("typed command fixture: {error}"));
        assert!(MessageId::from_str(&command_typed.payload.operation_id).is_ok());
        assert!((1..=256).contains(&command_typed.payload.probe_value.chars().count()));
        let command_reencoded = encode(command_typed.metadata(), command_typed.payload())
            .unwrap_or_else(|error| panic!("re-encode command fixture: {error}"));
        assert_eq!(command_reencoded.as_bytes(), command_canonical);
        assert_eq!(
            command
                .metadata()
                .scope()
                .tenant_fence()
                .map(|(_, _, epoch)| epoch.get()),
            Some(u64::MAX)
        );

        let event_canonical = &event_bytes[..event_bytes.len() - 1];
        let event = decode_envelope(event_canonical)
            .unwrap_or_else(|error| panic!("decode event fixture: {error}"));
        let event_typed = decode_typed::<ProbeObserved>(&event, event.metadata().descriptor())
            .unwrap_or_else(|error| panic!("typed event fixture: {error}"));
        assert!(MessageId::from_str(&event_typed.payload.operation_id).is_ok());
        assert!(event_typed.payload.accepted);
        let event_reencoded = encode(event_typed.metadata(), event_typed.payload())
            .unwrap_or_else(|error| panic!("re-encode event fixture: {error}"));
        assert_eq!(event_reencoded.as_bytes(), event_canonical);
        assert_ne!(
            command.metadata().descriptor(),
            event.metadata().descriptor()
        );
    }
}
