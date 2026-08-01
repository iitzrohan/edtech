//! Provider-neutral message identities, contract metadata, authority fences, and encoded bytes.
//!
//! This crate owns validated message concepts. It must not know about serialization, databases,
//! async runtimes, logging, configuration, secrets, application workflows, or transports.

use std::{fmt, num::NonZeroU32, str::FromStr};

use tenancy_domain::{AssignmentEpoch, CellId, TenantId};
use thiserror::Error;
use time::{OffsetDateTime, UtcOffset};
use uuid::Uuid;

/// Canonical media type for envelope version 1.
pub const MESSAGE_CONTENT_TYPE: &str = "application/vnd.edtech.message+json;version=1";
/// Inclusive maximum encoded envelope size.
pub const MAX_ENCODED_MESSAGE_BYTES: usize = 262_144;
/// Inclusive minimum encoded envelope size.
pub const MIN_ENCODED_MESSAGE_BYTES: usize = 2;

/// A UUID message-identity validation failure.
#[derive(Debug, Error)]
pub enum MessageIdentifierError {
    /// The supplied text is not a UUID.
    #[error("message identifier is not a valid UUID")]
    InvalidUuid(#[from] uuid::Error),
    /// The UUID does not use version 7.
    #[error("message identifier must use UUID version 7")]
    WrongVersion,
}

macro_rules! message_uuid_v7 {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Uuid);

        impl $name {
            /// Constructs the identifier after verifying UUID version 7.
            ///
            /// # Errors
            ///
            /// Returns [`MessageIdentifierError::WrongVersion`] for another UUID version.
            pub fn new(value: Uuid) -> Result<Self, MessageIdentifierError> {
                if value.get_version_num() == 7 {
                    Ok(Self(value))
                } else {
                    Err(MessageIdentifierError::WrongVersion)
                }
            }

            /// Borrows the validated UUID.
            #[must_use]
            pub const fn as_uuid(&self) -> &Uuid {
                &self.0
            }

            /// Returns the validated UUID.
            #[must_use]
            pub const fn into_uuid(self) -> Uuid {
                self.0
            }
        }

        impl FromStr for $name {
            type Err = MessageIdentifierError;

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
                formatter.debug_tuple(stringify!($name)).field(&self.0).finish()
            }
        }
    };
}

message_uuid_v7!(
    /// Transport deduplication identity for one immutable message.
    MessageId
);
message_uuid_v7!(
    /// Correlates messages participating in one workflow.
    CorrelationId
);

/// A validated immutable message-contract name.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MessageName(String);

/// A message-name grammar failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MessageNameError {
    /// The name is outside the inclusive byte bound.
    #[error("message name must be 10 through 160 ASCII bytes")]
    InvalidLength,
    /// The required namespace prefix is missing.
    #[error("message name must begin with edtech")]
    InvalidPrefix,
    /// There are fewer than four dot-separated segments or an empty segment.
    #[error("message name has invalid segments")]
    InvalidSegments,
    /// A segment contains forbidden characters or boundaries.
    #[error("message name contains a forbidden character")]
    InvalidCharacter,
    /// The final segment looks like a schema-version suffix.
    #[error("message name must not end in a version suffix")]
    VersionSuffix,
}

impl MessageName {
    /// Validates the stable message-name grammar.
    ///
    /// # Errors
    ///
    /// Returns a stable category when the name violates the contract grammar.
    pub fn new(value: impl Into<String>) -> Result<Self, MessageNameError> {
        let value = value.into();
        if !(10..=160).contains(&value.len()) || !value.is_ascii() {
            return Err(MessageNameError::InvalidLength);
        }
        if !value.starts_with("edtech.") {
            return Err(MessageNameError::InvalidPrefix);
        }
        let segments = value.split('.').collect::<Vec<_>>();
        if segments.len() < 4 || segments.iter().any(|segment| segment.is_empty()) {
            return Err(MessageNameError::InvalidSegments);
        }
        for segment in &segments {
            let bytes = segment.as_bytes();
            if !(1..=40).contains(&bytes.len())
                || !bytes
                    .iter()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
                || !bytes
                    .first()
                    .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
                || !bytes
                    .last()
                    .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
                || bytes.windows(2).any(|window| window == b"--")
            {
                return Err(MessageNameError::InvalidCharacter);
            }
        }
        let Some(final_segment) = segments.last() else {
            return Err(MessageNameError::InvalidSegments);
        };
        if is_version_suffix(final_segment) {
            return Err(MessageNameError::VersionSuffix);
        }
        Ok(Self(value))
    }

    /// Returns the validated name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn is_version_suffix(value: &str) -> bool {
    value.strip_prefix('v').is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    }) || value.strip_prefix("version-").is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    })
}

impl FromStr for MessageName {
    type Err = MessageNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl fmt::Display for MessageName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for MessageName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("MessageName").field(&self.0).finish()
    }
}

/// A non-zero bounded payload schema version.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MessageSchemaVersion(NonZeroU32);

/// A schema version is outside 1 through 65,535.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("message schema version must be between 1 and 65535")]
pub struct InvalidMessageSchemaVersion;

impl MessageSchemaVersion {
    /// Constructs a checked schema version.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidMessageSchemaVersion`] outside 1 through 65,535.
    pub fn new(value: u32) -> Result<Self, InvalidMessageSchemaVersion> {
        if value > 65_535 {
            return Err(InvalidMessageSchemaVersion);
        }
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(InvalidMessageSchemaVersion)
    }

    /// Returns the numeric schema version.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl fmt::Display for MessageSchemaVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Whether a message requests intent or records a committed fact.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MessageKind {
    /// Requested intent targeting exactly one authority.
    Command,
    /// Immutable fact with no command-style target.
    Event,
}

/// A validated caller-supplied UTC message timestamp with microsecond precision.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EmittedAt(OffsetDateTime);

/// A timestamp violates the envelope time bounds.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EmittedAtError {
    /// The timestamp is earlier than the Unix epoch.
    #[error("emitted_at must not be before the Unix epoch")]
    BeforeUnixEpoch,
    /// The timestamp cannot be represented in the canonical supported range.
    #[error("emitted_at is outside the supported range")]
    OutOfRange,
}

impl EmittedAt {
    /// Normalizes a caller-supplied timestamp to UTC and truncates to microseconds.
    ///
    /// # Errors
    ///
    /// Rejects pre-epoch timestamps and calendar years above 9999.
    pub fn new(value: OffsetDateTime) -> Result<Self, EmittedAtError> {
        let value = value.to_offset(UtcOffset::UTC);
        if value.unix_timestamp_nanos() < 0 {
            return Err(EmittedAtError::BeforeUnixEpoch);
        }
        if value.year() > 9999 {
            return Err(EmittedAtError::OutOfRange);
        }
        let nanos = value.nanosecond() / 1_000 * 1_000;
        value
            .replace_nanosecond(nanos)
            .map(Self)
            .map_err(|_| EmittedAtError::OutOfRange)
    }

    /// Returns the normalized timestamp.
    #[must_use]
    pub const fn as_offset_date_time(self) -> OffsetDateTime {
        self.0
    }

    /// Returns exact microseconds since the Unix epoch.
    #[must_use]
    pub const fn unix_timestamp_micros(self) -> i128 {
        self.0.unix_timestamp_nanos() / 1_000
    }
}

impl fmt::Debug for EmittedAt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("EmittedAt")
            .field(&self.unix_timestamp_micros())
            .finish()
    }
}

/// Authority that committed or requested a message.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum MessageAuthority {
    /// The Platform authority.
    Platform,
    /// One logical Cell authority.
    Cell(CellId),
}

/// An authority/scope component is not a valid domain identifier or epoch.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("message authority or scope component is invalid")]
pub struct InvalidMessageScopeComponent;

impl MessageAuthority {
    /// Constructs a Cell authority from validated topology-neutral text.
    ///
    /// # Errors
    ///
    /// Rejects an invalid logical Cell identifier.
    pub fn cell(value: impl Into<String>) -> Result<Self, InvalidMessageScopeComponent> {
        CellId::new(value)
            .map(Self::Cell)
            .map_err(|_| InvalidMessageScopeComponent)
    }
}

/// Complete routing and tenant-fencing scope.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum MessageScope {
    /// Platform-wide scope.
    Platform,
    /// Scope local to one Cell.
    Cell(CellId),
    /// Scope for one assigned tenant in one Cell at one epoch.
    Tenant {
        /// Tenant identity.
        tenant_id: TenantId,
        /// Expected logical Cell.
        cell_id: CellId,
        /// Complete non-zero assignment fence.
        assignment_epoch: AssignmentEpoch,
    },
}

impl MessageScope {
    /// Constructs a Cell scope from topology-neutral text.
    ///
    /// # Errors
    ///
    /// Rejects an invalid logical Cell identifier.
    pub fn cell(value: impl Into<String>) -> Result<Self, InvalidMessageScopeComponent> {
        CellId::new(value)
            .map(Self::Cell)
            .map_err(|_| InvalidMessageScopeComponent)
    }

    /// Constructs a complete tenant scope from primitive provider values.
    ///
    /// # Errors
    ///
    /// Rejects a non-UUIDv7 tenant, invalid logical Cell, or zero epoch.
    pub fn tenant(
        tenant_id: Uuid,
        cell_id: impl Into<String>,
        assignment_epoch: u64,
    ) -> Result<Self, InvalidMessageScopeComponent> {
        Ok(Self::Tenant {
            tenant_id: TenantId::new(tenant_id).map_err(|_| InvalidMessageScopeComponent)?,
            cell_id: CellId::new(cell_id).map_err(|_| InvalidMessageScopeComponent)?,
            assignment_epoch: AssignmentEpoch::new(assignment_epoch)
                .map_err(|_| InvalidMessageScopeComponent)?,
        })
    }

    /// Returns the Cell fence when this scope is Cell-local.
    #[must_use]
    pub const fn cell_id(&self) -> Option<&CellId> {
        match self {
            Self::Platform => None,
            Self::Cell(cell_id) | Self::Tenant { cell_id, .. } => Some(cell_id),
        }
    }

    /// Returns the tenant fence when this is tenant scope.
    #[must_use]
    pub const fn tenant_fence(&self) -> Option<(TenantId, &CellId, AssignmentEpoch)> {
        match self {
            Self::Tenant {
                tenant_id,
                cell_id,
                assignment_epoch,
            } => Some((*tenant_id, cell_id, *assignment_epoch)),
            Self::Platform | Self::Cell(_) => None,
        }
    }
}

/// Exactly one authority targeted by a command.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum MessageTarget {
    /// The Platform authority.
    Platform,
    /// One logical Cell authority.
    Cell(CellId),
}

impl MessageTarget {
    /// Constructs a Cell command target from topology-neutral text.
    ///
    /// # Errors
    ///
    /// Rejects an invalid logical Cell identifier.
    pub fn cell(value: impl Into<String>) -> Result<Self, InvalidMessageScopeComponent> {
        CellId::new(value)
            .map(Self::Cell)
            .map_err(|_| InvalidMessageScopeComponent)
    }
}

/// Immutable contract identity accepted by a typed consumer.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ContractDescriptor {
    kind: MessageKind,
    name: MessageName,
    schema_version: MessageSchemaVersion,
}

impl ContractDescriptor {
    /// Constructs a descriptor from independently validated values.
    #[must_use]
    pub const fn new(
        kind: MessageKind,
        name: MessageName,
        schema_version: MessageSchemaVersion,
    ) -> Self {
        Self {
            kind,
            name,
            schema_version,
        }
    }

    /// Returns the command/event kind.
    #[must_use]
    pub const fn kind(&self) -> MessageKind {
        self.kind
    }

    /// Returns the stable contract name.
    #[must_use]
    pub const fn name(&self) -> &MessageName {
        &self.name
    }

    /// Returns the payload schema version.
    #[must_use]
    pub const fn schema_version(&self) -> MessageSchemaVersion {
        self.schema_version
    }
}

/// A cross-field message metadata validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MessageMetadataError {
    /// A command does not have exactly one target.
    #[error("command metadata requires a target")]
    CommandTargetRequired,
    /// An event incorrectly contains a target.
    #[error("event metadata forbids a target")]
    EventTargetForbidden,
    /// The direct predecessor is the message itself.
    #[error("causation identifier must differ from message identifier")]
    SelfCausation,
    /// A Cell source or target conflicts with the scope Cell.
    #[error("message Cell authority does not align with scope")]
    CellScopeMismatch,
    /// A Cell source uses Platform scope.
    #[error("Cell source requires Cell or Tenant scope")]
    InvalidCellSourceScope,
}

/// Fully validated, provider-neutral envelope metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageMetadata {
    message_id: MessageId,
    descriptor: ContractDescriptor,
    emitted_at: EmittedAt,
    source: MessageAuthority,
    scope: MessageScope,
    target: Option<MessageTarget>,
    correlation_id: CorrelationId,
    causation_id: Option<MessageId>,
}

impl MessageMetadata {
    /// Validates and constructs immutable message metadata.
    ///
    /// # Errors
    ///
    /// Rejects invalid command/event target rules, self-causation, and Cell-scope mismatches.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        message_id: MessageId,
        descriptor: ContractDescriptor,
        emitted_at: EmittedAt,
        source: MessageAuthority,
        scope: MessageScope,
        target: Option<MessageTarget>,
        correlation_id: CorrelationId,
        causation_id: Option<MessageId>,
    ) -> Result<Self, MessageMetadataError> {
        match (descriptor.kind(), target.as_ref()) {
            (MessageKind::Command, None) => {
                return Err(MessageMetadataError::CommandTargetRequired);
            }
            (MessageKind::Event, Some(_)) => {
                return Err(MessageMetadataError::EventTargetForbidden);
            }
            (MessageKind::Command, Some(_)) | (MessageKind::Event, None) => {}
        }
        if causation_id == Some(message_id) {
            return Err(MessageMetadataError::SelfCausation);
        }
        if matches!(source, MessageAuthority::Cell(_)) && matches!(scope, MessageScope::Platform) {
            return Err(MessageMetadataError::InvalidCellSourceScope);
        }
        if let MessageAuthority::Cell(source_cell) = &source
            && scope.cell_id() != Some(source_cell)
        {
            return Err(MessageMetadataError::CellScopeMismatch);
        }
        if let Some(MessageTarget::Cell(target_cell)) = &target
            && scope.cell_id() != Some(target_cell)
        {
            return Err(MessageMetadataError::CellScopeMismatch);
        }
        Ok(Self {
            message_id,
            descriptor,
            emitted_at,
            source,
            scope,
            target,
            correlation_id,
            causation_id,
        })
    }

    /// Returns the transport identity.
    #[must_use]
    pub const fn message_id(&self) -> MessageId {
        self.message_id
    }
    /// Returns the immutable contract descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &ContractDescriptor {
        &self.descriptor
    }
    /// Returns the normalized emission timestamp.
    #[must_use]
    pub const fn emitted_at(&self) -> EmittedAt {
        self.emitted_at
    }
    /// Returns the source authority.
    #[must_use]
    pub const fn source(&self) -> &MessageAuthority {
        &self.source
    }
    /// Returns the complete scope.
    #[must_use]
    pub const fn scope(&self) -> &MessageScope {
        &self.scope
    }
    /// Returns the command target, if any.
    #[must_use]
    pub const fn target(&self) -> Option<&MessageTarget> {
        self.target.as_ref()
    }
    /// Returns the correlation identity.
    #[must_use]
    pub const fn correlation_id(&self) -> CorrelationId {
        self.correlation_id
    }
    /// Returns the direct predecessor identity, if any.
    #[must_use]
    pub const fn causation_id(&self) -> Option<MessageId> {
        self.causation_id
    }
}

/// A bounded encoded-envelope validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EncodedMessageError {
    /// The byte representation is shorter than a JSON value.
    #[error("encoded message is smaller than the minimum envelope size")]
    TooSmall,
    /// The byte representation exceeds the contract bound.
    #[error("encoded message exceeds the maximum envelope size")]
    TooLarge,
}

/// Validated metadata paired with exact immutable canonical envelope bytes.
#[derive(Clone, Eq, PartialEq)]
pub struct EncodedMessage {
    metadata: MessageMetadata,
    bytes: Vec<u8>,
}

impl EncodedMessage {
    /// Constructs a bounded encoded message without interpreting its bytes.
    ///
    /// # Errors
    ///
    /// Rejects bytes outside 2 through 262,144 bytes.
    pub fn new(metadata: MessageMetadata, bytes: Vec<u8>) -> Result<Self, EncodedMessageError> {
        match bytes.len() {
            0 | 1 => Err(EncodedMessageError::TooSmall),
            length if length > MAX_ENCODED_MESSAGE_BYTES => Err(EncodedMessageError::TooLarge),
            _ => Ok(Self { metadata, bytes }),
        }
    }

    /// Returns the validated metadata.
    #[must_use]
    pub const fn metadata(&self) -> &MessageMetadata {
        &self.metadata
    }
    /// Borrows the exact immutable envelope bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Returns the fixed envelope media type.
    #[must_use]
    pub const fn content_type(&self) -> &'static str {
        MESSAGE_CONTENT_TYPE
    }
}

impl fmt::Debug for EncodedMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncodedMessage")
            .field("metadata", &self.metadata)
            .field("content_type", &MESSAGE_CONTENT_TYPE)
            .field("byte_length", &self.bytes.len())
            .field("envelope", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Display for EncodedMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "encoded message {} ({} bytes, envelope=[REDACTED])",
            self.message_id_for_display(),
            self.bytes.len()
        )
    }
}

impl EncodedMessage {
    fn message_id_for_display(&self) -> MessageId {
        self.metadata.message_id()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tenancy_domain::{AssignmentEpoch, CellId, TenantId};
    use time::{Duration, OffsetDateTime, UtcOffset};
    use uuid::Uuid;

    use super::*;

    const MESSAGE_UUID: &str = "01890f47-7cc2-7a1b-8d5d-7f6ebc9c1001";
    const CORRELATION_UUID: &str = "01890f47-7cc2-7a1b-8d5d-7f6ebc9c1002";
    const TENANT_UUID: &str = "01890f47-7cc2-7a1b-8d5d-7f6ebc9c1003";

    fn descriptor(kind: MessageKind) -> ContractDescriptor {
        ContractDescriptor::new(
            kind,
            MessageName::from_str("edtech.qualification.probe.requested")
                .unwrap_or_else(|error| panic!("fixture name: {error}")),
            MessageSchemaVersion::new(1).unwrap_or_else(|error| panic!("fixture version: {error}")),
        )
    }

    fn emitted() -> EmittedAt {
        EmittedAt::new(OffsetDateTime::UNIX_EPOCH + Duration::seconds(1))
            .unwrap_or_else(|error| panic!("fixture time: {error}"))
    }

    #[test]
    fn message_identifiers_require_uuid_v7() {
        assert!(MessageId::from_str(MESSAGE_UUID).is_ok());
        assert!(CorrelationId::from_str(CORRELATION_UUID).is_ok());
        let v4 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
            .unwrap_or_else(|error| panic!("static UUID: {error}"));
        assert!(matches!(
            MessageId::new(v4),
            Err(MessageIdentifierError::WrongVersion)
        ));
    }

    #[test]
    fn message_name_enforces_the_complete_grammar() {
        for accepted in [
            "edtech.platform.tenant.provision-requested",
            "edtech.cell.tenant.provisioned",
            "edtech.qualification.probe.requested",
        ] {
            assert!(MessageName::from_str(accepted).is_ok(), "{accepted}");
        }
        for rejected in [
            "Tenant.Created",
            "edtech.tenant.v1",
            "edtech..tenant.created",
            "edtech/tenant/created",
            "edtech.tenant_created",
            "tenant.created",
            "edtech.tenant.foo.version-1",
            "edtech.tenant.foo.two--hyphens",
        ] {
            assert!(MessageName::from_str(rejected).is_err(), "{rejected}");
        }
    }

    #[test]
    fn schema_version_is_bounded() {
        assert_eq!(
            MessageSchemaVersion::new(1)
                .ok()
                .map(MessageSchemaVersion::get),
            Some(1)
        );
        assert_eq!(
            MessageSchemaVersion::new(65_535)
                .ok()
                .map(MessageSchemaVersion::get),
            Some(65_535)
        );
        assert!(MessageSchemaVersion::new(0).is_err());
        assert!(MessageSchemaVersion::new(65_536).is_err());
    }

    #[test]
    fn emitted_at_normalizes_and_truncates_without_rounding() {
        let offset =
            UtcOffset::from_hms(5, 30, 0).unwrap_or_else(|error| panic!("static offset: {error}"));
        let supplied =
            (OffsetDateTime::UNIX_EPOCH + Duration::nanoseconds(1_234_567_890)).to_offset(offset);
        let normalized =
            EmittedAt::new(supplied).unwrap_or_else(|error| panic!("valid time: {error}"));
        assert_eq!(normalized.as_offset_date_time().offset(), UtcOffset::UTC);
        assert_eq!(normalized.unix_timestamp_micros(), 1_234_567);
        assert!(EmittedAt::new(OffsetDateTime::UNIX_EPOCH - Duration::nanoseconds(1)).is_err());
    }

    #[test]
    fn metadata_enforces_kind_authority_scope_and_causation() {
        let id =
            MessageId::from_str(MESSAGE_UUID).unwrap_or_else(|error| panic!("fixture id: {error}"));
        let correlation = CorrelationId::from_str(CORRELATION_UUID)
            .unwrap_or_else(|error| panic!("fixture id: {error}"));
        let cell =
            CellId::from_str("cell-001").unwrap_or_else(|error| panic!("fixture cell: {error}"));
        let tenant = TenantId::from_str(TENANT_UUID)
            .unwrap_or_else(|error| panic!("fixture tenant: {error:?}"));
        let scope = MessageScope::Tenant {
            tenant_id: tenant,
            cell_id: cell.clone(),
            assignment_epoch: AssignmentEpoch::new(u64::MAX)
                .unwrap_or_else(|error| panic!("fixture epoch: {error}")),
        };
        assert!(
            MessageMetadata::new(
                id,
                descriptor(MessageKind::Command),
                emitted(),
                MessageAuthority::Platform,
                scope.clone(),
                Some(MessageTarget::Cell(cell.clone())),
                correlation,
                None,
            )
            .is_ok()
        );
        assert_eq!(
            MessageMetadata::new(
                id,
                descriptor(MessageKind::Command),
                emitted(),
                MessageAuthority::Platform,
                scope.clone(),
                None,
                correlation,
                None,
            ),
            Err(MessageMetadataError::CommandTargetRequired)
        );
        assert_eq!(
            MessageMetadata::new(
                id,
                descriptor(MessageKind::Event),
                emitted(),
                MessageAuthority::Cell(cell.clone()),
                scope,
                Some(MessageTarget::Cell(cell)),
                correlation,
                Some(id),
            ),
            Err(MessageMetadataError::EventTargetForbidden)
        );
    }

    #[test]
    fn encoded_message_bounds_and_debug_are_redacted() {
        let id =
            MessageId::from_str(MESSAGE_UUID).unwrap_or_else(|error| panic!("fixture id: {error}"));
        let metadata = MessageMetadata::new(
            id,
            descriptor(MessageKind::Event),
            emitted(),
            MessageAuthority::Platform,
            MessageScope::Platform,
            None,
            CorrelationId::from_str(CORRELATION_UUID)
                .unwrap_or_else(|error| panic!("fixture id: {error}")),
            None,
        )
        .unwrap_or_else(|error| panic!("fixture metadata: {error}"));
        assert!(EncodedMessage::new(metadata.clone(), vec![b'{']).is_err());
        assert!(
            EncodedMessage::new(metadata.clone(), vec![0; MAX_ENCODED_MESSAGE_BYTES + 1]).is_err()
        );
        let sentinel = b"{\"payload\":\"secret-sentinel\"}".to_vec();
        let encoded = EncodedMessage::new(metadata, sentinel)
            .unwrap_or_else(|error| panic!("bounded bytes: {error}"));
        let debug = format!("{encoded:?}");
        let display = encoded.to_string();
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret-sentinel"));
        assert!(!display.contains("secret-sentinel"));
    }
}
