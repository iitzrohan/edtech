//! Deterministic, non-production fixture builders shared by tests.
//!
//! This crate contains no service fake, external dependency fake, credential, or customer-like
//! data. Deployable binaries and production domain/application dependencies must never depend on
//! it.

use std::{collections::BTreeMap, str::FromStr};

use audit_domain::{AuditEventId, AuditEventIdError};
use auth_context::{AuthIdentifierError, PrincipalId, RequestId};
use provisioning_domain::{ProvisioningOperationId, ProvisioningOperationIdError};
use tenancy_domain::{CellId, CellIdError, OrganizationId, TenantId, UuidV7Error};

/// Returns a deterministic organization identifier fixture.
///
/// # Errors
///
/// Returns an error only if the compile-time fixture ceases to be a valid `UUIDv7` value.
pub fn organization_id() -> Result<OrganizationId, UuidV7Error> {
    OrganizationId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c1001")
}

/// Returns a deterministic tenant identifier fixture.
///
/// # Errors
///
/// Returns an error only if the compile-time fixture ceases to be a valid `UUIDv7` value.
pub fn tenant_id() -> Result<TenantId, UuidV7Error> {
    TenantId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c1002")
}

/// Returns a deterministic provisioning operation identifier fixture.
///
/// # Errors
///
/// Returns an error only if the compile-time fixture ceases to be a valid `UUIDv7` value.
pub fn provisioning_operation_id() -> Result<ProvisioningOperationId, ProvisioningOperationIdError>
{
    ProvisioningOperationId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c1003")
}

/// Returns a deterministic principal identifier fixture.
///
/// # Errors
///
/// Returns an error only if the compile-time fixture ceases to be a valid `UUIDv7` value.
pub fn principal_id() -> Result<PrincipalId, AuthIdentifierError> {
    PrincipalId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c1004")
}

/// Returns a deterministic request identifier fixture.
///
/// # Errors
///
/// Returns an error only if the compile-time fixture ceases to be a valid `UUIDv7` value.
pub fn request_id() -> Result<RequestId, AuthIdentifierError> {
    RequestId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c1005")
}

/// Returns a deterministic audit event identifier fixture.
///
/// # Errors
///
/// Returns an error only if the compile-time fixture ceases to be a valid `UUIDv7` value.
pub fn audit_event_id() -> Result<AuditEventId, AuditEventIdError> {
    AuditEventId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c1006")
}

/// Builds a valid topology-neutral Cell identifier from a deterministic numeric suffix.
///
/// # Errors
///
/// Returns [`CellIdError`] if a future formatting change violates the Cell grammar.
pub fn cell_id(suffix: u16) -> Result<CellId, CellIdError> {
    CellId::new(format!("cell-{suffix:03}"))
}

/// Builds the minimum explicit environment input for a Platform process.
#[must_use]
pub fn platform_environment(environment: &str) -> BTreeMap<String, String> {
    BTreeMap::from([(String::from("EDTECH__ENVIRONMENT"), environment.to_owned())])
}

/// Builds the minimum explicit environment input for a Cell process.
#[must_use]
pub fn cell_environment(environment: &str, cell_id: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (String::from("EDTECH__ENVIRONMENT"), environment.to_owned()),
        (String::from("EDTECH__CELL_ID"), cell_id.to_owned()),
    ])
}
