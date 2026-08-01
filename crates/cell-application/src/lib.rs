//! Cell-authority application boundary for tenant-serving use cases and owned ports.
//!
//! This crate owns provider-neutral execution authority values. It must remain independent of
//! other application crates, runtime frameworks, persistence, transport, configuration,
//! telemetry, and provider SDKs.

use auth_context::RequestScope;
use tenancy_domain::{AssignmentEpoch, CellId, TenantId};
use thiserror::Error;

/// The complete logical authority scope required for tenant-bound work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TenantExecutionScope {
    tenant_id: TenantId,
    cell_id: CellId,
    assignment_epoch: AssignmentEpoch,
}

impl TenantExecutionScope {
    /// Constructs a complete tenant execution authority scope.
    #[must_use]
    pub const fn new(
        tenant_id: TenantId,
        cell_id: CellId,
        assignment_epoch: AssignmentEpoch,
    ) -> Self {
        Self {
            tenant_id,
            cell_id,
            assignment_epoch,
        }
    }

    /// Returns the tenant being served.
    #[must_use]
    pub const fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Returns the logical Cell expected to serve the tenant.
    #[must_use]
    pub const fn cell_id(&self) -> &CellId {
        &self.cell_id
    }

    /// Returns the assignment fence observed for this work.
    #[must_use]
    pub const fn assignment_epoch(&self) -> AssignmentEpoch {
        self.assignment_epoch
    }
}

/// A request scope cannot authorize tenant execution.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("request scope does not contain tenant execution authority")]
pub struct MissingTenantExecutionScope;

impl TryFrom<&RequestScope> for TenantExecutionScope {
    type Error = MissingTenantExecutionScope;

    fn try_from(scope: &RequestScope) -> Result<Self, Self::Error> {
        match scope {
            RequestScope::Platform => Err(MissingTenantExecutionScope),
            RequestScope::Tenant {
                tenant_id,
                cell_id,
                assignment_epoch,
            } => Ok(Self::new(*tenant_id, cell_id.clone(), *assignment_epoch)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use auth_context::RequestScope;
    use tenancy_domain::{AssignmentEpoch, CellId, TenantId};

    use super::TenantExecutionScope;

    #[test]
    fn complete_values_construct_a_tenant_execution_scope() {
        let tenant_id = TenantId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c0001");
        let cell_id = CellId::from_str("cell-001");
        let epoch = AssignmentEpoch::new(u64::MAX);
        if let (Ok(tenant_id), Ok(cell_id), Ok(epoch)) = (tenant_id, cell_id, epoch) {
            let scope = TenantExecutionScope::new(tenant_id, cell_id.clone(), epoch);
            assert_eq!(scope.tenant_id(), tenant_id);
            assert_eq!(scope.cell_id(), &cell_id);
            assert_eq!(scope.assignment_epoch(), epoch);
        } else {
            panic!("fixed scope fixtures must remain valid");
        }
    }

    #[test]
    fn tenant_request_scope_converts_without_application_dependency() {
        let tenant_id = TenantId::from_str("01890f47-7cc2-7a1b-8d5d-7f6ebc9c0001");
        let cell_id = CellId::from_str("cell-001");
        if let (Ok(tenant_id), Ok(cell_id)) = (tenant_id, cell_id) {
            let request_scope = RequestScope::Tenant {
                tenant_id,
                cell_id,
                assignment_epoch: AssignmentEpoch::initial(),
            };
            assert!(TenantExecutionScope::try_from(&request_scope).is_ok());
            assert!(TenantExecutionScope::try_from(&RequestScope::Platform).is_err());
        } else {
            panic!("fixed scope fixtures must remain valid");
        }
    }
}
