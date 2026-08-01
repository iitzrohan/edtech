# Checkpoint 3 message-store qualification

- Profile: `full`
- Correctness checks passed: `73`
- Correctness checks failed: `0`
- PostgreSQL server version number: `180004`
- Rust: `rustc 1.97.1 (8bab26f4f 2026-07-14)`
- SQLx: `0.9.0`

Timings are machine-dependent observations, not pass thresholds or production capacity claims.

## Authority results

### platform

- Messages: `20000`
- Delivery rows: `20000`
- Inbox receipts: `10003`
- Enqueue/s: `11272`
- Claim/s: `912`
- Inbox insert/s: `2865`
- Claim latency p50/p95/p99 us: `94974/205745/223519`
- Inbox latency p50/p95/p99 us: `605/770/976`
- Maximum active-lease overlap: `0`
- Derived duplicate effects: `0`

### cell

- Messages: `20000`
- Delivery rows: `20000`
- Inbox receipts: `10001`
- Enqueue/s: `11130`
- Claim/s: `935`
- Inbox insert/s: `2668`
- Claim latency p50/p95/p99 us: `67931/240089/256869`
- Inbox latency p50/p95/p99 us: `614/776/899`
- Maximum active-lease overlap: `0`
- Derived duplicate effects: `0`

## Correctness checks

- `migration.concurrent_platform_migrators_serialize_and_rerun_idempotently`: pass
- `migration.concurrent_cell_migrators_serialize_and_rerun_idempotently`: pass
- `contract.platform_version_one_connects_without_message_capability`: pass
- `contract.platform_version_one_message_store_is_unavailable`: pass
- `contract.platform_version_three_fails_closed`: pass
- `contract.cell_version_one_connects_without_message_capability`: pass
- `contract.cell_version_three_fails_closed`: pass
- `migration.failing_transaction_leaves_no_partial_message_objects`: pass
- `catalog.platform_tables_have_migrator_owner`: pass
- `catalog.platform_public_has_no_table_grants`: pass
- `catalog.platform_authority_has_no_cross_store`: pass
- `catalog.platform_required_indexes_exist`: pass
- `catalog.platform_has_no_orphan_delivery_rows`: pass
- `catalog.platform_complete_message_schema_is_safe`: pass
- `catalog.cell_tables_have_migrator_owner`: pass
- `catalog.cell_public_has_no_table_grants`: pass
- `catalog.cell_authority_has_no_cross_store`: pass
- `catalog.cell_required_indexes_exist`: pass
- `catalog.cell_has_no_orphan_delivery_rows`: pass
- `catalog.cell_complete_message_schema_is_safe`: pass
- `catalog.unsafe_fixture_public_select_rejected`: pass
- `catalog.unsafe_fixture_api_delivery_update_rejected`: pass
- `catalog.unsafe_fixture_worker_immutable_update_rejected`: pass
- `catalog.unsafe_fixture_runtime_table_owner_rejected`: pass
- `catalog.unsafe_fixture_missing_claim_index_rejected`: pass
- `catalog.unsafe_fixture_missing_inbox_key_rejected`: pass
- `catalog.unsafe_fixture_missing_envelope_bound_rejected`: pass
- `catalog.unsafe_fixture_missing_epoch_bound_rejected`: pass
- `catalog.unsafe_fixture_cascade_delete_rejected`: pass
- `catalog.unsafe_fixture_public_message_table_rejected`: pass
- `privilege.api_roles_cannot_update_or_claim_delivery_state`: pass
- `privilege.api_roles_cannot_access_inbox_receipts`: pass
- `simulation.platform_api_enqueues_command`: pass
- `privilege.platform_api_cannot_claim_before_sql`: pass
- `simulation.claim_returns_exact_envelope_bytes`: pass
- `simulation.cell_receipt_and_derived_event_commit_atomically`: pass
- `privilege.cell_api_can_idempotently_enqueue_but_cannot_claim`: pass
- `simulation.expired_first_lease_is_stale`: pass
- `simulation.reclaim_uses_new_lease_and_increments_attempt`: pass
- `simulation.command_redelivery_is_suppressed`: pass
- `simulation.current_platform_lease_marks_published`: pass
- `simulation.platform_event_receipt_commits`: pass
- `simulation.acknowledgment_loss_redelivery_is_suppressed`: pass
- `simulation.same_identity_changed_bytes_is_conflict`: pass
- `simulation.different_consumer_processes_same_event_once`: pass
- `simulation.current_cell_lease_marks_published`: pass
- `simulation.one_platform_command_remains`: pass
- `cell_fencing.absent_tenant_rejected`: pass
- `cell_fencing.absent_tenant_rejected.leaves_no_receipt`: pass
- `cell_fencing.disabled_tenant_rejected`: pass
- `cell_fencing.disabled_tenant_rejected.leaves_no_receipt`: pass
- `cell_fencing.stale_epoch_rejected`: pass
- `cell_fencing.stale_epoch_rejected.leaves_no_receipt`: pass
- `cell_fencing.newer_unregistered_epoch_rejected`: pass
- `cell_fencing.newer_unregistered_epoch_rejected.leaves_no_receipt`: pass
- `cell_fencing.wrong_target_cell_rejected`: pass
- `cell_fencing.wrong_target_cell_rejected.leaves_no_receipt`: pass
- `cell_fencing.wrong_source_cell_rejected`: pass
- `cell_atomicity.canary_and_outbox_commit_together_under_rls`: pass
- `cell_atomicity.committed_canary_is_tenant_visible`: pass
- `cell_atomicity.forced_failure_after_outbox_rolls_back_both_effects`: pass
- `profile.exact_outbound_message_parameters`: pass
- `outbox.committed_enqueue_creates_exact_message_and_delivery_rows`: pass
- `outbox.identical_reenqueue_is_idempotent`: pass
- `outbox.same_identity_changed_bytes_is_conflict`: pass
- `outbox.rollback_removes_message_and_delivery_atomically`: pass
- `outbox.expired_leases_are_reclaimed_with_new_fences`: pass
- `outbox.stale_leases_cannot_publish_or_reschedule`: pass
- `outbox.concurrent_claimers_have_no_active_lease_overlap`: pass
- `outbox.every_message_remains_accounted_for`: pass
- `inbox.profile_uses_exact_attempt_and_duplicate_ratio`: pass
- `inbox.concurrent_identical_deliveries_create_one_receipt`: pass
- `direct_transfer.profile_exact_typed_pairs_decode`: pass
