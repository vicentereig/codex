use super::inbox::InboxFailureInjector;
use super::inbox::InboxWriteError;
use super::inbox::NoInboxFailure;
use super::inbox::finish_inbox;
use super::inbox::internal;
use super::inbox_claim::claim_receipt;
use super::inbox_maintenance::expire_payloads;
use super::inbox_maintenance::reclaim_leases;
use super::inclusion_outcome::record_transport_outcome;
use super::inclusion_selection::record_selection;
use crate::StateRuntime;
use crate::model::coordination_inbox::ClaimInboxReceipt;
use crate::model::coordination_inbox::ClaimInboxReceiptOutcome;
use crate::model::coordination_inbox::InboxMaintenanceBatch;
use crate::model::coordination_inbox::InboxMaintenanceOutcome;
use crate::model::coordination_inbox::RecordInboxSelection;
use crate::model::coordination_inbox::RecordInboxSelectionOutcome;
use crate::model::coordination_inbox::RecordInboxTransportOutcome;
use crate::model::coordination_inbox::RecordInboxTransportOutcomeResult;

impl StateRuntime {
    pub(crate) async fn claim_coordination_receipt_for_inclusion(
        &self,
        params: ClaimInboxReceipt,
    ) -> Result<ClaimInboxReceiptOutcome, InboxWriteError> {
        self.claim_coordination_receipt_for_inclusion_with(params, &NoInboxFailure)
            .await
    }

    pub(super) async fn claim_coordination_receipt_for_inclusion_with(
        &self,
        params: ClaimInboxReceipt,
        injector: &dyn InboxFailureInjector,
    ) -> Result<ClaimInboxReceiptOutcome, InboxWriteError> {
        let mut connection = self.begin_inbox().await?;
        let result = claim_receipt(&mut connection, params, injector).await;
        finish_inbox(&mut connection, result, injector).await
    }

    pub(crate) async fn record_coordination_inclusion_selection(
        &self,
        params: RecordInboxSelection,
    ) -> Result<RecordInboxSelectionOutcome, InboxWriteError> {
        self.record_coordination_inclusion_selection_with(params, &NoInboxFailure)
            .await
    }

    pub(super) async fn record_coordination_inclusion_selection_with(
        &self,
        params: RecordInboxSelection,
        injector: &dyn InboxFailureInjector,
    ) -> Result<RecordInboxSelectionOutcome, InboxWriteError> {
        let mut connection = self.begin_inbox().await?;
        let result = record_selection(&mut connection, params, injector).await;
        finish_inbox(&mut connection, result, injector).await
    }

    pub(crate) async fn record_coordination_inbox_transport_outcome(
        &self,
        params: RecordInboxTransportOutcome,
    ) -> Result<RecordInboxTransportOutcomeResult, InboxWriteError> {
        self.record_coordination_inbox_transport_outcome_with(params, &NoInboxFailure)
            .await
    }

    pub(super) async fn record_coordination_inbox_transport_outcome_with(
        &self,
        params: RecordInboxTransportOutcome,
        injector: &dyn InboxFailureInjector,
    ) -> Result<RecordInboxTransportOutcomeResult, InboxWriteError> {
        let mut connection = self.begin_inbox().await?;
        let result = record_transport_outcome(&mut connection, params, injector).await;
        finish_inbox(&mut connection, result, injector).await
    }

    pub(crate) async fn reclaim_expired_coordination_inbox_leases(
        &self,
        params: InboxMaintenanceBatch,
    ) -> Result<InboxMaintenanceOutcome, InboxWriteError> {
        self.reclaim_expired_coordination_inbox_leases_with(params, &NoInboxFailure)
            .await
    }

    pub(super) async fn reclaim_expired_coordination_inbox_leases_with(
        &self,
        params: InboxMaintenanceBatch,
        injector: &dyn InboxFailureInjector,
    ) -> Result<InboxMaintenanceOutcome, InboxWriteError> {
        let mut connection = self.begin_inbox().await?;
        let result = reclaim_leases(&mut connection, params, injector).await;
        finish_inbox(&mut connection, result, injector).await
    }

    pub(crate) async fn expire_coordination_inbox_payloads(
        &self,
        params: InboxMaintenanceBatch,
    ) -> Result<InboxMaintenanceOutcome, InboxWriteError> {
        self.expire_coordination_inbox_payloads_with(params, &NoInboxFailure)
            .await
    }

    pub(super) async fn expire_coordination_inbox_payloads_with(
        &self,
        params: InboxMaintenanceBatch,
        injector: &dyn InboxFailureInjector,
    ) -> Result<InboxMaintenanceOutcome, InboxWriteError> {
        let mut connection = self.begin_inbox().await?;
        let result = expire_payloads(&mut connection, params, injector).await;
        finish_inbox(&mut connection, result, injector).await
    }

    async fn begin_inbox(
        &self,
    ) -> Result<sqlx::pool::PoolConnection<sqlx::Sqlite>, InboxWriteError> {
        let mut connection = self.pool.acquire().await.map_err(internal)?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .map_err(internal)?;
        Ok(connection)
    }
}
