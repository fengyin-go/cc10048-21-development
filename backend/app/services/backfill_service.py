"""补算服务语义别名。"""
from app.services.reconciliation import (
    PartitionReconciliationService,
    reconciliation as backfill_service,
    reconciliation,
)

BackfillService = PartitionReconciliationService

__all__ = ["BackfillService", "backfill_service", "reconciliation"]
