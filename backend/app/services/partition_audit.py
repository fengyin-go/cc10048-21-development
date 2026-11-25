"""分区审计服务的语义别名，核心实现见 reconciliation。"""
from app.services.reconciliation import (
    PartitionReconciliationService,
    reconciliation as partition_audit_service,
    reconciliation,
)

PartitionAuditService = PartitionReconciliationService

__all__ = [
    "PartitionReconciliationService",
    "PartitionAuditService",
    "reconciliation",
    "partition_audit_service",
]
