from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from compliance_gate.Engine.reports.definitions import ReportRequest
from compliance_gate.Engine.reports.runner import ReportRunner


def explain_report(
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str,
    request: ReportRequest,
) -> dict[str, Any]:
    return ReportRunner.preview(
        db,
        tenant_id=tenant_id,
        dataset_version_id=dataset_version_id,
        request=request,
    )
