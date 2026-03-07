from __future__ import annotations

import json
import logging
import sys

from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.materialization.materialize_machines import materialize_machines_spine
from compliance_gate.Engine.reports.definitions import ReportRequest
from compliance_gate.Engine.reports.runner import ReportRunner
from compliance_gate.infra.db.session import SessionLocal

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def run_cli() -> int:
    if len(sys.argv) < 3:
        log.error("Usage: python -m compliance_gate.Engine.interfaces.cli [materialize|report] <dataset_version_id> [template_name]")
        return 1

    action = sys.argv[1]
    dataset_version_id = sys.argv[2]
    tenant_id = engine_settings.default_tenant_id

    db = SessionLocal()
    try:
        if action == "materialize":
            artifact = materialize_machines_spine(db, tenant_id, dataset_version_id)
            log.info("materialized: %s (%s rows)", artifact.path, artifact.row_count)
            return 0

        if action == "report":
            template_name = sys.argv[3] if len(sys.argv) >= 4 else "machines_status_summary"
            rows, plan = ReportRunner.execute(
                db,
                tenant_id=tenant_id,
                dataset_version_id=dataset_version_id,
                request=ReportRequest(template_name=template_name),
            )
            print(json.dumps({"query": plan.query, "row_count": len(rows), "data": rows[:20]}, ensure_ascii=False, indent=2))
            return 0

        log.error("unknown action: %s", action)
        return 1
    except Exception as exc:
        log.exception("engine cli failed: %s", exc)
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(run_cli())
