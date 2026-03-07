from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
from sqlalchemy.orm import Session

from compliance_gate.Engine.catalog import datasets as dataset_catalog
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.reports.definitions import (
    ReportExecutionPlan,
    ReportRequest,
    load_template,
    resolve_effective_limit,
)
from compliance_gate.infra.db.models_engine import EngineArtifact, EngineRun

log = logging.getLogger(__name__)


class ReportRunner:
    @staticmethod
    def _artifact_for_report(
        db: Session, tenant_id: str, dataset_version_id: str
    ) -> EngineArtifact:
        artifact = (
            db.query(EngineArtifact)
            .filter(
                EngineArtifact.tenant_id == tenant_id,
                EngineArtifact.dataset_version_id == dataset_version_id,
                EngineArtifact.artifact_type == "parquet",
                EngineArtifact.artifact_name == "machines_final",
            )
            .first()
        )
        if not artifact:
            raise ValueError("machines_final artifact not found; materialize first")

        if not Path(artifact.path).exists():
            raise ValueError("artifact parquet path not found on disk")

        return artifact

    @staticmethod
    def _build_query(template_name: str, parquet_path: str, limit: int) -> str:
        template = load_template(template_name)
        if template.kind == "machines_status_summary":
            return f"""
                WITH base AS (
                    SELECT primary_status, primary_status_label, flags
                    FROM read_parquet('{parquet_path}')
                ),
                status_counts AS (
                    SELECT
                        primary_status AS key,
                        any_value(primary_status_label) AS label,
                        COUNT(*)::BIGINT AS count,
                        'status' AS type
                    FROM base
                    GROUP BY 1
                ),
                flag_counts AS (
                    SELECT
                        f.flag AS key,
                        f.flag AS label,
                        COUNT(*)::BIGINT AS count,
                        'flag' AS type
                    FROM base,
                    UNNEST(COALESCE(flags, [])) AS f(flag)
                    GROUP BY 1
                )
                SELECT key, label, count, type
                FROM status_counts
                UNION ALL
                SELECT key, label, count, type
                FROM flag_counts
                ORDER BY type, count DESC, key
                LIMIT {limit}
            """
        raise ValueError(f"unsupported template kind: {template.kind}")

    @staticmethod
    def _fetch_query_rows(query: str) -> list[dict[str, Any]]:
        conn = duckdb.connect(database=":memory:")
        try:
            result = conn.execute(query)
            columns = [col[0] for col in result.description]
            rows = result.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]
        finally:
            conn.close()

    @staticmethod
    def preview(
        db: Session,
        *,
        tenant_id: str,
        dataset_version_id: str,
        request: ReportRequest,
    ) -> dict[str, Any]:
        artifact = ReportRunner._artifact_for_report(db, tenant_id, dataset_version_id)
        template = load_template(request.template_name)
        limit = resolve_effective_limit(template, request.limit)
        query = ReportRunner._build_query(template.template_name, artifact.path, limit)

        conn = duckdb.connect(database=":memory:")
        try:
            explain_rows = conn.execute(f"EXPLAIN {query}").fetchall()
            explain_plan = explain_rows[0][1] if explain_rows else ""
        finally:
            conn.close()

        sample_query = f"SELECT * FROM ({query}) AS t LIMIT 20"
        sample_rows = ReportRunner._fetch_query_rows(sample_query)

        return {
            "query": query,
            "explain_plan": explain_plan,
            "sample": sample_rows,
        }

    @staticmethod
    def execute(
        db: Session,
        *,
        tenant_id: str,
        dataset_version_id: str,
        request: ReportRequest,
    ) -> tuple[list[dict[str, Any]], ReportExecutionPlan]:
        artifact = ReportRunner._artifact_for_report(db, tenant_id, dataset_version_id)
        template = load_template(request.template_name)
        effective_limit = resolve_effective_limit(template, request.limit)
        query = ReportRunner._build_query(template.template_name, artifact.path, effective_limit)

        run = EngineRun(
            tenant_id=tenant_id,
            dataset_version_id=dataset_version_id,
            run_type="report",
            status="running",
            started_at=datetime.now(UTC),
        )
        db.add(run)
        db.flush()

        start = time.perf_counter()
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(ReportRunner._fetch_query_rows, query)
                rows = future.result(timeout=engine_settings.report_timeout_seconds)

            run.status = "success"
            run.ended_at = datetime.now(UTC)
            run.metrics_json = json.dumps(
                {
                    "template": template.template_name,
                    "row_count": len(rows),
                    "elapsed_ms": round((time.perf_counter() - start) * 1000, 2),
                },
                ensure_ascii=False,
            )
            db.commit()

            plan = ReportExecutionPlan(
                template_name=template.template_name,
                query=query,
                effective_limit=effective_limit,
            )
            return rows, plan

        except FuturesTimeout as exc:
            run.status = "error"
            run.ended_at = datetime.now(UTC)
            run.error_truncated = f"report timeout after {engine_settings.report_timeout_seconds}s"
            db.commit()
            raise TimeoutError(run.error_truncated) from exc
        except Exception as exc:
            run.status = "error"
            run.ended_at = datetime.now(UTC)
            run.error_truncated = dataset_catalog.truncate_error(str(exc))
            db.commit()
            log.error("Report execution failed: %s", run.error_truncated)
            raise
