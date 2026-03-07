from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from hashlib import sha256

import polars as pl
from sqlalchemy import text
from sqlalchemy.orm import Session

from compliance_gate.domains.machines.classification.models import MachineRecord
from compliance_gate.domains.machines.classification.orchestrator import evaluate_machine
from compliance_gate.domains.machines.ingest.pipeline import run_ingest_pipeline
from compliance_gate.Engine.catalog import datasets as dataset_catalog
from compliance_gate.Engine.materialization.parquet_writer import ParquetWriter
from compliance_gate.Engine.spines.machines_final import (
    MACHINES_FINAL_COLUMNS,
    MACHINES_FINAL_SPINE,
)
from compliance_gate.infra.db.models_engine import EngineArtifact, EngineRun

log = logging.getLogger(__name__)


def _lock_key(tenant_id: str, dataset_version_id: str, namespace: str) -> int:
    digest = sha256(f"{namespace}:{tenant_id}:{dataset_version_id}".encode()).hexdigest()
    return int(digest[:15], 16)


def _acquire_materialize_lock(db: Session, tenant_id: str, dataset_version_id: str) -> None:
    if db.bind is None or db.bind.dialect.name != "postgresql":
        return
    key = _lock_key(tenant_id, dataset_version_id, "machines_materialize")
    db.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": key})


def _to_machines_final_df(records: list[dict]) -> pl.DataFrame:
    rows: list[dict] = []
    for raw in records:
        machine = MachineRecord(**raw)
        status = evaluate_machine(machine)
        rows.append(
            {
                "machine_id": machine.hostname,
                "hostname": machine.hostname,
                "pa_code": machine.pa_code,
                "primary_status": status.primary_status,
                "primary_status_label": status.primary_status_label,
                "flags": status.flags,
                "has_ad": machine.has_ad,
                "has_uem": machine.has_uem,
                "has_edr": machine.has_edr,
                "has_asset": machine.has_asset,
                "last_seen_date_ms": machine.last_seen_date_ms,
            }
        )

    if not rows:
        return pl.DataFrame(
            schema={
                "machine_id": pl.String,
                "hostname": pl.String,
                "pa_code": pl.String,
                "primary_status": pl.String,
                "primary_status_label": pl.String,
                "flags": pl.List(pl.String),
                "has_ad": pl.Boolean,
                "has_uem": pl.Boolean,
                "has_edr": pl.Boolean,
                "has_asset": pl.Boolean,
                "last_seen_date_ms": pl.Int64,
            }
        )

    df = pl.DataFrame(rows)
    # Keep deterministic schema ordering for parquet contracts.
    return df.select(list(MACHINES_FINAL_COLUMNS))


def materialize_machines_spine(
    db: Session,
    tenant_id: str,
    dataset_version_id: str | None,
) -> EngineArtifact:
    if not tenant_id:
        raise ValueError("tenant_id is required")
    version = dataset_catalog.get_dataset_version(
        db,
        tenant_id=tenant_id,
        dataset_version_id=dataset_version_id,
        source_type=MACHINES_FINAL_SPINE.domain,
    )

    run = EngineRun(
        tenant_id=tenant_id,
        dataset_version_id=version.id,
        run_type="materialize",
        status="running",
        started_at=datetime.now(UTC),
    )
    db.add(run)
    db.flush()

    try:
        _acquire_materialize_lock(db, tenant_id, version.id)

        target_path = dataset_catalog.get_parquet_path(
            tenant_id,
            MACHINES_FINAL_SPINE.domain,
            version.id,
            MACHINES_FINAL_SPINE.name,
        )

        artifact = (
            db.query(EngineArtifact)
            .filter(
                EngineArtifact.tenant_id == tenant_id,
                EngineArtifact.dataset_version_id == version.id,
                EngineArtifact.artifact_type == "parquet",
                EngineArtifact.artifact_name == MACHINES_FINAL_SPINE.name,
            )
            .first()
        )

        if artifact and target_path.exists() and artifact.checksum:
            current_checksum = ParquetWriter.calculate_checksum(target_path)
            if current_checksum == artifact.checksum:
                run.status = "success"
                run.ended_at = datetime.now(UTC)
                run.metrics_json = json.dumps(
                    {
                        "idempotent": True,
                        "row_count": artifact.row_count,
                        "path": str(target_path),
                    },
                    ensure_ascii=False,
                )
                db.commit()
                return artifact

        start = time.perf_counter()
        data_dir = dataset_catalog.resolve_data_dir(version)
        configs = dataset_catalog.resolve_profile_configs(db, version)
        ingest_result = run_ingest_pipeline(
            data_dir,
            dataset_version_id=version.id,
            configs=configs,
        )

        df = _to_machines_final_df(ingest_result.records)
        if df.height <= 0:
            raise ValueError("materialization produced zero rows")

        row_count, checksum, metrics = ParquetWriter.write_dataframe(df, target_path)
        if row_count <= 0:
            raise ValueError("materialization produced invalid row_count")

        schema_json = json.dumps(ParquetWriter.read_schema(target_path), ensure_ascii=False)

        if artifact is None:
            artifact = EngineArtifact(
                tenant_id=tenant_id,
                dataset_version_id=version.id,
                domain=MACHINES_FINAL_SPINE.domain,
                artifact_type="parquet",
                artifact_name=MACHINES_FINAL_SPINE.name,
                path=str(target_path),
                checksum=checksum,
                row_count=row_count,
                schema_json=schema_json,
            )
            db.add(artifact)
        else:
            artifact.path = str(target_path)
            artifact.checksum = checksum
            artifact.row_count = row_count
            artifact.schema_json = schema_json

        run.status = "success"
        run.ended_at = datetime.now(UTC)
        run.metrics_json = json.dumps(
            {
                "elapsed_ms": round((time.perf_counter() - start) * 1000, 2),
                "materialization_elapsed_ms": round(metrics.elapsed_ms, 2),
                "row_count": row_count,
                "warning_count": len(ingest_result.warnings),
            },
            ensure_ascii=False,
        )
        db.commit()
        db.refresh(artifact)
        return artifact

    except Exception as exc:
        run.status = "error"
        run.ended_at = datetime.now(UTC)
        run.error_truncated = dataset_catalog.truncate_error(str(exc))
        db.commit()
        log.error("materialize_machines_spine failed: %s", run.error_truncated)
        raise
