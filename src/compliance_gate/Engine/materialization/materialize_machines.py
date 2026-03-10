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
from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabConfig
from compliance_gate.domains.machines.ingest.pipeline import run_ingest_pipeline
from compliance_gate.Engine.catalog import datasets as dataset_catalog
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.materialization.parquet_writer import ParquetWriter
from compliance_gate.Engine.rulesets import (
    ClassificationOutput,
    ClassificationRuntimeMode,
    classify_records,
    compile_published_ruleset,
    ensure_baseline_ruleset_for_tenant,
    get_classification_mode,
    get_classification_migration_state,
    record_divergences,
)
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
    return _to_machines_final_df_with_selected(records, configs={}, classification_outputs=None)


def _extract_selected_data(
    raw_sources: dict | None,
    configs: dict[str, CsvTabConfig],
) -> dict[str, object]:
    selected: dict[str, object] = {}
    if not isinstance(raw_sources, dict):
        return selected

    for source, source_raw in raw_sources.items():
        if not isinstance(source_raw, dict):
            continue
        profile = configs.get(source)
        if not profile:
            continue

        for column in profile.selected_columns:
            if column in source_raw:
                selected[f"{source}.{column}"] = source_raw[column]

    return selected


def _to_machines_final_df_with_selected(
    records: list[dict],
    configs: dict[str, CsvTabConfig],
    classification_outputs: list[ClassificationOutput] | None,
) -> pl.DataFrame:
    rows: list[dict] = []
    for index, raw in enumerate(records):
        machine = MachineRecord(**raw)
        if classification_outputs is None:
            status = evaluate_machine(machine)
            primary_status = status.primary_status
            primary_status_label = status.primary_status_label
            flags = status.flags
        else:
            status = classification_outputs[index]
            primary_status = status.primary_status
            primary_status_label = status.primary_status_label
            flags = status.flags
        selected_data = _extract_selected_data(raw.get("raw_sources"), configs)
        row = {
            "machine_id": machine.hostname,
            "hostname": machine.hostname,
            "pa_code": machine.pa_code,
            "primary_status": primary_status,
            "primary_status_label": primary_status_label,
            "flags": flags,
            "has_ad": machine.has_ad,
            "has_uem": machine.has_uem,
            "has_edr": machine.has_edr,
            "has_asset": machine.has_asset,
            "last_seen_date_ms": machine.last_seen_date_ms,
            "main_user": machine.main_user,
            "ad_os": machine.ad_os,
            "us_ad": machine.us_ad,
            "us_uem": machine.us_uem,
            "us_edr": machine.us_edr,
            "uem_extra_user_logado": machine.uem_extra_user_logado,
            "edr_os": machine.edr_os,
            "status_check_win11": machine.status_check_win11,
            "uem_serial": machine.uem_serial,
            "edr_serial": machine.edr_serial,
            "chassis": machine.chassis,
            "selected_data_json": json.dumps(selected_data, ensure_ascii=False),
        }
        # Keep selected columns materialized as direct parquet fields for fast scans.
        row.update(selected_data)
        rows.append(row)

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
                "main_user": pl.String,
                "ad_os": pl.String,
                "us_ad": pl.String,
                "us_uem": pl.String,
                "us_edr": pl.String,
                "uem_extra_user_logado": pl.String,
                "edr_os": pl.String,
                "status_check_win11": pl.String,
                "uem_serial": pl.String,
                "edr_serial": pl.String,
                "chassis": pl.String,
                "selected_data_json": pl.String,
            }
        )

    df = pl.DataFrame(rows)
    preferred_order = [
        *list(MACHINES_FINAL_COLUMNS),
        "main_user",
        "ad_os",
        "us_ad",
        "us_uem",
        "us_edr",
        "uem_extra_user_logado",
        "edr_os",
        "status_check_win11",
        "uem_serial",
        "edr_serial",
        "chassis",
        "selected_data_json",
    ]
    ordered_columns = [column for column in preferred_order if column in df.columns]
    dynamic_columns = [column for column in df.columns if column not in ordered_columns]
    return df.select([*ordered_columns, *dynamic_columns])


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
    baseline_result = ensure_baseline_ruleset_for_tenant(
        db,
        tenant_id=tenant_id,
        actor="system",
    )
    migration_state = get_classification_migration_state(db, tenant_id=tenant_id)

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
        classification_mode, configured_ruleset_name = get_classification_mode(
            db,
            tenant_id=tenant_id,
        )

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

        if (
            classification_mode == ClassificationRuntimeMode.LEGACY
            and artifact
            and target_path.exists()
            and artifact.checksum
        ):
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

        compiled_ruleset = None
        if classification_mode in {
            ClassificationRuntimeMode.SHADOW,
            ClassificationRuntimeMode.DECLARATIVE,
        }:
            resolved_ruleset_name = (
                configured_ruleset_name
                or baseline_result.get("ruleset_name")
                or engine_settings.classification_default_ruleset_name
            )
            compiled_ruleset = compile_published_ruleset(
                db=db,
                tenant_id=tenant_id,
                ruleset_name=resolved_ruleset_name,
            )

        cutover_phase = (
            migration_state.phase
            if classification_mode == ClassificationRuntimeMode.DECLARATIVE
            and not migration_state.is_default
            else None
        )
        classification_batch = classify_records(
            ingest_result.records,
            mode=classification_mode,
            compiled_ruleset=compiled_ruleset,
            cutover_phase=cutover_phase,
        )
        df = _to_machines_final_df_with_selected(
            ingest_result.records,
            configs=configs,
            classification_outputs=classification_batch.outputs,
        )
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

        if classification_mode == ClassificationRuntimeMode.SHADOW and classification_batch.divergences:
            record_divergences(
                db,
                tenant_id=tenant_id,
                dataset_version_id=version.id,
                run_id=run.id,
                ruleset_name=classification_batch.metrics.ruleset_name
                or engine_settings.classification_default_ruleset_name,
                divergences=[
                    {
                        "machine_id": divergence.machine_id,
                        "hostname": divergence.hostname,
                        "legacy_primary_status": divergence.legacy_primary_status,
                        "legacy_primary_status_label": divergence.legacy_primary_status_label,
                        "legacy_flags": divergence.legacy_flags,
                        "declarative_primary_status": divergence.declarative_primary_status,
                        "declarative_primary_status_label": divergence.declarative_primary_status_label,
                        "declarative_flags": divergence.declarative_flags,
                        "diff": {
                            **divergence.diff,
                            "declarative_rule_keys": divergence.rule_keys,
                            "severity": divergence.severity,
                            "divergence_kind": divergence.divergence_kind,
                        },
                    }
                    for divergence in classification_batch.divergences
                ],
            )

        classification_metrics = {
            "mode": classification_batch.metrics.mode.value,
            "ruleset_name": classification_batch.metrics.ruleset_name,
            "cutover_phase": classification_batch.metrics.cutover_phase.value
            if classification_batch.metrics.cutover_phase
            else None,
            "migration_phase": migration_state.phase.value if not migration_state.is_default else None,
            "baseline_ruleset_name": baseline_result.get("ruleset_name"),
            "baseline_published_version": baseline_result.get("published_version"),
            "rows_scanned": classification_batch.metrics.rows_scanned,
            "rows_classified": classification_batch.metrics.rows_classified,
            "elapsed_ms": classification_batch.metrics.elapsed_ms,
            "rule_hits": classification_batch.metrics.rule_hits,
            "divergences": classification_batch.metrics.divergences,
        }

        run.status = "success"
        run.ended_at = datetime.now(UTC)
        run.metrics_json = json.dumps(
            {
                "elapsed_ms": round((time.perf_counter() - start) * 1000, 2),
                "materialization_elapsed_ms": round(metrics.elapsed_ms, 2),
                "row_count": row_count,
                "warning_count": len(ingest_result.warnings),
                **classification_metrics,
                "classification": classification_metrics,
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
