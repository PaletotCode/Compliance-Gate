from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl
from sqlalchemy.orm import Session

from compliance_gate.Engine.catalog.datasets import get_materialized_artifact
from compliance_gate.Engine.catalog.schemas import (
    CatalogColumnProfile,
    MachinesFinalCatalogSnapshot,
)
from compliance_gate.Engine.errors import GuardrailViolation
from compliance_gate.Engine.expressions.types import ExpressionDataType, normalize_expression_type

MAX_SAMPLE_SIZE = 20
MAX_SAMPLE_VALUE_CHARS = 120


def get_machines_final_catalog(
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str,
    sample_size: int = 5,
) -> MachinesFinalCatalogSnapshot:
    if sample_size < 1 or sample_size > MAX_SAMPLE_SIZE:
        raise GuardrailViolation(
            "sample_size fora do intervalo permitido.",
            details={"sample_size": sample_size, "max_sample_size": MAX_SAMPLE_SIZE},
            hint=f"Use sample_size entre 1 e {MAX_SAMPLE_SIZE}.",
        )

    try:
        artifact = get_materialized_artifact(
            db,
            tenant_id=tenant_id,
            dataset_version_id=dataset_version_id,
            artifact_name="machines_final",
            artifact_type="parquet",
        )
    except ValueError as exc:
        raise GuardrailViolation(
            "Artefato materializado não encontrado para o dataset informado.",
            details={"dataset_version_id": dataset_version_id, "reason": "artifact_not_found"},
            hint="Execute a materialização do dataset antes de abrir o catálogo.",
        ) from exc
    artifact_path = Path(artifact.path)
    if not artifact_path.exists():
        raise GuardrailViolation(
            "Artefato materializado não encontrado em disco.",
            details={"dataset_version_id": dataset_version_id, "reason": "artifact_missing_on_disk"},
            hint="Materialize o dataset novamente antes de abrir o catálogo.",
        )

    try:
        df = pl.read_parquet(artifact_path)
    except Exception as exc:
        raise GuardrailViolation(
            "Falha ao abrir o parquet materializado.",
            details={"dataset_version_id": dataset_version_id, "reason": type(exc).__name__},
            hint="Reexecute a materialização do dataset para regenerar o arquivo.",
        ) from exc

    row_count = df.height
    columns: list[CatalogColumnProfile] = [
        _build_column_profile(df, column_name, sample_size=sample_size)
        for column_name in df.columns
    ]

    return MachinesFinalCatalogSnapshot(
        tenant_id=tenant_id,
        dataset_version_id=dataset_version_id,
        row_count=row_count,
        columns=columns,
    )


def _build_column_profile(
    df: pl.DataFrame, column_name: str, *, sample_size: int
) -> CatalogColumnProfile:
    series = df.get_column(column_name)
    row_count = df.height
    null_count = int(series.null_count())
    null_rate = round((null_count / row_count) if row_count else 0.0, 4)

    data_type = normalize_expression_type(str(series.dtype))
    rendered_type = data_type.value if data_type != ExpressionDataType.UNKNOWN else str(series.dtype)

    return CatalogColumnProfile(
        name=column_name,
        data_type=rendered_type,
        sample_values=_sample_values(series, sample_size=sample_size),
        null_rate=null_rate,
        approx_cardinality=_approx_cardinality(df, column_name, series),
    )


def _sample_values(series: pl.Series, *, sample_size: int) -> list[Any]:
    values = series.drop_nulls().head(sample_size).to_list()
    return [_normalize_sample_value(value) for value in values]


def _normalize_sample_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False)
        if len(text) > MAX_SAMPLE_VALUE_CHARS:
            return f"{text[:MAX_SAMPLE_VALUE_CHARS - 3]}..."
        return text
    if isinstance(value, str) and len(value) > MAX_SAMPLE_VALUE_CHARS:
        return f"{value[:MAX_SAMPLE_VALUE_CHARS - 3]}..."
    return value


def _approx_cardinality(df: pl.DataFrame, column_name: str, series: pl.Series) -> int:
    if series.is_empty():
        return 0
    try:
        value = df.select(pl.col(column_name).approx_n_unique().alias("cardinality")).item()
        if value is None:
            return 0
        return int(value)
    except Exception:
        # Fallback rápido para tipos aninhados onde approx_n_unique pode falhar.
        return int(series.drop_nulls().head(5_000).n_unique())
