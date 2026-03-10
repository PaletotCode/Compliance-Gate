from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from sqlalchemy.orm import Session

from compliance_gate.Engine.catalog.datasets import get_materialized_artifact, truncate_error
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.declarative import get_segment, get_transformation, get_view
from compliance_gate.Engine.errors import GuardrailViolation, InvalidExpressionSyntax, TypeMismatch
from compliance_gate.Engine.expressions import (
    BinaryOpNode,
    ColumnRefNode,
    ExpressionDataType,
    ExpressionNode,
    ExpressionValidationOptions,
    FunctionCallNode,
    LiteralNode,
    LogicalOpNode,
    UnaryOpNode,
    validate_expression,
)
from compliance_gate.Engine.expressions.types import normalize_expression_type
from compliance_gate.Engine.models import EngineRun
from compliance_gate.Engine.runtime.schemas import (
    SegmentPreviewResult,
    ViewPreviewResult,
    ViewRunResult,
)
from compliance_gate.Engine.segments import SegmentPayloadV1
from compliance_gate.Engine.views import (
    SortDirection,
    ViewDerivedColumn,
    ViewPayloadV1,
    ViewSortSpec,
)


def preview_segment(
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str,
    segment_id: str | None = None,
    expression: ExpressionNode | None = None,
    limit: int | None = None,
) -> SegmentPreviewResult:
    options = _validation_options()
    warnings: list[str] = []
    requested_limit = limit or 20
    effective_limit = requested_limit
    if requested_limit > engine_settings.max_preview_rows:
        effective_limit = engine_settings.max_preview_rows
        warnings.append(
            f"limit reduzido para {engine_settings.max_preview_rows} por guardrail de preview."
        )
    if effective_limit <= 0:
        raise GuardrailViolation(
            "limit inválido para preview.",
            details={"limit": effective_limit},
            hint="Informe um limit maior que zero.",
        )

    base_lf, column_types = _base_lazyframe(db, tenant_id=tenant_id, dataset_version_id=dataset_version_id)
    filter_expr = _resolve_segment_filter(
        db,
        tenant_id=tenant_id,
        segment_id=segment_id,
        inline_expression=expression,
        column_types=column_types,
        options=options,
    )

    filtered_lf = base_lf.filter(filter_expr)

    def _work() -> tuple[SegmentPreviewResult, dict[str, Any]]:
        total_rows = _collect_count(base_lf)
        matched_rows = _collect_count(filtered_lf)
        sample_rows = filtered_lf.limit(effective_limit).collect().to_dicts()
        result = SegmentPreviewResult(
            total_rows=total_rows,
            matched_rows=matched_rows,
            match_rate=round((matched_rows / total_rows) if total_rows else 0.0, 4),
            sample_rows=sample_rows,
            warnings=warnings,
        )
        metrics = {
            "rows_scanned": total_rows,
            "rows_returned": len(sample_rows),
            "matched_rows": matched_rows,
            "limit": effective_limit,
        }
        return result, metrics

    return _tracked_run(
        db,
        tenant_id=tenant_id,
        dataset_version_id=dataset_version_id,
        run_type="segment_preview",
        work=_work,
    )


def preview_view(
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str,
    view_id: str | None = None,
    inline_view_payload: ViewPayloadV1 | None = None,
    limit: int | None = None,
) -> ViewPreviewResult:
    if bool(view_id) == bool(inline_view_payload):
        raise GuardrailViolation(
            "Informe exatamente um entre view_id ou inline_view_payload.",
            details={"reason": "invalid_preview_input"},
            hint="Use somente um dos campos para preview.",
        )

    warnings: list[str] = []
    payload = inline_view_payload or get_view(
        db, tenant_id=tenant_id, view_id=str(view_id)
    ).payload
    _validate_dataset_scope(payload, dataset_version_id=dataset_version_id)

    requested_limit = limit or 20
    effective_limit = requested_limit
    max_allowed = min(engine_settings.max_preview_rows, payload.row_limit)
    if requested_limit > max_allowed:
        effective_limit = max_allowed
        warnings.append(f"limit reduzido para {max_allowed} por guardrail de preview.")
    if effective_limit <= 0:
        raise GuardrailViolation(
            "limit inválido para preview.",
            details={"limit": effective_limit},
            hint="Informe um limit maior que zero.",
        )

    view_lf, selected_columns, local_warnings = _build_view_lazyframe(
        db,
        tenant_id=tenant_id,
        dataset_version_id=dataset_version_id,
        payload=payload,
    )
    warnings.extend(local_warnings)

    def _work() -> tuple[ViewPreviewResult, dict[str, Any]]:
        total_rows = _collect_count(view_lf)
        sample_rows = view_lf.limit(effective_limit).collect().to_dicts()
        result = ViewPreviewResult(
            total_rows=total_rows,
            returned_rows=len(sample_rows),
            sample_rows=sample_rows,
            warnings=warnings,
        )
        metrics = {
            "rows_scanned": total_rows,
            "rows_returned": len(sample_rows),
            "limit": effective_limit,
            "selected_columns": selected_columns,
        }
        return result, metrics

    return _tracked_run(
        db,
        tenant_id=tenant_id,
        dataset_version_id=dataset_version_id,
        run_type="view_preview",
        work=_work,
    )


def run_view(
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str,
    view_id: str,
    page: int,
    size: int,
    search: str | None = None,
    sort_override: ViewSortSpec | None = None,
) -> ViewRunResult:
    if page < 1:
        raise GuardrailViolation(
            "page inválida.",
            details={"page": page},
            hint="Informe page >= 1.",
        )
    if size < 1:
        raise GuardrailViolation(
            "size inválido.",
            details={"size": size},
            hint="Informe size >= 1.",
        )

    effective_size = min(size, engine_settings.max_view_page_size)
    warnings: list[str] = []
    if effective_size != size:
        warnings.append(
            f"size reduzido para {engine_settings.max_view_page_size} por guardrail de paginação."
        )

    view_record = get_view(db, tenant_id=tenant_id, view_id=view_id)
    payload = view_record.payload
    _validate_dataset_scope(payload, dataset_version_id=dataset_version_id)

    view_lf, selected_columns, local_warnings = _build_view_lazyframe(
        db,
        tenant_id=tenant_id,
        dataset_version_id=dataset_version_id,
        payload=payload,
        search=search,
        sort_override=sort_override,
    )
    warnings.extend(local_warnings)

    offset = (page - 1) * effective_size

    def _work() -> tuple[ViewRunResult, dict[str, Any]]:
        total_rows = _collect_count(view_lf)
        paginated_rows = (
            view_lf.slice(offset=offset, length=effective_size).collect().to_dicts()
        )
        result = ViewRunResult(
            total_rows=total_rows,
            page=page,
            size=effective_size,
            has_next=(page * effective_size) < total_rows,
            has_previous=page > 1,
            columns=selected_columns,
            items=paginated_rows,
            warnings=warnings,
        )
        metrics = {
            "rows_scanned": total_rows,
            "rows_returned": len(paginated_rows),
            "page": page,
            "size": effective_size,
        }
        return result, metrics

    return _tracked_run(
        db,
        tenant_id=tenant_id,
        dataset_version_id=dataset_version_id,
        run_type="view_run",
        work=_work,
    )


def _build_view_lazyframe(  # noqa: C901
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str,
    payload: ViewPayloadV1,
    search: str | None = None,
    sort_override: ViewSortSpec | None = None,
) -> tuple[pl.LazyFrame, list[str], list[str]]:
    payload.validate_guardrails(max_row_limit=engine_settings.max_report_rows)
    column_types = _base_column_types(db, tenant_id=tenant_id, dataset_version_id=dataset_version_id)
    options = _validation_options()
    payload.validate_types(column_types=column_types, options=options)

    lf = _base_lazyframe(db, tenant_id=tenant_id, dataset_version_id=dataset_version_id)[0]
    warnings: list[str] = []
    available_types = dict(column_types)

    selected_columns: list[str] = []
    derived_exprs: list[pl.Expr] = []
    for column in payload.columns:
        if isinstance(column, ViewDerivedColumn):
            transformation = get_transformation(
                db,
                tenant_id=tenant_id,
                transformation_id=column.transformation_id,
            ).payload
            transformation.validate_types(column_types=available_types, options=options)
            output_name = column.alias or transformation.output_column_name
            expr = _compile_expression(
                transformation.expression,
                available_types,
            ).alias(output_name)
            available_types[output_name] = ExpressionDataType(transformation.output_type.value)
            derived_exprs.append(expr)
            selected_columns.append(output_name)
            continue

        name = column.column_name
        if name not in available_types:
            raise GuardrailViolation(
                "Coluna base não encontrada para a view.",
                details={"column": name, "reason": "view_column_not_found"},
                hint="Use colunas disponíveis no catálogo.",
            )
        selected_columns.append(name)

    if not selected_columns:
        raise GuardrailViolation(
            "A view precisa de pelo menos uma coluna.",
            details={"reason": "empty_view_columns"},
            hint="Adicione colunas base ou derivadas na definição da view.",
        )

    if derived_exprs:
        lf = lf.with_columns(derived_exprs)

    filter_exprs: list[pl.Expr] = []
    for segment_id in payload.filters.segment_ids:
        segment = get_segment(db, tenant_id=tenant_id, segment_id=segment_id).payload
        segment.validate_types(column_types=available_types, options=options)
        filter_exprs.append(_compile_expression(segment.filter_expression, available_types))

    if payload.filters.ad_hoc_expression is not None:
        validate_expression(
            payload.filters.ad_hoc_expression,
            column_types=available_types,
            expected_type=ExpressionDataType.BOOL,
            options=options,
        )
        filter_exprs.append(_compile_expression(payload.filters.ad_hoc_expression, available_types))

    if filter_exprs:
        lf = lf.filter(_combine_with_and(filter_exprs))

    if search:
        if "hostname" not in available_types:
            warnings.append("search ignorado: coluna hostname não disponível.")
        else:
            pattern = f"(?i){re.escape(search)}"
            lf = lf.filter(
                pl.col("hostname")
                .cast(pl.String, strict=False)
                .fill_null("")
                .str.contains(pattern)
            )

    final_sort = sort_override or payload.sort
    if final_sort:
        if final_sort.column_name not in selected_columns:
            raise GuardrailViolation(
                "Coluna de ordenação não está na seleção da view.",
                details={
                    "sort_column": final_sort.column_name,
                    "reason": "sort_column_not_selected",
                },
                hint="Inclua a coluna na view ou ajuste o sort.",
            )
        lf = lf.sort(
            by=final_sort.column_name,
            descending=(final_sort.direction == SortDirection.DESC),
        )

    lf = lf.select([pl.col(name) for name in _unique(selected_columns)])
    lf = lf.limit(payload.row_limit)
    return lf, _unique(selected_columns), warnings


def _resolve_segment_filter(
    db: Session,
    *,
    tenant_id: str,
    segment_id: str | None,
    inline_expression: ExpressionNode | None,
    column_types: dict[str, ExpressionDataType],
    options: ExpressionValidationOptions,
) -> pl.Expr:
    if bool(segment_id) == bool(inline_expression):
        raise GuardrailViolation(
            "Informe exatamente um entre segment_id ou expression.",
            details={"reason": "invalid_segment_preview_input"},
            hint="Use apenas um dos campos para preview.",
        )

    if segment_id:
        payload = get_segment(db, tenant_id=tenant_id, segment_id=segment_id).payload
    else:
        payload = SegmentPayloadV1(filter_expression=inline_expression)

    payload.validate_types(column_types=column_types, options=options)
    return _compile_expression(payload.filter_expression, column_types)


def _base_lazyframe(
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str,
) -> tuple[pl.LazyFrame, dict[str, ExpressionDataType]]:
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
            hint="Materialize o dataset antes de executar preview/run.",
        ) from exc
    path = Path(artifact.path)
    if not path.exists():
        raise GuardrailViolation(
            "Artefato materializado não encontrado em disco.",
            details={"dataset_version_id": dataset_version_id, "reason": "artifact_missing_on_disk"},
            hint="Execute materialização antes de rodar preview/run.",
        )
    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()
    column_types: dict[str, ExpressionDataType] = {
        name: normalize_expression_type(str(dtype)) for name, dtype in schema.items()
    }
    return lf, column_types


def _base_column_types(
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str,
) -> dict[str, ExpressionDataType]:
    _, column_types = _base_lazyframe(
        db, tenant_id=tenant_id, dataset_version_id=dataset_version_id
    )
    return column_types


def _compile_expression(  # noqa: C901
    node: ExpressionNode,
    column_types: dict[str, ExpressionDataType],
) -> pl.Expr:
    if isinstance(node, LiteralNode):
        if node.value_type.value == "date":
            return pl.lit(date.fromisoformat(str(node.value)))
        return pl.lit(node.value)

    if isinstance(node, ColumnRefNode):
        if node.column not in column_types:
            raise GuardrailViolation(
                "Coluna não encontrada durante execução da expressão.",
                details={"column": node.column, "reason": "runtime_unknown_column"},
                hint="Revalide a expressão com o catálogo atual.",
            )
        return pl.col(node.column)

    if isinstance(node, UnaryOpNode):
        operand = _compile_expression(node.operand, column_types)
        if node.operator == "NOT":
            return ~operand
        raise InvalidExpressionSyntax(
            "Operador unário não suportado.",
            details={"operator": node.operator},
        )

    if isinstance(node, BinaryOpNode):
        left = _compile_expression(node.left, column_types)
        right = _compile_expression(node.right, column_types)
        if node.operator == "==":
            return left == right
        if node.operator == "!=":
            return left != right
        if node.operator == ">":
            return left > right
        if node.operator == ">=":
            return left >= right
        if node.operator == "<":
            return left < right
        if node.operator == "<=":
            return left <= right
        if node.operator == "IN":
            right_type = validate_expression(
                node.right,
                column_types=column_types,
            )
            if str(right_type.value).startswith("list["):
                return right.list.contains(left)
            raise TypeMismatch(
                "IN exige operando direito de coleção.",
                details={"right_type": right_type.value},
                hint="Use IN apenas contra colunas do tipo lista.",
            )
        raise InvalidExpressionSyntax(
            "Operador binário não suportado.",
            details={"operator": node.operator},
        )

    if isinstance(node, LogicalOpNode):
        compiled = [_compile_expression(clause, column_types) for clause in node.clauses]
        if node.operator == "AND":
            return _combine_with_and(compiled)
        if node.operator == "OR":
            expr = compiled[0]
            for clause in compiled[1:]:
                expr = expr | clause
            return expr
        raise InvalidExpressionSyntax(
            "Operador lógico não suportado.",
            details={"operator": node.operator},
        )

    if isinstance(node, FunctionCallNode):
        args = [_compile_expression(arg, column_types) for arg in node.arguments]
        if node.function_name == "regex_extract":
            pattern = _literal_string(node.arguments[1], "regex_extract.pattern")
            group = _literal_int(node.arguments[2], "regex_extract.group")
            return args[0].cast(pl.String, strict=False).str.extract(pattern, group_index=group)
        if node.function_name == "split_part":
            delimiter = _literal_string(node.arguments[1], "split_part.delimiter")
            index = _literal_int(node.arguments[2], "split_part.index")
            return (
                args[0]
                .cast(pl.String, strict=False)
                .str.split(by=delimiter)
                .list.get(index)
            )
        if node.function_name == "substring":
            start = _literal_int(node.arguments[1], "substring.start")
            length = _literal_int(node.arguments[2], "substring.length")
            return args[0].cast(pl.String, strict=False).str.slice(start, length)
        if node.function_name == "upper":
            return args[0].cast(pl.String, strict=False).str.to_uppercase()
        if node.function_name == "lower":
            return args[0].cast(pl.String, strict=False).str.to_lowercase()
        if node.function_name == "trim":
            return args[0].cast(pl.String, strict=False).str.strip_chars()
        if node.function_name == "date_diff_days":
            source = args[0].cast(pl.Date, strict=False)
            target_node = node.arguments[1]
            if isinstance(target_node, LiteralNode):
                target = pl.lit(date.today())
            else:
                target = args[1].cast(pl.Date, strict=False)
            return (target - source).dt.total_days()
        if node.function_name == "coalesce":
            return pl.coalesce(args)

        raise InvalidExpressionSyntax(
            "Função não suportada.",
            details={"function_name": node.function_name},
        )

    raise InvalidExpressionSyntax("Nó de expressão não suportado.")


def _collect_count(lf: pl.LazyFrame) -> int:
    return int(lf.select(pl.len().alias("total")).collect().item())


def _literal_string(node: ExpressionNode, node_path: str) -> str:
    if not isinstance(node, LiteralNode) or node.value_type.value != "string":
        raise InvalidExpressionSyntax(
            "Argumento literal string esperado.",
            details={"node_path": node_path},
        )
    return str(node.value)


def _literal_int(node: ExpressionNode, node_path: str) -> int:
    if not isinstance(node, LiteralNode) or node.value_type.value != "int":
        raise InvalidExpressionSyntax(
            "Argumento literal inteiro esperado.",
            details={"node_path": node_path},
        )
    return int(node.value)


def _combine_with_and(expressions: list[pl.Expr]) -> pl.Expr:
    if not expressions:
        return pl.lit(True)
    expr = expressions[0]
    for clause in expressions[1:]:
        expr = expr & clause
    return expr


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _validate_dataset_scope(payload: ViewPayloadV1, *, dataset_version_id: str) -> None:
    expected = payload.dataset_scope.dataset_version_id
    if expected != dataset_version_id:
        raise GuardrailViolation(
            "dataset_version_id da view não corresponde ao dataset solicitado.",
            details={
                "view_dataset_version_id": expected,
                "requested_dataset_version_id": dataset_version_id,
                "reason": "dataset_scope_mismatch",
            },
            hint="Atualize a view ou execute no dataset_version correto.",
        )


def _validation_options() -> ExpressionValidationOptions:
    return ExpressionValidationOptions(
        max_nodes=engine_settings.expression_max_nodes,
        max_depth=engine_settings.expression_max_depth,
    )


def _tracked_run(
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str,
    run_type: str,
    work: Callable[[], tuple[Any, dict[str, Any]]],
):
    run = EngineRun(
        tenant_id=tenant_id,
        dataset_version_id=dataset_version_id,
        run_type=run_type,
        status="running",
        started_at=datetime.now(UTC),
    )
    db.add(run)
    db.flush()

    started = time.perf_counter()
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(work)
            result, metrics = future.result(timeout=engine_settings.declarative_timeout_seconds)

        run.status = "success"
        run.ended_at = datetime.now(UTC)
        run.metrics_json = json.dumps(
            {
                **metrics,
                "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
            },
            ensure_ascii=False,
        )
        db.commit()
        return result
    except FuturesTimeout as exc:
        run.status = "error"
        run.ended_at = datetime.now(UTC)
        run.error_truncated = (
            f"execution timeout after {engine_settings.declarative_timeout_seconds}s"
        )
        db.commit()
        raise TimeoutError(run.error_truncated) from exc
    except Exception as exc:
        run.status = "error"
        run.ended_at = datetime.now(UTC)
        run.error_truncated = truncate_error(str(exc))
        db.commit()
        raise
