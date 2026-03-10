from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from types import SimpleNamespace
from typing import Any

from compliance_gate.domains.machines.classification.models import MachineRecord
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.errors import (
    DeclarativeEngineError,
    GuardrailViolation,
    InvalidExpressionSyntax,
    RuleOutputConflict,
    ShadowDivergenceWarning,
    UnreachableRuleWarning,
)
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
)
from compliance_gate.Engine.rulesets import runtime as runtime_module
from compliance_gate.Engine.rulesets.runtime import (
    ClassificationRuntimeMode,
    CompiledRuleSet,
    classify_records,
    compile_ruleset_record,
    evaluate_declarative,
    machine_record_column_types,
)
from compliance_gate.Engine.rulesets.schemas import RuleBlockKind, RuleSetPayloadV2
from compliance_gate.Engine.rulesets.store import RuleSetRecord

_BINARY_OPERATORS = {
    "==",
    "!=",
    ">",
    ">=",
    "<",
    "<=",
    "IN",
    "+",
    "-",
    "*",
    "/",
}
_UNARY_OPERATORS = {"NOT"}
_LOGICAL_OPERATORS = {"AND", "OR"}
_FUNCTIONS = {
    "is_null",
    "is_not_null",
    "contains",
    "starts_with",
    "ends_with",
    "regex_match",
    "regex_extract",
    "split_part",
    "substring",
    "upper",
    "lower",
    "trim",
    "date_now",
    "now_ms",
    "date_diff",
    "date_diff_days",
    "coalesce",
    "to_int",
}


@dataclass(slots=True)
class _ValidationContext:
    issues: list[dict[str, Any]]
    warnings: list[dict[str, Any]]


def validate_ruleset_payload(
    payload: RuleSetPayloadV2,
    *,
    column_types: Mapping[str, str | ExpressionDataType] | None = None,
    options: ExpressionValidationOptions | None = None,
) -> dict[str, Any]:
    resolved_options = options or ExpressionValidationOptions(
        max_nodes=engine_settings.expression_max_nodes,
        max_depth=engine_settings.expression_max_depth,
    )
    resolved_columns = column_types or machine_record_column_types()
    ctx = _ValidationContext(issues=[], warnings=[])
    stages: list[dict[str, Any]] = []

    syntax_stage_issues = _run_syntax_stage(payload)
    stages.append(
        {
            "stage": "syntax",
            "ok": len(syntax_stage_issues) == 0,
            "issues": syntax_stage_issues,
            "warnings": [],
        }
    )
    ctx.issues.extend(syntax_stage_issues)
    if syntax_stage_issues:
        stages.append(_skipped_stage("semantics"))
        stages.append(_skipped_stage("viability"))
        return _validation_response(stages=stages, ctx=ctx)

    semantics_stage_issues = _run_semantics_stage(
        payload,
        column_types=resolved_columns,
        options=resolved_options,
    )
    stages.append(
        {
            "stage": "semantics",
            "ok": len(semantics_stage_issues) == 0,
            "issues": semantics_stage_issues,
            "warnings": [],
        }
    )
    ctx.issues.extend(semantics_stage_issues)
    if semantics_stage_issues:
        stages.append(_skipped_stage("viability"))
        return _validation_response(stages=stages, ctx=ctx)

    viability_issues, viability_warnings = _run_viability_stage(
        payload,
        column_types=resolved_columns,
        options=resolved_options,
    )
    stages.append(
        {
            "stage": "viability",
            "ok": len(viability_issues) == 0,
            "issues": viability_issues,
            "warnings": viability_warnings,
        }
    )
    ctx.issues.extend(viability_issues)
    ctx.warnings.extend(viability_warnings)
    return _validation_response(stages=stages, ctx=ctx)


def explain_row(
    compiled_ruleset: CompiledRuleSet,
    *,
    row: dict[str, Any],
) -> dict[str, Any]:
    machine = _safe_machine_record(row)
    row_data = machine.model_dump()
    final = evaluate_declarative(compiled_ruleset, machine)

    traces: list[dict[str, Any]] = []
    evaluation_order: list[str] = []
    order_counter = 0

    special_match = None
    special_evaluated, special_match, order_counter = _evaluate_block(
        compiled_ruleset.special_rules,
        row_data=row_data,
        traces=traces,
        evaluation_order=evaluation_order,
        order_counter=order_counter,
        stop_on_first_match=True,
        skip_reason="bypass_special",
    )

    primary_match = None
    if special_match is None:
        _, primary_match, order_counter = _evaluate_block(
            compiled_ruleset.primary_rules,
            row_data=row_data,
            traces=traces,
            evaluation_order=evaluation_order,
            order_counter=order_counter,
            stop_on_first_match=True,
            skip_reason="first_match_wins",
        )
        _evaluate_block(
            compiled_ruleset.flag_rules,
            row_data=row_data,
            traces=traces,
            evaluation_order=evaluation_order,
            order_counter=order_counter,
            stop_on_first_match=False,
            skip_reason="none",
        )
    else:
        for entry in compiled_ruleset.primary_rules:
            traces.append(_build_skipped_trace(entry, reason="bypass_special"))
        for entry in compiled_ruleset.flag_rules:
            traces.append(_build_skipped_trace(entry, reason="bypass_special"))

    decision_reason = _decision_reason(
        special_match=special_match,
        primary_match=primary_match,
        matched_rules=final.matched_rule_keys,
        default_status=compiled_ruleset.default_primary_status,
    )

    return {
        "machine_id": machine.hostname,
        "hostname": machine.hostname,
        "final_output": {
            "primary_status": final.primary_status,
            "primary_status_label": final.primary_status_label,
            "flags": final.flags,
        },
        "matched_rules": final.matched_rule_keys,
        "evaluation_order": evaluation_order,
        "rules": traces,
        "decision_reason": decision_reason,
        "special_evaluated": special_evaluated,
    }


def explain_sample(
    compiled_ruleset: CompiledRuleSet,
    *,
    rows: list[dict[str, Any]],
    limit: int | None = None,
) -> dict[str, Any]:
    if len(rows) > engine_settings.classification_max_rows:
        raise GuardrailViolation(
            "Quantidade de linhas excede o limite permitido para explain-sample.",
            details={
                "reason": "explain_sample_row_limit_exceeded",
                "max_rows": engine_settings.classification_max_rows,
                "actual_rows": len(rows),
            },
            hint="Reduza a amostra enviada.",
        )
    resolved_limit = min(limit or 10, len(rows))
    explained = [explain_row(compiled_ruleset, row=row) for row in rows[:resolved_limit]]
    return {
        "total_rows": len(rows),
        "explained_rows": resolved_limit,
        "rows": explained,
    }


def dry_run_ruleset(
    compiled_ruleset: CompiledRuleSet,
    *,
    rows: list[dict[str, Any]],
    mode: ClassificationRuntimeMode,
    explain_sample_limit: int = 5,
) -> dict[str, Any]:
    batch = classify_records(
        rows,
        mode=mode,
        compiled_ruleset=compiled_ruleset,
    )
    status_counts = Counter(item.primary_status for item in batch.outputs)
    flag_counts: Counter[str] = Counter()
    for item in batch.outputs:
        flag_counts.update(item.flags)

    warnings: list[dict[str, Any]] = []
    if mode == ClassificationRuntimeMode.SHADOW and batch.metrics.divergences > 0:
        warning = ShadowDivergenceWarning(
            details={
                "divergences": batch.metrics.divergences,
                "rows_scanned": batch.metrics.rows_scanned,
            },
        )
        warnings.append(_issue_from_exception(warning, stage="dry_run", severity="warning"))

    explain_payload = explain_sample(
        compiled_ruleset,
        rows=rows,
        limit=explain_sample_limit,
    )

    return {
        "mode": mode.value,
        "rows_scanned": batch.metrics.rows_scanned,
        "rows_classified": batch.metrics.rows_classified,
        "elapsed_ms": batch.metrics.elapsed_ms,
        "rule_hits": dict(batch.metrics.rule_hits),
        "divergences": batch.metrics.divergences,
        "status_counts": dict(status_counts),
        "flag_counts": dict(flag_counts),
        "warnings": warnings,
        "sample_explain": explain_payload,
    }


def compile_ruleset_from_payload(
    payload: RuleSetPayloadV2,
    *,
    ruleset_name: str = "preview-ruleset",
    version: int = 0,
    options: ExpressionValidationOptions | None = None,
) -> CompiledRuleSet:
    record = RuleSetRecord(
        definition=SimpleNamespace(name=ruleset_name),
        version=SimpleNamespace(version=version),
        payload=payload,
    )
    return compile_ruleset_record(record=record, options=options)


def _run_syntax_stage(payload: RuleSetPayloadV2) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for block_index, block in enumerate(payload.blocks):
        for entry_index, entry in enumerate(block.entries):
            base = f"blocks[{block_index}].entries[{entry_index}]"
            try:
                _validate_expression_syntax(entry.condition, node_path=f"{base}.condition")
            except DeclarativeEngineError as exc:
                issues.append(_issue_from_exception(exc, stage="syntax"))
    return issues


def _run_semantics_stage(
    payload: RuleSetPayloadV2,
    *,
    column_types: Mapping[str, str | ExpressionDataType],
    options: ExpressionValidationOptions,
) -> list[dict[str, Any]]:
    try:
        payload.validate_types(column_types=column_types, options=options)
        return []
    except DeclarativeEngineError as exc:
        return [_issue_from_exception(exc, stage="semantics")]


def _run_viability_stage(
    payload: RuleSetPayloadV2,
    *,
    column_types: Mapping[str, str | ExpressionDataType],
    options: ExpressionValidationOptions,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    issues: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    try:
        compile_ruleset_from_payload(
            payload,
            options=options,
        )
    except DeclarativeEngineError as exc:
        issues.append(_issue_from_exception(exc, stage="viability"))
        return issues, warnings

    output_conflicts = _detect_output_conflicts(payload)
    issues.extend(output_conflicts)
    warnings.extend(_detect_unreachable_warnings(payload))
    return issues, warnings


def _detect_output_conflicts(payload: RuleSetPayloadV2) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    for block_index, block in enumerate(payload.blocks):
        for entry_index, entry in enumerate(block.entries):
            node_path = f"blocks[{block_index}].entries[{entry_index}].output"
            output = entry.output

            status_values = _values_by_keys(output, ("primary_status", "status", "key"))
            if len(set(status_values.values())) > 1:
                conflicts.append(
                    _issue_from_exception(
                        RuleOutputConflict(
                            details={
                                "node_path": node_path,
                                "kind": "primary_status_alias",
                                "values": status_values,
                            },
                        ),
                        stage="viability",
                    )
                )

            label_values = _values_by_keys(output, ("primary_status_label", "status_label", "label"))
            if len(set(label_values.values())) > 1:
                conflicts.append(
                    _issue_from_exception(
                        RuleOutputConflict(
                            details={
                                "node_path": node_path,
                                "kind": "status_label_alias",
                                "values": label_values,
                            },
                        ),
                        stage="viability",
                    )
                )

            flag = output.get("flag")
            flags = output.get("flags")
            if isinstance(flag, str) and isinstance(flags, list):
                normalized_flags = [item.strip() for item in flags if isinstance(item, str)]
                if flag.strip() and flag.strip() not in normalized_flags:
                    conflicts.append(
                        _issue_from_exception(
                            RuleOutputConflict(
                                details={
                                    "node_path": node_path,
                                    "kind": "flag_alias",
                                    "flag": flag.strip(),
                                    "flags": normalized_flags,
                                },
                            ),
                            stage="viability",
                        )
                    )
    return conflicts


def _detect_unreachable_warnings(payload: RuleSetPayloadV2) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for block_index, block in enumerate(payload.blocks):
        if block.kind not in {RuleBlockKind.SPECIAL, RuleBlockKind.PRIMARY}:
            continue

        ordered = sorted(enumerate(block.entries), key=lambda item: (item[1].priority, item[0]))
        always_true_rule_key: str | None = None
        for local_order, (entry_index, entry) in enumerate(ordered):
            rule_key = entry.rule_key or f"{block.kind.value}:{entry.priority}:{entry_index}"
            node_path = f"blocks[{block_index}].entries[{entry_index}].condition"
            if always_true_rule_key is not None:
                warning = UnreachableRuleWarning(
                    details={
                        "node_path": node_path,
                        "block_kind": block.kind.value,
                        "rule_key": rule_key,
                        "blocked_by_rule_key": always_true_rule_key,
                        "order_index": local_order,
                    },
                )
                warnings.append(_issue_from_exception(warning, stage="viability", severity="warning"))
                continue

            constant = _constant_bool(entry.condition)
            if constant is True:
                always_true_rule_key = rule_key
    return warnings


def _validate_expression_syntax(node: ExpressionNode, *, node_path: str) -> None:
    if isinstance(node, LiteralNode):
        return
    if isinstance(node, ColumnRefNode):
        return
    if isinstance(node, UnaryOpNode):
        if node.operator not in _UNARY_OPERATORS:
            raise InvalidExpressionSyntax(
                "Operador unário não suportado.",
                details={"node_path": node_path, "operator": node.operator},
                hint="Use apenas operador NOT.",
            )
        _validate_expression_syntax(node.operand, node_path=f"{node_path}.operand")
        return
    if isinstance(node, BinaryOpNode):
        if node.operator not in _BINARY_OPERATORS:
            raise InvalidExpressionSyntax(
                "Operador binário não suportado.",
                details={"node_path": node_path, "operator": node.operator},
            )
        _validate_expression_syntax(node.left, node_path=f"{node_path}.left")
        _validate_expression_syntax(node.right, node_path=f"{node_path}.right")
        return
    if isinstance(node, LogicalOpNode):
        if node.operator not in _LOGICAL_OPERATORS:
            raise InvalidExpressionSyntax(
                "Operador lógico não suportado.",
                details={"node_path": node_path, "operator": node.operator},
            )
        if len(node.clauses) < 2:
            raise InvalidExpressionSyntax(
                "Operação lógica exige ao menos duas cláusulas.",
                details={"node_path": node_path, "operator": node.operator},
            )
        for index, clause in enumerate(node.clauses):
            _validate_expression_syntax(clause, node_path=f"{node_path}.clauses[{index}]")
        return
    if isinstance(node, FunctionCallNode):
        if node.function_name not in _FUNCTIONS:
            raise InvalidExpressionSyntax(
                "Função não suportada.",
                details={"node_path": node_path, "function_name": node.function_name},
            )
        for index, argument in enumerate(node.arguments):
            _validate_expression_syntax(argument, node_path=f"{node_path}.arguments[{index}]")
        return
    raise InvalidExpressionSyntax(
        "Nó de expressão inválido.",
        details={"node_path": node_path},
    )


def _evaluate_block(
    entries,
    *,
    row_data: dict[str, Any],
    traces: list[dict[str, Any]],
    evaluation_order: list[str],
    order_counter: int,
    stop_on_first_match: bool,
    skip_reason: str,
) -> tuple[int, str | None, int]:
    matched_rule: str | None = None
    evaluated_count = 0
    for index, entry in enumerate(entries):
        if matched_rule is not None and stop_on_first_match:
            traces.append(_build_skipped_trace(entry, reason=skip_reason))
            continue

        order_counter += 1
        evaluated_count += 1
        evaluation_order.append(entry.rule_key)
        condition_result, failed_conditions, condition_trace = _evaluate_condition(
            entry.condition,
            row_data=row_data,
        )
        matched = bool(condition_result)
        traces.append(
            {
                "rule_key": entry.rule_key,
                "block_kind": entry.block_kind.value,
                "priority": entry.priority,
                "evaluation_order": order_counter,
                "evaluated": True,
                "matched": matched,
                "condition_result": condition_result,
                "failed_conditions": failed_conditions,
                "condition_trace": condition_trace,
                "skip_reason": None,
                "output_preview": entry.output,
                "entry_index": index,
            }
        )
        if matched and stop_on_first_match:
            matched_rule = entry.rule_key

    return evaluated_count, matched_rule, order_counter


def _evaluate_condition(
    condition: ExpressionNode,
    *,
    row_data: dict[str, Any],
) -> tuple[bool, list[dict[str, Any]], list[dict[str, Any]]]:
    trace: list[dict[str, Any]] = []
    value, failures = _eval_node(
        condition,
        row_data=row_data,
        node_path="root",
        trace=trace,
    )
    result = bool(value)
    if not result and not failures:
        failures = [
            {
                "node_path": "root",
                "message": "Condição retornou falso.",
                "details": {"value": _json_safe(value)},
            }
        ]
    return result, failures, trace


def _eval_node(
    node: ExpressionNode,
    *,
    row_data: dict[str, Any],
    node_path: str,
    trace: list[dict[str, Any]],
) -> tuple[Any, list[dict[str, Any]]]:
    if isinstance(node, LogicalOpNode):
        failures: list[dict[str, Any]] = []
        if node.operator == "AND":
            for index, clause in enumerate(node.clauses):
                value, clause_failures = _eval_node(
                    clause,
                    row_data=row_data,
                    node_path=f"{node_path}.clauses[{index}]",
                    trace=trace,
                )
                if not bool(value):
                    failures.extend(
                        clause_failures
                        or [
                            {
                                "node_path": f"{node_path}.clauses[{index}]",
                                "message": "Cláusula AND retornou falso.",
                                "details": {"value": _json_safe(value)},
                            }
                        ]
                    )
                    trace.append(
                        {
                            "node_path": node_path,
                            "node_type": "logical_op",
                            "operator": node.operator,
                            "value": False,
                        }
                    )
                    return False, failures
            trace.append(
                {
                    "node_path": node_path,
                    "node_type": "logical_op",
                    "operator": node.operator,
                    "value": True,
                }
            )
            return True, []

        if node.operator == "OR":
            for index, clause in enumerate(node.clauses):
                value, clause_failures = _eval_node(
                    clause,
                    row_data=row_data,
                    node_path=f"{node_path}.clauses[{index}]",
                    trace=trace,
                )
                if bool(value):
                    trace.append(
                        {
                            "node_path": node_path,
                            "node_type": "logical_op",
                            "operator": node.operator,
                            "value": True,
                        }
                    )
                    return True, []
                failures.extend(
                    clause_failures
                    or [
                        {
                            "node_path": f"{node_path}.clauses[{index}]",
                            "message": "Cláusula OR retornou falso.",
                            "details": {"value": _json_safe(value)},
                        }
                    ]
                )
            trace.append(
                {
                    "node_path": node_path,
                    "node_type": "logical_op",
                    "operator": node.operator,
                    "value": False,
                }
            )
            return False, failures

    value = _evaluate_expression_value(node, row_data=row_data)
    trace.append(
        {
            "node_path": node_path,
            "node_type": node.node_type,
            "value": _json_safe(value),
        }
    )
    if bool(value):
        return value, []
    return value, [
        {
            "node_path": node_path,
            "message": "Condição retornou falso.",
            "details": {"value": _json_safe(value)},
        }
    ]


def _evaluate_expression_value(node: ExpressionNode, *, row_data: dict[str, Any]) -> Any:
    try:
        program = runtime_module._compile_expression(node)
        return program(row_data)
    except DeclarativeEngineError:
        raise
    except Exception as exc:  # pragma: no cover - defensive branch
        raise GuardrailViolation(
            "Falha ao avaliar expressão durante explain.",
            details={
                "reason": "explain_expression_evaluation_error",
                "error_type": type(exc).__name__,
            },
            hint="Revise a condição da regra antes de executar explain.",
        ) from exc


def _decision_reason(
    *,
    special_match: str | None,
    primary_match: str | None,
    matched_rules: list[str],
    default_status: str,
) -> str:
    if special_match:
        return f"Regra special `{special_match}` acionou bypass e definiu a decisão final."
    if primary_match:
        flags = [rule for rule in matched_rules if rule.startswith("flags:")]
        if flags:
            return (
                f"Regra primary `{primary_match}` definiu o status; flags aditivas aplicadas: "
                f"{', '.join(flags)}."
            )
        return f"Regra primary `{primary_match}` definiu o status final (first-match-wins)."
    flags = [rule for rule in matched_rules if rule.startswith("flags:")]
    if flags:
        return (
            f"Nenhuma primary bateu; status default `{default_status}` aplicado com flags: "
            f"{', '.join(flags)}."
        )
    return f"Nenhuma regra special/primary bateu; status default `{default_status}` aplicado."


def _build_skipped_trace(entry, *, reason: str) -> dict[str, Any]:
    return {
        "rule_key": entry.rule_key,
        "block_kind": entry.block_kind.value,
        "priority": entry.priority,
        "evaluation_order": None,
        "evaluated": False,
        "matched": False,
        "condition_result": None,
        "failed_conditions": [],
        "condition_trace": [],
        "skip_reason": reason,
        "output_preview": entry.output,
        "entry_index": None,
    }


def _constant_bool(expression: ExpressionNode) -> bool | None:
    if _contains_column_ref(expression):
        return None
    try:
        value = _evaluate_expression_value(expression, row_data={})
    except DeclarativeEngineError:
        return None
    return bool(value)


def _contains_column_ref(node: ExpressionNode) -> bool:
    if isinstance(node, ColumnRefNode):
        return True
    if isinstance(node, UnaryOpNode):
        return _contains_column_ref(node.operand)
    if isinstance(node, BinaryOpNode):
        return _contains_column_ref(node.left) or _contains_column_ref(node.right)
    if isinstance(node, LogicalOpNode):
        return any(_contains_column_ref(clause) for clause in node.clauses)
    if isinstance(node, FunctionCallNode):
        return any(_contains_column_ref(argument) for argument in node.arguments)
    return False


def _values_by_keys(output: dict[str, Any], keys: tuple[str, ...]) -> dict[str, str]:
    values: dict[str, str] = {}
    for key in keys:
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            values[key] = value.strip()
    return values


def _validation_response(*, stages: list[dict[str, Any]], ctx: _ValidationContext) -> dict[str, Any]:
    return {
        "is_valid": len(ctx.issues) == 0,
        "stages": stages,
        "issues": ctx.issues,
        "warnings": ctx.warnings,
        "summary": {
            "error_count": len(ctx.issues),
            "warning_count": len(ctx.warnings),
        },
    }


def _skipped_stage(stage: str) -> dict[str, Any]:
    return {
        "stage": stage,
        "ok": False,
        "issues": [
            {
                "code": "StageSkipped",
                "message": "Estágio não executado porque o estágio anterior falhou.",
                "details": {"stage": stage},
                "hint": "Corrija os erros do estágio anterior e tente novamente.",
                "node_path": None,
                "stage": stage,
                "severity": "info",
            }
        ],
        "warnings": [],
    }


def _issue_from_exception(
    exc: DeclarativeEngineError,
    *,
    stage: str,
    severity: str = "error",
) -> dict[str, Any]:
    payload = exc.to_payload()
    details = dict(payload.details)
    node_path = payload.node_path
    if node_path is None:
        raw_node_path = details.get("node_path")
        node_path = raw_node_path if isinstance(raw_node_path, str) else None
    return {
        "code": payload.code,
        "message": payload.message,
        "details": details,
        "hint": payload.hint,
        "node_path": node_path,
        "stage": stage,
        "severity": severity,
    }


def _safe_machine_record(raw: dict[str, Any]) -> MachineRecord:
    try:
        return MachineRecord(**raw)
    except Exception as exc:  # pragma: no cover - defensive branch
        raise GuardrailViolation(
            "Linha inválida para explain.",
            details={
                "reason": "invalid_row_payload",
                "error_type": type(exc).__name__,
            },
            hint="Envie os campos obrigatórios de MachineRecord para explain-row.",
            node_path="row",
        ) from exc


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)
