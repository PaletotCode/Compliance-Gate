from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from types import UnionType
from typing import Any, get_args, get_origin

from compliance_gate.domains.machines.classification.models import (
    MachineRecord,
    MachineStatusResult,
)
from compliance_gate.domains.machines.classification.orchestrator import evaluate_machine
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.errors import GuardrailViolation, InvalidExpressionSyntax
from compliance_gate.Engine.expressions import (
    BinaryOpNode,
    ColumnRefNode,
    ExpressionDataType,
    ExpressionNode,
    ExpressionValidationOptions,
    FunctionCallNode,
    LiteralNode,
    LiteralValueType,
    LogicalOpNode,
    UnaryOpNode,
    validate_expression,
)
from compliance_gate.Engine.rulesets.schemas import (
    ClassificationMigrationPhase,
    ClassificationRuntimeMode,
    RuleBlockKind,
    RuleSetPayloadV2,
)
from compliance_gate.Engine.rulesets.store import RuleSetRecord, get_ruleset_by_name
from compliance_gate.Engine.rulesets.template_library import (
    special_primary_status_keys,
    status_severity_by_key,
)

DEFAULT_CLASSIFICATION_CONTEXT: dict[str, Any] = {
    "stale_days": int(engine_settings.classification_stale_days),
}
SPECIAL_PRIMARY_STATUSES = special_primary_status_keys(
    stale_days=int(engine_settings.classification_stale_days),
)
STATUS_SEVERITY_BY_KEY = status_severity_by_key(
    stale_days=int(engine_settings.classification_stale_days),
)


@dataclass(slots=True)
class RuleSetRuntimeConfig:
    mode: ClassificationRuntimeMode
    ruleset_name: str | None


@dataclass(slots=True)
class ClassificationOutput:
    primary_status: str
    primary_status_label: str
    flags: list[str] = field(default_factory=list)
    matched_rule_keys: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ClassificationDivergence:
    machine_id: str
    hostname: str
    legacy_primary_status: str
    legacy_primary_status_label: str
    legacy_flags: list[str]
    declarative_primary_status: str
    declarative_primary_status_label: str
    declarative_flags: list[str]
    diff: dict[str, Any]
    rule_keys: list[str] = field(default_factory=list)
    severity: str = "UNKNOWN"
    divergence_kind: str = "unknown"


@dataclass(slots=True)
class ClassificationRuntimeMetrics:
    mode: ClassificationRuntimeMode
    ruleset_name: str | None
    cutover_phase: ClassificationMigrationPhase | None
    rows_scanned: int
    rows_classified: int
    elapsed_ms: float
    rule_hits: dict[str, int]
    divergences: int


@dataclass(slots=True)
class ClassificationBatchResult:
    outputs: list[ClassificationOutput]
    metrics: ClassificationRuntimeMetrics
    divergences: list[ClassificationDivergence]


@dataclass(slots=True)
class _CompiledRuleEntry:
    rule_key: str
    block_kind: RuleBlockKind
    priority: int
    condition: ExpressionNode
    output: dict[str, Any]
    predicate: Callable[[dict[str, Any]], bool]


@dataclass(slots=True)
class CompiledRuleSet:
    ruleset_name: str
    ruleset_version: int
    special_rules: list[_CompiledRuleEntry]
    primary_rules: list[_CompiledRuleEntry]
    flag_rules: list[_CompiledRuleEntry]
    default_primary_status: str
    default_primary_status_label: str


def machine_record_column_types() -> dict[str, ExpressionDataType]:
    column_types: dict[str, ExpressionDataType] = {}
    for name, field_info in MachineRecord.model_fields.items():
        expression_type = _annotation_to_expression_type(field_info.annotation)
        if expression_type == ExpressionDataType.UNKNOWN:
            continue
        column_types[name] = expression_type
    return column_types


def compile_published_ruleset(
    *,
    db,
    tenant_id: str,
    ruleset_name: str,
    options: ExpressionValidationOptions | None = None,
) -> CompiledRuleSet:
    record = get_ruleset_by_name(
        db,
        tenant_id=tenant_id,
        name=ruleset_name,
        resolution="published",
    )
    return compile_ruleset_record(record=record, options=options)


def compile_ruleset_record(
    *,
    record: RuleSetRecord,
    options: ExpressionValidationOptions | None = None,
) -> CompiledRuleSet:
    resolved_options = options or ExpressionValidationOptions(
        max_nodes=engine_settings.expression_max_nodes,
        max_depth=engine_settings.expression_max_depth,
    )
    return _compile_ruleset_payload(
        payload=record.payload,
        ruleset_name=record.definition.name,
        version=record.version.version,
        options=resolved_options,
    )


def classify_records(
    records: list[dict[str, Any]],
    *,
    mode: ClassificationRuntimeMode,
    compiled_ruleset: CompiledRuleSet | None,
    context: dict[str, Any] | None = None,
    cutover_phase: ClassificationMigrationPhase | None = None,
) -> ClassificationBatchResult:
    _guard_classification_input(records)

    resolved_context = context or DEFAULT_CLASSIFICATION_CONTEXT
    timeout_seconds = max(1, engine_settings.classification_timeout_seconds)
    deadline = time.perf_counter() + timeout_seconds

    outputs: list[ClassificationOutput] = []
    divergences: list[ClassificationDivergence] = []
    rule_hits: dict[str, int] = {}
    start = time.perf_counter()

    for row_index, raw in enumerate(records):
        _check_deadline(deadline=deadline, row_index=row_index)

        machine = MachineRecord(**raw)
        legacy_result = evaluate_machine(machine, stale_days_config=int(resolved_context.get("stale_days", 45)))

        if mode == ClassificationRuntimeMode.LEGACY:
            outputs.append(
                ClassificationOutput(
                    primary_status=legacy_result.primary_status,
                    primary_status_label=legacy_result.primary_status_label,
                    flags=list(legacy_result.flags),
                    matched_rule_keys=[],
                )
            )
            continue

        if compiled_ruleset is None:
            raise GuardrailViolation(
                "Modo declarativo requer RuleSet compilado.",
                details={"reason": "compiled_ruleset_missing", "mode": mode.value},
                hint="Publique um RuleSet e configure o tenant para shadow/declarative.",
            )

        declarative_result = evaluate_declarative(
            compiled_ruleset,
            machine,
            deadline=deadline,
        )
        for key in declarative_result.matched_rule_keys:
            rule_hits[key] = rule_hits.get(key, 0) + 1

        if mode == ClassificationRuntimeMode.SHADOW:
            outputs.append(
                ClassificationOutput(
                    primary_status=legacy_result.primary_status,
                    primary_status_label=legacy_result.primary_status_label,
                    flags=list(legacy_result.flags),
                    matched_rule_keys=declarative_result.matched_rule_keys,
                )
            )
            diff = _build_diff(
                legacy=legacy_result,
                declarative=declarative_result,
                rule_keys=declarative_result.matched_rule_keys,
            )
            if diff:
                divergence_kind = _infer_divergence_kind(diff)
                severity = _infer_divergence_severity(diff)
                if len(divergences) < engine_settings.classification_max_divergences_per_run:
                    divergences.append(
                        ClassificationDivergence(
                            machine_id=str(raw.get("hostname") or raw.get("machine_id") or ""),
                            hostname=str(raw.get("hostname") or ""),
                            legacy_primary_status=legacy_result.primary_status,
                            legacy_primary_status_label=legacy_result.primary_status_label,
                            legacy_flags=list(legacy_result.flags),
                            declarative_primary_status=declarative_result.primary_status,
                            declarative_primary_status_label=declarative_result.primary_status_label,
                            declarative_flags=list(declarative_result.flags),
                            diff=diff,
                            rule_keys=list(declarative_result.matched_rule_keys),
                            severity=severity,
                            divergence_kind=divergence_kind,
                        )
                    )
                continue

        if mode == ClassificationRuntimeMode.DECLARATIVE and cutover_phase is not None:
            outputs.append(
                _apply_cutover_phase_output(
                    phase=cutover_phase,
                    legacy=legacy_result,
                    declarative=declarative_result,
                )
            )
            continue

        outputs.append(declarative_result)

    elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
    metrics = ClassificationRuntimeMetrics(
        mode=mode,
        ruleset_name=compiled_ruleset.ruleset_name if compiled_ruleset else None,
        cutover_phase=cutover_phase if mode == ClassificationRuntimeMode.DECLARATIVE else None,
        rows_scanned=len(records),
        rows_classified=len(outputs),
        elapsed_ms=elapsed_ms,
        rule_hits=rule_hits,
        divergences=len(divergences),
    )
    return ClassificationBatchResult(outputs=outputs, metrics=metrics, divergences=divergences)


def evaluate_declarative(
    compiled_ruleset: CompiledRuleSet,
    machine: MachineRecord,
    *,
    deadline: float | None = None,
) -> ClassificationOutput:
    row = machine.model_dump()

    special_hit = _first_match(compiled_ruleset.special_rules, row=row, deadline=deadline)
    if special_hit is not None:
        return ClassificationOutput(
            primary_status=_resolve_primary_status(
                special_hit.output,
                fallback_status=compiled_ruleset.default_primary_status,
            ),
            primary_status_label=_resolve_primary_status_label(
                special_hit.output,
                fallback_label=compiled_ruleset.default_primary_status_label,
                fallback_status=compiled_ruleset.default_primary_status,
            ),
            flags=_resolve_flags(special_hit.output),
            matched_rule_keys=[special_hit.rule_key],
        )

    matched_keys: list[str] = []
    primary_status = compiled_ruleset.default_primary_status
    primary_status_label = compiled_ruleset.default_primary_status_label

    primary_hit = _first_match(compiled_ruleset.primary_rules, row=row, deadline=deadline)
    if primary_hit is not None:
        matched_keys.append(primary_hit.rule_key)
        primary_status = _resolve_primary_status(
            primary_hit.output,
            fallback_status=compiled_ruleset.default_primary_status,
        )
        primary_status_label = _resolve_primary_status_label(
            primary_hit.output,
            fallback_label=compiled_ruleset.default_primary_status_label,
            fallback_status=primary_status,
        )

    flags: list[str] = []
    for entry in compiled_ruleset.flag_rules:
        if deadline is not None:
            _check_deadline(deadline=deadline, row_index=-1)
        if _evaluate_predicate(entry=entry, row=row):
            matched_keys.append(entry.rule_key)
            flags.extend(_resolve_flags(entry.output))

    return ClassificationOutput(
        primary_status=primary_status,
        primary_status_label=primary_status_label,
        flags=_unique_preserve_order(flags),
        matched_rule_keys=matched_keys,
    )


def _apply_cutover_phase_output(
    *,
    phase: ClassificationMigrationPhase,
    legacy: MachineStatusResult,
    declarative: ClassificationOutput,
) -> ClassificationOutput:
    if phase == ClassificationMigrationPhase.A:
        # Fase A: mantém primary legado e aplica somente flags declarativas.
        # Special legado ainda é bypass completo.
        if legacy.primary_status in SPECIAL_PRIMARY_STATUSES:
            flags = list(legacy.flags)
        else:
            flags = list(declarative.flags)
        return ClassificationOutput(
            primary_status=legacy.primary_status,
            primary_status_label=legacy.primary_status_label,
            flags=flags,
            matched_rule_keys=list(declarative.matched_rule_keys),
        )

    if phase == ClassificationMigrationPhase.B:
        # Fase B: primary+flags declarativos, exceto special legado.
        if legacy.primary_status in SPECIAL_PRIMARY_STATUSES:
            return ClassificationOutput(
                primary_status=legacy.primary_status,
                primary_status_label=legacy.primary_status_label,
                flags=list(legacy.flags),
                matched_rule_keys=list(declarative.matched_rule_keys),
            )
        return ClassificationOutput(
            primary_status=declarative.primary_status,
            primary_status_label=declarative.primary_status_label,
            flags=list(declarative.flags),
            matched_rule_keys=list(declarative.matched_rule_keys),
        )

    if phase == ClassificationMigrationPhase.C:
        # Fase C: special+primary+flags declarativos com dual-check ainda ativo.
        return ClassificationOutput(
            primary_status=declarative.primary_status,
            primary_status_label=declarative.primary_status_label,
            flags=list(declarative.flags),
            matched_rule_keys=list(declarative.matched_rule_keys),
        )

    # Fase D: declarative-only.
    return ClassificationOutput(
        primary_status=declarative.primary_status,
        primary_status_label=declarative.primary_status_label,
        flags=list(declarative.flags),
        matched_rule_keys=list(declarative.matched_rule_keys),
    )


def _compile_ruleset_payload(
    *,
    payload: RuleSetPayloadV2,
    ruleset_name: str,
    version: int,
    options: ExpressionValidationOptions,
) -> CompiledRuleSet:
    column_types = machine_record_column_types()
    total_rules = sum(len(block.entries) for block in payload.blocks)
    if total_rules > engine_settings.classification_max_rules:
        raise GuardrailViolation(
            "RuleSet excede o número máximo de regras permitido.",
            details={
                "reason": "ruleset_rule_count_exceeded",
                "max_rules": engine_settings.classification_max_rules,
                "actual_rules": total_rules,
            },
            hint="Reduza o número de regras no RuleSet publicado.",
        )

    block_by_kind = payload.blocks_by_kind()
    compiled_special = _compile_block(
        kind=RuleBlockKind.SPECIAL,
        entries=block_by_kind.get(RuleBlockKind.SPECIAL).entries
        if block_by_kind.get(RuleBlockKind.SPECIAL)
        else [],
        column_types=column_types,
        options=options,
    )
    compiled_primary = _compile_block(
        kind=RuleBlockKind.PRIMARY,
        entries=block_by_kind.get(RuleBlockKind.PRIMARY).entries
        if block_by_kind.get(RuleBlockKind.PRIMARY)
        else [],
        column_types=column_types,
        options=options,
    )
    compiled_flags = _compile_block(
        kind=RuleBlockKind.FLAGS,
        entries=block_by_kind.get(RuleBlockKind.FLAGS).entries
        if block_by_kind.get(RuleBlockKind.FLAGS)
        else [],
        column_types=column_types,
        options=options,
    )

    return CompiledRuleSet(
        ruleset_name=ruleset_name,
        ruleset_version=version,
        special_rules=compiled_special,
        primary_rules=compiled_primary,
        flag_rules=compiled_flags,
        default_primary_status=engine_settings.classification_default_primary_status,
        default_primary_status_label=engine_settings.classification_default_primary_status_label,
    )


def _compile_block(
    *,
    kind: RuleBlockKind,
    entries,
    column_types: dict[str, ExpressionDataType],
    options: ExpressionValidationOptions,
) -> list[_CompiledRuleEntry]:
    ordered = sorted(enumerate(entries), key=lambda item: (item[1].priority, item[0]))
    compiled: list[_CompiledRuleEntry] = []
    for _sequence, (original_index, entry) in enumerate(ordered):
        validate_expression(
            entry.condition,
            column_types=column_types,
            expected_type=ExpressionDataType.BOOL,
            options=options,
        )
        _validate_entry_output(kind=kind, output=entry.output)
        rule_key = entry.rule_key or f"{kind.value}:{entry.priority}:{original_index}"
        compiled.append(
            _CompiledRuleEntry(
                rule_key=f"{kind.value}:{rule_key}",
                block_kind=kind,
                priority=entry.priority,
                condition=entry.condition,
                output=dict(entry.output),
                predicate=_compile_boolean_expression(entry.condition),
            )
        )
    return compiled


def _validate_entry_output(*, kind: RuleBlockKind, output: dict[str, Any]) -> None:
    if kind in {RuleBlockKind.SPECIAL, RuleBlockKind.PRIMARY}:
        if _resolve_primary_status(output, fallback_status=""):
            return
        raise GuardrailViolation(
            "Regra special/primary precisa definir primary_status no output.",
            details={"reason": "invalid_rule_output", "block_kind": kind.value},
            hint="Inclua 'primary_status' ou 'status' no output da regra.",
        )

    if kind == RuleBlockKind.FLAGS:
        flags = _resolve_flags(output)
        if flags:
            return
        raise GuardrailViolation(
            "Regra de flags precisa definir pelo menos um flag no output.",
            details={"reason": "invalid_rule_output", "block_kind": kind.value},
            hint="Inclua 'flag' ou 'flags' no output da regra.",
        )


def _compile_boolean_expression(node: ExpressionNode) -> Callable[[dict[str, Any]], bool]:
    program = _compile_expression(node)

    def predicate(row: dict[str, Any]) -> bool:
        result = program(row)
        return bool(result)

    return predicate


def _compile_expression(node: ExpressionNode) -> Callable[[dict[str, Any]], Any]:  # noqa: C901
    if isinstance(node, LiteralNode):
        if node.value_type == LiteralValueType.DATE:
            literal_date = date.fromisoformat(str(node.value))
            return lambda _row, value=literal_date: value
        return lambda _row, value=node.value: value

    if isinstance(node, ColumnRefNode):
        return lambda row, column=node.column: row.get(column)

    if isinstance(node, UnaryOpNode):
        operand = _compile_expression(node.operand)
        if node.operator == "NOT":
            return lambda row: not bool(operand(row))
        raise InvalidExpressionSyntax(
            "Operador unário não suportado no runtime declarativo.",
            details={"operator": node.operator},
        )

    if isinstance(node, BinaryOpNode):
        left = _compile_expression(node.left)
        right = _compile_expression(node.right)
        if node.operator == "==":
            return lambda row: left(row) == right(row)
        if node.operator == "!=":
            return lambda row: left(row) != right(row)
        if node.operator == ">":
            return lambda row: _safe_compare(
                left(row),
                right(row),
                lambda left_value, right_value: left_value > right_value,
            )
        if node.operator == ">=":
            return lambda row: _safe_compare(
                left(row),
                right(row),
                lambda left_value, right_value: left_value >= right_value,
            )
        if node.operator == "<":
            return lambda row: _safe_compare(
                left(row),
                right(row),
                lambda left_value, right_value: left_value < right_value,
            )
        if node.operator == "<=":
            return lambda row: _safe_compare(
                left(row),
                right(row),
                lambda left_value, right_value: left_value <= right_value,
            )
        if node.operator == "IN":
            return lambda row: _safe_in(left(row), right(row))
        if node.operator == "+":
            return lambda row: _safe_math(left(row), right(row), "+")
        if node.operator == "-":
            return lambda row: _safe_math(left(row), right(row), "-")
        if node.operator == "*":
            return lambda row: _safe_math(left(row), right(row), "*")
        if node.operator == "/":
            return lambda row: _safe_math(left(row), right(row), "/")
        raise InvalidExpressionSyntax(
            "Operador binário não suportado no runtime declarativo.",
            details={"operator": node.operator},
        )

    if isinstance(node, LogicalOpNode):
        clauses = [_compile_expression(clause) for clause in node.clauses]
        if node.operator == "AND":
            return lambda row: all(bool(clause(row)) for clause in clauses)
        if node.operator == "OR":
            return lambda row: any(bool(clause(row)) for clause in clauses)
        raise InvalidExpressionSyntax(
            "Operador lógico não suportado no runtime declarativo.",
            details={"operator": node.operator},
        )

    if isinstance(node, FunctionCallNode):
        args = [_compile_expression(argument) for argument in node.arguments]
        fn = node.function_name

        if fn == "is_null":
            return lambda row: args[0](row) is None
        if fn == "is_not_null":
            return lambda row: args[0](row) is not None
        if fn == "contains":
            return lambda row: _safe_contains(args[0](row), args[1](row))
        if fn == "starts_with":
            return lambda row: _safe_string(args[0](row)).startswith(_safe_string(args[1](row)))
        if fn == "ends_with":
            return lambda row: _safe_string(args[0](row)).endswith(_safe_string(args[1](row)))
        if fn == "regex_match":
            pattern = _literal_pattern(node.arguments[1])
            compiled_pattern = re.compile(pattern)
            return lambda row: bool(compiled_pattern.search(_safe_string(args[0](row))))
        if fn == "regex_extract":
            pattern = _literal_pattern(node.arguments[1])
            group = _literal_int(node.arguments[2])
            compiled_pattern = re.compile(pattern)
            return lambda row: _safe_regex_extract(compiled_pattern, _safe_string(args[0](row)), group)
        if fn == "split_part":
            delimiter = _safe_string(_literal_value(node.arguments[1]))
            index = _literal_int(node.arguments[2])
            return lambda row: _safe_split_part(_safe_string(args[0](row)), delimiter, index)
        if fn == "substring":
            start = _literal_int(node.arguments[1])
            length = _literal_int(node.arguments[2])
            return lambda row: _safe_string(args[0](row))[start : start + length]
        if fn == "upper":
            return lambda row: _safe_string(args[0](row)).upper()
        if fn == "lower":
            return lambda row: _safe_string(args[0](row)).lower()
        if fn == "trim":
            return lambda row: _safe_string(args[0](row)).strip()
        if fn == "date_now":
            return lambda _row: date.today()
        if fn == "now_ms":
            return lambda _row: int(time.time() * 1000)
        if fn == "date_diff":
            unit = _safe_string(_literal_value(node.arguments[2])).lower()
            return lambda row: _safe_date_diff(args[0](row), args[1](row), unit)
        if fn == "date_diff_days":
            return lambda row: _safe_date_diff(args[0](row), args[1](row), "days")
        if fn == "coalesce":
            return lambda row: _safe_coalesce(*(compiled(row) for compiled in args))
        if fn == "to_int":
            return lambda row: _safe_to_int(args[0](row))

        raise InvalidExpressionSyntax(
            "Função não suportada no runtime declarativo.",
            details={"function_name": fn},
        )

    raise InvalidExpressionSyntax("Nó de expressão não suportado no runtime declarativo.")


def _guard_classification_input(records: list[dict[str, Any]]) -> None:
    if len(records) > engine_settings.classification_max_rows:
        raise GuardrailViolation(
            "Quantidade de linhas excede o limite de classificação declarativa.",
            details={
                "reason": "classification_row_limit_exceeded",
                "max_rows": engine_settings.classification_max_rows,
                "actual_rows": len(records),
            },
            hint="Processe o dataset em lotes menores.",
        )

    if not records:
        return

    sample_size = min(len(records), 100)
    sample_bytes = sum(len(str(item)) for item in records[:sample_size])
    avg_bytes = sample_bytes / sample_size
    estimated_mb = (avg_bytes * len(records)) / (1024 * 1024)
    if estimated_mb > engine_settings.classification_memory_budget_mb:
        raise GuardrailViolation(
            "Estimativa de memória excede o orçamento da classificação declarativa.",
            details={
                "reason": "classification_memory_budget_exceeded",
                "estimated_mb": round(estimated_mb, 2),
                "memory_budget_mb": engine_settings.classification_memory_budget_mb,
            },
            hint="Reduza o volume por execução ou aumente o orçamento operacional.",
        )


def _check_deadline(*, deadline: float, row_index: int) -> None:
    if time.perf_counter() <= deadline:
        return
    raise GuardrailViolation(
        "Classificação declarativa excedeu o timeout configurado.",
        details={
            "reason": "classification_timeout",
            "timeout_seconds": engine_settings.classification_timeout_seconds,
            "row_index": row_index,
        },
        hint="Reduza complexidade das regras ou aumente o timeout operacional.",
    )


def _first_match(
    entries: list[_CompiledRuleEntry],
    *,
    row: dict[str, Any],
    deadline: float | None,
) -> _CompiledRuleEntry | None:
    for entry in entries:
        if deadline is not None:
            _check_deadline(deadline=deadline, row_index=-1)
        if _evaluate_predicate(entry=entry, row=row):
            return entry
    return None


def _evaluate_predicate(*, entry: _CompiledRuleEntry, row: dict[str, Any]) -> bool:
    try:
        return bool(entry.predicate(row))
    except Exception as exc:  # pragma: no cover - defensive path
        raise GuardrailViolation(
            "Falha ao avaliar condição de regra declarativa.",
            details={
                "reason": "rule_evaluation_error",
                "rule_key": entry.rule_key,
                "block_kind": entry.block_kind.value,
                "error_type": type(exc).__name__,
            },
            hint="Revise a condição da regra e os tipos das colunas envolvidas.",
        ) from exc


def _build_diff(
    *,
    legacy: MachineStatusResult,
    declarative: ClassificationOutput,
    rule_keys: list[str] | None = None,
) -> dict[str, Any]:
    diff: dict[str, Any] = {}
    if legacy.primary_status != declarative.primary_status:
        diff["primary_status"] = {
            "legacy": legacy.primary_status,
            "declarative": declarative.primary_status,
        }
    if legacy.primary_status_label != declarative.primary_status_label:
        diff["primary_status_label"] = {
            "legacy": legacy.primary_status_label,
            "declarative": declarative.primary_status_label,
        }
    legacy_flags = _unique_preserve_order(list(legacy.flags))
    declarative_flags = _unique_preserve_order(list(declarative.flags))
    if legacy_flags != declarative_flags:
        diff["flags"] = {
            "legacy": legacy_flags,
            "declarative": declarative_flags,
        }
    if not diff:
        return {}

    normalized_rule_keys = _unique_preserve_order(list(rule_keys or []))
    if normalized_rule_keys:
        diff["declarative_rule_keys"] = normalized_rule_keys
    diff["divergence_kind"] = _infer_divergence_kind(diff)
    diff["severity"] = _infer_divergence_severity(diff)
    return diff


def _infer_divergence_kind(diff: dict[str, Any]) -> str:
    keys = [key for key in ("primary_status", "primary_status_label", "flags") if key in diff]
    if len(keys) == 1:
        return keys[0]
    if len(keys) > 1:
        return "mixed"
    return "unknown"


def _infer_divergence_severity(diff: dict[str, Any]) -> str:
    if isinstance(diff.get("severity"), str):
        explicit = str(diff["severity"]).strip().upper()
        if explicit:
            return explicit

    severities: list[str] = []
    status_diff = diff.get("primary_status")
    if isinstance(status_diff, dict):
        for side in ("legacy", "declarative"):
            status_key = status_diff.get(side)
            if isinstance(status_key, str):
                mapped = STATUS_SEVERITY_BY_KEY.get(status_key)
                if mapped:
                    severities.append(mapped.upper())

    flags_diff = diff.get("flags")
    if isinstance(flags_diff, dict):
        for side in ("legacy", "declarative"):
            status_list = flags_diff.get(side)
            if not isinstance(status_list, list):
                continue
            for status_key in status_list:
                if isinstance(status_key, str):
                    mapped = STATUS_SEVERITY_BY_KEY.get(status_key)
                    if mapped:
                        severities.append(mapped.upper())

    if not severities:
        return "UNKNOWN"
    return _max_severity(severities)


def _max_severity(severities: list[str]) -> str:
    rank = {"DANGER": 4, "WARNING": 3, "INFO": 2, "SUCCESS": 1, "UNKNOWN": 0}
    best = "UNKNOWN"
    for value in severities:
        normalized = value.strip().upper()
        if rank.get(normalized, 0) > rank.get(best, 0):
            best = normalized
    return best


def _resolve_primary_status(output: dict[str, Any], fallback_status: str) -> str:
    for key in ("primary_status", "status", "key"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return fallback_status


def _resolve_primary_status_label(
    output: dict[str, Any],
    *,
    fallback_label: str,
    fallback_status: str,
) -> str:
    for key in ("primary_status_label", "status_label", "label"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    if fallback_label:
        return fallback_label
    return fallback_status


def _resolve_flags(output: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    direct = output.get("flag")
    if isinstance(direct, str) and direct.strip():
        flags.append(direct.strip())

    list_value = output.get("flags")
    if isinstance(list_value, list):
        for item in list_value:
            if isinstance(item, str) and item.strip():
                flags.append(item.strip())

    return _unique_preserve_order(flags)


def _unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _annotation_to_expression_type(annotation: Any) -> ExpressionDataType:  # noqa: C901
    origin = get_origin(annotation)

    if origin in (UnionType,):
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if not args:
            return ExpressionDataType.UNKNOWN
        return _annotation_to_expression_type(args[0])

    if origin is not None and str(origin) == "typing.Union":
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if not args:
            return ExpressionDataType.UNKNOWN
        return _annotation_to_expression_type(args[0])

    if annotation is str:
        return ExpressionDataType.STRING
    if annotation is bool:
        return ExpressionDataType.BOOL
    if annotation is int:
        return ExpressionDataType.INT
    if annotation is float:
        return ExpressionDataType.FLOAT
    if annotation is date:
        return ExpressionDataType.DATE

    if origin is list:
        args = get_args(annotation)
        if not args:
            return ExpressionDataType.UNKNOWN
        inner = _annotation_to_expression_type(args[0])
        if inner == ExpressionDataType.STRING:
            return ExpressionDataType.LIST_STRING
        if inner == ExpressionDataType.INT:
            return ExpressionDataType.LIST_INT
        if inner == ExpressionDataType.FLOAT:
            return ExpressionDataType.LIST_FLOAT
        if inner == ExpressionDataType.BOOL:
            return ExpressionDataType.LIST_BOOL
        if inner == ExpressionDataType.DATE:
            return ExpressionDataType.LIST_DATE
    return ExpressionDataType.UNKNOWN


def _safe_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _safe_compare(left: Any, right: Any, comparator: Callable[[Any, Any], bool]) -> bool:
    if left is None or right is None:
        return False
    try:
        return bool(comparator(left, right))
    except Exception:
        return False


def _safe_in(left: Any, right: Any) -> bool:
    if right is None:
        return False
    if isinstance(right, (list, tuple, set)):
        return left in right
    return False


def _safe_math(left: Any, right: Any, operator: str) -> float | int | None:
    if left is None or right is None:
        return None
    try:
        left_number = float(left)
        right_number = float(right)
    except (TypeError, ValueError):
        return None

    if operator == "+":
        result = left_number + right_number
    elif operator == "-":
        result = left_number - right_number
    elif operator == "*":
        result = left_number * right_number
    elif operator == "/":
        if right_number == 0:
            return None
        result = left_number / right_number
    else:
        return None

    if operator != "/" and float(result).is_integer() and isinstance(left, int) and isinstance(right, int):
        return int(result)
    return result


def _safe_contains(container: Any, item: Any) -> bool:
    if container is None:
        return False
    if isinstance(container, str):
        return _safe_string(item) in container
    if isinstance(container, (list, tuple, set)):
        return item in container
    return False


def _safe_regex_extract(pattern: re.Pattern[str], source: str, group: int) -> str | None:
    match = pattern.search(source)
    if not match:
        return None
    try:
        return match.group(group)
    except IndexError:
        return None


def _safe_split_part(value: str, delimiter: str, index: int) -> str | None:
    parts = value.split(delimiter)
    if 0 <= index < len(parts):
        return parts[index]
    return None


def _safe_coalesce(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _safe_to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value != value:  # NaN guard
            return None
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    return None


def _safe_date_diff(first: Any, second: Any, unit: str) -> int | None:
    source = _to_date(first)
    target = _to_date(second)
    if source is None:
        return None
    if target is None:
        if isinstance(second, str) and second.strip().lower() == "now":
            target = date.today()
        else:
            target = date.today()

    delta_days = (target - source).days
    if unit == "hours":
        return delta_days * 24
    if unit == "days":
        return delta_days
    if unit == "weeks":
        return int(delta_days / 7)
    if unit == "months":
        return int(delta_days / 30)
    if unit == "years":
        return int(delta_days / 365)
    return delta_days


def _to_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def _literal_pattern(node: ExpressionNode) -> str:
    value = _literal_value(node)
    if not isinstance(value, str):
        raise InvalidExpressionSyntax(
            "Pattern literal inválido no runtime declarativo.",
            details={"reason": "pattern_not_literal_string"},
        )
    return value


def _literal_int(node: ExpressionNode) -> int:
    value = _literal_value(node)
    if isinstance(value, bool):
        raise InvalidExpressionSyntax(
            "Literal inteiro inválido no runtime declarativo.",
            details={"reason": "int_literal_invalid"},
        )
    if not isinstance(value, int):
        raise InvalidExpressionSyntax(
            "Literal inteiro inválido no runtime declarativo.",
            details={"reason": "int_literal_invalid"},
        )
    return value


def _literal_value(node: ExpressionNode) -> Any:
    if not isinstance(node, LiteralNode):
        raise InvalidExpressionSyntax(
            "Literal esperado no runtime declarativo.",
            details={"reason": "literal_expected"},
        )
    return node.value
