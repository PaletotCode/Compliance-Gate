from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from compliance_gate.Engine.rulesets.schemas import RuleBlockKind, RuleSetPayloadV2

DEFAULT_STALE_DAYS = 45
MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000
DEFAULT_LEGACY_OS_DEFINITIONS: tuple[str, ...] = (
    "Windows 7",
    "Windows 8",
    "Windows XP",
    "Windows Server 2008",
    "Windows Server 2012",
)


@dataclass(frozen=True, slots=True)
class LegacyRuleTemplate:
    rule_key: str
    block_kind: RuleBlockKind
    precedence: int
    legacy_module: str
    status_key: str
    status_label: str
    severity: str
    is_flag: bool
    description: str
    condition: dict[str, Any]
    output: dict[str, Any]


def build_legacy_baseline_ruleset_payload(
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    legacy_os_definitions: Sequence[str] | None = None,
) -> RuleSetPayloadV2:
    templates = build_legacy_rule_templates(
        stale_days=stale_days,
        legacy_os_definitions=legacy_os_definitions,
    )
    block_order = [RuleBlockKind.SPECIAL, RuleBlockKind.PRIMARY, RuleBlockKind.FLAGS]
    blocks: list[dict[str, Any]] = []

    for block_kind in block_order:
        entries = [
            {
                "rule_key": template.rule_key,
                "description": template.description,
                "priority": template.precedence,
                "condition": template.condition,
                "output": template.output,
            }
            for template in templates
            if template.block_kind == block_kind
        ]
        if entries:
            blocks.append({"kind": block_kind.value, "entries": entries})

    return RuleSetPayloadV2.model_validate({"schema_version": 2, "blocks": blocks})


def build_legacy_rule_templates(
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    legacy_os_definitions: Sequence[str] | None = None,
) -> list[LegacyRuleTemplate]:
    stale_days_ms = int(stale_days) * MILLISECONDS_PER_DAY
    normalized_os_terms = _normalized_legacy_os_terms(legacy_os_definitions)

    templates = [
        LegacyRuleTemplate(
            rule_key="special_gap",
            block_kind=RuleBlockKind.SPECIAL,
            precedence=1,
            legacy_module="gap_de_nomes.rule",
            status_key="GAP",
            status_label="🔴 GAP DE NOMES",
            severity="INFO",
            is_flag=False,
            description="Regra especial: máquina virtual de gap de nomes.",
            condition=_eq_bool("is_virtual_gap", True),
            output={
                "primary_status": "GAP",
                "primary_status_label": "🔴 GAP DE NOMES",
            },
        ),
        LegacyRuleTemplate(
            rule_key="special_available",
            block_kind=RuleBlockKind.SPECIAL,
            precedence=2,
            legacy_module="disponivel.rule",
            status_key="AVAILABLE",
            status_label="ℹ️ DISPONÍVEL",
            severity="INFO",
            is_flag=False,
            description="Regra especial: máquina marcada como disponível em estoque.",
            condition=_eq_bool("is_available_in_asset", True),
            output={
                "primary_status": "AVAILABLE",
                "primary_status_label": "ℹ️ DISPONÍVEL",
            },
        ),
        LegacyRuleTemplate(
            rule_key="primary_inconsistency",
            block_kind=RuleBlockKind.PRIMARY,
            precedence=1,
            legacy_module="inconsistencia_de_base.rule",
            status_key="INCONSISTENCY",
            status_label="🧩 INCONSISTÊNCIA DE BASE",
            severity="DANGER",
            is_flag=False,
            description="!has_ad AND (has_uem OR has_edr).",
            condition=_and(
                _eq_bool("has_ad", False),
                _or(_eq_bool("has_uem", True), _eq_bool("has_edr", True)),
            ),
            output={
                "primary_status": "INCONSISTENCY",
                "primary_status_label": "🧩 INCONSISTÊNCIA DE BASE",
            },
        ),
        LegacyRuleTemplate(
            rule_key="primary_phantom",
            block_kind=RuleBlockKind.PRIMARY,
            precedence=2,
            legacy_module="fantasma_ad.rule",
            status_key="PHANTOM",
            status_label="👻 FANTASMA (AD)",
            severity="WARNING",
            is_flag=False,
            description="!has_ad AND !has_uem AND !has_edr.",
            condition=_and(
                _eq_bool("has_ad", False),
                _eq_bool("has_uem", False),
                _eq_bool("has_edr", False),
            ),
            output={
                "primary_status": "PHANTOM",
                "primary_status_label": "👻 FANTASMA (AD)",
            },
        ),
        LegacyRuleTemplate(
            rule_key="primary_rogue",
            block_kind=RuleBlockKind.PRIMARY,
            precedence=3,
            legacy_module="perigo_sem_agente.rule",
            status_key="ROGUE",
            status_label="🚨 PERIGO (SEM AGENTE)",
            severity="DANGER",
            is_flag=False,
            description="has_ad AND !has_uem AND !has_edr.",
            condition=_and(
                _eq_bool("has_ad", True),
                _eq_bool("has_uem", False),
                _eq_bool("has_edr", False),
            ),
            output={
                "primary_status": "ROGUE",
                "primary_status_label": "🚨 PERIGO (SEM AGENTE)",
            },
        ),
        LegacyRuleTemplate(
            rule_key="primary_missing_uem",
            block_kind=RuleBlockKind.PRIMARY,
            precedence=4,
            legacy_module="falta_uem.rule",
            status_key="MISSING_UEM",
            status_label="⚠️ FALTA UEM",
            severity="WARNING",
            is_flag=False,
            description="has_ad AND !has_uem AND has_edr.",
            condition=_and(
                _eq_bool("has_ad", True),
                _eq_bool("has_uem", False),
                _eq_bool("has_edr", True),
            ),
            output={
                "primary_status": "MISSING_UEM",
                "primary_status_label": "⚠️ FALTA UEM",
            },
        ),
        LegacyRuleTemplate(
            rule_key="primary_missing_edr",
            block_kind=RuleBlockKind.PRIMARY,
            precedence=5,
            legacy_module="falta_edr.rule",
            status_key="MISSING_EDR",
            status_label="⚠️ FALTA EDR",
            severity="WARNING",
            is_flag=False,
            description="has_ad AND has_uem AND !has_edr.",
            condition=_and(
                _eq_bool("has_ad", True),
                _eq_bool("has_uem", True),
                _eq_bool("has_edr", False),
            ),
            output={
                "primary_status": "MISSING_EDR",
                "primary_status_label": "⚠️ FALTA EDR",
            },
        ),
        LegacyRuleTemplate(
            rule_key="primary_missing_asset",
            block_kind=RuleBlockKind.PRIMARY,
            precedence=6,
            legacy_module="falta_asset.rule",
            status_key="MISSING_ASSET",
            status_label="📦 FALTA ASSET",
            severity="WARNING",
            is_flag=False,
            description="has_ad AND !has_asset AND (has_uem != has_edr).",
            condition=_and(
                _eq_bool("has_ad", True),
                _eq_bool("has_asset", False),
                _bin("!=", _col("has_uem"), _col("has_edr")),
            ),
            output={
                "primary_status": "MISSING_ASSET",
                "primary_status_label": "📦 FALTA ASSET",
            },
        ),
        LegacyRuleTemplate(
            rule_key="primary_swap",
            block_kind=RuleBlockKind.PRIMARY,
            precedence=7,
            legacy_module="troca_serial.rule",
            status_key="SWAP",
            status_label="🔄 TROCA DE SERIAL",
            severity="WARNING",
            is_flag=False,
            description="uem_serial e edr_serial presentes e diferentes.",
            condition=_and(
                _fn("is_not_null", _col("uem_serial")),
                _fn("is_not_null", _col("edr_serial")),
                _bin("!=", _col("uem_serial"), _col("edr_serial")),
            ),
            output={
                "primary_status": "SWAP",
                "primary_status_label": "🔄 TROCA DE SERIAL",
            },
        ),
        LegacyRuleTemplate(
            rule_key="primary_clone",
            block_kind=RuleBlockKind.PRIMARY,
            precedence=8,
            legacy_module="duplicado.rule",
            status_key="CLONE",
            status_label="👯 DUPLICADO",
            severity="WARNING",
            is_flag=False,
            description="serial_is_cloned.",
            condition=_eq_bool("serial_is_cloned", True),
            output={
                "primary_status": "CLONE",
                "primary_status_label": "👯 DUPLICADO",
            },
        ),
        LegacyRuleTemplate(
            rule_key="primary_offline",
            block_kind=RuleBlockKind.PRIMARY,
            precedence=9,
            legacy_module="offline.rule",
            status_key="OFFLINE",
            status_label="💤 OFFLINE",
            severity="WARNING",
            is_flag=False,
            description=f"last_seen_date_ms > 0 e agora-ms > {stale_days} dias.",
            condition=_and(
                _bin(">", _col("last_seen_date_ms"), _lit_int(0)),
                _bin(
                    ">",
                    _bin("-", _fn("now_ms"), _col("last_seen_date_ms")),
                    _lit_int(stale_days_ms),
                ),
            ),
            output={
                "primary_status": "OFFLINE",
                "primary_status_label": "💤 OFFLINE",
            },
        ),
        LegacyRuleTemplate(
            rule_key="primary_compliant",
            block_kind=RuleBlockKind.PRIMARY,
            precedence=10,
            legacy_module="seguro_ok.rule",
            status_key="COMPLIANT",
            status_label="✅ SEGURO (OK)",
            severity="SUCCESS",
            is_flag=False,
            description="Fallback default do fluxo primário.",
            condition=_lit_bool(True),
            output={
                "primary_status": "COMPLIANT",
                "primary_status_label": "✅ SEGURO (OK)",
            },
        ),
        LegacyRuleTemplate(
            rule_key="flag_legacy_os",
            block_kind=RuleBlockKind.FLAGS,
            precedence=1,
            legacy_module="sistema_legado.rule",
            status_key="LEGACY",
            status_label="🧓 SISTEMA LEGADO",
            severity="WARNING",
            is_flag=True,
            description="ad_os contém assinatura de sistema legado.",
            condition=_legacy_os_condition(normalized_os_terms),
            output={"flag": "LEGACY"},
        ),
        LegacyRuleTemplate(
            rule_key="flag_pa_mismatch",
            block_kind=RuleBlockKind.FLAGS,
            precedence=2,
            legacy_module="divergencia_pa_x_usuario.rule",
            status_key="PA_MISMATCH",
            status_label="🟠 DIVERGÊNCIA PA x USUÁRIO",
            severity="WARNING",
            is_flag=True,
            description="Sufixo numérico hostname x usuário logado diverge.",
            condition=_pa_mismatch_condition(),
            output={"flag": "PA_MISMATCH"},
        ),
    ]
    return templates


def list_legacy_rule_inventory(
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    legacy_os_definitions: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    templates = build_legacy_rule_templates(
        stale_days=stale_days,
        legacy_os_definitions=legacy_os_definitions,
    )
    return [
        {
            "rule_key": template.rule_key,
            "block_kind": template.block_kind.value,
            "precedence": template.precedence,
            "legacy_module": template.legacy_module,
            "status_key": template.status_key,
            "status_label": template.status_label,
            "severity": template.severity,
            "is_flag": template.is_flag,
            "description": template.description,
            "condition": template.condition,
            "output": template.output,
        }
        for template in templates
    ]


def status_severity_by_key(
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    legacy_os_definitions: Sequence[str] | None = None,
) -> dict[str, str]:
    templates = build_legacy_rule_templates(
        stale_days=stale_days,
        legacy_os_definitions=legacy_os_definitions,
    )
    return {template.status_key: template.severity for template in templates}


def special_primary_status_keys(
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    legacy_os_definitions: Sequence[str] | None = None,
) -> set[str]:
    templates = build_legacy_rule_templates(
        stale_days=stale_days,
        legacy_os_definitions=legacy_os_definitions,
    )
    return {
        template.status_key
        for template in templates
        if template.block_kind == RuleBlockKind.SPECIAL and not template.is_flag
    }


def _normalized_legacy_os_terms(legacy_os_definitions: Sequence[str] | None) -> list[str]:
    source = legacy_os_definitions or DEFAULT_LEGACY_OS_DEFINITIONS
    terms = [term.strip().upper() for term in source if isinstance(term, str) and term.strip()]
    if not terms:
        terms = [term.upper() for term in DEFAULT_LEGACY_OS_DEFINITIONS]
    return terms


def _legacy_os_condition(legacy_os_terms: Sequence[str]) -> dict[str, Any]:
    contains_clauses = [
        _fn("contains", _fn("upper", _col("ad_os")), _lit_string(term)) for term in legacy_os_terms
    ]
    return _and(
        _fn("is_not_null", _col("ad_os")),
        _or(*contains_clauses),
    )


def _pa_mismatch_condition() -> dict[str, Any]:
    host_suffix = _host_suffix_expr()
    user_suffix = _user_suffix_expr()
    return _and(
        _fn("is_not_null", host_suffix),
        _fn("is_not_null", user_suffix),
        _bin("!=", host_suffix, user_suffix),
    )


def _host_suffix_expr() -> dict[str, Any]:
    return _fn(
        "to_int",
        _fn(
            "regex_extract",
            _fn("trim", _col("hostname")),
            _lit_string(r"_(\d{1,2})$"),
            _lit_int(1),
        ),
    )


def _user_suffix_expr() -> dict[str, Any]:
    return _fn(
        "to_int",
        _fn(
            "regex_extract",
            _normalized_user_expr(),
            _lit_string(r"_(\d{1,2})$"),
            _lit_int(1),
        ),
    )


def _normalized_user_expr() -> dict[str, Any]:
    candidate = _candidate_user_expr()
    return _fn(
        "coalesce",
        _fn(
            "regex_extract",
            candidate,
            _lit_string(r"([^\\]+)$"),
            _lit_int(1),
        ),
        candidate,
    )


def _candidate_user_expr() -> dict[str, Any]:
    return _fn(
        "coalesce",
        _col("uem_extra_user_logado"),
        _col("main_user"),
        _lit_string(""),
    )


def _col(name: str) -> dict[str, Any]:
    return {"node_type": "column_ref", "column": name}


def _lit_string(value: str) -> dict[str, Any]:
    return {"node_type": "literal", "value_type": "string", "value": value}


def _lit_bool(value: bool) -> dict[str, Any]:
    return {"node_type": "literal", "value_type": "bool", "value": value}


def _lit_int(value: int) -> dict[str, Any]:
    return {"node_type": "literal", "value_type": "int", "value": int(value)}


def _eq_bool(column: str, value: bool) -> dict[str, Any]:
    return _bin("==", _col(column), _lit_bool(value))


def _bin(operator: str, left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    return {
        "node_type": "binary_op",
        "operator": operator,
        "left": left,
        "right": right,
    }


def _fn(function_name: str, *arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "node_type": "function_call",
        "function_name": function_name,
        "arguments": list(arguments),
    }


def _and(*clauses: dict[str, Any]) -> dict[str, Any]:
    if len(clauses) == 1:
        return clauses[0]
    return {"node_type": "logical_op", "operator": "AND", "clauses": list(clauses)}


def _or(*clauses: dict[str, Any]) -> dict[str, Any]:
    if len(clauses) == 1:
        return clauses[0]
    return {"node_type": "logical_op", "operator": "OR", "clauses": list(clauses)}

