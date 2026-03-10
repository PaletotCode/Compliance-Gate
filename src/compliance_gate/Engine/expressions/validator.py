from __future__ import annotations

import difflib
import re
from collections.abc import Mapping
from dataclasses import dataclass

from compliance_gate.Engine.errors import (
    ExcessiveComplexity,
    InvalidExpressionSyntax,
    RegexCompileError,
    TypeMismatch,
    UnknownColumn,
    UnsupportedOperatorForType,
)
from compliance_gate.Engine.expressions.ast import (
    BinaryOpNode,
    ColumnRefNode,
    ExpressionNode,
    FunctionCallNode,
    LiteralNode,
    LogicalOpNode,
    UnaryOpNode,
)
from compliance_gate.Engine.expressions.types import (
    ExpressionDataType,
    LiteralValueType,
    is_numeric,
    list_item_type,
    normalize_expression_type,
)


@dataclass(slots=True)
class ExpressionValidationOptions:
    max_nodes: int = 256
    max_depth: int = 16


def validate_expression(
    expression: ExpressionNode,
    *,
    column_types: Mapping[str, str | ExpressionDataType],
    expected_type: ExpressionDataType | None = None,
    options: ExpressionValidationOptions | None = None,
) -> ExpressionDataType:
    validator = _ExpressionValidator(
        column_types=column_types,
        options=options or ExpressionValidationOptions(),
    )
    inferred = validator.infer(expression, node_path="root", depth=1)
    if expected_type and not _types_compatible(inferred, expected_type):
        raise TypeMismatch(
            "Tipo final da expressão não corresponde ao esperado.",
            details={
                "node_path": "root",
                "expected_type": expected_type.value,
                "actual_type": inferred.value,
            },
            hint="Ajuste a expressão para produzir o tipo solicitado.",
        )
    return inferred


def validate_boolean_expression(
    expression: ExpressionNode,
    *,
    column_types: Mapping[str, str | ExpressionDataType],
    options: ExpressionValidationOptions | None = None,
) -> None:
    validate_expression(
        expression,
        column_types=column_types,
        expected_type=ExpressionDataType.BOOL,
        options=options,
    )


class _ExpressionValidator:
    def __init__(
        self,
        *,
        column_types: Mapping[str, str | ExpressionDataType],
        options: ExpressionValidationOptions,
    ) -> None:
        self.options = options
        self.node_count = 0
        self._columns_by_name: dict[str, ExpressionDataType] = {
            name: normalize_expression_type(value) for name, value in column_types.items()
        }
        self._columns_by_lower: dict[str, str] = {
            name.lower(): name for name in self._columns_by_name
        }

    def infer(self, node: ExpressionNode, *, node_path: str, depth: int) -> ExpressionDataType:
        self._touch(node_path=node_path, depth=depth)

        if isinstance(node, LiteralNode):
            return _literal_to_type(node)
        if isinstance(node, ColumnRefNode):
            return self._infer_column(node, node_path=node_path)
        if isinstance(node, UnaryOpNode):
            return self._infer_unary(node, node_path=node_path, depth=depth)
        if isinstance(node, BinaryOpNode):
            return self._infer_binary(node, node_path=node_path, depth=depth)
        if isinstance(node, LogicalOpNode):
            return self._infer_logical(node, node_path=node_path, depth=depth)
        if isinstance(node, FunctionCallNode):
            return self._infer_function(node, node_path=node_path, depth=depth)

        raise InvalidExpressionSyntax(
            "Tipo de nó não suportado.",
            details={"node_path": node_path},
        )

    def _touch(self, *, node_path: str, depth: int) -> None:
        self.node_count += 1
        if self.node_count > self.options.max_nodes:
            raise ExcessiveComplexity(
                "A expressão excede o número máximo de nós permitido.",
                details={
                    "node_path": node_path,
                    "max_nodes": self.options.max_nodes,
                },
                hint="Remova cláusulas redundantes e divida a lógica em etapas.",
            )
        if depth > self.options.max_depth:
            raise ExcessiveComplexity(
                "A expressão excede a profundidade máxima permitida.",
                details={
                    "node_path": node_path,
                    "max_depth": self.options.max_depth,
                },
                hint="Reduza o encadeamento de funções e operadores.",
            )

    def _infer_column(self, node: ColumnRefNode, *, node_path: str) -> ExpressionDataType:
        exact = self._columns_by_name.get(node.column)
        if exact:
            return exact
        mapped_name = self._columns_by_lower.get(node.column.lower())
        if mapped_name:
            return self._columns_by_name[mapped_name]
        suggestions = difflib.get_close_matches(
            node.column,
            list(self._columns_by_name.keys()),
            n=3,
            cutoff=0.5,
        )
        raise UnknownColumn(
            details={
                "node_path": node_path,
                "column": node.column,
                "suggestions": suggestions,
            },
            hint="Use uma coluna disponível no catálogo do parquet.",
        )

    def _infer_unary(self, node: UnaryOpNode, *, node_path: str, depth: int) -> ExpressionDataType:
        operand_type = self.infer(node.operand, node_path=f"{node_path}.operand", depth=depth + 1)
        if node.operator == "NOT" and operand_type != ExpressionDataType.BOOL:
            raise TypeMismatch(
                "Operador NOT exige operando booleano.",
                details={
                    "node_path": node_path,
                    "operator": node.operator,
                    "expected_type": ExpressionDataType.BOOL.value,
                    "actual_type": operand_type.value,
                },
            )
        return ExpressionDataType.BOOL

    def _infer_binary(  # noqa: C901
        self,
        node: BinaryOpNode,
        *,
        node_path: str,
        depth: int,
    ) -> ExpressionDataType:
        left_type = self.infer(node.left, node_path=f"{node_path}.left", depth=depth + 1)
        right_type = self.infer(node.right, node_path=f"{node_path}.right", depth=depth + 1)

        if node.operator in {"==", "!="}:
            if not _types_compatible(left_type, right_type):
                raise TypeMismatch(
                    "Operação de igualdade exige tipos compatíveis.",
                    details={
                        "node_path": node_path,
                        "operator": node.operator,
                        "left_type": left_type.value,
                        "right_type": right_type.value,
                    },
                )
            return ExpressionDataType.BOOL

        if node.operator in {">", ">=", "<", "<="}:
            if not _supports_ordering(left_type) or not _supports_ordering(right_type):
                raise UnsupportedOperatorForType(
                    details={
                        "node_path": node_path,
                        "operator": node.operator,
                        "left_type": left_type.value,
                        "right_type": right_type.value,
                    },
                    hint="Use comparações apenas entre tipos ordenáveis.",
                )
            if not _types_compatible(left_type, right_type):
                raise TypeMismatch(
                    "Comparação exige tipos compatíveis.",
                    details={
                        "node_path": node_path,
                        "operator": node.operator,
                        "left_type": left_type.value,
                        "right_type": right_type.value,
                    },
                )
            return ExpressionDataType.BOOL

        if node.operator == "IN":
            right_item_type = list_item_type(right_type)
            if not right_item_type:
                raise UnsupportedOperatorForType(
                    "Operador IN exige coleção no operando da direita.",
                    details={
                        "node_path": node_path,
                        "operator": node.operator,
                        "right_type": right_type.value,
                    },
                    hint="Use IN apenas com colunas/listas de valores.",
                )
            if not _types_compatible(left_type, right_item_type):
                raise TypeMismatch(
                    "IN exige que o tipo da esquerda corresponda ao tipo da coleção.",
                    details={
                        "node_path": node_path,
                        "left_type": left_type.value,
                        "right_item_type": right_item_type.value,
                    },
                )
            return ExpressionDataType.BOOL

        if node.operator in {"+", "-", "*", "/"}:
            if not is_numeric(left_type) or not is_numeric(right_type):
                raise UnsupportedOperatorForType(
                    "Operadores aritméticos exigem operandos numéricos.",
                    details={
                        "node_path": node_path,
                        "operator": node.operator,
                        "left_type": left_type.value,
                        "right_type": right_type.value,
                    },
                    hint="Use tipos int/float em operações matemáticas.",
                )
            if node.operator == "/":
                return ExpressionDataType.FLOAT
            if ExpressionDataType.FLOAT in {left_type, right_type}:
                return ExpressionDataType.FLOAT
            return ExpressionDataType.INT

        raise InvalidExpressionSyntax(
            "Operador binário não suportado.",
            details={"node_path": node_path, "operator": node.operator},
        )

    def _infer_logical(
        self, node: LogicalOpNode, *, node_path: str, depth: int
    ) -> ExpressionDataType:
        if len(node.clauses) < 2:
            raise InvalidExpressionSyntax(
                "Operação lógica exige pelo menos duas cláusulas.",
                details={"node_path": node_path, "operator": node.operator},
            )
        for idx, clause in enumerate(node.clauses):
            clause_type = self.infer(
                clause,
                node_path=f"{node_path}.clauses[{idx}]",
                depth=depth + 1,
            )
            if clause_type != ExpressionDataType.BOOL:
                raise TypeMismatch(
                    "Operações lógicas aceitam apenas cláusulas booleanas.",
                    details={
                        "node_path": f"{node_path}.clauses[{idx}]",
                        "expected_type": ExpressionDataType.BOOL.value,
                        "actual_type": clause_type.value,
                    },
                )
        return ExpressionDataType.BOOL

    def _infer_function(  # noqa: C901
        self,
        node: FunctionCallNode,
        *,
        node_path: str,
        depth: int,
    ) -> ExpressionDataType:
        fn = node.function_name

        if fn in {"is_null", "is_not_null"}:
            self._expect_arg_count(node, expected=1, node_path=node_path)
            self.infer(node.arguments[0], node_path=f"{node_path}.arguments[0]", depth=depth + 1)
            return ExpressionDataType.BOOL

        if fn == "contains":
            self._expect_arg_count(node, expected=2, node_path=node_path)
            left = self.infer(
                node.arguments[0], node_path=f"{node_path}.arguments[0]", depth=depth + 1
            )
            right = self.infer(
                node.arguments[1], node_path=f"{node_path}.arguments[1]", depth=depth + 1
            )
            if left == ExpressionDataType.STRING:
                self._ensure_type(
                    actual=right,
                    expected=ExpressionDataType.STRING,
                    node_path=f"{node_path}.arguments[1]",
                )
                return ExpressionDataType.BOOL

            left_item_type = list_item_type(left)
            if left_item_type is None:
                raise UnsupportedOperatorForType(
                    "contains exige string ou lista no primeiro argumento.",
                    details={
                        "node_path": f"{node_path}.arguments[0]",
                        "actual_type": left.value,
                    },
                    hint="Use contains(texto, trecho) ou contains(lista, item).",
                )
            if not _types_compatible(left_item_type, right):
                raise TypeMismatch(
                    "contains(lista, item) exige tipos compatíveis entre item e lista.",
                    details={
                        "node_path": node_path,
                        "list_item_type": left_item_type.value,
                        "item_type": right.value,
                    },
                )
            return ExpressionDataType.BOOL

        if fn in {"starts_with", "ends_with"}:
            self._expect_arg_count(node, expected=2, node_path=node_path)
            for arg_idx in (0, 1):
                arg_type = self.infer(
                    node.arguments[arg_idx],
                    node_path=f"{node_path}.arguments[{arg_idx}]",
                    depth=depth + 1,
                )
                self._ensure_type(
                    actual=arg_type,
                    expected=ExpressionDataType.STRING,
                    node_path=f"{node_path}.arguments[{arg_idx}]",
                )
            return ExpressionDataType.BOOL

        if fn == "regex_match":
            self._expect_arg_count(node, expected=2, node_path=node_path)
            source_type = self.infer(
                node.arguments[0], node_path=f"{node_path}.arguments[0]", depth=depth + 1
            )
            pattern_type = self.infer(
                node.arguments[1], node_path=f"{node_path}.arguments[1]", depth=depth + 1
            )
            self._ensure_type(
                actual=source_type,
                expected=ExpressionDataType.STRING,
                node_path=f"{node_path}.arguments[0]",
            )
            self._ensure_type(
                actual=pattern_type,
                expected=ExpressionDataType.STRING,
                node_path=f"{node_path}.arguments[1]",
            )
            pattern_node = node.arguments[1]
            if not (
                isinstance(pattern_node, LiteralNode)
                and pattern_node.value_type == LiteralValueType.STRING
            ):
                raise InvalidExpressionSyntax(
                    "regex_match exige pattern literal string.",
                    details={"node_path": f"{node_path}.arguments[1]"},
                    hint="Use um literal string para pattern.",
                )
            self._ensure_compilable_regex(
                pattern_node=pattern_node, node_path=f"{node_path}.arguments[1]"
            )
            return ExpressionDataType.BOOL

        if fn == "regex_extract":
            self._expect_arg_count(node, expected=3, node_path=node_path)
            source_type = self.infer(
                node.arguments[0], node_path=f"{node_path}.arguments[0]", depth=depth + 1
            )
            pattern_type = self.infer(
                node.arguments[1], node_path=f"{node_path}.arguments[1]", depth=depth + 1
            )
            group_type = self.infer(
                node.arguments[2], node_path=f"{node_path}.arguments[2]", depth=depth + 1
            )
            self._ensure_type(
                actual=source_type,
                expected=ExpressionDataType.STRING,
                node_path=f"{node_path}.arguments[0]",
            )
            self._ensure_type(
                actual=pattern_type,
                expected=ExpressionDataType.STRING,
                node_path=f"{node_path}.arguments[1]",
            )
            self._ensure_type(
                actual=group_type,
                expected=ExpressionDataType.INT,
                node_path=f"{node_path}.arguments[2]",
            )

            pattern_node = node.arguments[1]
            if not (
                isinstance(pattern_node, LiteralNode)
                and pattern_node.value_type == LiteralValueType.STRING
            ):
                raise InvalidExpressionSyntax(
                    "regex_extract exige pattern literal string.",
                    details={"node_path": f"{node_path}.arguments[1]"},
                    hint="Use um literal string para pattern.",
                )
            self._ensure_compilable_regex(
                pattern_node=pattern_node, node_path=f"{node_path}.arguments[1]"
            )

            group_node = node.arguments[2]
            if not (
                isinstance(group_node, LiteralNode)
                and group_node.value_type == LiteralValueType.INT
            ):
                raise InvalidExpressionSyntax(
                    "regex_extract exige group literal inteiro.",
                    details={"node_path": f"{node_path}.arguments[2]"},
                    hint="Use literal inteiro para group.",
                )
            if int(group_node.value) < 0:
                raise InvalidExpressionSyntax(
                    "regex_extract exige group >= 0.",
                    details={"node_path": f"{node_path}.arguments[2]"},
                )
            return ExpressionDataType.STRING

        if fn == "split_part":
            self._expect_arg_count(node, expected=3, node_path=node_path)
            arg0 = self.infer(
                node.arguments[0], node_path=f"{node_path}.arguments[0]", depth=depth + 1
            )
            arg1 = self.infer(
                node.arguments[1], node_path=f"{node_path}.arguments[1]", depth=depth + 1
            )
            arg2 = self.infer(
                node.arguments[2], node_path=f"{node_path}.arguments[2]", depth=depth + 1
            )
            self._ensure_type(
                actual=arg0,
                expected=ExpressionDataType.STRING,
                node_path=f"{node_path}.arguments[0]",
            )
            self._ensure_type(
                actual=arg1,
                expected=ExpressionDataType.STRING,
                node_path=f"{node_path}.arguments[1]",
            )
            self._ensure_type(
                actual=arg2,
                expected=ExpressionDataType.INT,
                node_path=f"{node_path}.arguments[2]",
            )
            if not (
                isinstance(node.arguments[1], LiteralNode)
                and node.arguments[1].value_type == LiteralValueType.STRING
            ):
                raise InvalidExpressionSyntax(
                    "split_part exige delimiter literal string.",
                    details={"node_path": f"{node_path}.arguments[1]"},
                    hint="Use literal string para delimiter.",
                )
            index_node = node.arguments[2]
            if not (
                isinstance(index_node, LiteralNode)
                and index_node.value_type == LiteralValueType.INT
            ):
                raise InvalidExpressionSyntax(
                    "split_part exige index literal inteiro.",
                    details={"node_path": f"{node_path}.arguments[2]"},
                    hint="Use literal inteiro para index.",
                )
            if int(index_node.value) < 0:
                raise InvalidExpressionSyntax(
                    "split_part exige index >= 0.",
                    details={"node_path": f"{node_path}.arguments[2]"},
                )
            return ExpressionDataType.STRING

        if fn == "substring":
            self._expect_arg_count(node, expected=3, node_path=node_path)
            arg0 = self.infer(
                node.arguments[0], node_path=f"{node_path}.arguments[0]", depth=depth + 1
            )
            arg1 = self.infer(
                node.arguments[1], node_path=f"{node_path}.arguments[1]", depth=depth + 1
            )
            arg2 = self.infer(
                node.arguments[2], node_path=f"{node_path}.arguments[2]", depth=depth + 1
            )
            self._ensure_type(
                actual=arg0,
                expected=ExpressionDataType.STRING,
                node_path=f"{node_path}.arguments[0]",
            )
            self._ensure_type(
                actual=arg1,
                expected=ExpressionDataType.INT,
                node_path=f"{node_path}.arguments[1]",
            )
            self._ensure_type(
                actual=arg2,
                expected=ExpressionDataType.INT,
                node_path=f"{node_path}.arguments[2]",
            )
            if not (
                isinstance(node.arguments[1], LiteralNode)
                and node.arguments[1].value_type == LiteralValueType.INT
            ):
                raise InvalidExpressionSyntax(
                    "substring exige start literal inteiro.",
                    details={"node_path": f"{node_path}.arguments[1]"},
                    hint="Use literal inteiro para start.",
                )
            if int(node.arguments[1].value) < 0:
                raise InvalidExpressionSyntax(
                    "substring exige start >= 0.",
                    details={"node_path": f"{node_path}.arguments[1]"},
                )
            if not (
                isinstance(node.arguments[2], LiteralNode)
                and node.arguments[2].value_type == LiteralValueType.INT
            ):
                raise InvalidExpressionSyntax(
                    "substring exige length literal inteiro.",
                    details={"node_path": f"{node_path}.arguments[2]"},
                    hint="Use literal inteiro para length.",
                )
            if int(node.arguments[2].value) < 0:
                raise InvalidExpressionSyntax(
                    "substring exige length >= 0.",
                    details={"node_path": f"{node_path}.arguments[2]"},
                )
            return ExpressionDataType.STRING

        if fn in {"upper", "lower", "trim"}:
            self._expect_arg_count(node, expected=1, node_path=node_path)
            arg_type = self.infer(
                node.arguments[0], node_path=f"{node_path}.arguments[0]", depth=depth + 1
            )
            self._ensure_type(
                actual=arg_type,
                expected=ExpressionDataType.STRING,
                node_path=f"{node_path}.arguments[0]",
            )
            return ExpressionDataType.STRING

        if fn == "date_now":
            self._expect_arg_count(node, expected=0, node_path=node_path)
            return ExpressionDataType.DATE

        if fn == "now_ms":
            self._expect_arg_count(node, expected=0, node_path=node_path)
            return ExpressionDataType.INT

        if fn == "date_diff":
            self._expect_arg_count(node, expected=3, node_path=node_path)
            first = self.infer(
                node.arguments[0], node_path=f"{node_path}.arguments[0]", depth=depth + 1
            )
            self._ensure_type(
                actual=first,
                expected=ExpressionDataType.DATE,
                node_path=f"{node_path}.arguments[0]",
            )
            self._ensure_date_or_now(
                node.arguments[1], node_path=f"{node_path}.arguments[1]", depth=depth
            )

            unit_type = self.infer(
                node.arguments[2],
                node_path=f"{node_path}.arguments[2]",
                depth=depth + 1,
            )
            self._ensure_type(
                actual=unit_type,
                expected=ExpressionDataType.STRING,
                node_path=f"{node_path}.arguments[2]",
            )
            unit_node = node.arguments[2]
            if not (
                isinstance(unit_node, LiteralNode)
                and unit_node.value_type == LiteralValueType.STRING
            ):
                raise InvalidExpressionSyntax(
                    "date_diff exige unit literal string.",
                    details={"node_path": f"{node_path}.arguments[2]"},
                    hint="Use literal string para unit (days/weeks/months/years).",
                )
            unit_value = str(unit_node.value).strip().lower()
            if unit_value not in {"hours", "days", "weeks", "months", "years"}:
                raise InvalidExpressionSyntax(
                    "date_diff recebeu unit inválida.",
                    details={
                        "node_path": f"{node_path}.arguments[2]",
                        "unit": unit_value,
                    },
                    hint="Use unit em {hours, days, weeks, months, years}.",
                )
            return ExpressionDataType.INT

        if fn == "date_diff_days":
            self._expect_arg_count(node, expected=2, node_path=node_path)
            first = self.infer(
                node.arguments[0], node_path=f"{node_path}.arguments[0]", depth=depth + 1
            )
            self._ensure_type(
                actual=first,
                expected=ExpressionDataType.DATE,
                node_path=f"{node_path}.arguments[0]",
            )
            self._ensure_date_or_now(
                node.arguments[1], node_path=f"{node_path}.arguments[1]", depth=depth
            )
            return ExpressionDataType.INT

        if fn == "coalesce":
            self._expect_min_arg_count(node, minimum=2, node_path=node_path)
            common_type: ExpressionDataType | None = None
            for index, argument in enumerate(node.arguments):
                arg_type = self.infer(
                    argument,
                    node_path=f"{node_path}.arguments[{index}]",
                    depth=depth + 1,
                )
                common_type = (
                    arg_type if common_type is None else _common_type(common_type, arg_type)
                )
                if common_type is None:
                    raise TypeMismatch(
                        "coalesce exige tipos compatíveis.",
                        details={
                            "node_path": node_path,
                            "argument_index": index,
                        },
                    )
            return common_type or ExpressionDataType.NULL

        if fn == "to_int":
            self._expect_arg_count(node, expected=1, node_path=node_path)
            arg_type = self.infer(
                node.arguments[0], node_path=f"{node_path}.arguments[0]", depth=depth + 1
            )
            if arg_type in {
                ExpressionDataType.STRING,
                ExpressionDataType.INT,
                ExpressionDataType.FLOAT,
                ExpressionDataType.BOOL,
                ExpressionDataType.NULL,
                ExpressionDataType.UNKNOWN,
            }:
                return ExpressionDataType.INT
            raise UnsupportedOperatorForType(
                "to_int exige argumento escalar (string/int/float/bool).",
                details={
                    "node_path": f"{node_path}.arguments[0]",
                    "actual_type": arg_type.value,
                },
                hint="Use to_int apenas com valores escalares convertíveis.",
            )

        raise InvalidExpressionSyntax(
            "Função não suportada.",
            details={"node_path": node_path, "function_name": fn},
        )

    def _ensure_date_or_now(self, node: ExpressionNode, *, node_path: str, depth: int) -> None:
        if isinstance(node, LiteralNode):
            if node.value_type != LiteralValueType.STRING:
                raise TypeMismatch(
                    "Esperado literal 'now' ou expressão date.",
                    details={"node_path": node_path},
                )
            if str(node.value).lower() != "now":
                raise InvalidExpressionSyntax(
                    "Literal aceito neste argumento é apenas 'now'.",
                    details={"node_path": node_path},
                    hint="Use 'now' ou uma expressão de data.",
                )
            return

        if isinstance(node, FunctionCallNode) and node.function_name == "date_now":
            self._expect_arg_count(node, expected=0, node_path=node_path)
            return

        value_type = self.infer(node, node_path=node_path, depth=depth + 1)
        self._ensure_type(
            actual=value_type,
            expected=ExpressionDataType.DATE,
            node_path=node_path,
        )

    def _ensure_compilable_regex(self, *, pattern_node: LiteralNode, node_path: str) -> None:
        try:
            re.compile(str(pattern_node.value))
        except re.error as exc:
            raise RegexCompileError(
                details={
                    "node_path": node_path,
                    "pattern": pattern_node.value,
                },
            ) from exc

    def _expect_arg_count(self, node: FunctionCallNode, *, expected: int, node_path: str) -> None:
        if len(node.arguments) != expected:
            raise InvalidExpressionSyntax(
                "Quantidade de argumentos inválida para a função.",
                details={
                    "node_path": node_path,
                    "function_name": node.function_name,
                    "expected_arity": expected,
                    "actual_arity": len(node.arguments),
                },
            )

    def _expect_min_arg_count(
        self, node: FunctionCallNode, *, minimum: int, node_path: str
    ) -> None:
        if len(node.arguments) < minimum:
            raise InvalidExpressionSyntax(
                "Quantidade de argumentos inválida para a função.",
                details={
                    "node_path": node_path,
                    "function_name": node.function_name,
                    "minimum_arity": minimum,
                    "actual_arity": len(node.arguments),
                },
            )

    def _ensure_type(
        self,
        *,
        actual: ExpressionDataType,
        expected: ExpressionDataType,
        node_path: str,
    ) -> None:
        if not _types_compatible(actual, expected):
            raise TypeMismatch(
                "Tipo incompatível no argumento da função.",
                details={
                    "node_path": node_path,
                    "expected_type": expected.value,
                    "actual_type": actual.value,
                },
            )


def _literal_to_type(node: LiteralNode) -> ExpressionDataType:
    if node.value_type == LiteralValueType.STRING:
        return ExpressionDataType.STRING
    if node.value_type == LiteralValueType.INT:
        return ExpressionDataType.INT
    if node.value_type == LiteralValueType.FLOAT:
        return ExpressionDataType.FLOAT
    if node.value_type == LiteralValueType.BOOL:
        return ExpressionDataType.BOOL
    if node.value_type == LiteralValueType.DATE:
        return ExpressionDataType.DATE
    if node.value_type == LiteralValueType.NULL:
        return ExpressionDataType.NULL
    return ExpressionDataType.UNKNOWN


def _supports_ordering(value: ExpressionDataType) -> bool:
    return value in {
        ExpressionDataType.STRING,
        ExpressionDataType.INT,
        ExpressionDataType.FLOAT,
        ExpressionDataType.DATE,
    }


def _types_compatible(left: ExpressionDataType, right: ExpressionDataType) -> bool:
    if left == right:
        return True
    if ExpressionDataType.UNKNOWN in {left, right}:
        return True
    if ExpressionDataType.NULL in {left, right}:
        return True
    if is_numeric(left) and is_numeric(right):
        return True
    return False


def _common_type(left: ExpressionDataType, right: ExpressionDataType) -> ExpressionDataType | None:
    if left == right:
        return left
    if left == ExpressionDataType.UNKNOWN:
        return right
    if right == ExpressionDataType.UNKNOWN:
        return left
    if left == ExpressionDataType.NULL:
        return right
    if right == ExpressionDataType.NULL:
        return left
    if is_numeric(left) and is_numeric(right):
        if ExpressionDataType.FLOAT in {left, right}:
            return ExpressionDataType.FLOAT
        return ExpressionDataType.INT
    return None
