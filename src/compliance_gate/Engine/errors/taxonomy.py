from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel, Field


class EngineErrorPayload(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)
    hint: str
    node_path: str | None = None


class DeclarativeEngineError(Exception):
    code: ClassVar[str] = "DeclarativeEngineError"
    default_message: ClassVar[str] = "Erro na engine declarativa."
    default_hint: ClassVar[str] = "Revise a definição e tente novamente."

    def __init__(
        self,
        message: str | None = None,
        *,
        details: dict[str, Any] | None = None,
        hint: str | None = None,
        node_path: str | None = None,
    ) -> None:
        self.message = message or self.default_message
        self.details = details or {}
        self.hint = hint or self.default_hint
        if node_path is not None:
            self.details.setdefault("node_path", node_path)
        self.node_path = (
            str(self.details.get("node_path"))
            if isinstance(self.details.get("node_path"), str)
            else None
        )
        super().__init__(self.message)

    def to_payload(self) -> EngineErrorPayload:
        return EngineErrorPayload(
            code=self.code,
            message=self.message,
            details=self.details,
            hint=self.hint,
            node_path=self.node_path,
        )

    def to_dict(self) -> dict[str, Any]:
        return self.to_payload().model_dump()


class InvalidExpressionSyntax(DeclarativeEngineError):
    code = "InvalidExpressionSyntax"
    default_message = "A expressão declarativa está malformada."
    default_hint = "Revise os campos obrigatórios do nó indicado."


class UnknownColumn(DeclarativeEngineError):
    code = "UnknownColumn"
    default_message = "A coluna informada não existe no catálogo."
    default_hint = "Selecione uma coluna retornada pelo catálogo de dados."


class TypeMismatch(DeclarativeEngineError):
    code = "TypeMismatch"
    default_message = "Os tipos da expressão são incompatíveis."
    default_hint = "Ajuste os operandos para tipos compatíveis."


class UnsupportedOperatorForType(DeclarativeEngineError):
    code = "UnsupportedOperatorForType"
    default_message = "Operador não suportado para o tipo informado."
    default_hint = "Troque o operador ou normalize o tipo do operando."


class RegexCompileError(DeclarativeEngineError):
    code = "RegexCompileError"
    default_message = "O padrão regex não pôde ser compilado."
    default_hint = "Corrija a sintaxe da regex antes de salvar."


class ExcessiveComplexity(DeclarativeEngineError):
    code = "ExcessiveComplexity"
    default_message = "A expressão excede os limites de complexidade."
    default_hint = "Divida a lógica em expressões menores."


class GuardrailViolation(DeclarativeEngineError):
    code = "GuardrailViolation"
    default_message = "A definição viola um limite de segurança."
    default_hint = "Ajuste o limite solicitado e tente novamente."


class RuleOutputConflict(DeclarativeEngineError):
    code = "RuleOutputConflict"
    default_message = "O output da regra contém campos conflitantes."
    default_hint = "Mantenha apenas uma forma canônica para status/label/flags."


class UnreachableRuleWarning(DeclarativeEngineError):
    code = "UnreachableRuleWarning"
    default_message = "Há regra que nunca será avaliada pela ordem atual."
    default_hint = "Revise prioridades e remova regras inalcançáveis."


class ShadowDivergenceWarning(DeclarativeEngineError):
    code = "ShadowDivergenceWarning"
    default_message = "O resultado declarativo divergiu do legado no shadow mode."
    default_hint = "Analise divergências antes de promover para declarative."
