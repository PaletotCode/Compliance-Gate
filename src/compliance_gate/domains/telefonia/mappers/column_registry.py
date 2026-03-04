"""
Column Registry for Telefonia Domain.
Defines canonical names and accepted aliases (header-first approach).
"""

class TelefoniaColumnRegistry:
    CANONICAL_COLUMNS = {
        "number": ["number", "numero", "telefone", "linha"],
        "pa_code": ["pa code", "pa", "agencia", "codigo pa"],
        "user": ["user", "usuario", "colaborador"],
        "status": ["status", "estado"]
    }

    REQUIRED_COLUMNS = ["number", "pa_code"]

    @classmethod
    def resolve_alias(cls, header: str) -> str | None:
        normalized_header = header.strip().lower()
        for canonical, aliases in cls.CANONICAL_COLUMNS.items():
            if normalized_header == canonical or normalized_header in aliases:
                return canonical
        return None
