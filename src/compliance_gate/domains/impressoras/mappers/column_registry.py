"""
Column Registry for Impressoras Domain.
Defines canonical names and accepted aliases (header-first approach).
"""

class ImpressorasColumnRegistry:
    CANONICAL_COLUMNS = {
        "name": ["name", "nome", "impressora", "printer"],
        "pa_code": ["pa code", "pa", "agencia", "codigo pa"],
        "ip": ["ip", "ip address", "endereco ip"],
        "status": ["status", "estado", "condicao"]
    }

    REQUIRED_COLUMNS = ["name", "pa_code"]

    @classmethod
    def resolve_alias(cls, header: str) -> str | None:
        normalized_header = header.strip().lower()
        for canonical, aliases in cls.CANONICAL_COLUMNS.items():
            if normalized_header == canonical or normalized_header in aliases:
                return canonical
        return None
