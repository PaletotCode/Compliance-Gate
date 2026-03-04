"""
Column Registry for Machines Domain.
Defines canonical names and accepted aliases (header-first approach).
"""

from typing import Dict, List

class MachinesColumnRegistry:
    CANONICAL_COLUMNS = {
        "hostname": ["hostname", "host name", "machine name", "nome da maquina", "nome do computador"],
        "ad_logon": ["ad logon", "adlogon", "ultimo logon ad", "last logon ad"],
        "ad_pwd_set": ["ad pwd set", "adpwdset", "pwd last set", "password last set"],
        "ad_os": ["ad os", "ados", "sistema operacional ad", "operating system ad"],
        "uem_user": ["uem user", "uemuser", "usuario uem", "uem username"],
        "uem_seen": ["uem seen", "uemseen", "ultimo visto uem", "last seen uem", "uem console last seen"],
        "edr_user": ["edr user", "edruser", "usuario edr", "edr username"],
        "edr_seen": ["edr seen", "edrseen", "ultimo visto edr", "last seen edr"],
        "model": ["model", "modelo", "system model"],
        "ip": ["ip", "ip address", "endereco ip"],
        "tags": ["tags", "etiquetas"],
        "pa_code": ["pa code", "post code", "pa", "codigo pa"]
    }

    REQUIRED_COLUMNS = ["hostname", "pa_code"]

    @classmethod
    def resolve_alias(cls, header: str) -> str | None:
        normalized_header = header.strip().lower()
        for canonical, aliases in cls.CANONICAL_COLUMNS.items():
            if normalized_header == canonical or normalized_header in aliases:
                return canonical
        return None
