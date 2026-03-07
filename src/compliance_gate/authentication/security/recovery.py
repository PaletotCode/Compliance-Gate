from __future__ import annotations

import hmac
import secrets
from hashlib import sha256

from compliance_gate.authentication.config import auth_settings

_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def normalize_recovery_code(code: str) -> str:
    return "".join(ch for ch in code.upper() if ch.isalnum())


def generate_recovery_codes(count: int | None = None) -> list[str]:
    total = count if count is not None else auth_settings.auth_recovery_codes_count
    codes: list[str] = []
    for _ in range(total):
        raw = "".join(secrets.choice(_ALPHABET) for _ in range(12))
        codes.append(f"{raw[:4]}-{raw[4:8]}-{raw[8:]}")
    return codes


def hash_recovery_code(code: str) -> str:
    normalized = normalize_recovery_code(code)
    digest = hmac.new(
        auth_settings.auth_recovery_pepper.encode("utf-8"),
        normalized.encode("utf-8"),
        sha256,
    ).hexdigest()
    return digest


def verify_recovery_code(code: str, code_hash: str) -> bool:
    candidate_hash = hash_recovery_code(code)
    return hmac.compare_digest(candidate_hash, code_hash)
