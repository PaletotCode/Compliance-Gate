from __future__ import annotations

from passlib.context import CryptContext

_pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def validate_password_strength(password: str) -> None:
    if len(password) < 8:
        raise ValueError("password must have at least 8 characters")
    if password.lower() == password:
        raise ValueError("password must include at least one uppercase character")
    if password.upper() == password:
        raise ValueError("password must include at least one lowercase character")
    if not any(ch.isdigit() for ch in password):
        raise ValueError("password must include at least one digit")


def hash_password(password: str) -> str:
    validate_password_strength(password)
    return _pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    if not password_hash:
        return False
    return _pwd_context.verify(password, password_hash)
