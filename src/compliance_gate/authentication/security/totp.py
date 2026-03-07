from __future__ import annotations

import base64
import hashlib
from io import BytesIO

import pyotp
import qrcode
from cryptography.fernet import Fernet, InvalidToken

from compliance_gate.authentication.config import auth_settings


def generate_secret() -> str:
    return pyotp.random_base32()


def build_otpauth_url(secret: str, username: str, tenant_id: str) -> str:
    account_name = f"{username}@{tenant_id}"
    return pyotp.totp.TOTP(secret).provisioning_uri(
        name=account_name,
        issuer_name=auth_settings.auth_mfa_issuer,
    )


def verify_totp(secret: str, code: str) -> bool:
    totp = pyotp.TOTP(secret)
    clean_code = "".join(ch for ch in code if ch.isdigit())
    return totp.verify(clean_code, valid_window=1)


def qr_code_base64_png(otpauth_url: str) -> str:
    qr = qrcode.QRCode(version=1, box_size=8, border=2)
    qr.add_data(otpauth_url)
    qr.make(fit=True)
    image = qr.make_image(fill_color="black", back_color="white")
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def _fernet() -> Fernet:
    seed = auth_settings.auth_secret_protection_key.encode("utf-8")
    key = base64.urlsafe_b64encode(hashlib.sha256(seed).digest())
    return Fernet(key)


def protect_secret(secret: str) -> str:
    return _fernet().encrypt(secret.encode("utf-8")).decode("utf-8")


def reveal_secret(protected_secret: str) -> str:
    try:
        return _fernet().decrypt(protected_secret.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise ValueError("invalid protected mfa secret") from exc
