import pyotp

from compliance_gate.authentication.security.totp import (
    build_otpauth_url,
    generate_secret,
    protect_secret,
    qr_code_base64_png,
    reveal_secret,
    verify_totp,
)


def test_totp_generation_and_verification() -> None:
    secret = generate_secret()
    code = pyotp.TOTP(secret).now()

    assert verify_totp(secret, code) is True


def test_totp_secret_protection_roundtrip() -> None:
    secret = generate_secret()
    protected = protect_secret(secret)
    assert protected != secret
    assert reveal_secret(protected) == secret


def test_otpauth_and_qr_generation() -> None:
    secret = generate_secret()
    otpauth = build_otpauth_url(secret, "admin", "tenant-1")

    assert otpauth.startswith("otpauth://totp/")
    qr_b64 = qr_code_base64_png(otpauth)
    assert len(qr_b64) > 20
