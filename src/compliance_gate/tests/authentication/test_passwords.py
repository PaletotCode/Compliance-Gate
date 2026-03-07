from compliance_gate.authentication.security.passwords import hash_password, verify_password


def test_password_hash_and_verify_roundtrip() -> None:
    password = "StrongPass123"
    hashed = hash_password(password)

    assert hashed != password
    assert verify_password(password, hashed) is True
    assert verify_password("WrongPass123", hashed) is False
