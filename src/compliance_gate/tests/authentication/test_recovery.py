from compliance_gate.authentication.security.recovery import (
    generate_recovery_codes,
    hash_recovery_code,
    verify_recovery_code,
)


def test_recovery_codes_are_generated_and_verifiable() -> None:
    codes = generate_recovery_codes(3)
    assert len(codes) == 3

    code = codes[0]
    code_hash = hash_recovery_code(code)
    assert verify_recovery_code(code, code_hash) is True
    assert verify_recovery_code("ZZZZ-ZZZZ-ZZZZ", code_hash) is False
