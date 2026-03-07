# AUTH CORE v1 - Notes

## Decisões de implementação
- Auth isolado em `src/compliance_gate/authentication/`.
- JWT stateless (`HS256`) com claims: `sub`, `tenant_id`, `role`, `username`, `iss`, `aud`, `exp`.
- MFA TOTP com `pyotp`, QR em base64 PNG com `qrcode`.
- Secret TOTP protegido em repouso com `cryptography.fernet` (derivado de `AUTH_SECRET_PROTECTION_KEY`).
- Recovery codes com hash HMAC-SHA256 + pepper (`AUTH_RECOVERY_PEPPER`).
- Rate-limit anti brute-force via Redis (`5 tentativas / 10 min`, lock de `10 min` por username/IP).
- Operações sensíveis (MFA enable, consumo de recovery code, reset de senha) com transações curtas e advisory lock em PostgreSQL.

## Migração de schema
Migration: `5f1a9c4f8b20_auth_core_v1.py`
- `tenants`: adiciona coluna `name`.
- `users`: evolui para auth local (`username`, `password_hash`, `role`, `is_active`, `mfa_enabled`, `mfa_secret_protected`, `require_password_change`).
- cria `recovery_codes`.
- cria `auth_audit`.
- constraints: `unique(tenant_id, username)`, check de role.

## Integrações
- `csv-tabs`, `datasets/machines`, `machines` agora usam `current_user`/`tenant_id` por token.
- removidos `MOCK_USER_ID` e `default_tenant_id` dessas rotas.
- permissões:
  - TI_ADMIN: mutate (`csv-tabs`, `datasets ingest`, debug)
  - DIRECTOR: leitura (`machines`, list/get datasets, leitura de csv-tabs)

## Reteste
Script: `retests/scripts/run_auth_retests.py`
- sobe docker
- login bootstrap admin
- cria director
- MFA setup + confirm
- login challenge com TOTP
- reset de senha com TOTP
- valida RBAC em `csv-tabs`, `datasets`, `machines`
