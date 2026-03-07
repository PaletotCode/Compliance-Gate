# Authentication Module (AUTH CORE v1)

Este módulo concentra toda a lógica de autenticação/autorização do Compliance Gate.

## Objetivo
- Autenticação local (`username + password`)
- MFA TOTP compatível com Microsoft Authenticator
- Recovery codes
- Reset de senha sem e-mail/SMS
- RBAC mínimo (`TI_ADMIN`, `DIRECTOR`)
- Multi-tenant com `tenant_id` vindo do token JWT

## Estrutura
- `config.py`: parâmetros de JWT, MFA, bootstrap e rate-limit.
- `models.py`: modelos de auth (`tenants`, `users`, `recovery_codes`, `auth_audit`).
- `security/`: hashing de senha, JWT, TOTP, recovery codes.
- `services/`: regras de negócio (login, MFA, reset, users admin).
- `http/`: dependencies (`get_current_user`, `require_role`) e rotas `/api/v1/auth/*`.
- `storage/repo.py`: operações transacionais curtas + advisory lock para mutações sensíveis.
- `rate_limit/limiter.py`: anti brute-force e lockouts usando Redis.

## Fluxos

### 1) Login
1. `POST /api/v1/auth/login` com `username` e `password`.
1. Se MFA não habilitado: retorna `access_token`.
1. Se MFA habilitado sem `totp_code`: retorna `{ mfa_required: true, challenge_id }`.
1. Repetir `POST /login` com `totp_code` e `challenge_id` para receber token.

### 2) MFA Setup (Microsoft Authenticator)
1. Logado, chamar `POST /api/v1/auth/mfa/setup`.
1. Escanear `qr_code_base64_png` no Microsoft Authenticator.
1. Confirmar com `POST /api/v1/auth/mfa/confirm` enviando `totp_code`.
1. Salvar os `recovery_codes` (são exibidos uma vez).

### 3) Reset de senha sem comunicação externa
- `POST /api/v1/auth/password/reset`
- Body: `username`, `new_password` e `totp_code` **ou** `recovery_code`.

## RBAC aplicado nos endpoints atuais
- `csv-tabs`
  - criação/edição/promoção de profile: `TI_ADMIN`
  - leitura/preview: `TI_ADMIN` e `DIRECTOR`
- `datasets/machines`
  - ingest/preview: `TI_ADMIN`
  - list/get: `TI_ADMIN` e `DIRECTOR`
- `machines`
  - leitura (`filters/table/summary/timeline/history`): `TI_ADMIN` e `DIRECTOR`
  - debug: `TI_ADMIN`

## Variáveis principais
- `AUTH_JWT_SECRET`
- `AUTH_JWT_ISSUER`
- `AUTH_JWT_AUDIENCE`
- `AUTH_ACCESS_TOKEN_TTL_MINUTES`
- `AUTH_MFA_ISSUER`
- `AUTH_RECOVERY_PEPPER`
- `AUTH_SECRET_PROTECTION_KEY`
- `AUTH_BOOTSTRAP_ADMIN_USERNAME`
- `AUTH_BOOTSTRAP_ADMIN_PASSWORD`

## Retestes
Para validar fluxo completo auth + RBAC em Docker:

```bash
python retests/scripts/run_auth_retests.py
```

O script sobe stack docker, faz bootstrap/login, habilita MFA, testa reset e valida permissões em `csv-tabs`, `datasets` e `machines`.
