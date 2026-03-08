# Authentication Module (AUTH CORE v1)

Este módulo concentra autenticação/autorização local do Compliance Gate em modo **cookie HttpOnly only** (sem Bearer header).

## Objetivo
- Autenticação local (`username + password`)
- MFA TOTP compatível com Microsoft Authenticator
- Recovery codes
- Reset de senha sem e-mail/SMS
- RBAC (`TI_ADMIN`, `DIRECTOR`, `TI_OPERATOR`, `AUDITOR`)
- Multi-tenant com `tenant_id` assinado no JWT armazenado em cookie HttpOnly
- Proteção CSRF por **double submit cookie**

## Estrutura
- `config.py`: parâmetros de JWT/cookies/CSRF, MFA, bootstrap e rate-limit.
- `models.py`: modelos de auth (`tenants`, `users`, `recovery_codes`, `auth_audit`).
- `security/`: hashing de senha, JWT, TOTP, recovery codes.
- `services/`: regras de negócio (login, MFA, reset, users admin).
- `http/`: dependencies (`get_current_user`, `require_role`), utilitários de cookie/CSRF e rotas `/api/v1/auth/*`.
- `storage/repo.py`: operações transacionais curtas + advisory lock para mutações sensíveis.
- `rate_limit/limiter.py`: anti brute-force e lockouts usando Redis.

## Contrato de autenticação (cookie-only)

### 1) Login
1. `POST /api/v1/auth/login` com `username` e `password`.
1. Se MFA habilitado sem `totp_code`: retorna `{ mfa_required: true, challenge_id }`.
1. Se MFA concluído: retorna `{ expires_in, user }` (sem `access_token` no JSON).
1. Em sucesso, o backend define:
   - cookie HttpOnly de autenticação (`AUTH_COOKIE_NAME`, default `cg_access`)
   - cookie CSRF (`CSRF_COOKIE_NAME`, default `cg_csrf`, não HttpOnly)

### 2) Sessão autenticada
- `GET /api/v1/auth/me` e todas as rotas protegidas validam **somente** o cookie HttpOnly.
- Header `Authorization: Bearer ...` não autentica.

### 3) Logout
- `POST /api/v1/auth/logout`
- Limpa cookies de autenticação e CSRF.

### 4) CSRF (double submit)
- Para `POST/PUT/PATCH/DELETE` (exceto `POST /auth/login`), o backend exige:
  - cookie `cg_csrf` (ou valor configurado)
  - header `X-CSRF-Token` (ou valor configurado)
  - ambos com o mesmo valor
- Ausente/divergente: `403`.
- Requisições `GET` não exigem CSRF.

## Frontend/API client
- Sempre usar `withCredentials: true`.
- Em `POST/PUT/PATCH/DELETE`, enviar header CSRF lido do cookie (`X-CSRF-Token: <cg_csrf>`).
- Não enviar `Authorization: Bearer ...`.

## Dev vs Prod
- Dev HTTP local:
  - `AUTH_COOKIE_SECURE=false`
  - `AUTH_COOKIE_SAMESITE=lax`
- Produção HTTPS:
  - `AUTH_COOKIE_SECURE=true`
  - `AUTH_COOKIE_SAMESITE=lax` ou `strict` (conforme necessidade de navegação)

## Variáveis principais
- `AUTH_JWT_SECRET`
- `AUTH_JWT_ISSUER`
- `AUTH_JWT_AUDIENCE`
- `AUTH_TOKEN_TTL_MINUTES`
- `AUTH_COOKIE_NAME`
- `AUTH_COOKIE_SECURE`
- `AUTH_COOKIE_SAMESITE`
- `AUTH_COOKIE_PATH`
- `CSRF_COOKIE_NAME`
- `CSRF_HEADER_NAME`
- `CSRF_ENABLED`
- `AUTH_MFA_ISSUER`
- `AUTH_RECOVERY_PEPPER`
- `AUTH_SECRET_PROTECTION_KEY`
- `AUTH_BOOTSTRAP_ADMIN_USERNAME`
- `AUTH_BOOTSTRAP_ADMIN_PASSWORD`

## Retestes
Para validar fluxo auth + RBAC + CSRF em Docker:

```bash
python retests/scripts/run_auth_retests.py
python retests/scripts/run_rbac_retests.py
python retests/scripts/run_engine_retests.py
```

Os scripts usam cookie jar com CSRF automático e validam explicitamente que POST sem header CSRF é bloqueado.
