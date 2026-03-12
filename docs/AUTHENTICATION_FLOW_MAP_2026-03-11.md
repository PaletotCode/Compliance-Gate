# Mapeamento Completo do Fluxo de Autenticação (Auth Core v1)

## Escopo mapeado
- Backend: `src/compliance_gate/authentication/*`, middleware CSRF em `src/compliance_gate/main.py`
- Frontend auth canvas: `frontend/src/auth/ui/AuthenticationCanvas.tsx`
- Cliente HTTP/cookies: `frontend/src/api/client.ts`, `frontend/src/auth/store.ts`
- Execução operacional: `Makefile`, `docker-compose.yml`

## Fluxo ponta-a-ponta
1. **Login inicial**
   - Frontend envia `POST /api/v1/auth/login` com `username/password`.
   - Backend (`AuthService.authenticate`) valida lock rate-limit, usuário ativo e senha.
   - Se sucesso sem MFA: gera JWT e responde `LoginSuccessResponse`.
   - Backend grava cookies:
     - `cg_access` (HttpOnly)
     - `cg_csrf` (não HttpOnly)

2. **Desafio MFA em login**
   - Se `user.mfa_enabled=true` e sem `totp_code`, backend retorna:
     - `{ "mfa_required": true, "challenge_id": "<uuid>" }`
   - Frontend apresenta etapa de código MFA.
   - Frontend reenvia `POST /auth/login` com `challenge_id` + `totp_code`.
   - Backend consome challenge, valida TOTP e autentica.

3. **Onboarding MFA (usuário sem MFA)**
   - Após login sem MFA habilitado, frontend chama `POST /auth/mfa/setup`.
   - Backend gera segredo TOTP e salva segredo pendente em Redis (`auth:mfa:pending:{user_id}`).
   - Backend retorna:
     - `otpauth_url`
     - `qr_code_base64_png`
     - `instructions`
   - Frontend envia `POST /auth/mfa/confirm` com `totp_code`.
   - Backend valida TOTP do segredo pendente, protege segredo com Fernet e persiste em `users.mfa_secret_protected`, `users.mfa_enabled=true`.
   - Backend gera recovery codes, persiste hashes em `recovery_codes` e audita `MFA_SETUP`.

4. **Sessão autenticada**
   - Rotas protegidas usam apenas cookie HttpOnly (`get_current_user`).
   - `GET /auth/me` retorna `UserPublic`.
   - Em `401`, frontend limpa estado local com `session.notifyUnauthorized()`.

5. **Reset de senha**
   - `POST /auth/password/reset` valida fator (`totp_code` ou `recovery_code`).
   - Em sucesso, troca `password_hash`, mantém MFA habilitado.

## Persistência real da MFA
- Persistência definitiva da MFA está no Postgres (`users.mfa_enabled`, `users.mfa_secret_protected`, `recovery_codes`).
- Segredo de setup pendente é temporário (Redis/memória), válido só até confirmação.

## Causa raiz reproduzida (problema reportado)
- O comando operacional usado para “fechar a aplicação” estava apagando volumes Docker:
  - `make stop` -> `docker compose down -v --remove-orphans`
  - `make backend-down` -> `docker compose down -v --remove-orphans`
- Isso removia o volume `postgres_data`, recriando banco limpo no próximo start.
- Resultado: usuário bootstrap voltava com `mfa_enabled=false`, exigindo novo vínculo no Microsoft Authenticator.

## Correção aplicada
- `make backend-down` agora **preserva volumes**.
- `make stop` agora **preserva volumes**.
- Novo `make backend-reset` mantido para reset destrutivo intencional.

## Riscos monitorados e falhas possíveis
- Reinício do backend entre `mfa/setup` e `mfa/confirm` pode invalidar setup pendente (TTL/armazenamento temporário).
- Divergência de relógio cliente-servidor pode causar falha de TOTP.
- Usuário admin com MFA habilitado pode quebrar scripts que assumem login sem challenge (ex.: automações de criação de usuário).
