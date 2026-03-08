# Compliance Gate Frontend

Frontend comercial com fluxo de autenticação conectado ao backend real (`/api/v1/auth/*`) e canvas visual integrado sem redesign.

## Stack
- React 18 + TypeScript + Vite
- TanStack Router + TanStack Query
- Axios + Zustand
- TailwindCSS + lucide-react
- Vitest (unit)

## Setup rápido
1. `cd frontend`
2. `cp .env.example .env`
3. Ajuste variáveis:
   - `VITE_API_BASE_URL` (padrão: `http://localhost:8000`)
   - `VITE_CSRF_COOKIE_NAME` (padrão: `cg_csrf`)
   - `VITE_CSRF_HEADER_NAME` (padrão: `X-CSRF-Token`)
4. `npm install`

## Rotas
- `/auth`: canvas de autenticação (template integrado)
- `/success`: confirmação de sessão autenticada
- `/`: redireciona automaticamente para `/auth` ou `/success`

## Como validar (gate)
1. Servidor de desenvolvimento:
   - `npm run dev`
2. Build de produção:
   - `npm run build`
3. Testes unitários:
   - `npm test`
4. Runner de fluxo auth por passos (sem UI):
   - da raiz do repo: `node frontend/scripts/auth_flow_check.ts`
   - dentro de `frontend/`: `node scripts/auth_flow_check.ts`

## Runner `auth_flow_check.ts`
Script: `frontend/scripts/auth_flow_check.ts`

Ele executa, em sequência:
- `PASSO 1: login -> ok` (ou `challenge`, e resolve com `AUTH_CHECK_TOTP_CODE` se necessário)
- `PASSO 2: mfa setup -> ok`
- `PASSO 3: mfa confirm -> ok`
- `PASSO 4: /me -> ok`

Se qualquer passo falhar, retorna `exit code 1`.

### Variáveis aceitas pelo runner
- `AUTH_CHECK_BASE_URL` (default: `http://localhost:8000`)
- `AUTH_CHECK_USERNAME` (default fallback: `admin`)
- `AUTH_CHECK_PASSWORD` (default fallback: `Admin1234`)
- `AUTH_CHECK_TOTP_CODE` (obrigatória apenas se o login inicial retornar challenge MFA)

Exemplo:

```bash
AUTH_CHECK_BASE_URL=http://localhost:8000 \
AUTH_CHECK_USERNAME=admin \
AUTH_CHECK_PASSWORD=Admin1234 \
node frontend/scripts/auth_flow_check.ts
```

## Observações
- O background do login usa asset local (`src/assets/login-bg.jpg`), sem dependência de imagem externa em runtime.
- O canvas mantém estrutura/classe/animações do template, com handlers conectados para:
  - login com challenge MFA (`challenge_id`)
  - setup MFA com QR real (`qr_code_base64_png`)
  - confirm MFA (`totp_code`) + cópia de `otpauth_url` e `recovery_codes`
  - reset de senha (`username`, `new_password`, `totp_code` ou `recovery_code`)
- O client HTTP opera em cookie HttpOnly only: `withCredentials: true` e envio automático de CSRF em `POST/PUT/PATCH/DELETE`.
