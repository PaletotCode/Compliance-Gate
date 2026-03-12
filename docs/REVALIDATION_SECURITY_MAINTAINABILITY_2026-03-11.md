# Revalidation Report — Security, Tests, Centralization, Maintainability (2026-03-11)

## Scope
- Revalidation after latest implementation round.
- Covered: backend tests, frontend quality gates, frontend pentest, centralization indicators, residual risks.

## 1) Test and Quality Revalidation

## Backend
- Command: `.venv/bin/pytest -q`
- Result: `132 passed`
- Status: PASS

## Frontend
- Command: `npm run lint`
- Result: exit 0 with 1 warning (TanStack `useReactTable` compatibility warning; non-blocking)
- Status: PASS (with warning)

- Command: `npm run test`
- Result: `11 files`, `22 tests` passed
- Status: PASS

- Command: `npm run build`
- Result: build success
- Status: PASS

## 2) Security Revalidation (Pentesting)

## Dependency scan
- Command: `npm audit --json`
- Result: vulnerabilities total `0`
- Status: PASS
- Evidence: `pentesting/frontend/npm_audit_frontend_2026-03-11_revalidate.json`

- Command: `npm audit --omit=dev --json`
- Result: vulnerabilities total `0`
- Status: PASS
- Evidence: `pentesting/frontend/npm_audit_prod_frontend_2026-03-11_revalidate.json`

## Dynamic HTTP scan (frontend preview)
- Command: `nmap -sV -p 4173 --script http-title,http-methods,http-security-headers,http-server-header 127.0.0.1`
- Result: security headers now observed in response (CSP, XFO, XCTO, Referrer-Policy, Permissions-Policy, COOP, CORP)
- Status: PASS (hardening effective)
- Evidence: `pentesting/frontend/nmap_frontend_4173_2026-03-11_revalidate.txt`

## Header matrix
- Result:
  - present: `content-security-policy`, `x-frame-options`, `x-content-type-options`, `referrer-policy`, `permissions-policy`, `cross-origin-opener-policy`, `cross-origin-resource-policy`
  - missing: `strict-transport-security`, `cross-origin-embedder-policy`
- Status: PARTIAL PASS (residual hardening backlog)
- Evidence: `pentesting/frontend/security_header_matrix_2026-03-11_revalidate.txt`

## Probe checks
- OPTIONS: allowed methods returned by preview server (`GET,HEAD,PUT,PATCH,POST,DELETE`)
- Path traversal probe: no file disclosure; SPA response only
- XSS reflected probe: payload not reflected in HTML response
- Evidence:
  - `pentesting/frontend/curl_options_root_2026-03-11_revalidate.txt`
  - `pentesting/frontend/curl_path_traversal_probe_2026-03-11_revalidate.txt`
  - `pentesting/frontend/curl_xss_probe_2026-03-11_revalidate.html`

## Static security surface
- No `dangerouslySetInnerHTML`, `eval`, `new Function`, `document.write` found in frontend source scan.
- Remaining local storage usage is non-secret state (`dataset_version_id`, theme).
- Evidence: `pentesting/frontend/static_patterns_2026-03-11_revalidate.txt`

## Backend auth/security configuration surface
- Observed secure controls:
  - CSRF enforcement enabled and checked in middleware.
  - Cookie-only auth with server-side validation.
- Residual risks (config defaults):
  - `auth_jwt_secret` default placeholder.
  - `auth_secret_protection_key` default placeholder.
  - `auth_recovery_pepper` default placeholder.
  - `auth_cookie_secure=False` by default.
- Status: PARTIAL PASS (production config hardening required)
- Evidence: `pentesting/backend/auth_config_surface_2026-03-11_revalidate.txt`

## 3) Centralization and Maintainability Revalidation

## Positive deltas confirmed
- Frontend API envelope unwrap centralized.
  - Previous duplicated local `unwrap` helpers removed.
  - Current duplicate pattern scan for `MaybeEnvelope/unwrap` returned zero findings.

- Backend declarative error mapping centralized.
  - Unified helper in `src/compliance_gate/Engine/interfaces/error_http.py`.
  - Applied in `api.py`, `declarative_api.py`, `rulesets_api.py`.

## Remaining hotspots (still large files)
- `Engine/interfaces/rulesets_api.py` (1226)
- `Engine/interfaces/declarative_api.py` (575)
- `http/routes/datasets.py` (632)
- `http/routes/csv_tabs.py` (481)
- `http/routes/workspace_uploads.py` (439)

## Contract consistency snapshot
- Router endpoints scanned: `96`
- Occurrences of `response_model=` in `http/routes + Engine/interfaces`: `83`
- Gap exists; response contract standardization is improved but not yet complete.

## 4) Updated Risk Rating
- Backend architecture/maintainability: `7.4 / 10` (was 7.1)
- Frontend security posture: `8.1 / 10` (was 7.4)
- Industrial readiness (current implementation): `7.5 / 10` (was 6.9)

## 5) Residual Priority Backlog
1. Production-only hardening: enforce `auth_cookie_secure=true` and non-default secrets via environment gate.
2. Add HSTS at edge/proxy in TLS environments.
3. Decide if COEP is required for your runtime model and add if compatible.
4. Continue route decomposition of `datasets/csv_tabs/workspace_uploads` into service layer.
5. Close response model coverage gap for full contract uniformity.

