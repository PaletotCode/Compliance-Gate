# Compliance Gate – Frontend Skeleton

Frontend base focused on authentication infrastructure, ready for commercial-grade UI to be added later. No screens or visual layout are shipped here—only the plumbing.

## Stack
- React 18 + TypeScript + Vite
- TanStack Router + TanStack Query + TanStack Virtual (for future DataGrid virtualized lists)
- TailwindCSS + shadcn/ui primitives (tokens only) + Radix UI + lucide-react
- Zustand for auth state
- ESLint + Prettier
- Vitest + Testing Library (Playwright optional later)

## Getting started
1. `cd frontend`
2. `cp .env.example .env` and set `VITE_API_BASE_URL` (default `http://localhost:8000`).
   - `VITE_AUTH_SESSION_MODE` can be `bearer` (default, stores token) or `cookie` (HttpOnly session; Authorization header is skipped).
3. Install deps: `npm install`
4. Dev server: `npm run dev`
5. Build: `npm run build`
6. Preview build: `npm run preview`

## Auth flow (headless)
- `src/api/client.ts` centralizes HTTP with Axios. It attaches the Bearer token (when in bearer mode) and triggers a global unauthorized hook on 401/403.
- `src/auth/session.ts` manages session strategy (localStorage/memory bearer token or cookie mode). Subscribers are notified on unauthorized events.
- `src/auth/api.ts` wraps backend endpoints (`/api/v1/auth/login`, `/me`, `/mfa/setup`, `/mfa/confirm`, `/password/reset`, `/logout`).
- `src/auth/store.ts` (Zustand) exposes `login`, `logout`, `fetchMe`, `ensureSession`, `beginMfaSetup`, `confirmMfa`, `resetPassword`. It persists tokens via `session` and keeps `user` + status in sync.
- Guards in `src/auth/guard.ts` provide `requireAuth` and `requireRole` to be used inside route `beforeLoad` hooks.
- Routes are minimal: `__root` shell, `/` validates the session (redirects to `/auth/callback` if missing), `/auth/callback` processes a `token` query param and triggers `ensureSession`. Components render `null` (no UI).

## Project layout
```
frontend/
  src/
    api/          # HTTP client, endpoints, shared API types
    auth/         # session manager, auth store, guards, endpoint wrappers
    routes/       # TanStack Router tree (minimal, headless)
    state/        # Query Client
    styles/       # Tailwind globals (design tokens only)
    lib/          # env/config helpers, utilities
    tests/        # Vitest setup + unit tests
  temp/           # placeholders for templates/notes from Canvas (not production code)
```

## Tests
- Unit tests: `npm test`
- Watch mode: `npm run test:watch`
- Coverage: `npm run coverage`

## Linting & formatting
- `npm run lint`
- `npm run format`

## Docker (production build)
- Build image: `docker build -f docker/Dockerfile -t compliance-gate-frontend .`
- Run: `docker run -p 8080:80 compliance-gate-frontend`
- Nginx serves the Vite build from `/usr/share/nginx/html` and proxies static assets efficiently (no backend proxy configured here).

## Notes
- No UI/layout was created intentionally. Components render `null` until the commercial template arrives.
- DataGrid/dashboard support is not implemented yet; only base dependencies and virtual/infinite-scroll-friendly state are prepared.
- Keep all frontend work inside `frontend/` to avoid touching the backend.
