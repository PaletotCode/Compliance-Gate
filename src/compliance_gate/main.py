import hmac
import logging

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from compliance_gate.authentication.config import auth_settings
from compliance_gate.authentication.http import routes as auth_routes
from compliance_gate.authentication.http.cookies import (
    ensure_csrf_cookie,
    is_csrf_exempt_path,
    is_state_changing_method,
)
from compliance_gate.authentication.services.users_service import UsersService
from compliance_gate.config.constants import API_PREFIX
from compliance_gate.config.logging import setup_logging
from compliance_gate.config.settings import settings
from compliance_gate.Engine.interfaces.api import router as engine_router
from compliance_gate.Engine.interfaces.declarative_api import router as declarative_engine_router
from compliance_gate.Engine.interfaces.rulesets_api import router as rulesets_engine_router
from compliance_gate.http.errors import setup_exception_handlers
from compliance_gate.http.routes import (
    csv_tabs,
    datasets,
    health,
    impressoras,
    machines,
    telefonia,
    workspace_uploads,
)
from compliance_gate.infra.db.session import SessionLocal

log = logging.getLogger(__name__)


def create_app() -> FastAPI:
    setup_logging()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description="Backend API for Compliance Gate",
        docs_url=f"{API_PREFIX}/docs",
        openapi_url=f"{API_PREFIX}/openapi.json",
        redoc_url=None,
    )

    # Restrictive CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_origin_regex=settings.cors_allow_origin_regex,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
        allow_headers=["*"],
    )

    # Global Exception Handlers
    setup_exception_handlers(app)

    @app.middleware("http")
    async def enforce_csrf(request: Request, call_next):
        if not auth_settings.csrf_enabled:
            return await call_next(request)

        if not is_state_changing_method(request.method):
            response = await call_next(request)
            ensure_csrf_cookie(request, response)
            return response

        if is_csrf_exempt_path(request.url.path):
            return await call_next(request)

        csrf_cookie = request.cookies.get(auth_settings.csrf_cookie_name)
        csrf_header = request.headers.get(auth_settings.csrf_header_name)
        if not csrf_cookie or not csrf_header:
            return JSONResponse(status_code=403, content={"detail": "csrf token missing"})
        if not hmac.compare_digest(csrf_cookie, csrf_header):
            return JSONResponse(status_code=403, content={"detail": "csrf token mismatch"})
        return await call_next(request)

    # Include Routers
    app.include_router(health.router)
    app.include_router(machines.router, prefix=API_PREFIX)
    app.include_router(telefonia.router, prefix=API_PREFIX)
    app.include_router(impressoras.router, prefix=API_PREFIX)
    app.include_router(datasets.router, prefix=API_PREFIX)
    app.include_router(csv_tabs.router, prefix=API_PREFIX)
    app.include_router(workspace_uploads.router, prefix=API_PREFIX)
    app.include_router(auth_routes.router, prefix=API_PREFIX)
    app.include_router(engine_router, prefix=API_PREFIX)
    app.include_router(declarative_engine_router, prefix=API_PREFIX)
    app.include_router(rulesets_engine_router, prefix=API_PREFIX)

    @app.on_event("startup")
    def _bootstrap_admin() -> None:
        db = SessionLocal()
        try:
            UsersService.ensure_bootstrap_admin(db)
        except Exception as exc:
            # Keep app startup resilient when DB is unavailable in lightweight test runs.
            log.warning("Bootstrap admin skipped: %s", exc)
        finally:
            db.close()

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run("compliance_gate.main:app", host="0.0.0.0", port=8000, reload=True)
