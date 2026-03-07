import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from compliance_gate.config.settings import settings
from compliance_gate.config.constants import API_PREFIX
from compliance_gate.config.logging import setup_logging
from compliance_gate.http.errors import setup_exception_handlers
from compliance_gate.http.routes import health, machines, telefonia, impressoras, datasets, csv_tabs

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
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
        allow_headers=["*"],
    )

    # Global Exception Handlers
    setup_exception_handlers(app)

    # Include Routers
    app.include_router(health.router)
    app.include_router(machines.router, prefix=API_PREFIX)
    app.include_router(telefonia.router, prefix=API_PREFIX)
    app.include_router(impressoras.router, prefix=API_PREFIX)
    app.include_router(datasets.router, prefix=API_PREFIX)
    app.include_router(csv_tabs.router, prefix=API_PREFIX)

    return app

app = create_app()

if __name__ == "__main__":
    uvicorn.run("compliance_gate.main:app", host="0.0.0.0", port=8000, reload=True)
