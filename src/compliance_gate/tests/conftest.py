import pytest
from fastapi.testclient import TestClient
import os
from pathlib import Path
from types import SimpleNamespace

from compliance_gate.authentication.http.dependencies import get_current_user
from compliance_gate.main import app

@pytest.fixture(scope="module")
def client() -> TestClient:
    os.environ["CG_DATA_DIR"] = str(Path(__file__).resolve().parents[3] / "workspace")
    fake_user = SimpleNamespace(
        id="test-user-id",
        tenant_id="default",
        username="test-admin",
        role="TI_ADMIN",
        is_active=True,
        mfa_enabled=False,
        require_password_change=False,
    )
    app.dependency_overrides[get_current_user] = lambda: fake_user
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.pop(get_current_user, None)
