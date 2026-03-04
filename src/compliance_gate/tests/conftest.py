import pytest
from fastapi.testclient import TestClient

from compliance_gate.main import app

@pytest.fixture(scope="module")
def client() -> TestClient:
    with TestClient(app) as c:
        yield c
