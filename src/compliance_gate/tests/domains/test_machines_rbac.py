from __future__ import annotations

from fastapi.testclient import TestClient

from compliance_gate.domains.machines.service import MachinesService
from compliance_gate.main import app


def test_machines_table_without_token_returns_401() -> None:
    with TestClient(app) as anon_client:
        response = anon_client.get("/api/v1/machines/table?page=1&size=10")
    assert response.status_code == 401


def test_machines_table_with_bearer_header_still_returns_401() -> None:
    with TestClient(app) as anon_client:
        response = anon_client.get(
            "/api/v1/machines/table?page=1&size=10",
            headers={"Authorization": "Bearer should-not-work"},
        )
    assert response.status_code == 401


def test_machines_table_cross_tenant_dataset_returns_404(monkeypatch, client) -> None:
    def fake_get_table_data(*_args, **_kwargs):
        raise MachinesService.DatasetAccessError("dataset_version not found for tenant")

    monkeypatch.setattr(MachinesService, "get_table_data", fake_get_table_data)

    response = client.get(
        "/api/v1/machines/table?page=1&size=10&dataset_version_id=tenant-a-dataset"
    )
    assert response.status_code == 404
    assert "dataset_version" in response.json()["detail"]
