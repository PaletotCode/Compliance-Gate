from __future__ import annotations

from types import SimpleNamespace

from compliance_gate.Engine.errors import GuardrailViolation
from compliance_gate.main import app


class _DummyDB:
    pass


def test_engine_materialize_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import api as engine_api

    def fake_get_db():
        yield _DummyDB()

    def fake_materialize(_db, _tenant_id, _dataset_version_id):
        return SimpleNamespace(
            id="artifact-1",
            tenant_id="default",
            dataset_version_id="dataset-1",
            artifact_name="machines_final",
            path="/workspace/artifacts/default/machines/dataset-1/machines_final.parquet",
            checksum="abc",
            row_count=10,
        )

    monkeypatch.setattr(engine_api, "materialize_machines_spine", fake_materialize)
    app.dependency_overrides[engine_api.get_db] = fake_get_db
    try:
        response = client.post(
            "/api/v1/engine/materialize/machines?dataset_version_id=dataset-1&tenant_id=default"
        )
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["artifact_name"] == "machines_final"
        assert payload["row_count"] == 10
    finally:
        app.dependency_overrides.pop(engine_api.get_db, None)


def test_engine_report_run_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import api as engine_api

    def fake_get_db():
        yield _DummyDB()

    def fake_execute(db, *, tenant_id, dataset_version_id, request):
        _ = (db, tenant_id, dataset_version_id, request)
        rows = [
            {"key": "COMPLIANT", "label": "Compliant", "count": 7, "type": "status"},
            {"key": "ROGUE", "label": "Rogue", "count": 3, "type": "status"},
        ]
        plan = SimpleNamespace(template_name="machines_status_summary", query="SELECT 1")
        return rows, plan

    monkeypatch.setattr(engine_api.ReportRunner, "execute", fake_execute)
    app.dependency_overrides[engine_api.get_db] = fake_get_db
    try:
        response = client.post(
            "/api/v1/engine/reports/run?dataset_version_id=dataset-1&tenant_id=default",
            json={"template_name": "machines_status_summary"},
        )
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["template_name"] == "machines_status_summary"
        assert payload["row_count"] == 2
    finally:
        app.dependency_overrides.pop(engine_api.get_db, None)


def test_engine_internal_catalog_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import api as engine_api

    def fake_get_db():
        yield _DummyDB()

    def fake_catalog(_db, *, tenant_id, dataset_version_id, sample_size):
        _ = (tenant_id, dataset_version_id, sample_size)
        return {
            "tenant_id": "default",
            "dataset_version_id": "dataset-1",
            "row_count": 2,
            "columns": [
                {
                    "name": "hostname",
                    "data_type": "string",
                    "sample_values": ["HOST-01", "HOST-02"],
                    "null_rate": 0.0,
                    "approx_cardinality": 2,
                }
            ],
        }

    monkeypatch.setattr(engine_api, "get_machines_final_catalog", fake_catalog)
    app.dependency_overrides[engine_api.get_db] = fake_get_db
    try:
        response = client.get("/api/v1/engine/internal/catalog/machines-final?dataset_version_id=dataset-1")
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["dataset_version_id"] == "dataset-1"
        assert payload["columns"][0]["name"] == "hostname"
    finally:
        app.dependency_overrides.pop(engine_api.get_db, None)


def test_engine_materialize_structured_guardrail_error(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import api as engine_api

    def fake_get_db():
        yield _DummyDB()

    def fake_materialize(*_args, **_kwargs):
        raise GuardrailViolation(
            "Classificação declarativa excedeu o timeout configurado.",
            details={"reason": "classification_timeout"},
            hint="Reduza complexidade das regras ou aumente o timeout operacional.",
        )

    monkeypatch.setattr(engine_api, "materialize_machines_spine", fake_materialize)
    app.dependency_overrides[engine_api.get_db] = fake_get_db
    try:
        response = client.post(
            "/api/v1/engine/materialize/machines?dataset_version_id=dataset-1&tenant_id=default"
        )
        assert response.status_code == 422
        detail = response.json()["detail"]
        assert detail["code"] == "GuardrailViolation"
        assert detail["details"]["reason"] == "classification_timeout"
    finally:
        app.dependency_overrides.pop(engine_api.get_db, None)
