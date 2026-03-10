from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from compliance_gate.main import app


class _DummyDB:
    pass


def test_engine_catalog_public_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import declarative_api as engine_declarative_api

    def fake_get_db():
        yield _DummyDB()

    monkeypatch.setattr(
        engine_declarative_api,
        "get_machines_final_catalog",
        lambda *_args, **_kwargs: {
            "tenant_id": "default",
            "dataset_version_id": "dataset-1",
            "row_count": 2,
            "columns": [
                {
                    "name": "hostname",
                    "data_type": "string",
                    "sample_values": ["HOST-01"],
                    "null_rate": 0.0,
                    "approx_cardinality": 1,
                }
            ],
        },
    )
    app.dependency_overrides[engine_declarative_api.get_db] = fake_get_db
    try:
        response = client.get("/api/v1/engine/catalog/machines?dataset_version_id=dataset-1")
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["dataset_version_id"] == "dataset-1"
    finally:
        app.dependency_overrides.pop(engine_declarative_api.get_db, None)


def test_engine_segments_templates_endpoint(client) -> None:
    response = client.get("/api/v1/engine/segments/templates")
    assert response.status_code == 200
    payload = response.json()["data"]
    assert isinstance(payload, list)
    assert payload
    assert "key" in payload[0]


def test_engine_transformation_create_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import declarative_api as engine_declarative_api
    from compliance_gate.Engine.transformations import TransformationPayloadV1

    def fake_get_db():
        yield _DummyDB()

    def fake_create(*_args, **_kwargs):
        payload = TransformationPayloadV1(
            output_column_name="hostname_upper",
            output_type="string",
            expression={
                "node_type": "function_call",
                "function_name": "upper",
                "arguments": [{"node_type": "column_ref", "column": "hostname"}],
            },
        )
        return SimpleNamespace(
            definition=SimpleNamespace(
                id="tr-1",
                tenant_id="default",
                name="Upper Hostname",
                description="desc",
                created_by="u1",
                created_at=datetime.now(UTC),
                active_version=1,
            ),
            payload=payload,
        )

    monkeypatch.setattr(engine_declarative_api, "create_transformation", fake_create)
    app.dependency_overrides[engine_declarative_api.get_db] = fake_get_db
    try:
        response = client.post(
            "/api/v1/engine/transformations",
            json={
                "name": "Upper Hostname",
                "description": "desc",
                "output_column_name": "hostname_upper",
                "output_type": "string",
                "expression": {
                    "node_type": "function_call",
                    "function_name": "upper",
                    "arguments": [{"node_type": "column_ref", "column": "hostname"}],
                },
            },
        )
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["name"] == "Upper Hostname"
    finally:
        app.dependency_overrides.pop(engine_declarative_api.get_db, None)


def test_engine_view_run_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import declarative_api as engine_declarative_api

    def fake_get_db():
        yield _DummyDB()

    monkeypatch.setattr(
        engine_declarative_api,
        "run_view",
        lambda *_args, **_kwargs: {
            "total_rows": 3,
            "page": 1,
            "size": 2,
            "has_next": True,
            "has_previous": False,
            "columns": ["hostname", "primary_status"],
            "items": [{"hostname": "HOST-01", "primary_status": "COMPLIANT"}],
            "warnings": [],
        },
    )
    app.dependency_overrides[engine_declarative_api.get_db] = fake_get_db
    try:
        response = client.post(
            "/api/v1/engine/views/run?dataset_version_id=dataset-1",
            json={"view_id": "view-1", "page": 1, "size": 2},
        )
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["total_rows"] == 3
        assert payload["items"][0]["hostname"] == "HOST-01"
    finally:
        app.dependency_overrides.pop(engine_declarative_api.get_db, None)

