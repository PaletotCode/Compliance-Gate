from __future__ import annotations
from pathlib import Path
from types import SimpleNamespace

from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabDraftConfig
from compliance_gate.main import app


class _DummyDB:
    def commit(self) -> None:
        return None


class _DummyQueryableDB(_DummyDB):
    # Enables code paths guarded by hasattr(db, "query")
    def query(self, *_args, **_kwargs):
        raise RuntimeError("query should not be called in this test")


class _DummyMetrics:
    rows_read_total = 1
    rows_valid_total = 1
    total_elapsed_ms = 1.0
    join = None

    def to_dict(self) -> dict[str, int]:
        return {"rows_read_total": 1, "rows_valid_total": 1}


def _fake_ingest_result() -> SimpleNamespace:
    return SimpleNamespace(
        files=[],
        metrics=_DummyMetrics(),
        records=[{"hostname": "HOST-01"}],
        warnings=[],
    )


def test_datasets_ingest_accepts_profile_ids(monkeypatch, client) -> None:
    from compliance_gate.http.routes import datasets as datasets_route

    calls: list[tuple[str, str]] = []

    def fake_get_db():
        yield _DummyDB()

    def fake_get_profile_by_id(_db, profile_id):
        return SimpleNamespace(id=profile_id, tenant_id="default")

    def fake_get_active_payload(_db, profile_id):
        calls.append(("get_active_payload", profile_id))
        return {"profile_id": profile_id}

    def fake_create_dataset_version(_db, **kwargs):
        _ = kwargs
        return SimpleNamespace(id="dataset-version-1")

    def fake_run_ingest_pipeline(_data_dir: Path, **kwargs):
        _ = kwargs
        return _fake_ingest_result()

    monkeypatch.setattr(datasets_route.profiles_store, "get_profile_by_id", fake_get_profile_by_id)
    monkeypatch.setattr(
        datasets_route.profiles_store, "get_active_payload", fake_get_active_payload
    )
    monkeypatch.setattr(datasets_route.store, "create_dataset_version", fake_create_dataset_version)
    monkeypatch.setattr(datasets_route.store, "register_file", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(datasets_route.store, "save_metrics", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        datasets_route.store, "finalize_dataset_version", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(datasets_route, "run_ingest_pipeline", fake_run_ingest_pipeline)
    monkeypatch.setattr(datasets_route, "resolve_data_dir", lambda **_kwargs: Path("/workspace"))
    app.dependency_overrides[datasets_route.get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/datasets/machines/ingest",
            json={
                "upload_session_id": "session-1",
                "profile_ids": {
                    "AD": "p-ad",
                    "UEM": "p-uem",
                    "EDR": "p-edr",
                    "ASSET": "p-asset",
                },
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "success"
        assert len(calls) == 4
    finally:
        app.dependency_overrides.pop(datasets_route.get_db, None)


def test_datasets_ingest_rejects_profile_without_active_payload(monkeypatch, client) -> None:
    from compliance_gate.http.routes import datasets as datasets_route

    def fake_get_db():
        yield _DummyDB()

    def fake_get_profile_by_id(_db, profile_id):
        return SimpleNamespace(id=profile_id, tenant_id="default")

    monkeypatch.setattr(datasets_route.profiles_store, "get_profile_by_id", fake_get_profile_by_id)
    monkeypatch.setattr(
        datasets_route.profiles_store, "get_active_payload", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(datasets_route, "resolve_data_dir", lambda **_kwargs: Path("/workspace"))
    app.dependency_overrides[datasets_route.get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/datasets/machines/ingest",
            json={
                "upload_session_id": "session-1",
                "profile_ids": {
                    "AD": "p-ad",
                },
            },
        )
        assert response.status_code == 400
        assert "payload ativo" in response.json()["detail"]
    finally:
        app.dependency_overrides.pop(datasets_route.get_db, None)


def test_datasets_ingest_rejects_invalid_profile_id(monkeypatch, client) -> None:
    from compliance_gate.http.routes import datasets as datasets_route

    def fake_get_db():
        yield _DummyDB()

    monkeypatch.setattr(
        datasets_route.profiles_store, "get_profile_by_id", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(datasets_route, "resolve_data_dir", lambda **_kwargs: Path("/workspace"))
    app.dependency_overrides[datasets_route.get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/datasets/machines/ingest",
            json={
                "upload_session_id": "session-1",
                "profile_ids": {
                    "AD": "inexistente",
                },
            },
        )
        assert response.status_code == 400
        assert "inválido" in response.json()["detail"]
    finally:
        app.dependency_overrides.pop(datasets_route.get_db, None)


def test_datasets_preview_uses_session_draft_config(monkeypatch, client) -> None:
    from compliance_gate.http.routes import datasets as datasets_route

    captured: dict[str, object] = {}

    def fake_get_db():
        yield _DummyQueryableDB()

    def fake_run_preview(_data_dir: Path, *, configs: dict):
        captured["configs"] = configs
        return SimpleNamespace(
            to_response=lambda: {
                "status": "ok",
                "layouts": [],
                "source_samples": {"AD": []},
                "source_metrics": [{"source": "AD", "row_count": 0}],
                "summary": {"row_count": 0, "warning_count": 0},
                "warnings": [],
            }
        )

    monkeypatch.setattr(
        datasets_route.uploads_store,
        "get_session_by_id",
        lambda _db, _session_id: SimpleNamespace(id="session-1", tenant_id="default"),
    )
    monkeypatch.setattr(
        datasets_route.uploads_store,
        "get_draft_configs",
        lambda _db, *, session_id: {"AD": CsvTabDraftConfig(sic_column="Hostname")},
    )
    monkeypatch.setattr(
        datasets_route.profiles_store,
        "get_default_profile_payload",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(datasets_route, "run_preview", fake_run_preview)
    monkeypatch.setattr(datasets_route, "resolve_data_dir", lambda **_kwargs: Path("/workspace"))
    app.dependency_overrides[datasets_route.get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/datasets/machines/preview",
            json={"upload_session_id": "session-1", "profile_ids": {}},
        )
        assert response.status_code == 200
        config_ad = captured["configs"]["AD"]
        assert config_ad.sic_column == "Hostname"
    finally:
        app.dependency_overrides.pop(datasets_route.get_db, None)


def test_datasets_ingest_persists_profile_payload_snapshots(monkeypatch, client) -> None:
    from compliance_gate.http.routes import datasets as datasets_route

    captured: dict[str, object] = {}

    def fake_get_db():
        yield _DummyQueryableDB()

    def fake_create_dataset_version(_db, **kwargs):
        captured["create_kwargs"] = kwargs
        return SimpleNamespace(id="dataset-version-1")

    def fake_run_ingest_pipeline(_data_dir: Path, **kwargs):
        _ = kwargs
        return _fake_ingest_result()

    monkeypatch.setattr(
        datasets_route.uploads_store,
        "get_session_by_id",
        lambda _db, _session_id: SimpleNamespace(id="session-1", tenant_id="default"),
    )
    monkeypatch.setattr(
        datasets_route.uploads_store,
        "get_draft_configs",
        lambda _db, *, session_id: {"AD": CsvTabDraftConfig(sic_column="Hostname")},
    )
    monkeypatch.setattr(
        datasets_route.profiles_store,
        "get_default_profile_payload",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(datasets_route.store, "create_dataset_version", fake_create_dataset_version)
    monkeypatch.setattr(datasets_route.store, "register_file", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(datasets_route.store, "save_metrics", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        datasets_route.store, "finalize_dataset_version", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(datasets_route, "run_ingest_pipeline", fake_run_ingest_pipeline)
    monkeypatch.setattr(datasets_route, "resolve_data_dir", lambda **_kwargs: Path("/workspace"))
    app.dependency_overrides[datasets_route.get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/datasets/machines/ingest",
            json={"upload_session_id": "session-1", "profile_ids": {}},
        )
        assert response.status_code == 200
        payloads = captured["create_kwargs"]["profile_payloads_map"]
        assert payloads["AD"]["sic_column"] == "Hostname"
    finally:
        app.dependency_overrides.pop(datasets_route.get_db, None)
