from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import polars as pl

from compliance_gate.main import app


class _DummyDB:
    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def flush(self) -> None:
        return None


def test_datasets_preview_contract_accepts_upload_session(monkeypatch, client) -> None:
    from compliance_gate.http.routes import datasets as datasets_route

    captured: dict[str, object] = {}

    def fake_get_db():
        yield _DummyDB()

    def fake_run_preview(data_dir: Path, *, configs: dict):
        captured["data_dir"] = data_dir
        captured["configs"] = configs
        return SimpleNamespace(
            to_response=lambda: {
                "status": "ok",
                "layouts": [],
                "source_samples": {"AD": []},
                "source_metrics": [{"source": "AD", "row_count": 0}],
                "summary": {"row_count": 0, "warning_count": 0},
            }
        )

    monkeypatch.setattr(datasets_route, "resolve_data_dir", lambda **_kwargs: Path("/workspace"))
    monkeypatch.setattr(datasets_route, "run_preview", fake_run_preview)
    monkeypatch.setattr(
        datasets_route.profiles_store,
        "get_profile_by_id",
        lambda _db, _pid: SimpleNamespace(id="p-ad", tenant_id="default"),
    )
    monkeypatch.setattr(
        datasets_route.profiles_store,
        "get_active_payload",
        lambda _db, _pid: {"sic_column": "Hostname"},
    )
    app.dependency_overrides[datasets_route.get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/datasets/machines/preview",
            json={"upload_session_id": "sess-1", "profile_ids": {"AD": "p-ad"}},
        )
        assert response.status_code == 200
        payload = response.json()
        assert "layouts" in payload
        assert "source_samples" in payload
        assert "source_metrics" in payload
        assert "summary" in payload
        assert captured["configs"] == {"AD": {"sic_column": "Hostname"}}
    finally:
        app.dependency_overrides.pop(datasets_route.get_db, None)


def test_workspace_upload_rejects_non_csv_extension(monkeypatch, client, tmp_path) -> None:
    from compliance_gate.http.routes import workspace_uploads as ws_route

    def fake_get_db():
        yield _DummyDB()

    monkeypatch.setattr(ws_route.settings, "cg_upload_dir", str(tmp_path))
    monkeypatch.setattr(ws_route.settings, "cg_upload_max_file_mb", 1)
    monkeypatch.setattr(
        ws_route.uploads_store,
        "create_upload_session",
        lambda *_args, **_kwargs: SimpleNamespace(
            id="session-1",
            tenant_id="default",
            status="active",
            root_path="",
        ),
    )
    app.dependency_overrides[ws_route.get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/workspace/uploads",
            files={"AD": ("AD.txt", b"a,b\n1,2\n", "text/plain")},
        )
        assert response.status_code == 400
        assert "extension" in response.json()["detail"].lower()
    finally:
        app.dependency_overrides.pop(ws_route.get_db, None)


def test_csv_tabs_rename_and_delete_contract(monkeypatch, client) -> None:
    from compliance_gate.http.routes import csv_tabs as csv_tabs_route

    def fake_get_db():
        yield _DummyDB()

    profile = SimpleNamespace(
        id="profile-1",
        tenant_id="default",
        source="AD",
        scope="PRIVATE",
        owner_user_id="user-1",
        name="Old Name",
        active_version=1,
        is_default_for_source=False,
    )

    monkeypatch.setattr(csv_tabs_route.profiles_store, "get_profile_by_id", lambda *_args, **_kwargs: profile)
    monkeypatch.setattr(
        csv_tabs_route.profiles_store,
        "rename_profile",
        lambda *_args, **_kwargs: SimpleNamespace(**{**profile.__dict__, "name": "Novo Nome"}),
    )
    monkeypatch.setattr(csv_tabs_route.profiles_store, "get_active_payload", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(csv_tabs_route.profiles_store, "delete_profile", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(csv_tabs_route.auth_repo, "append_auth_audit", lambda *_args, **_kwargs: None)
    app.dependency_overrides[csv_tabs_route.get_db] = fake_get_db

    try:
        rename_resp = client.patch(
            "/api/v1/csv-tabs/profiles/profile-1/rename",
            json={"name": "Novo Nome"},
        )
        assert rename_resp.status_code == 200
        assert rename_resp.json()["name"] == "Novo Nome"

        delete_resp = client.delete("/api/v1/csv-tabs/profiles/profile-1")
        assert delete_resp.status_code == 200
        assert delete_resp.json()["status"] == "ok"
    finally:
        app.dependency_overrides.pop(csv_tabs_route.get_db, None)


def test_engine_materialized_table_contract(monkeypatch, client, tmp_path) -> None:
    from compliance_gate.Engine.interfaces import api as engine_api

    class _ArtifactQuery:
        def filter(self, *_args, **_kwargs):
            return self

        def first(self):
            return SimpleNamespace(
                path=str(tmp_path / "machines_final.parquet"),
                tenant_id="default",
                dataset_version_id="dataset-1",
            )

    class _EngineDB:
        def query(self, *_args, **_kwargs):
            return _ArtifactQuery()

    def fake_get_db():
        yield _EngineDB()

    monkeypatch.setattr(engine_api.Path, "exists", lambda _self: True)
    monkeypatch.setattr(
        engine_api.pl,
        "read_parquet",
        lambda _path: pl.DataFrame(
            [
                {
                    "machine_id": "HOST-01",
                    "hostname": "HOST-01",
                    "pa_code": "PA01",
                    "primary_status": "COMPLIANT",
                    "primary_status_label": "Compliant",
                    "flags": ["OFFLINE"],
                    "has_ad": True,
                    "has_uem": True,
                    "has_edr": True,
                    "has_asset": True,
                }
            ]
        ),
    )
    app.dependency_overrides[engine_api.get_db] = fake_get_db

    try:
        response = client.get("/api/v1/engine/tables/machines?dataset_version_id=dataset-1&page=1&size=10")
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["meta"]["total"] == 1
        assert payload["items"][0]["hostname"] == "HOST-01"
    finally:
        app.dependency_overrides.pop(engine_api.get_db, None)
