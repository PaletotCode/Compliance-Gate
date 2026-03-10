from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from compliance_gate.main import app


class _DummyDB:
    pass


def _preview_payload() -> dict:
    return {
        "schema_version": 2,
        "blocks": [
            {
                "kind": "primary",
                "entries": [
                    {
                        "rule_key": "missing_edr",
                        "priority": 1,
                        "condition": {
                            "node_type": "binary_op",
                            "operator": "==",
                            "left": {"node_type": "column_ref", "column": "has_edr"},
                            "right": {"node_type": "literal", "value_type": "bool", "value": False},
                        },
                        "output": {
                            "primary_status": "MISSING_EDR",
                            "primary_status_label": "Missing EDR",
                        },
                    }
                ],
            },
            {
                "kind": "flags",
                "entries": [
                    {
                        "rule_key": "flag_no_ad",
                        "priority": 10,
                        "condition": {
                            "node_type": "binary_op",
                            "operator": "==",
                            "left": {"node_type": "column_ref", "column": "has_ad"},
                            "right": {"node_type": "literal", "value_type": "bool", "value": False},
                        },
                        "output": {"flag": "NO_AD"},
                    }
                ],
            },
        ],
    }


def _build_record(*, version: int, status: str, ruleset_name: str = "machines-rules"):
    from compliance_gate.Engine.rulesets import RuleSetPayloadV2

    payload = RuleSetPayloadV2(
        blocks=[
            {
                "kind": "primary",
                "entries": [
                    {
                        "rule_key": "primary",
                        "priority": 1,
                        "condition": {
                            "node_type": "function_call",
                            "function_name": "is_not_null",
                            "arguments": [{"node_type": "column_ref", "column": "hostname"}],
                        },
                        "output": {"primary_status": "COMPLIANT"},
                    }
                ],
            }
        ]
    )
    definition = SimpleNamespace(
        id="rs-1",
        tenant_id="default",
        name=ruleset_name,
        description="desc",
        created_by="u1",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        active_version=version,
        published_version=version if status == "published" else None,
        is_archived=False,
    )
    version_model = SimpleNamespace(
        id=f"rsv-{version}",
        ruleset_id="rs-1",
        tenant_id="default",
        version=version,
        status=status,
        payload_json=payload.model_dump_json(),
        created_at=datetime.now(UTC),
        created_by="u1",
        validated_at=datetime.now(UTC) if status in {"validated", "published"} else None,
        validated_by="u1" if status in {"validated", "published"} else None,
        published_at=datetime.now(UTC) if status == "published" else None,
        published_by="u1" if status == "published" else None,
    )
    return SimpleNamespace(definition=definition, version=version_model, payload=payload)


def test_rulesets_list_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import rulesets_api

    def fake_get_db():
        yield _DummyDB()

    monkeypatch.setattr(
        rulesets_api,
        "list_rulesets",
        lambda *_args, **_kwargs: [_build_record(version=1, status="draft")],
    )
    app.dependency_overrides[rulesets_api.get_db] = fake_get_db
    try:
        response = client.get("/api/v1/engine/rulesets")
        assert response.status_code == 200
        payload = response.json()["data"]
        assert len(payload) == 1
        assert payload[0]["id"] == "rs-1"
        assert payload[0]["active_version"] == 1
    finally:
        app.dependency_overrides.pop(rulesets_api.get_db, None)


def test_ruleset_validate_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import rulesets_api

    def fake_get_db():
        yield _DummyDB()

    def fake_validate(*_args, **_kwargs):
        from compliance_gate.Engine.rulesets import RuleSetValidationResult

        return _build_record(version=2, status="validated"), RuleSetValidationResult(
            is_valid=True,
            issues=[],
        )

    monkeypatch.setattr(rulesets_api, "validate_ruleset_version", fake_validate)
    app.dependency_overrides[rulesets_api.get_db] = fake_get_db
    try:
        response = client.post(
            "/api/v1/engine/rulesets/rs-1/versions/2/validate",
            json={"column_types": {"hostname": "string"}},
        )
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["validation"]["is_valid"] is True
        assert payload["version"]["version"] == 2
        assert payload["version"]["status"] == "validated"
    finally:
        app.dependency_overrides.pop(rulesets_api.get_db, None)


def test_ruleset_internal_published_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import rulesets_api

    def fake_get_db():
        yield _DummyDB()

    monkeypatch.setattr(
        rulesets_api,
        "get_ruleset_by_name",
        lambda *_args, **_kwargs: _build_record(
            version=3, status="published", ruleset_name="official"
        ),
    )
    app.dependency_overrides[rulesets_api.get_db] = fake_get_db
    try:
        response = client.get("/api/v1/engine/internal/rulesets/published?name=official")
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["ruleset_name"] == "official"
        assert payload["resolved_as"] == "published"
        assert payload["version"]["version"] == 3
        assert payload["version"]["status"] == "published"
    finally:
        app.dependency_overrides.pop(rulesets_api.get_db, None)


def test_classification_mode_update_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import rulesets_api

    def fake_get_db():
        yield _DummyDB()

    monkeypatch.setattr(
        rulesets_api,
        "get_ruleset_by_name",
        lambda *_args, **_kwargs: _build_record(
            version=3, status="published", ruleset_name="official"
        ),
    )
    monkeypatch.setattr(
        rulesets_api,
        "set_classification_mode",
        lambda *_args, **_kwargs: SimpleNamespace(id="mode-1"),
    )
    monkeypatch.setattr(
        rulesets_api,
        "get_classification_mode_state",
        lambda *_args, **_kwargs: SimpleNamespace(
            mode=rulesets_api.ClassificationRuntimeMode.SHADOW,
            ruleset_name="official",
            updated_at=datetime.now(UTC),
            updated_by="test-user-id",
            is_default=False,
        ),
    )
    app.dependency_overrides[rulesets_api.get_db] = fake_get_db
    try:
        response = client.put(
            "/api/v1/engine/classification/mode",
            json={"mode": "shadow", "ruleset_name": "official"},
        )
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["mode"] == "shadow"
        assert payload["ruleset_name"] == "official"
        assert payload["source"] == "configured"
    finally:
        app.dependency_overrides.pop(rulesets_api.get_db, None)


def test_classification_mode_update_endpoint_supports_instant_rollback_to_legacy(
    monkeypatch, client
) -> None:
    from compliance_gate.Engine.interfaces import rulesets_api

    def fake_get_db():
        yield _DummyDB()

    ruleset_lookup_called = {"value": False}
    captured_mode_payload: dict[str, object] = {}

    def fake_get_ruleset_by_name(*_args, **_kwargs):
        ruleset_lookup_called["value"] = True
        return _build_record(version=3, status="published", ruleset_name="official")

    def fake_set_classification_mode(*_args, **kwargs):
        captured_mode_payload["mode"] = kwargs.get("mode")
        captured_mode_payload["ruleset_name"] = kwargs.get("ruleset_name")
        return SimpleNamespace(id="mode-1")

    monkeypatch.setattr(rulesets_api, "get_ruleset_by_name", fake_get_ruleset_by_name)
    monkeypatch.setattr(rulesets_api, "set_classification_mode", fake_set_classification_mode)
    monkeypatch.setattr(
        rulesets_api,
        "get_classification_mode_state",
        lambda *_args, **_kwargs: SimpleNamespace(
            mode=rulesets_api.ClassificationRuntimeMode.LEGACY,
            ruleset_name=None,
            updated_at=datetime.now(UTC),
            updated_by="test-user-id",
            is_default=False,
        ),
    )
    app.dependency_overrides[rulesets_api.get_db] = fake_get_db
    try:
        response = client.put(
            "/api/v1/engine/classification/mode",
            json={"mode": "legacy"},
        )
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["mode"] == "legacy"
        assert payload["ruleset_name"] is None
        assert ruleset_lookup_called["value"] is False
        assert captured_mode_payload["mode"] == rulesets_api.ClassificationRuntimeMode.LEGACY
        assert captured_mode_payload["ruleset_name"] is None
    finally:
        app.dependency_overrides.pop(rulesets_api.get_db, None)


def test_classification_migration_state_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import rulesets_api

    def fake_get_db():
        yield _DummyDB()

    monkeypatch.setattr(
        rulesets_api,
        "get_classification_migration_state",
        lambda *_args, **_kwargs: SimpleNamespace(
            phase=rulesets_api.ClassificationMigrationPhase.B,
            ruleset_id="rs-1",
            ruleset_name="official",
            baseline_version=3,
            parity_target_percent=99.9,
            last_parity_percent=99.95,
            last_parity_passed=True,
            last_dataset_version_id="dataset-1",
            last_run_id="run-1",
            updated_at=datetime.now(UTC),
            updated_by="user-1",
            is_default=False,
        ),
    )
    app.dependency_overrides[rulesets_api.get_db] = fake_get_db
    try:
        response = client.get("/api/v1/engine/classification/migration/state")
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["phase"] == "B"
        assert payload["ruleset_name"] == "official"
        assert payload["source"] == "configured"
    finally:
        app.dependency_overrides.pop(rulesets_api.get_db, None)


def test_classification_migration_promote_phase_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import rulesets_api

    def fake_get_db():
        yield _DummyDB()

    monkeypatch.setattr(
        rulesets_api,
        "ensure_baseline_ruleset_for_tenant",
        lambda *_args, **_kwargs: {
            "ruleset_id": "rs-1",
            "ruleset_name": "official",
            "published_version": 3,
            "created_now": False,
            "phase": "A",
        },
    )
    monkeypatch.setattr(
        rulesets_api,
        "promote_classification_migration_phase",
        lambda *_args, **_kwargs: SimpleNamespace(
            phase=rulesets_api.ClassificationMigrationPhase.C,
            ruleset_id="rs-1",
            ruleset_name="official",
            baseline_version=3,
            parity_target_percent=99.9,
            last_parity_percent=99.95,
            last_parity_passed=True,
            last_dataset_version_id="dataset-1",
            last_run_id="run-1",
            updated_at=datetime.now(UTC),
            updated_by="user-1",
            is_default=False,
        ),
    )
    app.dependency_overrides[rulesets_api.get_db] = fake_get_db
    try:
        response = client.put(
            "/api/v1/engine/classification/migration/promote-phase",
            json={"target_phase": "C", "enforce_parity": True},
        )
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["phase"] == "C"
        assert payload["ruleset_id"] == "rs-1"
    finally:
        app.dependency_overrides.pop(rulesets_api.get_db, None)


def test_classification_parity_report_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import rulesets_api

    def fake_get_db():
        yield _DummyDB()

    monkeypatch.setattr(
        rulesets_api,
        "build_shadow_parity_report",
        lambda *_args, **_kwargs: {
            "tenant_id": "default",
            "dataset_version_id": "dataset-1",
            "run_id": "run-1",
            "ruleset_name": "official",
            "rows_classified": 1000,
            "rows_scanned": 1000,
            "total_divergences": 1,
            "observed_divergence_rows": 1,
            "report_rows": 1,
            "report_truncated": False,
            "parity_percent": 99.9,
            "parity_target_percent": 99.9,
            "parity_ok": True,
            "by_dimension": {"primary_status": 1},
            "by_severity": {"DANGER": 1},
            "by_rule": {"primary:primary_rogue": 1},
        },
    )
    app.dependency_overrides[rulesets_api.get_db] = fake_get_db
    try:
        response = client.get("/api/v1/engine/classification/parity-report?dataset_version_id=dataset-1")
        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["parity_percent"] == 99.9
        assert payload["by_rule"]["primary:primary_rogue"] == 1
    finally:
        app.dependency_overrides.pop(rulesets_api.get_db, None)


def test_classification_migration_inventory_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import rulesets_api

    monkeypatch.setattr(
        rulesets_api,
        "get_legacy_rule_inventory",
        lambda *_args, **_kwargs: [
            {
                "rule_key": "primary_missing_edr",
                "block_kind": "primary",
                "precedence": 5,
                "legacy_module": "falta_edr.rule",
                "status_key": "MISSING_EDR",
                "status_label": "FALTA EDR",
                "severity": "WARNING",
                "is_flag": False,
                "description": "sample",
                "condition": {"node_type": "literal", "value_type": "bool", "value": True},
                "output": {"primary_status": "MISSING_EDR"},
            }
        ],
    )
    response = client.get("/api/v1/engine/classification/migration/inventory")
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload[0]["rule_key"] == "primary_missing_edr"


def test_classification_divergences_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import rulesets_api

    def fake_get_db():
        yield _DummyDB()

    monkeypatch.setattr(
        rulesets_api,
        "list_recent_divergences",
        lambda *_args, **_kwargs: [
            SimpleNamespace(
                id="div-1",
                tenant_id="default",
                dataset_version_id="dataset-1",
                run_id="run-1",
                ruleset_name="official",
                machine_id="HOST-01",
                hostname="HOST-01",
                legacy_primary_status="COMPLIANT",
                legacy_primary_status_label="Compliant",
                legacy_flags_json='["MISSING_EDR"]',
                declarative_primary_status="ROGUE",
                declarative_primary_status_label="Rogue",
                declarative_flags_json='["MISSING_EDR","STALE"]',
                diff_json='{"primary_status":{"legacy":"COMPLIANT","declarative":"ROGUE"}}',
                created_at=datetime.now(UTC),
            )
        ],
    )
    app.dependency_overrides[rulesets_api.get_db] = fake_get_db
    try:
        response = client.get("/api/v1/engine/classification/divergences?limit=10")
        assert response.status_code == 200
        payload = response.json()["data"]
        assert len(payload) == 1
        assert payload[0]["legacy_primary_status"] == "COMPLIANT"
        assert payload[0]["declarative_primary_status"] == "ROGUE"
        assert payload[0]["legacy_flags"] == ["MISSING_EDR"]
    finally:
        app.dependency_overrides.pop(rulesets_api.get_db, None)


def test_classification_metrics_endpoint(monkeypatch, client) -> None:
    from compliance_gate.Engine.interfaces import rulesets_api

    def fake_get_db():
        yield _DummyDB()

    monkeypatch.setattr(
        rulesets_api,
        "list_recent_classification_runs",
        lambda *_args, **_kwargs: [
            SimpleNamespace(
                id="run-1",
                tenant_id="default",
                dataset_version_id="dataset-1",
                status="success",
                started_at=datetime.now(UTC),
                ended_at=datetime.now(UTC),
                metrics_json='{"classification":{"mode":"shadow","ruleset_name":"official","rows_scanned":10,"rows_classified":10,"elapsed_ms":12.5,"rule_hits":{"primary:rule":7},"divergences":2}}',
                error_truncated=None,
            )
        ],
    )
    app.dependency_overrides[rulesets_api.get_db] = fake_get_db
    try:
        response = client.get("/api/v1/engine/classification/metrics?limit=10")
        assert response.status_code == 200
        payload = response.json()["data"]
        assert len(payload) == 1
        assert payload[0]["mode"] == "shadow"
        assert payload[0]["rows_scanned"] == 10
        assert payload[0]["divergences"] == 2
        assert payload[0]["rule_hits"]["primary:rule"] == 7
    finally:
        app.dependency_overrides.pop(rulesets_api.get_db, None)


def test_validate_ruleset_payload_endpoint(client) -> None:
    response = client.post(
        "/api/v1/engine/validate-ruleset",
        json={
            "payload": _preview_payload(),
            "column_types": {
                "hostname": "string",
                "pa_code": "string",
                "has_edr": "bool",
                "has_ad": "bool",
            },
        },
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["is_valid"] is True
    assert payload["summary"]["error_count"] == 0
    assert payload["summary"]["warning_count"] == 0


def test_explain_row_endpoint(client) -> None:
    response = client.post(
        "/api/v1/engine/rulesets/explain-row",
        json={
            "payload": _preview_payload(),
            "row": {
                "hostname": "HOST-01",
                "pa_code": "PA-1",
                "has_edr": False,
                "has_ad": False,
            },
            "ruleset_name": "preview-ruleset",
            "version": 7,
        },
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["final_output"]["primary_status"] == "MISSING_EDR"
    assert payload["final_output"]["flags"] == ["NO_AD"]
    assert payload["decision_reason"].startswith("Regra primary")


def test_dry_run_ruleset_endpoint(client) -> None:
    response = client.post(
        "/api/v1/engine/rulesets/dry-run-ruleset",
        json={
            "payload": _preview_payload(),
            "rows": [
                {
                    "hostname": "HOST-01",
                    "pa_code": "PA-1",
                    "has_edr": False,
                    "has_ad": False,
                }
            ],
            "mode": "declarative",
            "explain_sample_limit": 1,
        },
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["mode"] == "declarative"
    assert payload["rows_scanned"] == 1
    assert payload["rows_classified"] == 1
    assert payload["status_counts"]["MISSING_EDR"] == 1
    assert payload["sample_explain"]["explained_rows"] == 1


def test_validate_ruleset_payload_invalid_schema_returns_structured_error(client) -> None:
    response = client.post(
        "/api/v1/engine/rulesets/validate-ruleset",
        json={
            "payload": {
                "schema_version": 2,
                "blocks": [{"kind": "primary", "entries": [{"priority": 1}]}],
            }
        },
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "InvalidExpressionSyntax"
    assert "message" in detail
    assert "details" in detail
    assert "hint" in detail
    assert "node_path" in detail
