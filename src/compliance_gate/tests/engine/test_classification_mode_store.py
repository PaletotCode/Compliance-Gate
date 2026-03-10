from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from compliance_gate.authentication.models import Tenant
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.models import EngineRun
from compliance_gate.Engine.rulesets import (
    ClassificationRuntimeMode,
    get_classification_mode_state,
    list_recent_classification_runs,
    list_recent_divergences,
    record_divergences,
    set_classification_mode,
)
from compliance_gate.infra.db.models import AuditLog
from compliance_gate.infra.db.session import Base


def _expected_default_mode() -> ClassificationRuntimeMode:
    configured = (engine_settings.classification_mode_default or "legacy").strip().lower()
    for mode in ClassificationRuntimeMode:
        if mode.value == configured:
            return mode
    return ClassificationRuntimeMode.LEGACY


def _db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    db = session_factory()
    db.add(
        Tenant(
            id="tenant-1",
            slug="tenant-1",
            display_name="Tenant 1",
            name="Tenant 1",
            is_active=True,
        )
    )
    db.commit()
    return db, engine


def test_set_and_get_classification_mode_state_with_audit() -> None:
    db, engine = _db_session()
    try:
        initial = get_classification_mode_state(db, tenant_id="tenant-1")
        assert initial.is_default is True
        assert initial.mode == _expected_default_mode()

        updated = set_classification_mode(
            db,
            tenant_id="tenant-1",
            mode=ClassificationRuntimeMode.SHADOW,
            ruleset_name="official",
            updated_by="user-1",
        )
        assert updated.mode == "shadow"
        assert updated.ruleset_name == "official"

        state = get_classification_mode_state(db, tenant_id="tenant-1")
        assert state.is_default is False
        assert state.mode == ClassificationRuntimeMode.SHADOW
        assert state.ruleset_name == "official"
        assert state.updated_by == "user-1"

        actions = [
            row.action
            for row in db.query(AuditLog).filter(AuditLog.entity_type == "engine_classification").all()
        ]
        assert "CLASSIFICATION_MODE_UPDATE" in actions
    finally:
        db.close()
        engine.dispose()


def test_record_and_list_classification_divergences_and_runs() -> None:
    db, engine = _db_session()
    try:
        run = EngineRun(
            tenant_id="tenant-1",
            dataset_version_id=None,
            run_type="materialize",
            status="success",
            started_at=datetime.now(UTC),
            ended_at=datetime.now(UTC),
            metrics_json='{"classification":{"mode":"shadow"}}',
        )
        db.add(run)
        db.commit()
        db.refresh(run)

        inserted = record_divergences(
            db,
            tenant_id="tenant-1",
            dataset_version_id="dataset-1",
            run_id=run.id,
            ruleset_name="official",
            divergences=[
                {
                    "machine_id": "HOST-01",
                    "hostname": "HOST-01",
                    "legacy_primary_status": "COMPLIANT",
                    "legacy_primary_status_label": "Compliant",
                    "legacy_flags": ["LEGACY"],
                    "declarative_primary_status": "ROGUE",
                    "declarative_primary_status_label": "Rogue",
                    "declarative_flags": ["DECL"],
                    "diff": {"primary_status": {"legacy": "COMPLIANT", "declarative": "ROGUE"}},
                }
            ],
        )
        db.commit()
        assert inserted == 1

        divergences = list_recent_divergences(
            db,
            tenant_id="tenant-1",
            limit=10,
        )
        assert len(divergences) == 1
        assert divergences[0].run_id == run.id
        assert divergences[0].ruleset_name == "official"

        runs = list_recent_classification_runs(
            db,
            tenant_id="tenant-1",
            limit=10,
        )
        assert len(runs) == 1
        assert runs[0].id == run.id
    finally:
        db.close()
        engine.dispose()
