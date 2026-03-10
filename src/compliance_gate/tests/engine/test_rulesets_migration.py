from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from compliance_gate.authentication.models import Tenant
from compliance_gate.Engine.errors import GuardrailViolation
from compliance_gate.Engine.models import EngineClassificationDivergence, EngineRun
from compliance_gate.Engine.rulesets import (
    ClassificationMigrationPhase,
    build_shadow_parity_report,
    ensure_baseline_ruleset_for_tenant,
    get_classification_migration_state,
    get_ruleset_by_name,
    promote_classification_migration_phase,
)
from compliance_gate.infra.db.session import Base


@pytest.fixture()
def db_session():
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
    try:
        yield db
    finally:
        db.close()
        engine.dispose()


def test_ensure_baseline_ruleset_creates_and_publishes_by_tenant(db_session) -> None:
    result = ensure_baseline_ruleset_for_tenant(
        db_session,
        tenant_id="tenant-1",
        actor="user-1",
    )
    assert result["ruleset_name"]
    assert result["published_version"] >= 1

    published = get_ruleset_by_name(
        db_session,
        tenant_id="tenant-1",
        name=result["ruleset_name"],
        resolution="published",
    )
    assert published.version.version == result["published_version"]

    state = get_classification_migration_state(db_session, tenant_id="tenant-1")
    assert state.is_default is False
    assert state.phase == ClassificationMigrationPhase.A
    assert state.ruleset_id == result["ruleset_id"]
    assert state.baseline_version == result["published_version"]


def test_promote_phase_requires_parity_gate_by_default(db_session) -> None:
    ensure_baseline_ruleset_for_tenant(
        db_session,
        tenant_id="tenant-1",
        actor="user-1",
    )

    with pytest.raises(GuardrailViolation):
        promote_classification_migration_phase(
            db_session,
            tenant_id="tenant-1",
            target_phase=ClassificationMigrationPhase.B,
            updated_by="user-1",
            enforce_parity=True,
        )

    state = promote_classification_migration_phase(
        db_session,
        tenant_id="tenant-1",
        target_phase=ClassificationMigrationPhase.B,
        updated_by="user-1",
        enforce_parity=False,
    )
    assert state.phase == ClassificationMigrationPhase.B


def test_shadow_parity_report_aggregates_dimensions_rules_and_updates_snapshot(db_session) -> None:
    ensure_baseline_ruleset_for_tenant(
        db_session,
        tenant_id="tenant-1",
        actor="user-1",
    )
    run = EngineRun(
        id="run-1",
        tenant_id="tenant-1",
        dataset_version_id="dataset-1",
        run_type="materialize",
        status="success",
        started_at=datetime.now(UTC),
        ended_at=datetime.now(UTC),
        metrics_json=json.dumps(
            {
                "classification": {
                    "mode": "shadow",
                    "ruleset_name": "machines-classification",
                    "rows_scanned": 10,
                    "rows_classified": 10,
                    "divergences": 2,
                }
            }
        ),
    )
    db_session.add(run)
    db_session.flush()
    db_session.add(
        EngineClassificationDivergence(
            tenant_id="tenant-1",
            dataset_version_id="dataset-1",
            run_id=run.id,
            ruleset_name="machines-classification",
            machine_id="H-1",
            hostname="H-1",
            legacy_primary_status="COMPLIANT",
            legacy_primary_status_label="Compliant",
            legacy_flags_json="[]",
            declarative_primary_status="ROGUE",
            declarative_primary_status_label="Rogue",
            declarative_flags_json="[]",
            diff_json=json.dumps(
                {
                    "primary_status": {"legacy": "COMPLIANT", "declarative": "ROGUE"},
                    "declarative_rule_keys": ["primary:primary_rogue"],
                    "severity": "DANGER",
                    "divergence_kind": "primary_status",
                }
            ),
        )
    )
    db_session.add(
        EngineClassificationDivergence(
            tenant_id="tenant-1",
            dataset_version_id="dataset-1",
            run_id=run.id,
            ruleset_name="machines-classification",
            machine_id="H-2",
            hostname="H-2",
            legacy_primary_status="COMPLIANT",
            legacy_primary_status_label="Compliant",
            legacy_flags_json='["LEGACY"]',
            declarative_primary_status="COMPLIANT",
            declarative_primary_status_label="Compliant",
            declarative_flags_json="[]",
            diff_json=json.dumps(
                {
                    "flags": {"legacy": ["LEGACY"], "declarative": []},
                    "declarative_rule_keys": ["flags:flag_legacy_os"],
                    "severity": "WARNING",
                    "divergence_kind": "flags",
                }
            ),
        )
    )
    db_session.commit()

    report = build_shadow_parity_report(
        db_session,
        tenant_id="tenant-1",
        dataset_version_id="dataset-1",
        run_id="run-1",
        persist_snapshot=True,
        actor="user-1",
    )
    assert report["parity_percent"] == 80.0
    assert report["by_dimension"]["primary_status"] == 1
    assert report["by_dimension"]["flags"] == 1
    assert report["by_rule"]["primary:primary_rogue"] == 1
    assert report["by_rule"]["flags:flag_legacy_os"] == 1

    state = get_classification_migration_state(db_session, tenant_id="tenant-1")
    assert state.last_dataset_version_id == "dataset-1"
    assert state.last_run_id == "run-1"
    assert state.last_parity_percent == 80.0
