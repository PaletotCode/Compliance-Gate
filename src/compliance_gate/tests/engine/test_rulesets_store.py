from __future__ import annotations

import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from compliance_gate.authentication.models import Tenant
from compliance_gate.Engine.errors import UnknownColumn
from compliance_gate.Engine.rulesets import (
    RuleSetPayloadV2,
    RuleSetVersionStatus,
    create_ruleset,
    create_ruleset_version,
    get_ruleset,
    get_ruleset_version,
    publish_ruleset_version,
    rollback_ruleset,
    update_ruleset,
    validate_ruleset_version,
)
from compliance_gate.infra.db.models import AuditLog
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


def _payload(primary_threshold: int = 30) -> RuleSetPayloadV2:
    return RuleSetPayloadV2(
        blocks=[
            {
                "kind": "special",
                "entries": [
                    {
                        "rule_key": "special_legacy",
                        "priority": 1,
                        "condition": {
                            "node_type": "function_call",
                            "function_name": "starts_with",
                            "arguments": [
                                {"node_type": "column_ref", "column": "hostname"},
                                {"node_type": "literal", "value_type": "string", "value": "LEG-"},
                            ],
                        },
                        "output": {"bypass": True, "reason": "legacy"},
                    }
                ],
            },
            {
                "kind": "primary",
                "entries": [
                    {
                        "rule_key": "primary_inactive",
                        "priority": 10,
                        "condition": {
                            "node_type": "binary_op",
                            "operator": ">=",
                            "left": {"node_type": "column_ref", "column": "days_since_last_seen"},
                            "right": {
                                "node_type": "literal",
                                "value_type": "int",
                                "value": primary_threshold,
                            },
                        },
                        "output": {"primary_status": "INACTIVE"},
                    }
                ],
            },
            {
                "kind": "flags",
                "entries": [
                    {
                        "rule_key": "flag_perigo",
                        "priority": 100,
                        "condition": {
                            "node_type": "function_call",
                            "function_name": "contains",
                            "arguments": [
                                {"node_type": "column_ref", "column": "signals"},
                                {"node_type": "literal", "value_type": "string", "value": "PERIGO"},
                            ],
                        },
                        "output": {"flag": "PERIGO"},
                    }
                ],
            },
        ]
    )


def test_ruleset_store_lifecycle_with_publish_and_rollback(db_session) -> None:
    record = create_ruleset(
        db_session,
        tenant_id="tenant-1",
        name="machines-classification",
        description="Initial classification rules",
        created_by="user-1",
        payload=_payload(30),
    )
    assert record.definition.active_version == 1
    assert record.version.status == RuleSetVersionStatus.DRAFT.value

    updated = update_ruleset(
        db_session,
        tenant_id="tenant-1",
        ruleset_id=record.definition.id,
        name="machines-classification-v2",
        description="Updated metadata",
        updated_by="user-1",
    )
    assert updated.definition.name == "machines-classification-v2"

    validated_v1, validation_result_v1 = validate_ruleset_version(
        db_session,
        tenant_id="tenant-1",
        ruleset_id=record.definition.id,
        version=1,
        column_types={
            "hostname": "string",
            "days_since_last_seen": "int",
            "signals": "list[string]",
        },
        validated_by="user-1",
    )
    assert validation_result_v1.is_valid is True
    assert validated_v1.version.status == RuleSetVersionStatus.VALIDATED.value

    published_v1 = publish_ruleset_version(
        db_session,
        tenant_id="tenant-1",
        ruleset_id=record.definition.id,
        version=1,
        published_by="user-1",
    )
    assert published_v1.version.status == RuleSetVersionStatus.PUBLISHED.value

    version_2 = create_ruleset_version(
        db_session,
        tenant_id="tenant-1",
        ruleset_id=record.definition.id,
        created_by="user-1",
        source_version=1,
        payload=_payload(45),
    )
    assert version_2.version.version == 2
    assert version_2.version.status == RuleSetVersionStatus.DRAFT.value

    validate_ruleset_version(
        db_session,
        tenant_id="tenant-1",
        ruleset_id=record.definition.id,
        version=2,
        column_types={
            "hostname": "string",
            "days_since_last_seen": "int",
            "signals": "list[string]",
        },
        validated_by="user-1",
    )
    publish_ruleset_version(
        db_session,
        tenant_id="tenant-1",
        ruleset_id=record.definition.id,
        version=2,
        published_by="user-1",
    )

    rolled_back = rollback_ruleset(
        db_session,
        tenant_id="tenant-1",
        ruleset_id=record.definition.id,
        rolled_back_by="user-1",
    )
    assert rolled_back.version.version == 1
    assert rolled_back.version.status == RuleSetVersionStatus.PUBLISHED.value

    current = get_ruleset(
        db_session,
        tenant_id="tenant-1",
        ruleset_id=record.definition.id,
    )
    assert current.definition.active_version == 1
    assert current.definition.published_version == 1

    actions = {
        row.action
        for row in db_session.query(AuditLog).filter(AuditLog.entity_type == "engine_ruleset").all()
    }
    assert "RULESET_CREATE" in actions
    assert "RULESET_UPDATE" in actions
    assert "RULESET_VALIDATE" in actions
    assert "RULESET_PUBLISH" in actions
    assert "RULESET_ROLLBACK" in actions


def test_ruleset_validate_failure_persists_issue_payload(db_session) -> None:
    record = create_ruleset(
        db_session,
        tenant_id="tenant-1",
        name="invalid-rules",
        description=None,
        created_by="user-2",
        payload=RuleSetPayloadV2(
            blocks=[
                {
                    "kind": "primary",
                    "entries": [
                        {
                            "rule_key": "invalid_column",
                            "priority": 1,
                            "condition": {
                                "node_type": "binary_op",
                                "operator": "==",
                                "left": {
                                    "node_type": "column_ref",
                                    "column": "missing_col",
                                },
                                "right": {
                                    "node_type": "literal",
                                    "value_type": "string",
                                    "value": "x",
                                },
                            },
                            "output": {"primary_status": "UNKNOWN"},
                        }
                    ],
                }
            ]
        ),
    )

    with pytest.raises(UnknownColumn):
        validate_ruleset_version(
            db_session,
            tenant_id="tenant-1",
            ruleset_id=record.definition.id,
            version=1,
            column_types={"hostname": "string"},
            validated_by="user-2",
        )

    version_record = get_ruleset_version(
        db_session,
        tenant_id="tenant-1",
        ruleset_id=record.definition.id,
        version=1,
    )
    assert version_record.version.status == RuleSetVersionStatus.DRAFT.value

    issues = json.loads(version_record.version.validation_errors_json or "[]")
    assert issues
    assert issues[0]["code"] == "UnknownColumn"
