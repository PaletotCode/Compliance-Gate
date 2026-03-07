from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy.orm import Session

from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabConfig
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.infra.db.models import DatasetVersion
from compliance_gate.infra.storage import profiles_store


def resolve_tenant_id(tenant_id: str | None) -> str:
    return tenant_id or engine_settings.default_tenant_id


def build_artifact_dir(tenant_id: str, domain: str, dataset_version_id: str) -> Path:
    return Path(engine_settings.artifacts_base_dir) / tenant_id / domain / dataset_version_id


def get_parquet_path(
    tenant_id: str,
    domain: str,
    dataset_version_id: str,
    artifact_name: str,
) -> Path:
    return build_artifact_dir(tenant_id, domain, dataset_version_id) / f"{artifact_name}.parquet"


def get_dataset_version(
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str | None,
    source_type: str = "machines",
) -> DatasetVersion:
    if dataset_version_id:
        version = (
            db.query(DatasetVersion)
            .filter(
                DatasetVersion.id == dataset_version_id,
                DatasetVersion.tenant_id == tenant_id,
                DatasetVersion.source_type == source_type,
            )
            .first()
        )
        if not version:
            raise ValueError("dataset_version not found for tenant")
        return version

    latest = (
        db.query(DatasetVersion)
        .filter(
            DatasetVersion.tenant_id == tenant_id,
            DatasetVersion.source_type == source_type,
            DatasetVersion.status == "success",
        )
        .order_by(DatasetVersion.created_at.desc())
        .first()
    )
    if not latest:
        raise ValueError("no successful dataset_version found")
    return latest


def resolve_data_dir(version: DatasetVersion) -> Path:
    path = Path(version.data_dir or engine_settings.cg_data_dir)
    if not path.exists():
        raise ValueError(f"data_dir does not exist: {path}")
    return path


def resolve_profile_configs(db: Session, version: DatasetVersion) -> dict[str, CsvTabConfig]:
    if not version.used_profile_ids:
        return {}
    try:
        profile_ids = json.loads(version.used_profile_ids)
    except json.JSONDecodeError as exc:
        raise ValueError("invalid used_profile_ids in dataset_version") from exc

    configs: dict[str, CsvTabConfig] = {}
    for source_name, profile_id in profile_ids.items():
        payload = profiles_store.get_active_payload(db, profile_id)
        if payload:
            configs[source_name] = payload
    return configs


def truncate_error(message: str) -> str:
    if len(message) <= engine_settings.max_error_text_chars:
        return message
    return message[: engine_settings.max_error_text_chars]
