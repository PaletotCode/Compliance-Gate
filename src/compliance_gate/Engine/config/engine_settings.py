from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class EngineSettings(BaseSettings):
    """Engine Core runtime settings."""

    # Paths
    artifacts_base_dir: str = "/workspace/artifacts"
    cg_data_dir: str = "/workspace"
    default_tenant_id: str = "default"

    # Guardrails
    max_report_rows: int = 10_000
    report_timeout_seconds: int = 10
    max_error_text_chars: int = 500
    max_preview_rows: int = 200
    max_view_page_size: int = 500
    expression_max_nodes: int = 256
    expression_max_depth: int = 16
    declarative_timeout_seconds: int = 10

    # Materialization
    materialize_lock_namespace: str = "machines_materialize"
    classification_mode_default: str = "legacy"
    classification_default_ruleset_name: str = "machines-classification"
    classification_default_primary_status: str = "COMPLIANT"
    classification_default_primary_status_label: str = "✅ SEGURO (OK)"
    classification_timeout_seconds: int = 10
    classification_max_rules: int = 1_000
    classification_max_rows: int = 1_000_000
    classification_memory_budget_mb: int = 256
    classification_max_divergences_per_run: int = 5_000
    classification_stale_days: int = 45
    classification_legacy_os_definitions: str = (
        "Windows 7,Windows 8,Windows XP,Windows Server 2008,Windows Server 2012"
    )
    classification_migration_default_phase: str = "A"
    classification_migration_parity_threshold_percent: float = 99.9

    model_config = SettingsConfigDict(
        env_prefix="ENGINE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


engine_settings = EngineSettings()
