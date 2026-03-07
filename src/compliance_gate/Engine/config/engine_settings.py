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

    # Materialization
    materialize_lock_namespace: str = "machines_materialize"

    model_config = SettingsConfigDict(
        env_prefix="ENGINE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


engine_settings = EngineSettings()
