from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Compliance Gate"
    app_version: str = "0.1.0"
    environment: str = "development"
    log_level: str = "info"

    # DB Config
    database_url: str = "postgresql+psycopg2://postgres:postgres@localhost:5432/compliance_gate"

    # Redis Config
    redis_url: str = "redis://localhost:6379/0"

    # Data Platform
    cg_data_dir: str = "/workspace"
    cg_upload_dir: str = "/workspace/uploads"
    cg_upload_max_file_mb: int = 50
    default_tenant_id: str = "default"
    cors_allow_origins: list[str] = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://0.0.0.0:5173",
        "http://localhost:3000",
    ]
    cors_allow_origin_regex: str = r"^https?://(localhost|127\.0\.0\.1|0\.0\.0\.0)(:\d+)?$"

    @field_validator("cors_allow_origins", mode="before")
    @classmethod
    def _parse_cors_allow_origins(cls, value):
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()
