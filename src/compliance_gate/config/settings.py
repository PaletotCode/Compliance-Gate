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

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()
