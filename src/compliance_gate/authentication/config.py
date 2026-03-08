from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator


class AuthSettings(BaseSettings):
    # JWT
    auth_jwt_secret: str = "change-me-in-production"
    auth_jwt_algorithm: str = "HS256"
    auth_token_ttl_minutes: int = 45
    auth_jwt_issuer: str = "compliance-gate"
    auth_jwt_audience: str = "compliance-gate-api"

    # Cookie auth
    auth_cookie_name: str = "cg_access"
    auth_cookie_secure: bool = False
    auth_cookie_samesite: Literal["lax", "strict", "none"] = "lax"
    auth_cookie_path: str = "/"

    # CSRF (double submit cookie)
    csrf_cookie_name: str = "cg_csrf"
    csrf_header_name: str = "X-CSRF-Token"
    csrf_enabled: bool = True

    # MFA/TOTP
    auth_mfa_issuer: str = "Compliance Gate"
    auth_mfa_setup_ttl_seconds: int = 600
    auth_login_challenge_ttl_seconds: int = 300

    # Recovery codes / secret protection
    auth_recovery_codes_count: int = 8
    auth_recovery_pepper: str = "change-recovery-pepper"
    auth_secret_protection_key: str = "change-secret-protection-key"

    # Rate-limit / anti brute-force
    auth_rate_limit_window_seconds: int = 600
    auth_rate_limit_max_attempts: int = 5
    auth_lock_seconds: int = 600

    # Bootstrap admin (optional)
    auth_bootstrap_admin_username: str | None = None
    auth_bootstrap_admin_password: str | None = None
    auth_bootstrap_tenant_id: str | None = None
    auth_bootstrap_tenant_slug: str = "default"
    auth_bootstrap_tenant_name: str = "Default Tenant"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("auth_cookie_samesite", mode="before")
    @classmethod
    def _normalize_samesite(cls, value: str) -> str:
        return value.lower()


auth_settings = AuthSettings()
