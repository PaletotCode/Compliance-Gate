from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from compliance_gate.authentication.models import Role


class UserPublic(BaseModel):
    id: str
    tenant_id: str
    username: str
    role: Role
    is_active: bool
    mfa_enabled: bool
    require_password_change: bool


class CreateUserRequest(BaseModel):
    username: str = Field(min_length=3, max_length=128)
    role: Role = Role.DIRECTOR
    password: str = Field(min_length=8, max_length=128)
    tenant_id: str | None = None


class AdminResetPasswordRequest(BaseModel):
    password: str = Field(min_length=8, max_length=128)
    require_password_change: bool = True


class LoginRequest(BaseModel):
    username: str = Field(min_length=3, max_length=128)
    password: str = Field(min_length=1, max_length=128)
    totp_code: str | None = Field(default=None, min_length=6, max_length=12)
    challenge_id: str | None = None


class LoginChallengeResponse(BaseModel):
    mfa_required: Literal[True] = True
    challenge_id: str


class LoginSuccessResponse(BaseModel):
    access_token: str
    token_type: Literal["bearer"] = "bearer"
    expires_in: int
    user: UserPublic


class MfaSetupResponse(BaseModel):
    otpauth_url: str
    qr_code_base64_png: str
    instructions: str


class MfaConfirmRequest(BaseModel):
    totp_code: str = Field(min_length=6, max_length=12)


class MfaConfirmResponse(BaseModel):
    recovery_codes: list[str]


class PasswordResetRequest(BaseModel):
    username: str = Field(min_length=3, max_length=128)
    new_password: str = Field(min_length=8, max_length=128)
    totp_code: str | None = Field(default=None, min_length=6, max_length=12)
    recovery_code: str | None = Field(default=None, min_length=6, max_length=64)

    @model_validator(mode="after")
    def validate_reset_factor(self) -> "PasswordResetRequest":
        if not self.totp_code and not self.recovery_code:
            raise ValueError("totp_code or recovery_code is required")
        return self


class GenericMessageResponse(BaseModel):
    status: Literal["ok"] = "ok"
    message: str
