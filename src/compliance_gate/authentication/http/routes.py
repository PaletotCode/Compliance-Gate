from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from compliance_gate.authentication.http.dependencies import get_current_user, require_role
from compliance_gate.authentication.models import Role, User
from compliance_gate.authentication.schemas import (
    AdminResetPasswordRequest,
    CreateUserRequest,
    GenericMessageResponse,
    LoginChallengeResponse,
    LoginRequest,
    LoginSuccessResponse,
    MfaConfirmRequest,
    MfaConfirmResponse,
    MfaSetupResponse,
    PasswordResetRequest,
    UserPublic,
)
from compliance_gate.authentication.services.auth_service import AuthService, AuthServiceError
from compliance_gate.authentication.services.mfa_service import MFAService, MFAServiceError
from compliance_gate.authentication.services.reset_service import ResetService, ResetServiceError
from compliance_gate.authentication.services.users_service import UserServiceError, UsersService
from compliance_gate.authentication.storage import repo
from compliance_gate.infra.db.session import get_db

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/users", response_model=UserPublic, status_code=status.HTTP_201_CREATED)
def create_user(
    body: CreateUserRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
) -> UserPublic:
    target_tenant_id = body.tenant_id or current_user.tenant_id
    if target_tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=403, detail="cross-tenant user creation is not allowed")

    try:
        user = UsersService.create_user(
            db,
            actor=current_user,
            username=body.username,
            role=body.role,
            password=body.password,
            tenant_id=target_tenant_id,
        )
        return UsersService.to_public(user)
    except UserServiceError as exc:
        raise HTTPException(status_code=400, detail=exc.message) from exc


@router.post("/users/{user_id}/reset-password", response_model=GenericMessageResponse)
def admin_reset_password(
    user_id: str,
    body: AdminResetPasswordRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
) -> GenericMessageResponse:
    target_user = repo.get_user_by_id(db, user_id)
    if not target_user or target_user.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=404, detail="user not found")

    try:
        UsersService.admin_reset_password(
            db,
            actor=current_user,
            target_user=target_user,
            new_password=body.password,
            require_password_change=body.require_password_change,
        )
    except UserServiceError as exc:
        raise HTTPException(status_code=400, detail=exc.message) from exc

    return GenericMessageResponse(message="password reset completed")


@router.post("/login", response_model=LoginSuccessResponse | LoginChallengeResponse)
def login(
    body: LoginRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    # Optional first-run bootstrap via env.
    UsersService.ensure_bootstrap_admin(db)

    try:
        response = AuthService.authenticate(
            db,
            username=body.username,
            password=body.password,
            totp_code=body.totp_code,
            challenge_id=body.challenge_id,
            ip_address=request.client.host if request.client else None,
        )
    except AuthServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from exc

    return response


@router.get("/me", response_model=UserPublic)
def me(current_user: User = Depends(get_current_user)) -> UserPublic:
    return UsersService.to_public(current_user)


@router.post("/mfa/setup", response_model=MfaSetupResponse)
def mfa_setup(current_user: User = Depends(get_current_user)) -> MfaSetupResponse:
    return MFAService.setup_mfa(current_user)


@router.post("/mfa/confirm", response_model=MfaConfirmResponse)
def mfa_confirm(
    body: MfaConfirmRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> MfaConfirmResponse:
    try:
        return MFAService.confirm_mfa(db, user=current_user, totp_code=body.totp_code)
    except MFAServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from exc


@router.post("/password/reset", response_model=GenericMessageResponse)
def password_reset(body: PasswordResetRequest, db: Session = Depends(get_db)) -> GenericMessageResponse:
    users = repo.get_users_by_username(db, body.username)
    if len(users) != 1:
        raise HTTPException(status_code=401, detail="invalid credentials")

    user = users[0]
    try:
        ResetService.reset_password(
            db,
            user=user,
            new_password=body.new_password,
            totp_code=body.totp_code,
            recovery_code=body.recovery_code,
        )
    except ResetServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from exc

    return GenericMessageResponse(message="password updated")


@router.post("/logout", response_model=GenericMessageResponse)
def logout(_: User = Depends(get_current_user)) -> GenericMessageResponse:
    # Stateless JWT logout in v1 (no token revocation list).
    return GenericMessageResponse(message="logout acknowledged")
