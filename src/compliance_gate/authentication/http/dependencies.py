from __future__ import annotations

from typing import Callable

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from compliance_gate.authentication.config import auth_settings
from compliance_gate.authentication.models import Role, User
from compliance_gate.authentication.security.jwt import decode_access_token
from compliance_gate.authentication.storage import repo
from compliance_gate.infra.db.session import get_db


def get_current_user(
    request: Request,
    db: Session = Depends(get_db),
) -> User:
    token = request.cookies.get(auth_settings.auth_cookie_name)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing authentication cookie")

    try:
        payload = decode_access_token(token)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token") from exc

    user = repo.get_user_by_id(db, payload.sub)
    if not user or user.tenant_id != payload.tenant_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token subject")

    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="user is inactive")

    return user


def require_role(*roles: Role) -> Callable[[User], User]:
    allowed = {r.value if isinstance(r, Role) else str(r) for r in roles}

    def dependency(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role not in allowed:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="insufficient permissions")
        return current_user

    return dependency
