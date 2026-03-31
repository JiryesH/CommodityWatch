from __future__ import annotations

from typing import Annotated

from fastapi import Cookie, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.core.security import hash_token, utcnow
from app.db.models.auth import User, UserSession
from app.db.session import get_db_session


SettingsDep = Annotated[Settings, Depends(get_settings)]
SessionDep = Annotated[AsyncSession, Depends(get_db_session)]


async def get_optional_current_user(
    request: Request,
    session: SessionDep,
    settings: SettingsDep,
    session_token: Annotated[str | None, Cookie(alias="cw_session")] = None,
) -> User | None:
    cookie_name = settings.session_cookie_name
    token = session_token or request.cookies.get(cookie_name)
    if not token:
        return None
    token_hash = hash_token(token)
    stmt = (
        select(User)
        .join(UserSession, UserSession.user_id == User.id)
        .where(
            UserSession.session_token_hash == token_hash,
            UserSession.revoked_at.is_(None),
            UserSession.expires_at > utcnow(),
        )
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def require_current_user(
    user: Annotated[User | None, Depends(get_optional_current_user)],
) -> User:
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")
    return user
