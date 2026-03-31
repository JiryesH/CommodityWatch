from __future__ import annotations

from datetime import timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy import func, select

from app.api.deps import SessionDep, get_optional_current_user
from app.core.config import get_settings
from app.core.security import generate_token, hash_password, hash_token, normalize_email, session_expiry, utcnow, verify_password
from app.db.models.auth import User, UserSession
from app.schemas.auth import AuthResponse, AuthUser, LoginRequest, RegisterRequest


router = APIRouter(prefix="/api/auth", tags=["auth"])


def set_auth_cookies(response: Response, session_token: str, csrf_token: str) -> None:
    settings = get_settings()
    secure = settings.is_production
    response.set_cookie(
        settings.session_cookie_name,
        session_token,
        max_age=settings.session_max_age_seconds,
        httponly=True,
        secure=secure,
        samesite="lax",
        domain=settings.session_cookie_domain or None,
        path="/",
    )
    response.set_cookie(
        settings.session_csrf_cookie_name,
        csrf_token,
        max_age=settings.session_max_age_seconds,
        httponly=False,
        secure=secure,
        samesite="lax",
        domain=settings.session_cookie_domain or None,
        path="/",
    )


async def create_session(session: SessionDep, user: User, request: Request) -> tuple[str, str]:
    settings = get_settings()
    session_token = generate_token()
    csrf_token = generate_token()
    session.add(
        UserSession(
            user_id=user.id,
            session_token_hash=hash_token(session_token),
            csrf_token_hash=hash_token(csrf_token),
            ip_address=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            expires_at=session_expiry(settings.session_max_age_seconds),
        )
    )
    user.last_login_at = utcnow()
    await session.flush()
    return session_token, csrf_token


@router.post("/register", response_model=AuthResponse, status_code=status.HTTP_201_CREATED)
async def register(payload: RegisterRequest, request: Request, response: Response, session: SessionDep) -> AuthResponse:
    email = normalize_email(payload.email)
    existing = await session.scalar(select(User).where(func.lower(User.email) == email))
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User already exists.")

    user = User(
        email=email,
        password_hash=hash_password(payload.password),
        full_name=payload.full_name,
        timezone=str(timezone.utc),
    )
    session.add(user)
    await session.flush()
    session_token, csrf_token = await create_session(session, user, request)
    await session.commit()
    set_auth_cookies(response, session_token, csrf_token)
    return AuthResponse(user=AuthUser.model_validate(user), csrf_token=csrf_token)


@router.post("/login", response_model=AuthResponse)
async def login(payload: LoginRequest, request: Request, response: Response, session: SessionDep) -> AuthResponse:
    email = normalize_email(payload.email)
    user = await session.scalar(select(User).where(func.lower(User.email) == email))
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password.")

    current_token = request.cookies.get(get_settings().session_cookie_name)
    if current_token:
        existing_session = await session.scalar(
            select(UserSession).where(UserSession.session_token_hash == hash_token(current_token), UserSession.revoked_at.is_(None))
        )
        if existing_session:
            existing_session.revoked_at = utcnow()

    session_token, csrf_token = await create_session(session, user, request)
    await session.commit()
    set_auth_cookies(response, session_token, csrf_token)
    return AuthResponse(user=AuthUser.model_validate(user), csrf_token=csrf_token)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(request: Request, response: Response, session: SessionDep) -> Response:
    settings = get_settings()
    token = request.cookies.get(settings.session_cookie_name)
    if token:
        user_session = await session.scalar(
            select(UserSession).where(UserSession.session_token_hash == hash_token(token), UserSession.revoked_at.is_(None))
        )
        if user_session:
            user_session.revoked_at = utcnow()
            await session.commit()
    response.delete_cookie(settings.session_cookie_name, path="/", domain=settings.session_cookie_domain or None)
    response.delete_cookie(settings.session_csrf_cookie_name, path="/", domain=settings.session_cookie_domain or None)
    return response


@router.get("/me", response_model=AuthUser)
async def me(user: User = Depends(get_optional_current_user)) -> AuthUser:
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")
    return AuthUser.model_validate(user)
