from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field

from app.schemas.common import APIModel


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    full_name: str | None = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class AuthUser(APIModel):
    id: UUID
    email: EmailStr
    full_name: str | None
    plan_code: str
    account_status: str
    timezone: str
    created_at: datetime


class AuthResponse(APIModel):
    user: AuthUser
    csrf_token: str

