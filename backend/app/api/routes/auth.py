from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.db import get_session
from app.core.security import verify_password, create_access_token
from app.core.deps import get_current_user
from app.api.schemas import TokenOut, MeOut

router = APIRouter()


@router.post("/login", response_model=TokenOut)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: AsyncSession = Depends(get_session),
):
    q = text("SELECT username, password_hash, role, is_active FROM app.users WHERE username = :u")
    row = (await session.execute(q, {"u": form_data.username})).mappings().first()
    if not row or not row["is_active"]:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Bad credentials")
    if not verify_password(form_data.password, row["password_hash"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Bad credentials")

    token = create_access_token(sub=str(row["username"]), role=str(row["role"]))
    await session.execute(
        text("UPDATE app.users SET last_login_at = now() WHERE username = :u"), {"u": form_data.username}
    )
    await session.commit()
    return TokenOut(access_token=token)


@router.get("/me", response_model=MeOut)
async def me(user: dict = Depends(get_current_user)):
    return MeOut(**user)
