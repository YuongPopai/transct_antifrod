from __future__ import annotations

import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.settings import settings
from app.core.security import hash_password


async def ensure_admin(session: AsyncSession) -> None:
    row = (await session.execute(text("SELECT user_id FROM app.users WHERE username=:u"), {"u": settings.admin_username})).mappings().first()
    if row:
        return
    await session.execute(
        text("INSERT INTO app.users(username, password_hash, role) VALUES (:u, :ph, 'admin')"),
        {"u": settings.admin_username, "ph": hash_password(settings.admin_password)},
    )
    await session.commit()


async def retention_loop(stop_event: asyncio.Event, session_factory) -> None:
    """Delete events older than retention_days every 6 hours."""
    while not stop_event.is_set():
        try:
            async with session_factory() as session:
                await session.execute(
                    text("DELETE FROM app.events WHERE occurred_at < now() - (:d || ' days')::interval"),
                    {"d": settings.retention_days},
                )
                await session.commit()
        except Exception:
            pass
        await asyncio.sleep(6 * 3600)
