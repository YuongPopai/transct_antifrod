from __future__ import annotations

import asyncpg

from app.settings import DATABASE_URL


async def connect() -> asyncpg.Connection:
    url = DATABASE_URL
    if url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return await asyncpg.connect(url)
