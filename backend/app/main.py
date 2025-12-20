from __future__ import annotations

import asyncio

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.db.db import SessionLocal, healthcheck
from app.services.bootstrap import ensure_admin, retention_loop
from app.services.kafka_bridge import run_kafka_to_ws
from app.ws.manager import manager
from app.core.settings import settings

app = FastAPI(title="Antifraud Platform", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

_stop = asyncio.Event()
_tasks: list[asyncio.Task] = []


@app.on_event("startup")
async def on_startup() -> None:
    await healthcheck()

    if settings.app_env != 'local':
        if not settings.jwt_secret:
            raise RuntimeError("JWT secret must be set in non-local environments")
        if not settings.admin_password or settings.admin_password == 'admin':
            raise RuntimeError("ADMIN_PASSWORD must be set to a non-default value in non-local environments")

    async with SessionLocal() as session:
        await ensure_admin(session)

    _tasks.append(asyncio.create_task(retention_loop(_stop, SessionLocal)))

    async def _bridge_runner():
        while not _stop.is_set():
            try:
                await run_kafka_to_ws(_stop)
            except Exception:
                await asyncio.sleep(3)

    _tasks.append(asyncio.create_task(_bridge_runner()))


@app.on_event("shutdown")
async def on_shutdown() -> None:
    _stop.set()
    for t in _tasks:
        t.cancel()


@app.websocket("/ws/monitor")
async def ws_monitor(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(ws)
    except Exception:
        await manager.disconnect(ws)
