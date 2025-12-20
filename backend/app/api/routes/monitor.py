from __future__ import annotations

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import logging

from app.core.deps import get_current_user
from app.db.db import get_session
from app.api.schemas import MonitorEvent, MonitorSummary
from app.ws.manager import manager

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/monitor/recent", response_model=list[MonitorEvent])
async def recent(
    limit: int = Query(200, ge=1, le=1000),
    decision: str | None = Query(None),
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(get_current_user),
):
    extra = ""
    params = {"lim": limit}
    if decision:
        extra = "AND c.decision = :dec"
        params["dec"] = decision

    q = text(f"""
        SELECT 
          e.event_id::text AS event_id,
          en.entity_key AS entity_key,
          e.occurred_at AS occurred_at,
          e.payload AS payload,
          c.decision::text AS decision,
          c.risk::float8 AS risk,
          c.matched_rules AS matched_rules,
          c.reasons AS reasons,
          c.classified_at AS classified_at
        FROM app.classifications c
        JOIN app.events e ON e.event_pk = c.event_pk
        JOIN app.entities en ON en.entity_id = e.entity_id
        WHERE 1=1 {extra}
        ORDER BY c.classified_at DESC
        LIMIT :lim
    """)
    rows = (await session.execute(q, params)).mappings().all()
    out: list[MonitorEvent] = []
    for r in rows:
        out.append(MonitorEvent(
            event_id=r["event_id"],
            entity_key=r["entity_key"],
            occurred_at=r["occurred_at"],
            payload=dict(r["payload"]),
            decision=r["decision"],
            risk=float(r["risk"]),
            matched_rules=list(r["matched_rules"]),
            reasons=list(r["reasons"]),
            classified_at=r["classified_at"],
        ))
    return out


@router.get("/monitor/summary", response_model=MonitorSummary)
async def summary(
    limit: int = Query(200, ge=1, le=1000),
    entity_key: str | None = Query(None),
    rule_ids: list[int] | None = Query(None),
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(get_current_user),
):
    """Return aggregated counts and a recent list of events filtered by optional entity_key and rule_ids."""
    extra = ""
    params: dict = {"lim": limit}
    if entity_key:
        extra += " AND en.entity_key = :ek"
        params["ek"] = entity_key
    if rule_ids:
        extra += " AND EXISTS (SELECT 1 FROM jsonb_array_elements(c.matched_rules) mr WHERE (mr->>'rule_id')::int = ANY(:rids))"
        params["rids"] = rule_ids

    agg_q = text(f"""
        SELECT
          COUNT(*)::int AS total,
          COUNT(*) FILTER (WHERE c.decision = 'alert')::int AS alert,
          COUNT(*) FILTER (WHERE c.decision = 'legit')::int AS legit
        FROM app.classifications c
        JOIN app.events e ON e.event_pk = c.event_pk
        JOIN app.entities en ON en.entity_id = e.entity_id
        WHERE 1=1 {extra}
    """)
    agg_row = (await session.execute(agg_q, params)).mappings().first() or {"total": 0, "alert": 0, "legit": 0}
    total = int(agg_row.get("total", 0))
    alert = int(agg_row.get("alert", 0))
    legit = int(agg_row.get("legit", 0))
    alert_pct = (alert / total * 100.0) if total > 0 else 0.0
    legit_pct = (legit / total * 100.0) if total > 0 else 0.0

    q = text(f"""
        SELECT 
          e.event_id::text AS event_id,
          en.entity_key AS entity_key,
          e.occurred_at AS occurred_at,
          e.payload AS payload,
          c.decision::text AS decision,
          c.risk::float8 AS risk,
          c.matched_rules AS matched_rules,
          c.reasons AS reasons,
          c.classified_at AS classified_at
        FROM app.classifications c
        JOIN app.events e ON e.event_pk = c.event_pk
        JOIN app.entities en ON en.entity_id = e.entity_id
        WHERE 1=1 {extra}
        ORDER BY c.classified_at DESC
        LIMIT :lim
    """)
    rows = (await session.execute(q, params)).mappings().all()
    out: list[MonitorEvent] = []
    for r in rows:
        out.append(MonitorEvent(
            event_id=r["event_id"],
            entity_key=r["entity_key"],
            occurred_at=r["occurred_at"],
            payload=dict(r["payload"]),
            decision=r["decision"],
            risk=float(r["risk"]),
            matched_rules=list(r["matched_rules"]),
            reasons=list(r["reasons"]),
            classified_at=r["classified_at"],
        ))

    return {
        "total": total,
        "alert": alert,
        "legit": legit,
        "alert_pct": alert_pct,
        "legit_pct": legit_pct,
        "events": out,
    }


@router.delete("/monitor/clear")
async def clear_classifications(session: AsyncSession = Depends(get_session), user: dict = Depends(get_current_user)):
    """Delete all classifications (use with care)."""
    try:
        res = await session.execute(text("DELETE FROM app.classifications RETURNING class_pk"))
        await session.commit()
        rows = res.all() if hasattr(res, 'all') else None
        deleted = len(rows) if rows is not None else -1
        logger.info("User %s cleared classifications (deleted=%s)", user['username'], deleted)
        try:
            await manager.broadcast({"type": "monitor_cleared", "deleted": deleted})
        except Exception:
            logger.exception("Failed to broadcast monitor_cleared message")
        return {"ok": True, "deleted": deleted}
    except Exception as e:
        logger.exception("Failed to clear classifications")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/monitor/{event_id}")
async def delete_classification(event_id: str, session: AsyncSession = Depends(get_session), user: dict = Depends(get_current_user)):
    """Delete classification rows for a specific event id (removes it from Monitor)."""
    try:
        if event_id == 'clear':
            raise HTTPException(status_code=400, detail="Invalid event id")

        res = await session.execute(text(
            """
            DELETE FROM app.classifications c
            USING app.events e
            WHERE e.event_id = :eid AND c.event_pk = e.event_pk
            RETURNING c.class_pk
            """
        ), {"eid": event_id})
        await session.commit()
        rows = res.all() if hasattr(res, 'all') else None
        deleted = len(rows) if rows is not None else -1
        logger.info("User %s deleted classifications for event %s (deleted=%s)", user['username'], event_id, deleted)
        try:
            await manager.broadcast({"type": "monitor_deleted", "event_id": event_id, "deleted": deleted})
        except Exception:
            logger.exception("Failed to broadcast monitor_deleted message")
        return {"ok": True, "deleted": deleted}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to delete classification for %s", event_id)
        raise HTTPException(status_code=500, detail=str(e))
