from __future__ import annotations

from requests import session

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB

from app.core.deps import get_current_user
from app.db.db import get_session
from app.api.schemas import RuleCreate, RuleOut, RuleVersionCreate, RuleVersionOut, ValidateRuleIn, ValidateRuleOut
from app.services.rule_engine import validate_rule

router = APIRouter()


@router.get("/entities/{entity_id}/rules", response_model=list[RuleOut])
async def list_rules(entity_id: int, session: AsyncSession = Depends(get_session), user: dict = Depends(get_current_user)):
    q = text("""
        SELECT r.rule_id, r.entity_id, r.name, r.description, r.is_active, r.priority, r.severity::text AS severity, r.action::text AS action, r.created_at, r.updated_at,
               rav.rule_version_id AS active_version_id
        FROM app.rules r
        LEFT JOIN app.rule_active_version rav ON rav.rule_id = r.rule_id
        WHERE r.entity_id=:id
        ORDER BY r.priority ASC, r.rule_id DESC
    """)
    rows = (await session.execute(q, {"id": entity_id})).mappings().all()
    return [RuleOut(**r) for r in rows]


@router.post("/rules", response_model=RuleOut)
async def create_rule(body: RuleCreate, session: AsyncSession = Depends(get_session), user: dict = Depends(get_current_user)):
    q = text("""
        INSERT INTO app.rules(entity_id, name, description, is_active, priority, severity, action, created_by)
        VALUES (:eid, :n, :d, :a, :p, :s, :ac, :cb)
        RETURNING rule_id, entity_id, name, description, is_active, priority, severity, action, created_at, updated_at
    """)
    row = (await session.execute(q, {
        "eid": body.entity_id,
        "n": body.name,
        "d": body.description,
        "a": body.is_active,
        "p": body.priority,
        "s": body.severity,
        "ac": body.action,
        "cb": user["user_id"],
    })).mappings().first()
    await session.commit()
    return RuleOut(**row)


@router.put("/rules/{rule_id}", response_model=RuleOut)
async def update_rule(rule_id: int, body: RuleCreate, session: AsyncSession = Depends(get_session), user: dict = Depends(get_current_user)):
    q = text("""
        UPDATE app.rules
        SET entity_id=:eid, name=:n, description=:d, is_active=:a, priority=:p, severity=:s, action=:ac, updated_at=now()
        WHERE rule_id=:id
        RETURNING rule_id, entity_id, name, description, is_active, priority, severity, action, created_at, updated_at
    """)
    row = (await session.execute(q, {
        "id": rule_id,
        "eid": body.entity_id,
        "n": body.name,
        "d": body.description,
        "a": body.is_active,
        "p": body.priority,
        "s": body.severity,
        "ac": body.action,
    })).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Rule not found")
    await session.commit()
    return RuleOut(**row)


@router.delete("/rules/{rule_id}")
async def delete_rule(rule_id: int, session: AsyncSession = Depends(get_session), user: dict = Depends(get_current_user)):
    await session.execute(text("DELETE FROM app.rules WHERE rule_id=:id"), {"id": rule_id})
    await session.commit()
    return {"ok": True}


@router.get("/rules/{rule_id}/versions", response_model=list[RuleVersionOut])
async def list_versions(rule_id: int, session: AsyncSession = Depends(get_session), user: dict = Depends(get_current_user)):
    q = text("""
        SELECT rv.rule_version_id, rv.rule_id, rv.version_no, rv.expr_type, rv.expr, rv.created_at,
               COALESCE(rav.rule_version_id = rv.rule_version_id, false) AS is_active
        FROM app.rule_versions rv
        LEFT JOIN app.rule_active_version rav ON rav.rule_id = rv.rule_id
        WHERE rv.rule_id=:id
        ORDER BY rv.version_no DESC
    """)
    rows = (await session.execute(q, {"id": rule_id})).mappings().all()
    return [RuleVersionOut(**{**r, "expr": r["expr"], "is_active": bool(r["is_active"])}) for r in rows]

@router.post("/rules/{rule_id}/versions", response_model=RuleVersionOut)
async def create_version(
    rule_id: int,
    body: RuleVersionCreate,
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(get_current_user),
):
    vn_q = text("""
        SELECT COALESCE(MAX(version_no), 0) + 1 AS next_vn
        FROM app.rule_versions
        WHERE rule_id = :rid
    """)
    vn = (
        await session.execute(vn_q, {"rid": rule_id})
    ).scalar_one()

    q = text("""
        INSERT INTO app.rule_versions(
            rule_id, version_no, expr_type, expr, created_by
        )
        VALUES (:rid, :vn, :t, :e, :cb)
        RETURNING rule_version_id, rule_id, version_no, expr_type, expr, created_at
    """).bindparams(
        bindparam("e", type_=JSONB)
    )

    row = (
        await session.execute(
            q,
            {
                "rid": rule_id,
                "vn": vn,
                "t": body.expr_type,
                "e": body.expr,
                "cb": user["user_id"],
            },
        )
    ).mappings().first()

    await session.commit()
    if not row:
        raise HTTPException(status_code=500, detail="Failed to create rule version")
    return RuleVersionOut(**{**row, "expr": row["expr"], "is_active": False})


@router.post("/rules/{rule_id}/activate/{rule_version_id}")
async def activate_version(rule_id: int, rule_version_id: int, session: AsyncSession = Depends(get_session), user: dict = Depends(get_current_user)):
    await session.execute(text("""
        INSERT INTO app.rule_active_version(rule_id, rule_version_id)
        VALUES (:rid, :rvid)
        ON CONFLICT (rule_id) DO UPDATE SET rule_version_id=EXCLUDED.rule_version_id
    """), {"rid": rule_id, "rvid": rule_version_id})
    await session.commit()
    return {"ok": True}


@router.post("/rules/validate", response_model=ValidateRuleOut)
async def validate_rule_endpoint(body: ValidateRuleIn, user: dict = Depends(get_current_user)):
    try:
        validate_rule(body.expr_type, body.expr, body.sample_payload)
        return ValidateRuleOut(ok=True)
    except Exception as e:
        return ValidateRuleOut(ok=False, errors=[str(e)])
