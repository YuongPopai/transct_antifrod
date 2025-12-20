from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from asyncpg.exceptions import UniqueViolationError
from app.core.deps import get_current_user
from app.db.db import get_session
from app.api.schemas import EntityCreate, EntityOut, AttributeCreate, AttributeOut

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/entities", response_model=list[EntityOut])
async def list_entities(
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(get_current_user),
):
    q = text("""
        SELECT entity_id, entity_key, name, description, is_active, created_at, updated_at
        FROM app.entities
        ORDER BY entity_id DESC
    """)
    rows = (await session.execute(q)).mappings().all()
    return [EntityOut(**r) for r in rows]


@router.post("/entities", response_model=EntityOut)
async def create_entity(
    body: EntityCreate,
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(get_current_user),
):
    q = text("""
        INSERT INTO app.entities(entity_key, name, description, is_active, created_by)
        VALUES (:k, :n, :d, :a, :cb)
        RETURNING entity_id, entity_key, name, description, is_active, created_at, updated_at
    """)
    try:
        row = (await session.execute(q, {"k": body.entity_key, "n": body.name, "d": body.description, "a": body.is_active, "cb": user["user_id"]})).mappings().first()
        await session.commit()
    except IntegrityError as e:
        await session.rollback()
        if isinstance(getattr(e, 'orig', None), UniqueViolationError) or 'unique' in str(e).lower():
            raise HTTPException(status_code=409, detail="Entity with this key already exists")
        raise HTTPException(status_code=400, detail="Failed to create entity")
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    if not row:
        raise HTTPException(status_code=500, detail="Failed to create entity")
    return EntityOut(**row)


@router.put("/entities/{entity_id}", response_model=EntityOut)
async def update_entity(
    entity_id: int,
    body: EntityCreate,
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(get_current_user),
):
    q = text("""
        UPDATE app.entities
        SET entity_key=:k, name=:n, description=:d, is_active=:a, updated_at=now()
        WHERE entity_id=:id
        RETURNING entity_id, entity_key, name, description, is_active, created_at, updated_at
    """)
    row = (await session.execute(q, {"id": entity_id, "k": body.entity_key, "n": body.name, "d": body.description, "a": body.is_active})).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Entity not found")
    await session.commit()
    return EntityOut(**row)


@router.get("/entities/{entity_id}/dependencies")
async def entity_dependencies(
    entity_id: int,
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(get_current_user),
):
    q = text("SELECT count(*) FROM app.entity_attributes WHERE entity_id=:id")
    attrs = int(await session.scalar(text("SELECT count(*) FROM app.entity_attributes WHERE entity_id=:id"), {"id": entity_id}) or 0)
    rules = int(await session.scalar(text("SELECT count(*) FROM app.rules WHERE entity_id=:id"), {"id": entity_id}) or 0)
    events = int(await session.scalar(text("SELECT count(*) FROM app.events WHERE entity_id=:id"), {"id": entity_id}) or 0)
    classifications = int(await session.scalar(text("SELECT count(c.*) FROM app.classifications c JOIN app.events e ON c.event_pk = e.event_pk WHERE e.entity_id = :id"), {"id": entity_id}) or 0)
    return {"attributes": attrs, "rules": rules, "events": events, "classifications": classifications}


@router.delete("/entities/{entity_id}")
async def delete_entity(
    entity_id: int,
    force: bool = False,
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(get_current_user),
):
    attrs = int(await session.scalar(text("SELECT count(*) FROM app.entity_attributes WHERE entity_id=:id"), {"id": entity_id}) or 0)
    rules = int(await session.scalar(text("SELECT count(*) FROM app.rules WHERE entity_id=:id"), {"id": entity_id}) or 0)
    events = int(await session.scalar(text("SELECT count(*) FROM app.events WHERE entity_id=:id"), {"id": entity_id}) or 0)
    classifications = int(await session.scalar(text("SELECT count(c.*) FROM app.classifications c JOIN app.events e ON c.event_pk = e.event_pk WHERE e.entity_id = :id"), {"id": entity_id}) or 0)

    if not force and (attrs > 0 or rules > 0 or events > 0 or classifications > 0):
        detail = {"message": "Entity has dependent objects", "attributes": attrs, "rules": rules, "events": events, "classifications": classifications}
        raise HTTPException(status_code=409, detail=detail)

    if force and user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="force delete requires admin role")

    try:
        if not force:
            raise HTTPException(status_code=400, detail="force param required to delete entity with dependencies")

        deleted_classifications = 0
        deleted_events = 0
        deleted_entities = 0

        if classifications > 0:
            logger.info("Deleting %s classifications for entity %s", classifications, entity_id)
            res = await session.execute(text("DELETE FROM app.classifications WHERE event_pk IN (SELECT event_pk FROM app.events WHERE entity_id=:id) RETURNING 1"), {"id": entity_id})
            deleted_classifications = res.rowcount or 0

        if events > 0:
            logger.info("Deleting %s events for entity %s", events, entity_id)
            res = await session.execute(text("DELETE FROM app.events WHERE entity_id=:id RETURNING 1"), {"id": entity_id})
            deleted_events = res.rowcount or 0

        logger.info("Deleting entity %s (rules & attributes cascade)", entity_id)
        res = await session.execute(text("DELETE FROM app.entities WHERE entity_id=:id RETURNING entity_id"), {"id": entity_id})
        deleted_entities = res.rowcount or 0

        await session.commit()

        remaining_attrs = int(await session.scalar(text("SELECT count(*) FROM app.entity_attributes WHERE entity_id=:id"), {"id": entity_id}) or 0)
        remaining_rules = int(await session.scalar(text("SELECT count(*) FROM app.rules WHERE entity_id=:id"), {"id": entity_id}) or 0)
        remaining_events = int(await session.scalar(text("SELECT count(*) FROM app.events WHERE entity_id=:id"), {"id": entity_id}) or 0)
        remaining_classifications = int(await session.scalar(text("SELECT count(c.*) FROM app.classifications c JOIN app.events e ON c.event_pk = e.event_pk WHERE e.entity_id = :id"), {"id": entity_id}) or 0)

        logger.info("Post-delete remaining for entity %s: attrs=%s rules=%s events=%s classifications=%s", entity_id, remaining_attrs, remaining_rules, remaining_events, remaining_classifications)

        if deleted_entities == 0:
            raise HTTPException(status_code=404, detail="Entity not found or already deleted")

        return {
            "ok": True,
            "deleted": {"entities": deleted_entities, "events": deleted_events, "classifications": deleted_classifications},
            "remaining": {"attributes": remaining_attrs, "rules": remaining_rules, "events": remaining_events, "classifications": remaining_classifications},
        }
    except HTTPException:
        raise
    except Exception as e:
        await session.rollback()
        logger.exception("Error during force delete of entity %s", entity_id)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/entities/{entity_id}/attributes", response_model=list[AttributeOut])
async def list_attributes(
    entity_id: int,
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(get_current_user),
):
    q = text("""
      SELECT attr_id, entity_id, attr_key, name, type, is_required, default_value, description
      FROM app.entity_attributes
      WHERE entity_id=:id
      ORDER BY attr_id DESC
    """)
    rows = (await session.execute(q, {"id": entity_id})).mappings().all()
    return [AttributeOut(**r) for r in rows]


@router.post("/entities/{entity_id}/attributes", response_model=AttributeOut)
async def create_attribute(
    entity_id: int,
    body: AttributeCreate,
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(get_current_user),
):
    q = text("""
        INSERT INTO app.entity_attributes(
            entity_id, attr_key, name, type, is_required, default_value, description
        )
        VALUES (
            :entity_id, :attr_key, :name, :type, :is_required, :default_value, :description
        )
        RETURNING attr_id, entity_id, attr_key, name, type, is_required, default_value, description
    """).bindparams(
        bindparam("default_value", type_=JSONB)
    )

    try:
        row = (
            await session.execute(
                q,
                {
                    "entity_id": entity_id,
                    "attr_key": body.attr_key,
                    "name": body.name,
                    "type": body.type,
                    "is_required": body.is_required,
                    "default_value": body.default_value,
                    "description": body.description,
                },
            )
        ).mappings().first()
        await session.commit()
    except IntegrityError as e:
        await session.rollback()
        if isinstance(getattr(e, 'orig', None), UniqueViolationError) or 'unique' in str(e).lower():
            raise HTTPException(status_code=409, detail="Attribute with this key already exists for this entity")
        raise HTTPException(status_code=400, detail="Failed to create attribute")
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=400, detail=str(e))

    if not row:
        raise HTTPException(status_code=500, detail="Failed to create attribute")

    return AttributeOut(**row)

@router.put("/attributes/{attr_id}", response_model=AttributeOut)
async def update_attribute(
    attr_id: int,
    body: AttributeCreate,
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(get_current_user),
):
    q = text("""
      UPDATE app.entity_attributes
      SET attr_key=:k, name=:n, type=:t, is_required=:r, default_value=:dv::jsonb, description=:d
      WHERE attr_id=:id
      RETURNING attr_id, entity_id, attr_key, name, type, is_required, default_value, description
    """)
    row = (await session.execute(q, {
        "id": attr_id,
        "k": body.attr_key,
        "n": body.name,
        "t": body.type,
        "r": body.is_required,
        "dv": body.default_value if body.default_value is not None else None,
        "d": body.description,
    })).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Attribute not found")
    await session.commit()
    return AttributeOut(**row)


@router.delete("/attributes/{attr_id}")
async def delete_attribute(
    attr_id: int,
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(get_current_user),
):
    await session.execute(text("DELETE FROM app.entity_attributes WHERE attr_id=:id"), {"id": attr_id})
    await session.commit()
    return {"ok": True}
