from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from asyncpg.exceptions import UniqueViolationError

from app.core.deps import require_role
from app.db.db import get_session
from app.core.security import hash_password
from app.api.schemas import UserCreate, UserOut, UserPatch

router = APIRouter()

from aiokafka import AIOKafkaProducer
import uuid
from datetime import datetime, timezone
import random
import orjson
import asyncio

from app.core.settings import settings
from app.services.rule_engine import CompiledRule, eval_rule
from app.db.db import SessionLocal

_simulator_task: asyncio.Task | None = None
_simulator_stop: asyncio.Event | None = None
_simulator_last_result: dict | None = None


def _rand_value_for_type(t: str):
    if t == 'number':
        return random.choice([random.randint(0, 10), random.randint(0, 100)])
    if t == 'boolean':
        return random.choice([True, False])
    if t == 'datetime':
        return datetime.utcnow().isoformat() + 'Z'
    if t == 'object':
        return {"dummy": "x"}
    if t == 'array':
        return ["x", 1]
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=6))


def _generate_payload(attr_defs: dict[str, str]):
    payload = {}
    for k, t in attr_defs.items():
        payload[k] = _rand_value_for_type(t)
    if not payload:
        payload["value"] = random.randint(0, 100)
    return payload

async def _simulate_once_using_session(session: AsyncSession, entity_id: int | None = None, target_rule_id: int | None = None):
    """Run a single simulation pass using provided DB session. Returns summary dict.

    Optional params allow running the simulation only for a specific entity and/or rule.
    """
    if entity_id is not None:
        ents = (await session.execute(text("SELECT entity_id, entity_key FROM app.entities WHERE is_active=true AND entity_id=:eid"), {"eid": entity_id})).mappings().all()
    else:
        ents = (await session.execute(text("SELECT entity_id, entity_key FROM app.entities WHERE is_active=true"))).mappings().all()
    if not ents:
        return {"ok": True, "sent": 0, "summary": [], "debug": []}

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap)
    await producer.start()
    sent = 0
    summary = []
    debug = []
    try:
        for ent in ents:
            entity_id = ent["entity_id"]
            entity_key = ent["entity_key"]
            attrs = (await session.execute(text("SELECT attr_key, type FROM app.entity_attributes WHERE entity_id=:id"), {"id": entity_id})).mappings().all()
            attr_defs = {r["attr_key"]: r["type"] for r in attrs}
            if target_rule_id is None:
                rules = (await session.execute(text("SELECT rule_id, name FROM app.rules WHERE entity_id=:id AND is_active=true"), {"id": entity_id})).mappings().all()
            else:
                rules = (await session.execute(text("SELECT rule_id, name FROM app.rules WHERE entity_id=:id AND is_active=true AND rule_id = :rid"), {"id": entity_id, "rid": target_rule_id})).mappings().all()
            compiled_list: list[CompiledRule] = []
            for r in rules:
                rule_id = r["rule_id"]
                rule_name = r["name"]
                rv = (await session.execute(text("SELECT rv.rule_version_id, rv.expr_type, rv.expr FROM app.rule_versions rv JOIN app.rule_active_version rav ON rav.rule_version_id=rv.rule_version_id WHERE rv.rule_id=:rid"), {"rid": rule_id})).mappings().first()
                if not rv:
                    rv = (await session.execute(text("SELECT rule_version_id, expr_type, expr FROM app.rule_versions WHERE rule_id=:rid ORDER BY version_no DESC LIMIT 1"), {"rid": rule_id})).mappings().first()
                if not rv:
                    continue
                expr_val = rv['expr']
                if isinstance(expr_val, (bytes, bytearray)):
                    try:
                        expr_val = orjson.loads(expr_val)
                    except Exception:
                        pass
                if isinstance(expr_val, str):
                    try:
                        expr_val = orjson.loads(expr_val)
                    except Exception:
                        pass

                compiled = CompiledRule(rule_id=rule_id, name=rule_name, severity='warn', action='alert', priority=100, expr_type=rv['expr_type'], expr=expr_val)
                compiled_list.append(compiled)

                passes: list[dict] = []
                fails: list[dict] = []
                samples_debug = []

                def _add_sample(p: dict, matched: bool):
                    full = _generate_payload(attr_defs)
                    full.update(p)
                    samples_debug.append({"payload": full, "matched": matched})
                    if matched and len(passes) < 2:
                        passes.append(full)
                    if not matched and len(fails) < 2:
                        fails.append(full)

                def _synthesize_from_jsonlogic(expr):
                    if not isinstance(expr, dict) or len(expr) != 1:
                        return False
                    op, args = next(iter(expr.items()))
                    if op not in ("<", "<=", ">", ">=", "==", "!="):
                        return False
                    if not isinstance(args, list) or len(args) != 2:
                        return False
                    left, right = args[0], args[1]
                    var_name = None
                    const_val = None
                    if isinstance(left, dict) and left.get("var"):
                        var_name = left.get("var")
                        const_val = right
                    elif isinstance(right, dict) and right.get("var"):
                        var_name = right.get("var")
                        const_val = left
                    else:
                        return False
                    if not isinstance(var_name, str):
                        return False
                    if isinstance(const_val, (int, float)):
                        c = const_val
                    elif isinstance(const_val, str) and const_val.isdigit():
                        c = int(const_val)
                    else:
                        return False
                    if op in ("<", "<="):
                        _add_sample({var_name: c - 1}, True)
                        _add_sample({var_name: c + 1}, False)
                        return True
                    if op in (">", ">="):
                        _add_sample({var_name: c + 1}, True)
                        _add_sample({var_name: c - 1}, False)
                        return True
                    if op == "==":
                        _add_sample({var_name: c}, True)
                        _add_sample({var_name: c + 1}, False)
                        return True
                    if op == "!=":
                        _add_sample({var_name: c + 1}, True)
                        _add_sample({var_name: c}, False)
                        return True
                    return False

                def _synthesize_from_expr_source(expr_src: str):
                    import re
                    m = re.search(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*(==|=|!=|<=|>=|<|>)\s*([0-9]+(?:\.[0-9]+)?|'([^']*)'|\"([^\"]*)\")", expr_src)
                    if not m:
                        return False
                    var_name = m.group(1)
                    op = m.group(2)
                    raw_val = m.group(3)

                    if re.fullmatch(r"-?\d+(?:\.\d+)?", raw_val):
                        c = float(raw_val)
                        if op in ("<", "<="):
                            _add_sample({var_name: int(c) - 1}, True)
                            _add_sample({var_name: int(c) + 1}, False)
                            return True
                        if op in (">", ">="):
                            _add_sample({var_name: int(c) + 1}, True)
                            _add_sample({var_name: int(c) - 1}, False)
                            return True
                        if op in ("==", "="):
                            _add_sample({var_name: int(c)}, True)
                            _add_sample({var_name: int(c) + 1}, False)
                            return True
                        if op == "!=":
                            _add_sample({var_name: int(c) + 1}, True)
                            _add_sample({var_name: int(c)}, False)
                            return True
                        return False

                    s = None
                    if raw_val and ((raw_val[0] == "'" and raw_val[-1] == "'") or (raw_val[0] == '"' and raw_val[-1] == '"')):
                        s = raw_val[1:-1]
                    else:
                        s = raw_val

                    if op in ("==", "="):
                        _add_sample({var_name: s}, True)
                        _add_sample({var_name: s + "_diff"}, False)
                        return True
                    if op == "!=":
                        _add_sample({var_name: s + "_diff"}, True)
                        _add_sample({var_name: s}, False)
                        return True

                    return False

                if compiled.expr_type == 'jsonlogic':
                    _synthesize_from_jsonlogic(compiled.expr)
                elif compiled.expr_type == 'expr':
                    src = compiled.expr.get('source') if isinstance(compiled.expr, dict) else None
                    if isinstance(src, str):
                        _synthesize_from_expr_source(src)

                attempts = 0
                while (len(passes) < 2 or len(fails) < 2) and attempts < 1000:
                    attempts += 1
                    payload = _generate_payload(attr_defs)
                    try:
                        matched = eval_rule(compiled, payload)
                    except Exception as e:
                        samples_debug.append({"payload": payload, "error": str(e)})
                        continue
                    _add_sample(payload, bool(matched))
                sent_for_rule = 0
                for payload in passes + fails:
                    evt = {
                        "event_id": str(uuid.uuid4()),
                        "entity_key": entity_key,
                        "occurred_at": datetime.now(timezone.utc).isoformat(),
                        "payload": payload,
                        "meta": {"source": "simulator", "rule_id": rule_id},
                    }
                    await producer.send_and_wait(settings.topic_in, orjson.dumps(evt))
                    sent += 1
                    sent_for_rule += 1
                summary.append({"entity_id": entity_id, "entity_key": entity_key, "rule_id": rule_id, "sent": sent_for_rule})
                debug.append({"rule_id": rule_id, "attempts": attempts, "passes_found": len(passes), "fails_found": len(fails), "samples": samples_debug[:10]})

            combined_sent = 0
            try:
                if target_rule_id is None and len(compiled_list) > 1:
                    combined_samples: list[dict] = []
                    attempts = 0
                    while len(combined_samples) < 5 and attempts < 1000:
                        attempts += 1
                        payload = _generate_payload(attr_defs)
                        matched: list[dict] = []
                        for c in compiled_list:
                            try:
                                if eval_rule(c, payload):
                                    matched.append({"rule_id": c.rule_id, "name": c.name, "severity": c.severity})
                            except Exception:
                                ignore rule errors during synthesis
                                continue
                        if len(matched) > 1:
                            combined_samples.append({"payload": payload, "matched": matched})
                    for sample in combined_samples:
                        evt = {
                            "event_id": str(uuid.uuid4()),
                            "entity_key": entity_key,
                            "occurred_at": datetime.now(timezone.utc).isoformat(),
                            "payload": sample["payload"],
                            "meta": {"source": "simulator", "matched_rule_ids": [m["rule_id"] for m in sample["matched"]]},
                        }
                        await producer.send_and_wait(settings.topic_in, orjson.dumps(evt))
                        sent += 1
                        combined_sent += 1
                    if combined_sent:
                        summary.append({"entity_id": entity_id, "entity_key": entity_key, "rule_id": None, "sent_combined": combined_sent})
            except Exception as e:
                debug.append({"entity_id": entity_id, "synthesis_error": str(e)})

        return {"ok": True, "sent": sent, "summary": summary, "debug": debug}
    finally:
        await producer.stop()

async def _simulate_loop():
    global _simulator_stop, _simulator_last_result
    try:
        _simulator_stop = asyncio.Event()
        while not _simulator_stop.is_set():
            async with SessionLocal() as session:
                res = await _simulate_once_using_session(session)
                _simulator_last_result = res
            try:
                await asyncio.wait_for(_simulator_stop.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                continue
    finally:
        _simulator_stop = None
        return


@router.get("/users", response_model=list[UserOut])
async def list_users(
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(require_role("admin")),
):
    q = text("""
      SELECT user_id, username, role, is_active, created_at
      FROM app.users
      ORDER BY user_id DESC
    """)
    rows = (await session.execute(q)).mappings().all()
    return [UserOut(**r) for r in rows]


@router.post("/users", response_model=UserOut)
async def create_user(
    body: UserCreate,
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(require_role("admin")),
):
    q = text("""
      INSERT INTO app.users(username, password_hash, role)
      VALUES (:u, :ph, :r)
      RETURNING user_id, username, role, is_active, created_at
    """)
    try:
        row = (await session.execute(q, {"u": body.username, "ph": hash_password(body.password), "r": body.role})).mappings().first()
        await session.commit()
        return UserOut(**row)
    except IntegrityError as e:
        await session.rollback()
        if isinstance(getattr(e, 'orig', None), UniqueViolationError) or 'unique' in str(e).lower():
            raise HTTPException(status_code=409, detail="User with this username already exists")
        raise HTTPException(status_code=400, detail="Failed to create user")
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=400, detail="Failed to create user")


@router.get("/simulate_events")
async def simulate_events_status(
    action: str = 'status',
    user: dict = Depends(require_role("admin")),
):
    """Query simulator status (GET only supports 'status' action)."""
    if action != 'status':
        raise HTTPException(status_code=400, detail="GET only supports action=status")
    return {"ok": True, "running": bool(_simulator_task and not _simulator_task.done()), "last": _simulator_last_result}


@router.post("/simulate_events")
async def simulate_events(
    action: str = 'once',
    entity_id: int | None = Query(None),
    rule_id: int | None = Query(None),
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(require_role("admin")),
):
    """Generate synthetic events for entities and rules and send to Kafka topic_in.

    action: 'once' (default) | 'start' | 'stop'
    """
    global _simulator_task, _simulator_stop

    if action == 'status':
        return {"ok": True, "running": bool(_simulator_task and not _simulator_task.done()), "last": _simulator_last_result}

    if action == 'start':
        if _simulator_task and not _simulator_task.done():
            return {"ok": True, "running": True, "detail": "already running"}
        _simulator_task = asyncio.create_task(_simulate_loop())
        return {"ok": True, "running": True}

    if action == 'stop':
        if _simulator_stop:
            _simulator_stop.set()
        if _simulator_task:
            try:
                await asyncio.wait_for(_simulator_task, timeout=5.0)
            except Exception:
                pass
        _simulator_task = None
        return {"ok": True, "running": False}

    result = await _simulate_once_using_session(session, entity_id=entity_id, target_rule_id=rule_id)
    return result


@router.patch("/users/{user_id}", response_model=UserOut)
async def patch_user(
    user_id: int,
    body: UserPatch,
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(require_role("admin")),
):
    fields = []
    params = {"id": user_id}
    if body.password is not None:
        fields.append("password_hash=:ph")
        params["ph"] = hash_password(body.password)
    if body.role is not None:
        fields.append("role=:r")
        params["r"] = body.role
    if body.is_active is not None:
        fields.append("is_active=:a")
        params["a"] = body.is_active

    if not fields:
        raise HTTPException(status_code=400, detail="Nothing to update")

    q = text(f"""
      UPDATE app.users SET {', '.join(fields)}
      WHERE user_id=:id
      RETURNING user_id, username, role, is_active, created_at
    """)
    row = (await session.execute(q, params)).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    await session.commit()
    return UserOut(**row)


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: int,
    session: AsyncSession = Depends(get_session),
    user: dict = Depends(require_role("admin")),
):
    if user_id == user.get("user_id"):
        raise HTTPException(status_code=400, detail="Cannot delete yourself")

    target = (await session.execute(text("SELECT user_id, role FROM app.users WHERE user_id=:id"), {"id": user_id})).mappings().first()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    if target["role"] == "admin":
        admin_count = int(await session.scalar(text("SELECT count(*) FROM app.users WHERE role='admin'")) or 0)
        if admin_count <= 1:
            raise HTTPException(status_code=400, detail="Cannot delete the last admin user")

    await session.execute(text("DELETE FROM app.users WHERE user_id=:id"), {"id": user_id})
    await session.commit()
    return {"ok": True}
