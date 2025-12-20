from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import asyncpg
import orjson

from app.rule_engine import CompiledRule


@dataclass(frozen=True)
class Entity:
    entity_id: int
    entity_key: str


@dataclass(frozen=True)
class Attribute:
    attr_key: str
    type: str
    is_required: bool


class RulesCache:
    def __init__(self) -> None:
        self.entities: dict[str, Entity] = {}
        self.attributes: dict[int, dict[str, Attribute]] = {}
        self.rules_by_entity: dict[int, list[CompiledRule]] = {}

    async def refresh(self, conn: asyncpg.Connection) -> None:
        ents = await conn.fetch(
            "SELECT entity_id, entity_key FROM app.entities WHERE is_active=true"
        )
        self.entities = {r["entity_key"]: Entity(entity_id=r["entity_id"], entity_key=r["entity_key"]) for r in ents}

        attrs = await conn.fetch(
            """
            SELECT entity_id, attr_key, type::text AS type, is_required
            FROM app.entity_attributes
            """
        )
        by_e: dict[int, dict[str, Attribute]] = {}
        for r in attrs:
            by_e.setdefault(r["entity_id"], {})[r["attr_key"]] = Attribute(
                attr_key=r["attr_key"], type=r["type"], is_required=r["is_required"]
            )
        self.attributes = by_e

        rows = await conn.fetch(
            """
            SELECT r.rule_id, r.entity_id, r.name, r.severity::text AS severity, r.action::text AS action, r.priority,
                   rv.expr_type, rv.expr
            FROM app.rules r
            JOIN app.rule_active_version rav ON rav.rule_id=r.rule_id
            JOIN app.rule_versions rv ON rv.rule_version_id=rav.rule_version_id
            WHERE r.is_active=true
            ORDER BY r.entity_id, r.priority ASC, r.rule_id ASC
            """
        )
        rules_by: dict[int, list[CompiledRule]] = {}
        for r in rows:
            expr_val = r["expr"]
            if isinstance(expr_val, (bytes, bytearray)):
                try:
                    expr_val = orjson.loads(expr_val)
                except Exception:
                    pass
            elif isinstance(expr_val, str):
                try:
                    expr_val = orjson.loads(expr_val)
                except Exception:
                    pass

            rules_by.setdefault(r["entity_id"], []).append(
                CompiledRule(
                    rule_id=r["rule_id"],
                    name=r["name"],
                    severity=r["severity"],
                    action=r["action"],
                    priority=r["priority"],
                    expr_type=r["expr_type"],
                    expr=expr_val,
                )
            )
        self.rules_by_entity = rules_by
