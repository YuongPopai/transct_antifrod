from __future__ import annotations

from datetime import datetime
from typing import Any


def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def validate_payload(payload: dict, attrs: dict[str, Any]) -> list[dict]:
    """Return list of validation errors; empty => ok."""
    errors: list[dict] = []
    for k, a in attrs.items():
        if a.is_required and k not in payload:
            errors.append({"type": "missing", "path": f"$.{k}", "message": "required"})

    for k, v in payload.items():
        a = attrs.get(k)
        if a is None:
            continue
        t = a.type
        ok = True
        if t == "string":
            ok = isinstance(v, str)
        elif t == "number":
            ok = _is_number(v)
        elif t == "boolean":
            ok = isinstance(v, bool)
        elif t == "datetime":
            if isinstance(v, str):
                try:
                    datetime.fromisoformat(v.replace("Z", "+00:00"))
                except Exception:
                    ok = False
            else:
                ok = False
        elif t == "object":
            ok = isinstance(v, dict)
        elif t == "array":
            ok = isinstance(v, list)
        if not ok:
            errors.append({"type": "type", "path": f"$.{k}", "expected": t, "got": type(v).__name__})

    return errors
