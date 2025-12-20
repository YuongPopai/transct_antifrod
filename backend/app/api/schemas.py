from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, ConfigDict, AliasChoices


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"


class MeOut(BaseModel):
    user_id: int
    username: str
    role: str


class EntityCreate(BaseModel):
    entity_key: str = Field(pattern=r"^[a-z][a-z0-9_]{1,63}$")
    name: str
    description: Optional[str] = None
    is_active: bool = True


class EntityOut(BaseModel):
    entity_id: int
    entity_key: str
    name: str
    description: Optional[str] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime


class AttributeCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    attr_key: str = Field(validation_alias=AliasChoices("attr_key", "attrKey"))
    name: str
    type: str
    is_required: bool = Field(
        default=False,
        validation_alias=AliasChoices("is_required", "isRequired"),
    )
    default_value: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("default_value", "defaultValue"),
    )

    description: Optional[str] = None


class AttributeOut(BaseModel):
    attr_id: int
    entity_id: int
    attr_key: str
    name: str
    type: str
    is_required: bool
    default_value: Optional[Any] = None
    description: Optional[str] = None


class RuleCreate(BaseModel):
    entity_id: int
    name: str
    description: Optional[str] = None
    is_active: bool = True
    priority: int = 100
    severity: Literal["info", "warn", "high"] = "warn"
    action: Literal["alert", "ignore", "block", "force_review"] = "alert"


class RuleOut(BaseModel):
    rule_id: int
    entity_id: int
    name: str
    description: Optional[str] = None
    is_active: bool
    priority: int
    severity: str
    action: str
    created_at: datetime
    updated_at: datetime
    active_version_id: Optional[int] = None


class RuleVersionCreate(BaseModel):
    expr_type: Literal["jsonlogic", "expr"]
    expr: Any


class RuleVersionOut(BaseModel):
    rule_version_id: int
    rule_id: int
    version_no: int
    expr_type: str
    expr: Any
    created_at: datetime
    is_active: bool = False


class ValidateRuleIn(BaseModel):
    expr_type: Literal["jsonlogic", "expr"]
    expr: Any
    sample_payload: dict


class ValidateRuleOut(BaseModel):
    ok: bool
    errors: list[str] = []


class UserCreate(BaseModel):
    username: str
    password: str = Field(min_length=6)
    role: Literal["admin", "analyst", "senior_analyst"] = "analyst"


class UserOut(BaseModel):
    user_id: int
    username: str
    role: str
    is_active: bool
    created_at: datetime


class UserPatch(BaseModel):
    password: Optional[str] = Field(default=None, min_length=6)
    role: Optional[Literal["admin", "analyst", "senior_analyst"]] = None
    is_active: Optional[bool] = None

class UserOut(BaseModel):
    user_id: int
    username: str
    role: str
    is_active: bool
    created_at: datetime


class UserPatch(BaseModel):
    password: Optional[str] = Field(default=None, min_length=6)
    role: Optional[Literal["admin", "analyst"]] = None
    is_active: Optional[bool] = None


class MonitorEvent(BaseModel):
    event_id: str
    entity_key: str
    occurred_at: datetime
    payload: dict
    decision: Literal["legit", "alert", "invalid"]
    risk: float
    matched_rules: list[dict] = Field(default_factory=list)
    reasons: list[dict] = Field(default_factory=list)
    classified_at: datetime


class MonitorSummary(BaseModel):
    total: int
    alert: int
    legit: int
    alert_pct: float
    legit_pct: float
    events: list[MonitorEvent] = Field(default_factory=list)
