from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from lark import Lark, Transformer, v_args


def _var(path: str, data: dict) -> Any:
    cur: Any = data
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def eval_jsonlogic(expr: Any, data: dict) -> Any:
    """A small JSONLogic evaluator covering the operators we need.

    Supported: var, and, or, !, ==, !=, <, <=, >, >=, +, -, *, /, in, len, lower, upper, contains
    """
    if expr is None:
        return None
    if isinstance(expr, (bool, int, float, str)):
        return expr
    if isinstance(expr, list):
        return [eval_jsonlogic(x, data) for x in expr]
    if isinstance(expr, dict):
        if len(expr) != 1:
            raise ValueError("Invalid jsonlogic node")
        op, args = next(iter(expr.items()))
        if op == "var":
            if isinstance(args, list):
                path = str(args[0])
                default = args[1] if len(args) > 1 else None
                v = _var(path, data)
                return default if v is None else v
            return _var(str(args), data)

        if not isinstance(args, list):
            args = [args]
        ev = [eval_jsonlogic(a, data) for a in args]

        if op == "and":
            res = True
            for v in ev:
                res = bool(v)
                if not res:
                    return False
            return True
        if op == "or":
            for v in ev:
                if bool(v):
                    return True
            return False
        if op == "!":
            return not bool(ev[0])

        if op in ("==", "!=", "<", "<=", ">", ">="):
            a, b = ev[0], ev[1]
            if op == "==":
                return a == b
            if op == "!=":
                return a != b
            if op == "<":
                return a < b
            if op == "<=":
                return a <= b
            if op == ">":
                return a > b
            if op == ">=":
                return a >= b

        if op in ("+", "-", "*", "/"):
            a, b = float(ev[0]), float(ev[1])
            if op == "+":
                return a + b
            if op == "-":
                return a - b
            if op == "*":
                return a * b
            if op == "/":
                return a / b

        if op == "in":
            needle, hay = ev[0], ev[1]
            if isinstance(hay, list):
                return needle in hay
            if isinstance(hay, str) and isinstance(needle, str):
                return needle in hay
            return False

        if op == "len":
            s = ev[0]
            return len(s) if s is not None else 0
        if op == "lower":
            return str(ev[0]).lower()
        if op == "upper":
            return str(ev[0]).upper()
        if op == "contains":
            a, b = ev[0], ev[1]
            return str(b) in str(a)

        raise ValueError(f"Unsupported jsonlogic op: {op}")

    raise ValueError("Invalid jsonlogic expression")


_EXPR_GRAMMAR = r"""
?start: expr

?expr: or_expr
?or_expr: and_expr ("||" and_expr)*
?and_expr: not_expr ("&&" not_expr)*
?not_expr: "!" not_expr  -> not
         | comparison

?comparison: sum (COMP_OP sum)?
COMP_OP: "==" | "=" | "!=" | "<=" | ">=" | "<" | ">"

?sum: term (("+"|"-") term)*
?term: factor (("*"|"/") factor)*

?factor: NUMBER           -> number
       | STRING           -> string
       | TRUE             -> true
       | FALSE            -> false
       | NAME             -> var
       | NAME "." NAME    -> dotted_var
       | func_call
       | "(" expr ")"

?func_call: NAME "(" [args] ")"
?args: expr ("," expr)*

TRUE: "true"
FALSE: "false"

NAME: /[a-zA-Z_][a-zA-Z0-9_]*/
STRING: /"([^"\\]|\\.)*"/ | /'([^'\\]|\\.)*'/
NUMBER: /-?\d+(\.\d+)?/

%import common.WS
%ignore WS
"""


@v_args(inline=True)
class _ExprTransformer(Transformer):
    def __init__(self, data: dict, funcs: dict[str, Callable[..., Any]]):
        super().__init__()
        self.data = data
        self.funcs = funcs

    def number(self, tok):
        s = str(tok)
        return float(s) if "." in s else int(s)

    def string(self, tok):
        s = str(tok)
        if s[0] == s[-1] and s[0] in ("'", '"'):
            s = s[1:-1]
        return bytes(s, "utf-8").decode("unicode_escape")

    def true(self, _tok):
        return True

    def false(self, _tok):
        return False

    def var(self, name):
        return _var(str(name), self.data)

    def dotted_var(self, a, b):
        return _var(f"{a}.{b}", self.data)

    def not_(self, v):
        return not bool(v)

    def comparison(self, left, *rest):
        if not rest:
            return left
        op = str(rest[0])
        right = rest[1]
        if op == "==" or op == "=":
            return left == right
        if op == "!=":
            return left != right
        if op == "<":
            return left < right
        if op == "<=":
            return left <= right
        if op == ">":
            return left > right
        if op == ">=":
            return left >= right
        raise ValueError("bad comparator")

    def sum(self, first, *rest):
        v = first
        it = iter(rest)
        for op, term in zip(it, it):
            o = str(op)
            if o == "+":
                v = v + term
            else:
                v = v - term
        return v

    def term(self, first, *rest):
        v = first
        it = iter(rest)
        for op, fac in zip(it, it):
            o = str(op)
            if o == "*":
                v = v * fac
            else:
                v = v / fac
        return v

    def and_expr(self, first, *rest):
        v = bool(first)
        for x in rest:
            v = v and bool(x)
        return v

    def or_expr(self, first, *rest):
        v = bool(first)
        for x in rest:
            v = v or bool(x)
        return v

    def func_call(self, name, *args):
        fn = self.funcs.get(str(name))
        if not fn:
            raise ValueError(f"Unknown function: {name}")
        if args:
            if isinstance(args[0], list):
                return fn(*args[0])
            return fn(*args)
        return fn()

    def args(self, *items):
        return list(items)


_EXPR_PARSER = Lark(_EXPR_GRAMMAR, parser="lalr", maybe_placeholders=False)


def default_funcs() -> dict[str, Callable[..., Any]]:
    return {
        "len": lambda x: len(x) if x is not None else 0,
        "lower": lambda x: str(x).lower(),
        "upper": lambda x: str(x).upper(),
        "contains": lambda a, b: str(b) in str(a),
        "in": lambda needle, hay: needle in hay if isinstance(hay, list) else False,
        "abs": lambda x: abs(float(x)),
        "round": lambda x, n=0: round(float(x), int(n)),
        "startswith": lambda s, p: str(s).startswith(str(p)),
        "endswith": lambda s, p: str(s).endswith(str(p)),
    }


def eval_expr(source: str, data: dict, funcs: dict[str, Callable[..., Any]] | None = None) -> Any:
    funcs = funcs or default_funcs()
    tree = _EXPR_PARSER.parse(source)
    return _ExprTransformer(data, funcs).transform(tree)


@dataclass(frozen=True)
class CompiledRule:
    rule_id: int
    name: str
    severity: str
    action: str
    priority: int
    expr_type: str
    expr: Any


def eval_rule(compiled: CompiledRule, payload: dict) -> bool:
    expr = compiled.expr
    if compiled.expr_type == "jsonlogic":
        if isinstance(expr, (bytes, bytearray)):
            try:
                import orjson

                expr = orjson.loads(expr)
            except Exception:
                pass
        elif isinstance(expr, str):
            try:
                import orjson

                expr = orjson.loads(expr)
            except Exception:
                pass
        return bool(eval_jsonlogic(expr, payload))
    if compiled.expr_type == "expr":
        if isinstance(expr, str):
            src = expr
        else:
            src = expr.get("source") if isinstance(expr, dict) else None
        if not isinstance(src, str):
            raise ValueError("expr.source must be a string")
        return bool(eval_expr(src, payload))
    raise ValueError(f"Unknown expr_type {compiled.expr_type}")


def validate_rule(expr_type: str, expr: dict, sample_payload: dict) -> None:
    """Raise ValueError if invalid."""
    if expr_type == "jsonlogic":
        eval_jsonlogic(expr, sample_payload)
        return
    if expr_type == "expr":
        src = expr.get("source")
        if not isinstance(src, str) or not src.strip():
            raise ValueError("expr.source is required")
        _EXPR_PARSER.parse(src)
        return
    raise ValueError("Unknown expr_type")
