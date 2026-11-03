"""User-defined PROC FORMAT VALUE formats rendered as Spark SQL CASE expressions."""

from __future__ import annotations

import re
from contextvars import ContextVar
from dataclasses import dataclass

_QUOTED = r"""'(?:[^']|'')*'|"(?:[^"]|"")*\""""
_NUMBER = r"-?\d+(?:\.\d+)?|-?\.\d+"
_BOUND = rf"low|high|other|{_QUOTED}|{_NUMBER}|\."
_VALUE_HEADER = re.compile(r"value\s+(\$?[A-Za-z_]\w*)\s+(.*)", re.IGNORECASE | re.DOTALL)
_ENTRY = re.compile(rf"\s*(?P<range>[^=]+?)\s*=\s*(?P<label>{_QUOTED})\s*", re.DOTALL)
_RANGE_PART = re.compile(
    rf"(?P<low>{_BOUND})\s*(?P<lex><)?\s*-\s*(?P<hex><)?\s*(?P<high>{_BOUND})"
    rf"|(?P<single>{_BOUND})",
    re.IGNORECASE,
)


class FormatError(ValueError):
    """Raised for unsupported or malformed PROC FORMAT usage."""


@dataclass(frozen=True)
class _Entry:
    condition_parts: tuple[tuple, ...]
    label: str


@dataclass(frozen=True)
class UserFormat:
    name: str
    is_char: bool
    entries: tuple[_Entry, ...]
    other_label: str | None


_REGISTRY: ContextVar[dict[str, UserFormat] | None] = ContextVar(
    "sas2pyspark_user_formats", default=None
)


def reset_formats() -> None:
    _REGISTRY.set({})


def _formats() -> dict[str, UserFormat]:
    registry = _REGISTRY.get()
    if registry is None:
        registry = {}
        _REGISTRY.set(registry)
    return registry


def format_key(spec: str) -> str:
    return spec.strip().rstrip(".").lower()


def has_format(spec: str) -> bool:
    return format_key(spec) in _formats()


def _unquote(text: str) -> str:
    return text[1:-1].replace(text[0] * 2, text[0])


def _sql_string(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _parse_bound(token: str, is_char: bool):
    lowered = token.lower()
    if lowered in {"low", "high"}:
        return lowered
    if token == ".":
        return "missing"
    if token[0] in "'\"":
        if not is_char:
            raise FormatError("Character range value used in a numeric format.")
        return ("str", _unquote(token))
    if is_char:
        raise FormatError("Numeric range value used in a character format.")
    return ("num", token)


def _literal(bound) -> str:
    kind, text = bound
    return _sql_string(text) if kind == "str" else text


def register_value(statement: str) -> UserFormat:
    match = _VALUE_HEADER.fullmatch(statement.strip())
    if match is None:
        raise FormatError(f"Malformed VALUE statement: {statement[:60]!r}.")
    raw_name, body = match.groups()
    is_char = raw_name.startswith("$")
    name = raw_name.lower()
    if re.search(r"\(", body.split("=", 1)[0]):
        raise FormatError("VALUE format options such as (DEFAULT=) are unsupported.")
    entries: list[_Entry] = []
    other_label: str | None = None
    position = 0
    body = body.strip()
    if not body:
        raise FormatError("VALUE statement has no ranges.")
    while position < len(body):
        entry = _ENTRY.match(body, position)
        if entry is None:
            raise FormatError(f"Cannot parse format range near {body[position:position + 40]!r}.")
        position = entry.end()
        label = _unquote(entry.group("label"))
        parts: list[tuple] = []
        for piece in entry.group("range").split(","):
            piece = piece.strip()
            part = _RANGE_PART.fullmatch(piece)
            if part is None:
                raise FormatError(f"Unsupported format range {piece!r}.")
            if part.group("single") is not None:
                single = part.group("single")
                if single.lower() == "other":
                    if other_label is not None:
                        raise FormatError("OTHER appears more than once.")
                    other_label = label
                    continue
                parts.append(("eq", _parse_bound(single, is_char)))
            else:
                low = _parse_bound(part.group("low"), is_char)
                high = _parse_bound(part.group("high"), is_char)
                if "other" in {part.group("low").lower(), part.group("high").lower()}:
                    raise FormatError("OTHER cannot be part of a range.")
                parts.append(
                    ("range", low, high, bool(part.group("lex")), bool(part.group("hex")))
                )
        if parts:
            entries.append(_Entry(tuple(parts), label))
    result = UserFormat(name, is_char, tuple(entries), other_label)
    _formats()[name] = result
    return result


def _condition(part: tuple, value: str) -> str:
    if part[0] == "eq":
        bound = part[1]
        if bound == "missing":
            return f"({value}) IS NULL"
        return f"({value}) = {_literal(bound)}"
    _, low, high, low_exclusive, high_exclusive = part
    clauses = []
    if low not in {"low"}:
        if low == "missing" or low == "high":
            raise FormatError("Unsupported range bound.")
        clauses.append(f"({value}) {'>' if low_exclusive else '>='} {_literal(low)}")
    if high not in {"high"}:
        if high == "missing" or high == "low":
            raise FormatError("Unsupported range bound.")
        clauses.append(f"({value}) {'<' if high_exclusive else '<='} {_literal(high)}")
    if not clauses:
        return f"({value}) IS NOT NULL"
    return "(" + " AND ".join(clauses) + ")"


def render_user_format(value: str, spec: str) -> str:
    fmt = _formats()[format_key(spec)]
    branches = []
    for entry in fmt.entries:
        condition = " OR ".join(_condition(part, value) for part in entry.condition_parts)
        branches.append(f"WHEN {condition} THEN {_sql_string(entry.label)}")
    if fmt.other_label is not None:
        fallback = _sql_string(fmt.other_label)
    elif fmt.is_char:
        fallback = f"coalesce(CAST({value} AS STRING), '')"
    else:
        fallback = f"coalesce(CAST({value} AS STRING), '.')"
    return "CASE " + " ".join(branches) + f" ELSE {fallback} END"
