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
    picture: UserPicture | None = None


_REGISTRY: ContextVar[dict[str, UserFormat] | None] = ContextVar(
    "sas2pyspark_user_formats", default=None
)
_INFORMAT_REGISTRY: ContextVar[dict[str, "UserInformat"] | None] = ContextVar(
    "sas2pyspark_user_informats", default=None
)
_CNTLIN_DATASET: ContextVar[str | None] = ContextVar(
    "sas2pyspark_cntlin_dataset", default=None
)


@dataclass(frozen=True)
class UserInformat:
    name: str
    entries: tuple[tuple[str, str], ...]
    other_value: str | None
    is_char: bool


@dataclass(frozen=True)
class UserPicture:
    name: str
    mask: str
    prefix: str
    multiplier: float
    round_value: bool


def reset_formats() -> None:
    _REGISTRY.set({})
    _INFORMAT_REGISTRY.set({})
    _CNTLIN_DATASET.set(None)


def register_cntlin(dataset: str) -> None:
    _CNTLIN_DATASET.set(dataset.lower())


def cntlin_dataset() -> str | None:
    return _CNTLIN_DATASET.get()


def _formats() -> dict[str, UserFormat]:
    registry = _REGISTRY.get()
    if registry is None:
        registry = {}
        _REGISTRY.set(registry)
    return registry


def _informats() -> dict[str, UserInformat]:
    registry = _INFORMAT_REGISTRY.get()
    if registry is None:
        registry = {}
        _INFORMAT_REGISTRY.set(registry)
    return registry


def format_key(spec: str) -> str:
    return spec.strip().rstrip(".").lower()


def has_format(spec: str) -> bool:
    return format_key(spec) in _formats()


def has_informat(spec: str) -> bool:
    return format_key(spec) in _informats()


def has_cntlin() -> bool:
    return _CNTLIN_DATASET.get() is not None


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


def register_invalue(statement: str) -> UserInformat:
    match = re.fullmatch(
        r"invalue\s+(\$?[A-Za-z_]\w*)\s+(.+)",
        statement.strip(),
        re.I | re.S,
    )
    if match is None:
        raise FormatError(f"Malformed INVALUE statement: {statement[:60]!r}.")
    raw_name, body = match.groups()
    entries: list[tuple[str, str]] = []
    other_value: str | None = None
    entry_pattern = re.compile(
        rf"\s*(?P<key>{_QUOTED}|{_NUMBER}|other)\s*=\s*"
        rf"(?P<value>{_NUMBER}|\.|{_QUOTED})\s*",
        re.I,
    )
    position = 0
    while position < len(body):
        entry = entry_pattern.match(body, position)
        if entry is None:
            raise FormatError(
                f"Unsupported INVALUE entry near {body[position:position + 40]!r}."
            )
        position = entry.end()
        key, value = entry.group("key"), entry.group("value")
        if key.casefold() == "other":
            if other_value is not None:
                raise FormatError("INVALUE OTHER appears more than once.")
            other_value = value
            continue
        normalized_key = _unquote(key) if key[0] in "'\"" else key
        if any(existing == normalized_key for existing, _ in entries):
            raise FormatError(f"Duplicate INVALUE key {normalized_key!r}.")
        normalized_value = (
            "NULL"
            if value == "."
            else _sql_string(_unquote(value))
            if value[0] in "'\""
            else value
        )
        entries.append((normalized_key, normalized_value))
    result = UserInformat(
        raw_name.lower(),
        tuple(entries),
        (
            "NULL"
            if other_value == "."
            else _sql_string(_unquote(other_value))
            if other_value and other_value[0] in "'\""
            else other_value
        ),
        raw_name.startswith("$"),
    )
    _informats()[result.name] = result
    return result


def register_picture(statement: str) -> UserFormat:
    match = re.fullmatch(
        rf"picture\s+([A-Za-z_]\w*)\s*(.*?)({_QUOTED})(?:\s*(\(.*\)))?",
        statement.strip(),
        re.I | re.S,
    )
    if match is None:
        raise FormatError("PICTURE requires one supported numeric mask.")
    name, before_mask, raw_mask, after_options = match.groups()
    mask = _unquote(raw_mask)
    before_mask = re.sub(r"low\s*-\s*high\s*=\s*$", "", before_mask, flags=re.I).strip()
    options: list[str] = []
    for text in (before_mask, after_options or ""):
        if not text:
            continue
        if not (text.startswith("(") and text.endswith(")")):
            raise FormatError("PICTURE supports only LOW-HIGH and option groups.")
        options.extend(text[1:-1].split())
    parsed: dict[str, str | None] = {}
    for option in options:
        key, separator, value = option.partition("=")
        key = key.casefold()
        if key not in {"round", "prefix", "mult"}:
            raise FormatError(f"Unsupported PICTURE option {key.upper()!r}.")
        if key == "round":
            if separator:
                raise FormatError("PICTURE ROUND is a flag.")
            parsed[key] = None
        elif not separator or key in parsed:
            raise FormatError(f"PICTURE {key.upper()} option requires one value.")
        else:
            parsed[key] = value.strip("'\"")
    if not re.fullmatch(r"[09]+(?:\.[09]+)?", mask):
        raise FormatError(
            "PICTURE supports only simple 0/9 digit masks with an optional decimal."
        )
    prefix = parsed.get("prefix") or ""
    multiplier_text = parsed.get("mult", "1")
    try:
        multiplier = float(multiplier_text)
    except (TypeError, ValueError) as error:
        raise FormatError("PICTURE MULT= requires a numeric literal.") from error
    picture = UserPicture(name.lower(), mask, prefix, multiplier, "round" in parsed)
    result = UserFormat(name.lower(), False, (), None, picture)
    _formats()[result.name] = result
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
    fmt = _formats().get(format_key(spec))
    if fmt is None:
        if _CNTLIN_DATASET.get() is None:
            raise FormatError(f"Unknown user format {spec!r}.")
        return (
            "sas2pyspark_cntlin_format("
            f"{_sql_string(format_key(spec))}, CAST({value} AS STRING))"
        )
    if fmt.picture is not None:
        picture = fmt.picture
        integer, _, decimals = picture.mask.partition(".")
        width = len(picture.mask)
        places = len(decimals)
        number = f"CAST(({value}) AS DOUBLE) * {picture.multiplier}"
        if picture.round_value:
            number = f"round({number}, {places})"
        rendered = f"format_string('%0{width}.{places}f', {number})"
        if picture.prefix:
            rendered = f"concat({_sql_string(picture.prefix)}, {rendered})"
        return f"CASE WHEN ({value}) IS NULL THEN NULL ELSE {rendered} END"
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


def render_user_informat(value: str, spec: str) -> str:
    informat = _informats().get(format_key(spec))
    if informat is None:
        if _CNTLIN_DATASET.get() is None:
            raise FormatError(f"Unknown user informat {spec!r}.")
        return (
            "try_cast(sas2pyspark_cntlin_format("
            f"{_sql_string(format_key(spec))}, CAST({value} AS STRING)) AS DOUBLE)"
        )
    input_value = f"trim(CAST({value} AS STRING))"
    branches = [
        f"WHEN {input_value} = {_sql_string(key)} THEN {mapped}"
        for key, mapped in informat.entries
    ]
    fallback = informat.other_value or "NULL"
    expression = "CASE " + " ".join(branches) + f" ELSE {fallback} END"
    return expression if informat.is_char else f"try_cast(({expression}) AS DOUBLE)"
