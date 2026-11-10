"""Load client-supplied table schemas (PROC CONTENTS exports, DDL, JSON)."""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

_MAX_ASSET_BYTES = 50_000_000
_NUMERIC_SQL = re.compile(
    r"^(?:tinyint|smallint|int|integer|bigint|number|numeric|decimal|dec|float|"
    r"real|double|money|date|datetime|datetime2|timestamp|time|bit)\b",
    re.I,
)
_CHARACTER_SQL = re.compile(
    r"^(?:char|nchar|varchar|varchar2|nvarchar|nvarchar2|text|ntext|string|clob)\b",
    re.I,
)
_CREATE_TABLE = re.compile(
    r"create\s+table\s+(?:if\s+not\s+exists\s+)?"
    r"([\[\]\"`\w]+(?:\.[\[\]\"`\w]+)?)\s*\(",
    re.I,
)
_SQL_COMMENT = re.compile(r"--[^\n]*|/\*.*?\*/", re.S)
_CONSTRAINT = re.compile(
    r"^(?:primary|foreign|unique|constraint|key|index|check)\b", re.I
)


class SchemaAssetError(ValueError):
    """Raised when a schema asset cannot be read or is malformed."""


def _dataset_key(libname: str | None, member: str) -> str:
    member = member.strip().strip('[]"`')
    if "." in member:
        return member.casefold()
    library = (libname or "").strip().strip('[]"`') or "work"
    return f"{library}.{member}".casefold()


def _column_type(value: str) -> str:
    lowered = value.strip().casefold()
    if lowered in {"1", "num", "numeric", "number", "n"}:
        return "numeric"
    if lowered in {"2", "char", "character", "c", "string"}:
        return "character"
    if _NUMERIC_SQL.match(lowered):
        return "numeric"
    if _CHARACTER_SQL.match(lowered):
        return "character"
    return "unknown"


def _to_int(value: Any) -> int | None:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def _read_text(path: Path) -> str:
    try:
        if path.stat().st_size > _MAX_ASSET_BYTES:
            raise SchemaAssetError(f"Schema asset is too large: {str(path)!r}.")
        return path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeError) as error:
        raise SchemaAssetError(f"Cannot read schema asset {str(path)!r}: {error}") from error


def _contents_csv(path: Path) -> dict[str, list[dict[str, Any]]]:
    text = _read_text(path)
    rows = list(csv.DictReader(text.splitlines()))
    if not rows:
        return {}
    headers = {name.strip().casefold(): name for name in rows[0] if name}
    if "name" not in headers or not ({"memname", "dataset", "table"} & set(headers)):
        raise SchemaAssetError(
            f"{str(path)!r} must have NAME and MEMNAME (PROC CONTENTS OUT= layout) columns."
        )
    member_header = next(h for h in ("memname", "dataset", "table") if h in headers)

    def cell(row: dict[str, str], name: str) -> str | None:
        header = headers.get(name)
        value = row.get(header) if header else None
        return value.strip() if value is not None and value.strip() else None

    datasets: dict[str, list[dict[str, Any]]] = {}
    for line_number, row in enumerate(rows, start=2):
        member = cell(row, member_header)
        name = cell(row, "name")
        if not member or not name:
            raise SchemaAssetError(f"{str(path)!r} line {line_number}: missing MEMNAME or NAME.")
        key = _dataset_key(cell(row, "libname"), member)
        column: dict[str, Any] = {
            "name": name,
            "type": _column_type(cell(row, "type") or ""),
        }
        length = _to_int(cell(row, "length"))
        if length is not None:
            column["length"] = length
        for field in ("format", "informat", "label"):
            value = cell(row, field)
            if value:
                column[field] = value
        position = _to_int(cell(row, "varnum"))
        if position is not None:
            column["position"] = position
        datasets.setdefault(key, []).append(column)
    return datasets


def _json_schema(path: Path) -> dict[str, list[dict[str, Any]]]:
    try:
        value = json.loads(_read_text(path))
    except json.JSONDecodeError as error:
        raise SchemaAssetError(f"Invalid JSON schema asset {str(path)!r}: {error}") from error
    items = value.get("datasets") if isinstance(value, dict) else None
    if not isinstance(items, list):
        raise SchemaAssetError(f"{str(path)!r} must contain a 'datasets' list.")
    datasets: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            raise SchemaAssetError(f"{str(path)!r}: each dataset needs a string name.")
        columns = item.get("columns")
        if not isinstance(columns, list) or not columns:
            raise SchemaAssetError(f"{str(path)!r}: dataset {item['name']!r} needs columns.")
        key = _dataset_key(item.get("library"), item["name"])
        converted: list[dict[str, Any]] = []
        for position, raw in enumerate(columns, start=1):
            if not isinstance(raw, dict) or not isinstance(raw.get("name"), str) or not raw["name"].strip():
                raise SchemaAssetError(f"{str(path)!r}: dataset {item['name']!r} has an invalid column.")
            column: dict[str, Any] = {
                "name": raw["name"].strip(),
                "type": _column_type(str(raw.get("type", ""))),
                "position": position,
            }
            length = _to_int(raw.get("length"))
            if length is not None:
                column["length"] = length
            for field in ("format", "informat", "label"):
                if isinstance(raw.get(field), str) and raw[field].strip():
                    column[field] = raw[field].strip()
            converted.append(column)
        datasets[key] = converted
    return datasets


def _split_top_level(body: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    if "".join(current).strip():
        parts.append("".join(current))
    return parts


def _ddl(path: Path) -> dict[str, list[dict[str, Any]]]:
    text = _SQL_COMMENT.sub(" ", _read_text(path))
    datasets: dict[str, list[dict[str, Any]]] = {}
    for match in _CREATE_TABLE.finditer(text):
        depth = 1
        index = match.end()
        while index < len(text) and depth:
            depth += {"(": 1, ")": -1}.get(text[index], 0)
            index += 1
        if depth:
            raise SchemaAssetError(f"{str(path)!r}: unbalanced CREATE TABLE definition.")
        columns: list[dict[str, Any]] = []
        for definition in _split_top_level(text[match.end() : index - 1]):
            definition = definition.strip()
            if not definition or _CONSTRAINT.match(definition):
                continue
            parsed = re.match(r"([\[\]\"`\w]+)\s+(\w+)(?:\s*\(\s*(\d+)[^)]*\))?", definition)
            if not parsed:
                raise SchemaAssetError(f"{str(path)!r}: cannot parse column {definition[:60]!r}.")
            column: dict[str, Any] = {
                "name": parsed.group(1).strip('[]"`'),
                "type": _column_type(parsed.group(2)),
                "sql_type": parsed.group(2).casefold(),
                "position": len(columns) + 1,
            }
            if parsed.group(3) and column["type"] == "character":
                column["length"] = int(parsed.group(3))
            columns.append(column)
        if columns:
            datasets[_dataset_key(None, match.group(1))] = columns
    return datasets


_LOADERS = {".csv": _contents_csv, ".json": _json_schema, ".sql": _ddl, ".ddl": _ddl}


def load_schema_assets(path: str | Path) -> tuple[dict[str, dict[str, Any]], list[dict[str, str]]]:
    """Load ``.csv`` (PROC CONTENTS), ``.json`` and ``.sql``/``.ddl`` schema files.

    Returns ``({dataset: {"columns": [...], "origin": file}}, warnings)``. When a dataset
    appears in several files the first (sorted path order) wins and a warning is recorded.
    """
    root = Path(path)
    if root.is_dir():
        files = sorted(
            (item for item in root.rglob("*") if item.suffix.casefold() in _LOADERS),
            key=lambda item: item.as_posix().casefold(),
        )
    elif root.is_file():
        files = [root]
    else:
        raise SchemaAssetError(f"Schema asset path does not exist: {str(path)!r}.")
    if root.is_file() and root.suffix.casefold() not in _LOADERS:
        raise SchemaAssetError(
            f"Unsupported schema asset type {root.suffix!r}; use .csv, .json, .sql or .ddl."
        )

    assets: dict[str, dict[str, Any]] = {}
    warnings: list[dict[str, str]] = []
    for item in files:
        loaded = _LOADERS[item.suffix.casefold()](item)
        for key, columns in loaded.items():
            seen: set[str] = set()
            for column in columns:
                lowered = column["name"].casefold()
                if lowered in seen:
                    raise SchemaAssetError(
                        f"{item.name}: dataset {key!r} repeats column {column['name']!r}."
                    )
                seen.add(lowered)
            columns.sort(key=lambda column: column.get("position", 10**9))
            if key in assets:
                warnings.append(
                    {
                        "dataset": key,
                        "kept": assets[key]["origin"],
                        "ignored": item.name,
                    }
                )
                continue
            assets[key] = {"columns": columns, "origin": item.name}
    return assets, warnings
