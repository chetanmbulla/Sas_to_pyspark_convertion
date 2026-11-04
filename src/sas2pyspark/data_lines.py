"""Rewrite DATA steps that read inline data (DATALINES/CARDS) into a marker statement.

Supported: ``INPUT`` with list input (names optionally followed by ``$``), an
optional ``INFILE DATALINES DLM='c' [DSD] [MISSOVER|TRUNCOVER]`` statement, and
whitespace- or delimiter-separated rows. The marker carries the parsed rows so the
converter can emit ``spark.createDataFrame``.
"""

from __future__ import annotations

import csv
import json
import re

from .macro_processor import MacroExpansionError, MacroStatement

DATALINES_MARKER = "__sas2pyspark_datalines"

_DATA = re.compile(r"data\s+((?:\w+\.)?\w+)", re.I)
_RAW = re.compile(r"(?:datalines|cards|lines)\Z", re.I)
_NAME = re.compile(r"[A-Za-z_]\w*\Z")
_OPTION = re.compile(
    r"(dlm|delimiter)\s*=\s*('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\")", re.I
)


def rewrite_datalines(statements: list[MacroStatement]) -> list[MacroStatement]:
    result: list[MacroStatement] = []
    index = 0
    while index < len(statements):
        statement = statements[index]
        header = _DATA.fullmatch(statement.text.strip())
        if header is None:
            result.append(statement)
            index += 1
            continue
        end = index + 1
        while end < len(statements) and statements[end].text.strip().casefold() not in {
            "run",
            "quit",
        }:
            end += 1
        body = statements[index + 1 : end]
        raw_at = next(
            (i for i, item in enumerate(body) if _RAW.fullmatch(item.text.strip())),
            None,
        )
        if raw_at is None or end >= len(statements):
            result.extend(statements[index : end + 1])
            index = end + 1
            continue
        result.extend(_rewrite(statement, header.group(1), body, raw_at))
        index = end + 1
    return result


def _rewrite(
    data: MacroStatement, target: str, body: list[MacroStatement], raw_at: int
) -> list[MacroStatement]:
    delimiter: str | None = None
    dsd = False
    columns: list[tuple[str, str]] | None = None
    for item in body[:raw_at]:
        text = item.text.strip()
        lower = text.casefold()
        if lower.startswith("infile"):
            words = lower.split()
            if len(words) < 2 or words[1] not in {"datalines", "cards"}:
                raise MacroExpansionError(
                    "INFILE is supported only as INFILE DATALINES; external files "
                    "are unsupported.",
                    item,
                )
            option = _OPTION.search(text)
            if option is not None:
                delimiter = option.group(2)[1:-1]
            dsd = bool(re.search(r"\bdsd\b", lower))
            rest = _OPTION.sub(" ", lower[len(words[0]) + len(words[1]) + 1 :])
            leftover = re.sub(r"\b(dsd|missover|truncover)\b", " ", rest).split()
            if leftover:
                raise MacroExpansionError(
                    f"Unsupported INFILE option: {leftover[0]!r}.", item
                )
            if dsd and delimiter is None:
                delimiter = ","
        elif lower.startswith("input"):
            if columns is not None:
                raise MacroExpansionError("Only one INPUT statement is supported.", item)
            columns = _parse_input(text[5:], item)
        else:
            raise MacroExpansionError(
                "Only INFILE and INPUT may precede DATALINES in an inline-data "
                "DATA step.",
                item,
            )
    if columns is None:
        raise MacroExpansionError("DATALINES requires an INPUT statement.", data)
    if len(body) != raw_at + 2:
        raise MacroExpansionError(
            "DATALINES must be the last statement of the DATA step; other "
            "statements after it are unsupported.",
            data,
        )
    rows = _parse_rows(body[raw_at + 1], columns, delimiter, dsd)
    if target.casefold() == "_null_":
        return []
    payload = json.dumps({"name": target, "columns": columns, "rows": rows})
    return [MacroStatement(f"{DATALINES_MARKER} {payload}", data.line)]


def _parse_input(text: str, item: MacroStatement) -> list[tuple[str, str]]:
    tokens = re.findall(r"\$|[^\s$]+", text)
    columns: list[tuple[str, str]] = []
    for token in tokens:
        if token == "$":
            if not columns or columns[-1][1] == "char":
                raise MacroExpansionError("Misplaced $ in INPUT statement.", item)
            columns[-1] = (columns[-1][0], "char")
        elif _NAME.fullmatch(token):
            columns.append((token, "num"))
        else:
            raise MacroExpansionError(
                f"INPUT supports list input only (names with optional $); found "
                f"{token!r}. Informats, column ranges and pointer controls are "
                "unsupported.",
                item,
            )
    if not columns:
        raise MacroExpansionError("INPUT requires at least one variable.", item)
    names = [name.casefold() for name, _ in columns]
    if len(set(names)) != len(names):
        raise MacroExpansionError("INPUT variable names must be distinct.", item)
    return columns


def _parse_rows(
    raw: MacroStatement,
    columns: list[tuple[str, str]],
    delimiter: str | None,
    dsd: bool,
) -> list[list[object]]:
    rows: list[list[object]] = []
    for line in raw.text.splitlines():
        if not line.strip():
            continue
        if delimiter is None:
            fields = line.split()
        elif dsd:
            fields = next(csv.reader([line], delimiter=delimiter))
        else:
            fields = line.split(delimiter)
        if len(fields) > len(columns):
            raise MacroExpansionError(
                f"Data line has {len(fields)} values but INPUT lists "
                f"{len(columns)} variables: {line.strip()!r}.",
                raw,
            )
        row: list[object] = []
        for position, (name, kind) in enumerate(columns):
            value = fields[position].strip() if position < len(fields) else ""
            if kind == "char":
                row.append(value or None)
                continue
            if value in {"", "."}:
                row.append(None)
                continue
            try:
                row.append(float(value))
            except ValueError:
                raise MacroExpansionError(
                    f"Invalid numeric value {value!r} for variable {name} "
                    f"(add $ after the name for character data).",
                    raw,
                ) from None
        rows.append(row)
    return rows
