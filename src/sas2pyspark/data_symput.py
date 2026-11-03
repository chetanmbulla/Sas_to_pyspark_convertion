"""Rewrite ``CALL SYMPUT``/``SYMPUTX`` DATA-step statements into run-time assignments.

Each call becomes a small derived DATA step that keeps one column holding the
value expression, followed by a marker statement. The converter turns the marker
into a Spark session variable set from the last row of that step, matching SAS's
"last assignment wins" behaviour for the observation order of the frame.
"""

from __future__ import annotations

import re

from .macro_processor import MacroExpansionError, MacroStatement, _split_top_level

SYMPUT_MARKER = "__sas2pyspark_symput"
VALUE_COLUMN = "__sas2pyspark_value"

_DATA = re.compile(r"data\s+((?:\w+\.)?\w+)", re.I)
_MENTIONS_CALL = re.compile(r"(?:.*\bthen\s+)?call\s+symput", re.I | re.S)
_CALL = re.compile(
    r"(?:if\s+(?P<cond>.+?)\s+then\s+)?call\s+symputx?\s*\((?P<args>.*)\)\Z",
    re.I | re.S,
)
_BLOCK_KEYWORDS = {"do", "end", "select", "else"}


def rewrite_symput(statements: list[MacroStatement]) -> list[MacroStatement]:
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
        if end >= len(statements) or not any(
            _MENTIONS_CALL.match(item.text.strip()) for item in body
        ):
            result.extend(statements[index : end + 1])
            index = end + 1
            continue
        result.extend(_rewrite(statement, header.group(1), body, statements[end]))
        index = end + 1
    return result


def _rewrite(
    data: MacroStatement,
    target: str,
    body: list[MacroStatement],
    run: MacroStatement,
) -> list[MacroStatement]:
    for item in body:
        words = item.text.strip().split(None, 1)
        if words and words[0].casefold().rstrip(";") in _BLOCK_KEYWORDS:
            raise MacroExpansionError(
                "CALL SYMPUT is unsupported in DATA steps that use DO, SELECT or "
                "ELSE blocks.",
                item,
            )
    output: list[MacroStatement] = []
    if target.casefold() != "_null_":
        output.append(data)
        output.extend(
            item for item in body if not _MENTIONS_CALL.match(item.text.strip())
        )
        output.append(run)
    for position, item in enumerate(body):
        text = item.text.strip()
        if not _MENTIONS_CALL.match(text):
            continue
        call = _CALL.fullmatch(text)
        arguments = (
            _split_top_level(call.group("args"), item) if call is not None else []
        )
        name = arguments[0].strip() if arguments else ""
        if (
            call is None
            or len(arguments) not in {2, 3}
            or not re.fullmatch(r"(['\"])[A-Za-z_]\w*\1", name)
        ):
            raise MacroExpansionError(
                "CALL SYMPUT/SYMPUTX requires a literal macro variable name and a "
                "value expression, for example CALL SYMPUTX('total', sum_amount).",
                item,
            )
        variable = name[1:-1].casefold()
        derived = f"__sas2pyspark_symput_{variable}_{position}"
        output.append(MacroStatement(f"data {derived}", data.line))
        output.extend(
            earlier
            for earlier in body[:position]
            if not _MENTIONS_CALL.match(earlier.text.strip())
        )
        if call.group("cond"):
            output.append(MacroStatement(f"if {call.group('cond')}", item.line))
        output.append(MacroStatement(f"{VALUE_COLUMN} = {arguments[1].strip()}", item.line))
        output.append(MacroStatement(f"keep {VALUE_COLUMN}", item.line))
        output.append(run)
        output.append(MacroStatement(f"{SYMPUT_MARKER} {variable} {derived}", item.line))
    return output
