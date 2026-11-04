"""Support dataset options on SET inputs: WHERE=, FIRSTOBS=, OBS=, KEEP=, DROP=, RENAME=.

Each optioned input becomes a marker statement that the converter turns into an
intermediate frame; the SET statement then reads that frame. Options are applied in
this order: WHERE, FIRSTOBS/OBS (counted over the filtered rows), KEEP/DROP, RENAME.
"""

from __future__ import annotations

import json
import re

from .macro_processor import MacroExpansionError, MacroStatement

SETOPTS_MARKER = "__sas2pyspark_setopts"
_DATA = re.compile(r"data\s+((?:\w+\.)?\w+)", re.I)
_NAME = r"[A-Za-z_]\w*"
_KEY = re.compile(rf"\s*({_NAME})\s*=\s*")
_QUOTED = re.compile(r"('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\")")


def rewrite_set_options(statements: list[MacroStatement]) -> list[MacroStatement]:
    result: list[MacroStatement] = []
    counter = 0
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
        body = list(statements[index + 1 : end])
        markers: list[MacroStatement] = []
        for position, item in enumerate(body):
            text = item.text.strip()
            if not re.match(r"set\s", text, re.I) or "(" not in _QUOTED.sub("''", text):
                continue
            names: list[str] = []
            for source, options in _split_inputs(text[3:], item):
                if options is None:
                    names.append(source)
                    continue
                counter += 1
                helper = f"__sas2pyspark_set{counter}"
                payload = _parse_options(source, helper, options, item)
                markers.append(
                    MacroStatement(f"{SETOPTS_MARKER} {json.dumps(payload)}", item.line)
                )
                names.append(helper)
            body[position] = MacroStatement(f"set {' '.join(names)}", item.line)
        result.extend(markers)
        result.append(statement)
        result.extend(body)
        if end < len(statements):
            result.append(statements[end])
        index = end + 1
    return result


def _split_inputs(
    text: str, item: MacroStatement
) -> list[tuple[str, str | None]]:
    inputs: list[tuple[str, str | None]] = []
    position = 0
    while position < len(text):
        match = re.compile(rf"\s*((?:{_NAME}\.)?{_NAME})\s*").match(text, position)
        if match is None:
            raise MacroExpansionError(f"Unsupported SET input: {text.strip()!r}.", item)
        position = match.end()
        options: str | None = None
        if position < len(text) and text[position] == "(":
            closing = _matching(text, position, item)
            options = text[position + 1 : closing]
            position = closing + 1
        inputs.append((match.group(1), options))
    return inputs


def _matching(text: str, opening: int, item: MacroStatement) -> int:
    depth = 0
    quote: str | None = None
    for position in range(opening, len(text)):
        char = text[position]
        if quote:
            if char == quote:
                quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return position
    raise MacroExpansionError("Unbalanced parentheses in dataset options.", item)


def _parse_options(
    source: str, helper: str, text: str, item: MacroStatement
) -> dict[str, object]:
    payload: dict[str, object] = {
        "name": helper,
        "source": source,
        "where": None,
        "firstobs": None,
        "obs": None,
        "keep": [],
        "drop": [],
        "rename": [],
    }
    position = 0
    while position < len(text):
        if not text[position:].strip():
            break
        key = _KEY.match(text, position)
        if key is None:
            raise MacroExpansionError(f"Malformed dataset options: {text.strip()!r}.", item)
        name = key.group(1).casefold()
        position = key.end()
        if position < len(text) and text[position] == "(":
            closing = _matching(text, position, item)
            value = text[position + 1 : closing].strip()
            position = closing + 1
        else:
            following = re.compile(rf"\s+{_NAME}\s*=").search(text, position)
            stop = following.start() if following else len(text)
            value = text[position:stop].strip()
            position = stop
        if name == "where":
            payload["where"] = value
        elif name in {"firstobs", "obs"}:
            if name == "obs" and value.casefold() == "max":
                continue
            if not value.isdigit() or int(value) < 1 and name == "firstobs":
                raise MacroExpansionError(
                    f"{name.upper()}= requires a positive integer.", item
                )
            payload[name] = int(value)
        elif name in {"keep", "drop"}:
            columns = value.split()
            if not columns or any(not re.fullmatch(_NAME, column) for column in columns):
                raise MacroExpansionError(
                    f"{name.upper()}= requires a list of variable names.", item
                )
            payload[name] = columns
        elif name == "rename":
            pairs = re.findall(rf"({_NAME})\s*=\s*({_NAME})", value)
            rebuilt = " ".join(f"{old}={new}" for old, new in pairs)
            if not pairs or re.sub(r"\s*=\s*", "=", " ".join(value.split())) != rebuilt:
                raise MacroExpansionError(
                    "RENAME= requires pairs such as (old=new).", item
                )
            payload["rename"] = [list(pair) for pair in pairs]
        else:
            raise MacroExpansionError(
                f"Dataset option {name.upper()}= is unsupported; supported options are "
                "WHERE=, FIRSTOBS=, OBS=, KEEP=, DROP= and RENAME=.",
                item,
            )
    if payload["keep"] and payload["drop"]:
        raise MacroExpansionError("KEEP= and DROP= cannot be combined.", item)
    return payload
