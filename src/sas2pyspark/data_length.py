"""Support LENGTH and ATTRIB statements in DATA steps.

Character lengths (``$n``) truncate the variable with ``substr`` after the step's
assignments. Numeric lengths, labels, formats and informats have no Spark equivalent and
are ignored. A variable that is never assigned in the step is not created.
"""

from __future__ import annotations

import re

from .macro_processor import MacroExpansionError, MacroStatement

_DATA = re.compile(r"data\s+((?:\w+\.)?\w+)", re.I)
_VALUE = re.compile(
    r"\s*(?:([A-Za-z_]\w*)\s*=\s*('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|\S+)|(\$)|(\d+)\.?|([A-Za-z_]\w*))"
)
_IGNORED = {"label", "format", "informat", "transcode"}


def rewrite_length(statements: list[MacroStatement]) -> list[MacroStatement]:
    result: list[MacroStatement] = []
    index = 0
    while index < len(statements):
        statement = statements[index]
        if _DATA.fullmatch(statement.text.strip()) is None:
            result.append(statement)
            index += 1
            continue
        end = index + 1
        while end < len(statements) and statements[end].text.strip().casefold() not in {
            "run",
            "quit",
        }:
            end += 1
        closer = statements[end] if end < len(statements) else None
        result.append(statement)
        result.extend(_rewrite_step(list(statements[index + 1 : end]), closer))
        if closer is not None:
            result.append(closer)
        index = end + 1
    return result


def _parse_length(item: MacroStatement) -> dict[str, int]:
    text = item.text.strip()[len("length") :]
    lengths: dict[str, int] = {}
    names: list[str] = []
    character = False
    position = 0
    while position < len(text):
        if not text[position:].strip():
            break
        token = _VALUE.match(text, position)
        if token is None:
            raise MacroExpansionError(f"Unsupported LENGTH syntax: {text.strip()!r}.", item)
        position = token.end()
        key, value, dollar, number, name = token.groups()
        if key is not None:
            if key.casefold() != "default":
                raise MacroExpansionError(f"Unsupported LENGTH option {key!r}.", item)
        elif dollar:
            character = True
        elif number is not None:
            if not names:
                raise MacroExpansionError("LENGTH size must follow a variable name.", item)
            if character:
                for variable in names:
                    lengths[variable] = int(number)
            names, character = [], False
        else:
            names.append(name)
    if names:
        raise MacroExpansionError("Every LENGTH variable needs a size.", item)
    return lengths


def _parse_attrib(item: MacroStatement) -> dict[str, int]:
    text = item.text.strip()[len("attrib") :]
    lengths: dict[str, int] = {}
    names: list[str] = []
    seen_option = False
    position = 0
    while position < len(text):
        if not text[position:].strip():
            break
        token = _VALUE.match(text, position)
        if token is None or token.group(3) or token.group(4):
            raise MacroExpansionError(f"Unsupported ATTRIB syntax: {text.strip()!r}.", item)
        position = token.end()
        key, value, _, _, name = token.groups()
        if name is not None:
            if seen_option:
                names, seen_option = [], False
            names.append(name)
            continue
        seen_option = True
        option = key.casefold()
        if option == "length":
            size = re.fullmatch(r"(\$)?(\d+)\.?", value)
            if size is None:
                raise MacroExpansionError("ATTRIB LENGTH= must look like $8 or 8.", item)
            if size.group(1):
                for variable in names:
                    lengths[variable] = int(size.group(2))
        elif option not in _IGNORED:
            raise MacroExpansionError(f"Unsupported ATTRIB option {key!r}.", item)
    return lengths


def _rewrite_step(
    body: list[MacroStatement], closer: MacroStatement | None
) -> list[MacroStatement]:
    lengths: dict[str, int] = {}
    kept: list[MacroStatement] = []
    for item in body:
        text = item.text.strip()
        if re.match(r"length\s", text, re.I):
            lengths.update(_parse_length(item))
        elif re.match(r"attrib\s", text, re.I):
            lengths.update(_parse_attrib(item))
        else:
            kept.append(item)
    if not lengths:
        return kept
    assigned = {
        match.group(1).casefold()
        for item in kept
        if (match := re.match(r"(?:.*?\bthen\s+)?([A-Za-z_]\w*)\s*=(?!=)", item.text.strip(), re.I | re.S))
    }
    for item in kept:
        call = re.fullmatch(
            r"call\s+missing\s*\(([^()]*)\)", item.text.strip(), re.I | re.S
        )
        if call is not None:
            assigned.update(name.strip().casefold() for name in call.group(1).split(","))
    truncations = [
        MacroStatement(f"{name} = substr({name}, 1, {size})", closer.line if closer else 0)
        for name, size in lengths.items()
        if name.casefold() in assigned
    ]
    loop = next(
        (
            position
            for position, item in enumerate(kept)
            if re.match(r"do\s", item.text.strip(), re.I)
        ),
        len(kept),
    )
    return kept[:loop] + truncations + kept[loop:]
