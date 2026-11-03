"""Split DATA steps that route rows with OUTPUT into one DATA step per target.

Supported shape: ordinary DATA-step statements followed by a trailing block of
routing statements (``IF c THEN OUTPUT t;``, ``ELSE IF ...``, ``ELSE OUTPUT t;``,
``OUTPUT t;``). Each target becomes its own DATA step with a subsetting IF.
"""

from __future__ import annotations

import re

from .macro_processor import MacroExpansionError, MacroStatement

_DATA = re.compile(r"data\s+((?:(?:\w+\.)?\w+\s*)+)", re.I)
_NAME = r"(?:\w+\.)?\w+"
_NAMES = rf"(?:\s+({_NAME}(?:\s+{_NAME})*))?"
_IF_OUTPUT = re.compile(rf"if\s+(.+?)\s+then\s+output{_NAMES}", re.I | re.S)
_ELSE_IF_OUTPUT = re.compile(rf"else\s+if\s+(.+?)\s+then\s+output{_NAMES}", re.I | re.S)
_ELSE_OUTPUT = re.compile(rf"else\s+output{_NAMES}", re.I)
_PLAIN_OUTPUT = re.compile(rf"output{_NAMES}", re.I)
_MENTIONS_OUTPUT = re.compile(r"(?:.*\bthen\s+|else\s+)?output\b", re.I | re.S)

_Branch = tuple[str | None, tuple[str, ...]]


def rewrite_outputs(statements: list[MacroStatement]) -> list[MacroStatement]:
    result: list[MacroStatement] = []
    index = 0
    while index < len(statements):
        statement = statements[index]
        header = _DATA.fullmatch(statement.text.strip())
        if header is None or "=" in statement.text:
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
        body_keywords = {"do", "end", "select"}
        if end >= len(statements) or any(
            item.text.strip().split(None, 1)[0].casefold().rstrip(";") in body_keywords
            for item in body
            if item.text.strip()
        ) or not any(
            _MENTIONS_OUTPUT.match(item.text.strip()) for item in body
        ):
            result.extend(statements[index : end + 1])
            index = end + 1
            continue
        result.extend(_split(statement, header.group(1).split(), body, statements[end]))
        index = end + 1
    return result


def _split(
    data: MacroStatement,
    targets: list[str],
    body: list[MacroStatement],
    run: MacroStatement,
) -> list[MacroStatement]:
    lowered = [target.casefold() for target in targets]
    if len(set(lowered)) != len(lowered):
        raise MacroExpansionError("DATA statement output names must be distinct.", data)
    first = next(
        i for i, item in enumerate(body) if _MENTIONS_OUTPUT.match(item.text.strip())
    )
    prelude = body[:first]
    chains: list[list[_Branch]] = []
    for item in body[first:]:
        text = item.text.strip()
        if (found := _IF_OUTPUT.fullmatch(text)) is not None:
            chains.append([(found.group(1), _names(found.group(2), targets, item))])
        elif (found := _ELSE_IF_OUTPUT.fullmatch(text)) is not None:
            _extend(chains, item).append(
                (found.group(1), _names(found.group(2), targets, item))
            )
        elif (found := _ELSE_OUTPUT.fullmatch(text)) is not None:
            _extend(chains, item).append((None, _names(found.group(1), targets, item)))
        elif (found := _PLAIN_OUTPUT.fullmatch(text)) is not None:
            chains.append([(None, _names(found.group(1), targets, item))])
        else:
            raise MacroExpansionError(
                "OUTPUT routing must be the last statements of the DATA step: "
                "only IF/ELSE IF/ELSE ... OUTPUT and OUTPUT statements may follow.",
                item,
            )

    selection: dict[str, list[str] | None] = {}
    owner: dict[str, int] = {}
    for chain_index, chain in enumerate(chains):
        previous: list[str] = []
        for condition, names in chain:
            guards = [f"coalesce(not ({old}), true)" for old in previous]
            parts = ([f"({condition})"] if condition is not None else []) + guards
            effective = " and ".join(parts)
            for name in names:
                key = name.casefold().split(".")[-1]
                if owner.setdefault(key, chain_index) != chain_index:
                    raise MacroExpansionError(
                        f"Dataset {name!r} is written by more than one OUTPUT "
                        "routing group; duplicate output rows are unsupported.",
                        data,
                    )
                current = selection.get(key, [])
                if not effective:
                    selection[key] = None
                elif current is not None:
                    current.append(f"({effective})")
                    selection[key] = current
            if condition is not None:
                previous.append(condition)

    output: list[MacroStatement] = []
    for target in targets:
        key = target.casefold().split(".")[-1]
        if key not in selection:
            raise MacroExpansionError(
                f"Dataset {target!r} is never written by an OUTPUT statement.", data
            )
        output.append(MacroStatement(f"data {target}", data.line))
        output.extend(prelude)
        condition = selection[key]
        if condition is not None:
            output.append(MacroStatement(f"if {' or '.join(condition)}", data.line))
        output.append(run)
    return output


def _extend(chains: list[list[_Branch]], item: MacroStatement) -> list[_Branch]:
    if not chains or _closed(chains[-1]):
        raise MacroExpansionError("ELSE has no preceding IF ... THEN OUTPUT.", item)
    return chains[-1]


def _closed(chain: list[_Branch]) -> bool:
    return chain[-1][0] is None


def _names(
    group: str | None, targets: list[str], item: MacroStatement
) -> tuple[str, ...]:
    if group is None:
        return tuple(targets)
    names = group.split()
    known = {target.casefold().split(".")[-1] for target in targets}
    for name in names:
        if name.casefold().split(".")[-1] not in known:
            raise MacroExpansionError(
                f"OUTPUT dataset {name!r} is not listed in the DATA statement.", item
            )
    return tuple(names)
