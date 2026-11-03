"""Rewrite DATA-step IF/THEN DO blocks and THEN DELETE into simple conditionals."""

from __future__ import annotations

import re

from .macro_processor import MacroExpansionError, MacroStatement

_IF_DO = re.compile(r"if\s+(.+?)\s+then\s+do", re.I | re.S)
_IF_DELETE = re.compile(r"if\s+(.+?)\s+then\s+delete", re.I | re.S)
_ELSE_IF_DO = re.compile(r"else\s+if\s+(.+?)\s+then\s+do", re.I | re.S)
_ELSE_IF_ACTION = re.compile(r"else\s+if\s+(.+?)\s+then\s+(.+)", re.I | re.S)
_ELSE_DO = re.compile(r"else\s+do", re.I)
_ELSE_ACTION = re.compile(r"else\s+(.+)", re.I | re.S)
_ASSIGNED = re.compile(r"([A-Za-z_]\w*)\s*=(?!=)")
_NESTED = re.compile(r"(?:do|select|if|else|end)\b", re.I)

_Branch = tuple[str | None, list[MacroStatement]]


def rewrite_if_blocks(statements: list[MacroStatement]) -> list[MacroStatement]:
    output: list[MacroStatement] = []
    in_data_step = False
    index = 0
    while index < len(statements):
        statement = statements[index]
        text = statement.text.strip()
        lower = text.casefold()
        if lower.startswith("data "):
            in_data_step = True
        elif lower in {"run", "quit"}:
            in_data_step = False
        if not in_data_step:
            output.append(statement)
            index += 1
            continue
        if _IF_DO.fullmatch(text):
            branches, index = _read_chain(statements, index)
            output.extend(_emit_chain(branches, statement))
        elif (delete := _IF_DELETE.fullmatch(text)) is not None:
            output.append(
                MacroStatement(_delete_filter(delete.group(1)), statement.line)
            )
            index += 1
        else:
            output.append(statement)
            index += 1
    return output


def _read_body(
    statements: list[MacroStatement], index: int, opener: MacroStatement
) -> tuple[list[MacroStatement], int]:
    body: list[MacroStatement] = []
    while index < len(statements):
        text = statements[index].text.strip()
        if text.casefold() == "end":
            return body, index + 1
        if _NESTED.match(text):
            raise MacroExpansionError(
                "IF THEN DO blocks support only simple assignments and DELETE; "
                "nested DO, SELECT and IF/ELSE statements are unsupported.",
                statements[index],
            )
        body.append(statements[index])
        index += 1
    raise MacroExpansionError("IF THEN DO block is missing END.", opener)


def _read_chain(
    statements: list[MacroStatement], index: int
) -> tuple[list[_Branch], int]:
    opener = statements[index]
    match = _IF_DO.fullmatch(opener.text.strip())
    assert match is not None
    body, index = _read_body(statements, index + 1, opener)
    branches: list[_Branch] = [(match.group(1), body)]
    while index < len(statements):
        current = statements[index]
        text = current.text.strip()
        if not text.casefold().startswith("else"):
            break
        if (found := _ELSE_IF_DO.fullmatch(text)) is not None:
            body, index = _read_body(statements, index + 1, current)
            branches.append((found.group(1), body))
        elif (found := _ELSE_IF_ACTION.fullmatch(text)) is not None:
            branches.append(
                (found.group(1), [MacroStatement(found.group(2), current.line)])
            )
            index += 1
        elif _ELSE_DO.fullmatch(text):
            body, index = _read_body(statements, index + 1, current)
            branches.append((None, body))
            break
        elif (found := _ELSE_ACTION.fullmatch(text)) is not None:
            branches.append((None, [MacroStatement(found.group(1), current.line)]))
            index += 1
            break
        else:
            break
    return branches, index


def _delete_filter(condition: str) -> str:
    return f"if coalesce(not ({condition}), true)"


def _emit_chain(
    branches: list[_Branch], opener: MacroStatement
) -> list[MacroStatement]:
    assigned = {
        target.group(1).casefold()
        for _, body in branches
        for item in body
        if (target := _ASSIGNED.match(item.text.strip())) is not None
    }
    for condition, _ in branches:
        if condition is None:
            continue
        for name in assigned:
            if re.search(rf"\b{re.escape(name)}\b", condition, re.IGNORECASE):
                raise MacroExpansionError(
                    f"IF THEN DO condition references {name!r}, which is assigned "
                    "inside the block; later branches would observe the new value. "
                    "Assign to a different column or restructure the condition.",
                    opener,
                )
    output: list[MacroStatement] = []
    previous: list[str] = []
    for condition, body in branches:
        parts = [f"coalesce(not ({item}), true)" for item in previous]
        if condition is not None:
            parts.append(f"({condition})")
            previous.append(condition)
        effective = " and ".join(parts)
        for item in body:
            action = item.text.strip()
            if action.casefold() == "delete":
                output.append(MacroStatement(_delete_filter(effective), item.line))
            else:
                output.append(
                    MacroStatement(f"if {effective} then {action}", item.line)
                )
    return output
