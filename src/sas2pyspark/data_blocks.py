"""Rewrite DATA-step IF/THEN DO blocks, SELECT blocks and THEN DELETE into guarded statements.

Blocks may nest: every statement in a block becomes ``if <effective condition> then
<statement>`` where the effective condition combines all enclosing branch conditions
and the negation of earlier branches in the same chain.
"""

from __future__ import annotations

import re

from .macro_processor import MacroExpansionError, MacroStatement

_IF_DO = re.compile(r"if\s+(.+?)\s+then\s+do", re.I | re.S)
_IF_DELETE = re.compile(r"if\s+(.+?)\s+then\s+delete", re.I | re.S)
_IF_ACTION = re.compile(r"if\s+(.+?)\s+then\s+(?!do\b)(.+)", re.I | re.S)
_ELSE_IF_DO = re.compile(r"else\s+if\s+(.+?)\s+then\s+do", re.I | re.S)
_ELSE_IF_ACTION = re.compile(r"else\s+if\s+(.+?)\s+then\s+(?!do\b)(.+)", re.I | re.S)
_ELSE_DO = re.compile(r"else\s+do", re.I)
_ELSE_ACTION = re.compile(r"else\s+(?!do\b)(.+)", re.I | re.S)
_ASSIGNED = re.compile(r"([A-Za-z_]\w*)\s*=(?!=)")
_SELECT = re.compile(r"select(?:\s*\((.*)\))?", re.I | re.S)
_WHEN = re.compile(r"when\s*\(", re.I)
_OTHERWISE = re.compile(r"otherwise\b\s*(.*)", re.I | re.S)

_Entry = tuple[str | None, MacroStatement]
_Branch = tuple[str | None, list[_Entry]]


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
            output.extend(_to_statements(_flatten(branches, statement)))
        elif _needs_select_rewrite(statements, index):
            branches, index = _read_select(statements, index)
            output.extend(_to_statements(_flatten(branches, statement)))
        elif (delete := _IF_DELETE.fullmatch(text)) is not None:
            output.append(
                MacroStatement(_delete_filter(delete.group(1)), statement.line)
            )
            index += 1
        else:
            output.append(statement)
            index += 1
    return output


def _needs_select_rewrite(statements: list[MacroStatement], index: int) -> bool:
    match = _SELECT.fullmatch(statements[index].text.strip())
    if match is None:
        return False
    if match.group(1) is not None:
        return True
    for item in statements[index + 1 :]:
        text = item.text.strip()
        if text.casefold() == "end":
            return False
        if re.search(r"\bdo$", text, re.I):
            return True
    return False


def _to_statements(entries: list[_Entry]) -> list[MacroStatement]:
    output: list[MacroStatement] = []
    for condition, item in entries:
        action = item.text.strip()
        if action.casefold() == "delete":
            output.append(
                MacroStatement(_delete_filter(condition or "1 = 1"), item.line)
            )
        elif condition is None:
            output.append(item)
        else:
            output.append(MacroStatement(f"if {condition} then {action}", item.line))
    return output


def _read_body(
    statements: list[MacroStatement], index: int, opener: MacroStatement
) -> tuple[list[_Entry], int]:
    entries: list[_Entry] = []
    while index < len(statements):
        item = statements[index]
        text = item.text.strip()
        lower = text.casefold()
        if lower == "end":
            return entries, index + 1
        if (delete := _IF_DELETE.fullmatch(text)) is not None:
            entries.append((f"({delete.group(1)})", MacroStatement("delete", item.line)))
            index += 1
        elif _IF_DO.fullmatch(text) or _IF_ACTION.fullmatch(text):
            branches, index = _read_chain(statements, index)
            entries.extend(_flatten(branches, item))
        elif _SELECT.fullmatch(text):
            branches, index = _read_select(statements, index)
            entries.extend(_flatten(branches, item))
        elif re.match(r"(?:do|else|if|when|otherwise)\b", text, re.I):
            raise MacroExpansionError(
                "Iterative DO loops, stray ELSE/WHEN statements and subsetting IF "
                "statements are unsupported inside IF THEN DO and SELECT blocks.",
                item,
            )
        else:
            entries.append((None, item))
            index += 1
    raise MacroExpansionError("DO block is missing END.", opener)


def _read_chain(
    statements: list[MacroStatement], index: int
) -> tuple[list[_Branch], int]:
    opener = statements[index]
    text = opener.text.strip()
    branches: list[_Branch] = []
    if (match := _IF_DO.fullmatch(text)) is not None:
        body, index = _read_body(statements, index + 1, opener)
        branches.append((match.group(1), body))
    else:
        match = _IF_ACTION.fullmatch(text)
        assert match is not None
        branches.append(
            (match.group(1), [(None, MacroStatement(match.group(2), opener.line))])
        )
        index += 1
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
                (found.group(1), [(None, MacroStatement(found.group(2), current.line))])
            )
            index += 1
        elif _ELSE_DO.fullmatch(text):
            body, index = _read_body(statements, index + 1, current)
            branches.append((None, body))
            break
        elif (found := _ELSE_ACTION.fullmatch(text)) is not None:
            branches.append(
                (None, [(None, MacroStatement(found.group(1), current.line))])
            )
            index += 1
            break
        else:
            break
    return branches, index


def _read_select(
    statements: list[MacroStatement], index: int
) -> tuple[list[_Branch], int]:
    opener = statements[index]
    match = _SELECT.fullmatch(opener.text.strip())
    assert match is not None
    subject = match.group(1)
    index += 1
    branches: list[_Branch] = []
    while index < len(statements):
        current = statements[index]
        text = current.text.strip()
        if text.casefold() == "end":
            if not branches:
                raise MacroExpansionError("SELECT requires at least one WHEN.", opener)
            return branches, index + 1
        if (when := _WHEN.match(text)) is not None:
            closing = _closing(text, when.end() - 1, current)
            values = _split_commas(text[when.end() : closing])
            action = text[closing + 1 :].strip()
            if not values or not action:
                raise MacroExpansionError(
                    "WHEN requires a value list and a statement.", current
                )
            if subject is None:
                condition = " or ".join(f"({value})" for value in values)
            elif len(values) == 1:
                condition = f"({subject}) = ({values[0]})"
            else:
                condition = f"({subject}) in ({', '.join(values)})"
            entries, index = _read_action(statements, index, action)
            branches.append((condition, entries))
        elif (other := _OTHERWISE.fullmatch(text)) is not None:
            entries, index = _read_action(statements, index, other.group(1).strip())
            branches.append((None, entries))
        else:
            raise MacroExpansionError(
                "SELECT supports only WHEN, OTHERWISE and END statements.", current
            )
    raise MacroExpansionError("SELECT is missing END.", opener)


def _read_action(
    statements: list[MacroStatement], index: int, action: str
) -> tuple[list[_Entry], int]:
    current = statements[index]
    if action.casefold() == "do":
        return _read_body(statements, index + 1, current)
    if not action:
        return [], index + 1
    return [(None, MacroStatement(action, current.line))], index + 1


def _closing(text: str, opening: int, item: MacroStatement) -> int:
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
    raise MacroExpansionError("Unbalanced parentheses in WHEN.", item)


def _split_commas(text: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    quote: str | None = None
    for char in text:
        if quote:
            current.append(char)
            if char == quote:
                quote = None
        elif char in {"'", '"'}:
            quote = char
            current.append(char)
        elif char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if "".join(current).strip():
        parts.append("".join(current).strip())
    return parts


def _delete_filter(condition: str) -> str:
    return f"if coalesce(not ({condition}), true)"


def _and(parts: list[str]) -> str | None:
    return " and ".join(f"({part})" for part in parts) if parts else None


def _flatten(branches: list[_Branch], opener: MacroStatement) -> list[_Entry]:
    assigned = {
        target.group(1).casefold()
        for _, body in branches
        for _, item in body
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
    entries: list[_Entry] = []
    previous: list[str] = []
    for condition, body in branches:
        parts = [f"coalesce(not ({item}), true)" for item in previous]
        if condition is not None:
            parts.append(condition)
            previous.append(condition)
        for inner, item in body:
            combined = _and([*parts, *([inner] if inner else [])])
            entries.append((combined, item))
    return entries
