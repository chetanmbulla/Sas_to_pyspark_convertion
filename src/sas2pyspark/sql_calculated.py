"""Rewrite SAS PROC SQL ``CALCULATED alias`` references for Spark SQL."""

from __future__ import annotations

import re

_QUOTED = re.compile(r"('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\")")
_CALCULATED = re.compile(r"\bcalculated\s+([A-Za-z_]\w*)", re.I)
_ALIAS = re.compile(r"(.+?)\s+as\s+([A-Za-z_]\w*)\Z", re.I | re.S)


def _top_level_select_items(query: str) -> list[str] | None:
    """Return the first SELECT list's items, or None when it cannot be located."""
    masked = _QUOTED.sub(lambda match: "x" * len(match.group(0)), query)
    start = re.search(r"\bselect\b", masked, re.I)
    if start is None:
        return None
    depth = 0
    items: list[str] = []
    item_start = start.end()
    index = start.end()
    while index < len(masked):
        char = masked[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0:
            if char == ",":
                items.append(query[item_start:index])
                item_start = index + 1
            elif re.match(r"from\b", masked[index:], re.I) and (
                index == 0 or not masked[index - 1].isalnum() and masked[index - 1] != "_"
            ):
                items.append(query[item_start:index])
                return items
        index += 1
    return None


def rewrite_calculated(query: str) -> str:
    """Replace ``CALCULATED alias`` with the aliased expression in parentheses."""
    if _CALCULATED.search(_QUOTED.sub("''", query)) is None:
        return query
    items = _top_level_select_items(query)
    if items is None:
        raise ValueError("CALCULATED requires a SELECT ... FROM query.")
    aliases: dict[str, str] = {}
    for item in items:
        match = _ALIAS.fullmatch(item.strip())
        if match is None:
            continue
        expression = _replace(match.group(1), aliases)
        aliases[match.group(2).casefold()] = expression
    return _replace(query, aliases)


def _replace(text: str, aliases: dict[str, str]) -> str:
    def substitute(match: re.Match[str]) -> str:
        key = match.group(1).casefold()
        if key not in aliases:
            raise ValueError(
                f"CALCULATED {match.group(1)} does not refer to an earlier "
                "SELECT alias."
            )
        return f"({aliases[key]})"

    parts = _QUOTED.split(text)
    for index in range(0, len(parts), 2):
        parts[index] = _CALCULATED.sub(substitute, parts[index])
    return "".join(parts)
