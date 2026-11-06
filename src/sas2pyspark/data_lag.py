"""Support LAG/DIF in DATA steps via helper columns computed by Spark window functions.

Each ``LAGn(expr)`` or ``DIFn(expr)`` call is replaced by a helper column that is
assigned immediately before the statement that uses it, so earlier assignments and
subsetting statements are visible, as in SAS. An explicit DATA-step ORDER BY key
is required because Spark does not preserve SAS observation order.
"""

from __future__ import annotations

import re

from .data_order import OrderByError, order_by_sql, parse_order_by
from .macro_processor import MacroExpansionError, MacroStatement

_DATA = re.compile(r"data\s+((?:\w+\.)?\w+)", re.I)
_CALL = re.compile(r"\b(lag|dif)(\d*)\s*\(", re.I)
_QUOTED = re.compile(r"('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\")")
_MAX_LAG = 100


def rewrite_lag(statements: list[MacroStatement]) -> list[MacroStatement]:
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
        order_keys = _step_order(body)
        if end >= len(statements) or not any(
            _CALL.search(_QUOTED.sub("''", item.text)) for item in body
        ):
            result.extend(statements[index : end + 1])
            index = end + 1
            continue
        if order_keys is None:
            call = next(
                item
                for item in body
                if _CALL.search(_QUOTED.sub("''", item.text))
            )
            raise MacroExpansionError(
                "LAG/DIF requires a DATA-step ORDER BY with unique keys; "
                "Spark does not preserve SAS observation order.",
                call,
            )
        result.append(statement)
        helpers: list[str] = []
        for item in body:
            text, assignments = _rewrite_statement(item, len(helpers), order_keys)
            for name, expression in assignments:
                helpers.append(name)
                result.append(MacroStatement(f"{name} = {expression}", item.line))
            result.append(MacroStatement(text, item.line))
        if helpers:
            result.append(MacroStatement(f"drop {' '.join(helpers)}", statements[end].line))
        result.append(statements[end])
        index = end + 1
    return result


def _rewrite_statement(
    item: MacroStatement,
    offset: int,
    order_keys: tuple[tuple[str, bool], ...] | None,
) -> tuple[str, list[tuple[str, str]]]:
    parts = _QUOTED.split(item.text)
    assignments: list[tuple[str, str]] = []
    for position in range(0, len(parts), 2):
        text = parts[position]
        while (call := _CALL.search(text)) is not None:
            opening = call.end() - 1
            closing = _matching(text, opening, item)
            argument = text[opening + 1 : closing].strip()
            if not argument:
                raise MacroExpansionError(
                    f"{call.group(1).upper()} requires an argument.", item
                )
            if "," in _top_level_commas(argument):
                raise MacroExpansionError(
                    f"{call.group(1).upper()} takes exactly one argument.", item
                )
            lag = int(call.group(2)) if call.group(2) else 1
            if not 1 <= lag <= _MAX_LAG:
                raise MacroExpansionError(
                    f"{call.group(1).upper()} lag must be between 1 and {_MAX_LAG}.",
                    item,
                )
            order = (
                order_by_sql(order_keys)
                if order_keys
                else "order by monotonically_increasing_id()"
            )
            lagged = f"lag(({argument}), {lag}) over ({order})"
            expression = (
                lagged
                if call.group(1).casefold() == "lag"
                else f"(({argument}) - {lagged})"
            )
            name = f"__sas2pyspark_lag_{offset + len(assignments) + 1}"
            assignments.append((name, expression))
            text = text[: call.start()] + name + text[closing + 1 :]
        parts[position] = text
    return "".join(parts), assignments


def _step_order(
    body: list[MacroStatement],
) -> tuple[tuple[str, bool], ...] | None:
    found = []
    for item in body:
        try:
            keys = parse_order_by(item.text)
        except OrderByError as error:
            raise MacroExpansionError(str(error), item) from error
        if keys is not None:
            found.append((item, keys))
    if len(found) > 1:
        raise MacroExpansionError(
            "A DATA step supports only one ORDER BY statement.", found[1][0]
        )
    return found[0][1] if found else None


def _matching(text: str, opening: int, item: MacroStatement) -> int:
    depth = 0
    for position in range(opening, len(text)):
        if text[position] == "(":
            depth += 1
        elif text[position] == ")":
            depth -= 1
            if depth == 0:
                return position
    raise MacroExpansionError("Unbalanced parentheses in LAG/DIF call.", item)


def _top_level_commas(text: str) -> str:
    depth = 0
    found: list[str] = []
    for char in _QUOTED.sub("''", text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            found.append(char)
    return "".join(found)
