"""Support the automatic ``_N_`` variable in DATA steps.

``_N_`` is rewritten to a helper column numbered by Spark's
``monotonically_increasing_id()`` order, which is a best-effort stand-in for SAS
observation order.
"""

from __future__ import annotations

import re

from .data_order import OrderByError, order_by_sql, parse_order_by
from .macro_processor import MacroExpansionError, MacroStatement

HELPER = "__sas2pyspark_n"
_DATA = re.compile(r"data\s+((?:\w+\.)?\w+)", re.I)
_N = re.compile(r"\b_n_\b", re.I)
_INPUT_STATEMENT = re.compile(r"(?:set|merge|by)\b", re.I)
_QUOTED = re.compile(r"('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\")")


def rewrite_row_number(statements: list[MacroStatement]) -> list[MacroStatement]:
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
        order_keys = None
        for item in body:
            try:
                parsed_order = parse_order_by(item.text)
            except OrderByError as error:
                raise MacroExpansionError(str(error), item) from error
            if parsed_order is not None:
                order_keys = parsed_order
                break
        order_clause = (
            order_by_sql(order_keys)
            if order_keys
            else "order by monotonically_increasing_id()"
        )
        if end >= len(statements) or not any(
            _N.search(_QUOTED.sub("''", item.text)) for item in body
        ):
            result.extend(statements[index : end + 1])
            index = end + 1
            continue
        last_input = max(
            (i for i, item in enumerate(body) if _INPUT_STATEMENT.match(item.text.strip())),
            default=-1,
        )
        result.append(statement)
        for position, item in enumerate(body):
            result.append(_replace(item))
            if position == last_input:
                result.append(
                    MacroStatement(
                        f"{HELPER} = row_number() over "
                        f"({order_clause})",
                        item.line,
                    )
                )
        if last_input == -1:
            result.insert(
                len(result) - len(body),
                MacroStatement(
                    f"{HELPER} = row_number() over "
                    f"({order_clause})",
                    statement.line,
                ),
            )
        result.append(MacroStatement(f"drop {HELPER}", statements[end].line))
        result.append(statements[end])
        index = end + 1
    return result


def _replace(item: MacroStatement) -> MacroStatement:
    parts = _QUOTED.split(item.text)
    for position in range(0, len(parts), 2):
        parts[position] = _N.sub(HELPER, parts[position])
    return MacroStatement("".join(parts), item.line)
