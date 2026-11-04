"""Support character and conditional RETAIN as last-value-carried-forward.

A retained variable that is assigned only by plain or ``IF ... THEN`` assignments (not
the ``v = v + expr`` accumulator form handled by the converter) keeps its previous
value on rows where no assignment executes. This is computed with a window function:
the last executed assignment so far, falling back to the RETAIN initial value.

Rows are ordered by ``monotonically_increasing_id()``, a best-effort stand-in for SAS
observation order, and the window has no partition, so it runs on a single partition.
"""

from __future__ import annotations

import re

from .data_blocks import _IF_ACTION, _IF_DELETE, _flatten, _read_chain, _to_statements
from .data_order import OrderByError, order_by_sql, parse_order_by
from .macro_processor import MacroExpansionError, MacroStatement

_DATA = re.compile(r"data\s+((?:\w+\.)?\w+)", re.I)
_TOKEN = re.compile(
    r"\s*('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?|[A-Za-z_]\w*)"
)
_IF_THEN = re.compile(r"if\s+(.+?)\s+then\s+(.+)", re.I | re.S)
_WINDOW_FRAME = "rows between unbounded preceding and current row"


def rewrite_retain(statements: list[MacroStatement]) -> list[MacroStatement]:
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
        body = list(statements[index + 1 : end])
        closer = statements[end] if end < len(statements) else None
        result.append(statement)
        rewritten, helpers = _rewrite_step(body)
        result.extend(rewritten)
        if helpers and closer is not None:
            result.append(MacroStatement(f"drop {' '.join(helpers)}", closer.line))
        if closer is not None:
            result.append(closer)
        index = end + 1
    return result


def _parse_retain(item: MacroStatement) -> list[tuple[str, str | None]]:
    text = item.text.strip()[len("retain") :]
    pairs: list[tuple[str, str | None]] = []
    pending: list[str] = []
    position = 0
    while position < len(text):
        if not text[position:].strip():
            break
        token = _TOKEN.match(text, position)
        if token is None:
            raise MacroExpansionError(f"Unsupported RETAIN syntax: {text.strip()!r}.", item)
        position = token.end()
        value = token.group(1)
        if re.fullmatch(r"[A-Za-z_]\w*", value):
            pending.append(value)
        else:
            if not pending:
                raise MacroExpansionError("RETAIN initial value must follow a variable name.", item)
            pairs.extend((name, value) for name in pending)
            pending = []
    pairs.extend((name, None) for name in pending)
    return pairs


def _split_assignment(text: str) -> tuple[str | None, str, str] | None:
    condition: str | None = None
    action = text.strip()
    guarded = _IF_THEN.fullmatch(action)
    if guarded is not None:
        condition, action = guarded.group(1), guarded.group(2).strip()
    assignment = re.fullmatch(r"([A-Za-z_]\w*)\s*=(?!=)\s*(.+)", action, re.S)
    if assignment is None:
        return None
    return condition, assignment.group(1), assignment.group(2)


def _rewrite_step(body: list[MacroStatement]) -> tuple[list[MacroStatement], list[str]]:
    retain_items = [item for item in body if re.match(r"retain\s", item.text.strip(), re.I)]
    if not retain_items:
        return body, []
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
    window = f"over ({order_clause} {_WINDOW_FRAME})"
    declared: dict[str, tuple[str, str | None]] = {}
    for item in retain_items:
        for name, initial in _parse_retain(item):
            declared[name.casefold()] = (name, initial)

    assignments: dict[str, list[tuple[int, str | None, str]]] = {key: [] for key in declared}
    accumulators: set[str] = set()
    for position, item in enumerate(body):
        parts = _split_assignment(item.text)
        if parts is not None:
            condition, name, expression = parts
            key = name.casefold()
            if key in declared:
                numeric = declared[key][1] is not None and declared[key][1][0] not in "'\""
                if re.match(rf"{re.escape(name)}\s*\+", expression, re.I) or (
                    numeric
                    and re.search(rf"\b{re.escape(name)}\b", f"{condition or ''} {expression}", re.I)
                ):
                    accumulators.add(key)
                assignments[key].append((position, condition, expression))
        elif (sum_statement := re.match(r"([A-Za-z_]\w*)\s*\+", item.text.strip())) is not None:
            if sum_statement.group(1).casefold() in declared:
                accumulators.add(sum_statement.group(1).casefold())

    locf = [
        key
        for key, (_, initial) in declared.items()
        if key not in accumulators
        and (assignments[key] or initial is None or initial[0] in "'\"")
    ]
    if not locf:
        return body, []
    body = _flatten_chains(body, set(declared) - accumulators)
    assignments = {key: [] for key in declared}
    for position, item in enumerate(body):
        parts = _split_assignment(item.text)
        if parts is not None and parts[1].casefold() in declared:
            assignments[parts[1].casefold()].append((position, parts[0], parts[2]))
    retain_items = [item for item in body if re.match(r"retain\s", item.text.strip(), re.I)]
    if any(re.match(r"merge\s", item.text.strip(), re.I) for item in body):
        raise MacroExpansionError(
            "Character or conditional RETAIN is supported only in single-SET steps; "
            "MERGE is unsupported.",
            retain_items[0],
        )

    replacements: dict[int, list[MacroStatement]] = {}
    removed: set[int] = set()
    helpers: list[str] = []
    for key in locf:
        name, initial = declared[key]
        entries = assignments[key]
        for position, condition, expression in entries:
            for source in (condition or "", expression):
                if re.search(rf"\b{re.escape(name)}\b", source, re.I):
                    raise MacroExpansionError(
                        f"RETAIN variable {name!r} cannot be referenced in its own "
                        "assignment; the previous retained value is not available.",
                        body[position],
                    )
        if not entries:
            if initial is None:
                raise MacroExpansionError(
                    f"RETAIN variable {name!r} is never assigned; RETAIN needs a numeric "
                    "literal initial value or an assignment.",
                    retain_items[0],
                )
            continue
        removed.update(position for position, _, _ in entries)
        helper = f"__sas2pyspark_retain_{len(helpers) + 1}"
        helpers.append(helper)
        cases = " ".join(
            f"when ({condition or 'true'}) then named_struct('v', ({expression}))"
            for _, condition, expression in reversed(entries)
        )
        last = entries[-1][0]
        replacements[last] = [
            MacroStatement(f"{helper} = last(case {cases} end, true) {window}", body[last].line),
            MacroStatement(
                f"{name} = case when {helper} is null then {initial or 'null'} "
                f"else {helper}.v end",
                body[last].line,
            ),
        ]

    output: list[MacroStatement] = []
    for position, item in enumerate(body):
        if position in replacements:
            output.extend(replacements[position])
        elif position in removed:
            continue
        elif re.match(r"retain\s", item.text.strip(), re.I):
            kept = [
                (name, initial)
                for name, initial in _parse_retain(item)
                if name.casefold() not in locf
            ]
            if kept:
                output.append(MacroStatement(_format_retain(kept), item.line))
        else:
            output.append(item)
    return output, helpers


def _flatten_chains(body: list[MacroStatement], targets: set[str]) -> list[MacroStatement]:
    """Turn IF/ELSE chains that assign a last-value variable into guarded statements."""
    output: list[MacroStatement] = []
    index = 0
    while index < len(body):
        text = body[index].text.strip()
        if _IF_ACTION.fullmatch(text) and not _IF_DELETE.fullmatch(text):
            branches, following = _read_chain(body, index)
            entries = _flatten(branches, body[index])
            if any(
                (parts := _split_assignment(item.text)) is not None
                and parts[1].casefold() in targets
                for _, item in entries
            ):
                output.extend(_to_statements(entries))
                index = following
                continue
        output.append(body[index])
        index += 1
    return output


def _format_retain(pairs: list[tuple[str, str | None]]) -> str:
    parts: list[str] = []
    index = 0
    while index < len(pairs):
        initial = pairs[index][1]
        group = [pairs[index][0]]
        while index + 1 < len(pairs) and pairs[index + 1][1] == initial:
            index += 1
            group.append(pairs[index][0])
        parts.append(" ".join(group) + (f" {initial}" if initial is not None else ""))
        index += 1
    return "retain " + " ".join(parts)
