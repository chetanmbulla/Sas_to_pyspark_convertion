"""Static column inference for catalog datasets (SQL select lists, lineage, references)."""

from __future__ import annotations

import re
from typing import Any, Callable, Mapping

_STRING = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")
_TABLE_REF = re.compile(
    r"\b(?:from|join)\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)"
    r"(?:\s*\((?:[^()]|\((?:[^()]|\([^()]*\))*\))*\))?"
    r"(?:\s+(?:as\s+)?([A-Za-z_]\w*))?",
    re.I,
)
_NOT_ALIAS = frozenset(
    {
        "on", "where", "left", "right", "inner", "outer", "full", "cross", "natural",
        "join", "group", "order", "having", "union", "intersect", "except", "using",
        "set", "quit", "as", "by", "into", "when", "then", "else", "end",
    }
)
_QUALIFIED = re.compile(r"\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b(?!\s*\()")
_SELECT_LIST = re.compile(r"\bselect\s+(?:distinct\s+|all\s+)?", re.I)
_AS_ALIAS = re.compile(
    r"^(?P<expr>.+?)\s+as\s+(?P<alias>[A-Za-z_]\w*)"
    r"(?P<tail>(?:\s+(?:length|format|informat|label)\s*=\s*(?:'[^']*'|\"[^\"]*\"|\S+))*)\s*$",
    re.I | re.S,
)
_LENGTH_TAIL = re.compile(r"length\s*=\s*(\d+)", re.I)
_FORMAT_TAIL = re.compile(r"\bformat\s*=\s*(\S+)", re.I)
_SQL_BARE = re.compile(r"^(?:([A-Za-z_]\w*)\.)?([A-Za-z_]\w*)$")
_STAR = re.compile(r"^(?:([A-Za-z_]\w*)\.)?\*$")
_BARE_KEYWORDS = frozenset({"null", "distinct", "all", "case", "missing"})


_STRING_LITERAL = re.compile(r"(?:'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\")[dtnDTN]?")
_CALCULATED = re.compile(r"\bcalculated\s+[A-Za-z_]\w*", re.I)
_EXPRESSION_IDENT = re.compile(r"(?<![\w.&$])([A-Za-z_]\w*)(?:\.([A-Za-z_]\w*))?(?!\w)(\s*\()?")
_EXPRESSION_KEYWORDS = frozenset(
    {
        "and", "or", "not", "in", "eq", "ne", "lt", "gt", "le", "ge", "case", "when",
        "then", "else", "end", "is", "null", "missing", "between", "like", "contains",
        "distinct", "as", "by", "asc", "desc", "do", "to", "if", "_n_", "_error_",
        "calculated",
    }
)


def expression_refs(expression: str) -> list[tuple[str | None, str]]:
    """Return ordered unique (qualifier, column) identifiers read by a SAS expression.

    Function calls, keywords, literals, macro variables, ``first.``/``last.`` flags and
    ``CALCULATED`` references are excluded.
    """
    cleaned = _CALCULATED.sub(" ", _STRING_LITERAL.sub("''", expression))
    found: list[tuple[str | None, str]] = []
    for match in _EXPRESSION_IDENT.finditer(cleaned):
        first, second, call = match.groups()
        if call:
            continue
        if not second and cleaned[match.end() : match.end() + 1] == ".":
            continue  # SAS format name such as d_month. or best12.
        qualifier, column = (first, second) if second else (None, first)
        if qualifier and qualifier.casefold() in {"first", "last"}:
            continue
        if column.casefold() in _EXPRESSION_KEYWORDS:
            continue
        entry = (qualifier.casefold() if qualifier else None, column)
        if entry not in found:
            found.append(entry)
    return found


def _mask_strings(text: str) -> str:
    return _STRING.sub(lambda match: "'" + "x" * (len(match.group()) - 2) + "'", text)


def _split_top_level(text: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in text:
        depth += {"(": 1, ")": -1}.get(char, 0)
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return parts


def _top_level_from(text: str) -> int | None:
    depth = 0
    lowered = text.casefold()
    for index, char in enumerate(lowered):
        depth += {"(": 1, ")": -1}.get(char, 0)
        if (
            depth == 0
            and lowered.startswith("from", index)
            and (index == 0 or not (lowered[index - 1].isalnum() or lowered[index - 1] == "_"))
            and (index + 4 >= len(lowered) or lowered[index + 4].isspace())
        ):
            return index
    return None


def parse_sql_statement(
    text: str, normalize: Callable[[str], str]
) -> dict[str, Any] | None:
    """Parse one PROC SQL statement into target, tables (alias map) and select items.

    ``normalize`` maps a dataset token to a catalog name or raises ValueError.
    Returns None when the statement has no SELECT.
    """
    masked = _mask_strings(text)
    select = _SELECT_LIST.search(masked)
    if select is None:
        return None
    tables: list[tuple[str, str | None]] = []
    for match in _TABLE_REF.finditer(masked):
        alias = match.group(2)
        if alias and alias.casefold() in _NOT_ALIAS:
            alias = None
        try:
            tables.append((normalize(match.group(1)), alias))
        except ValueError:
            continue
    aliases = {alias.casefold(): name for name, alias in tables if alias}

    body = text[select.end() :]
    masked_body = masked[select.end() :]
    from_index = _top_level_from(masked_body)
    list_text = body[:from_index] if from_index is not None else body
    items: list[dict[str, Any]] = []
    for raw in _split_top_level(list_text):
        raw = raw.strip()
        if not raw:
            continue
        star = _STAR.match(raw)
        if star:
            items.append({"star": True, "qualifier": (star.group(1) or "").casefold() or None})
            continue
        item: dict[str, Any] = {"star": False}
        alias_match = _AS_ALIAS.match(raw)
        expression = raw
        if alias_match:
            item["name"] = alias_match.group("alias")
            expression = alias_match.group("expr").strip()
            tail = alias_match.group("tail") or ""
            length = _LENGTH_TAIL.search(tail)
            fmt = _FORMAT_TAIL.search(tail)
            if length:
                item["length"] = int(length.group(1))
            if fmt:
                item["format"] = fmt.group(1)
        item["expression"] = expression
        bare = _SQL_BARE.match(expression)
        if bare and bare.group(2).casefold() not in _BARE_KEYWORDS:
            item.setdefault("name", bare.group(2))
            item["source_column"] = bare.group(2)
            item["qualifier"] = (bare.group(1) or "").casefold() or None
        elif _STRING.fullmatch(expression):
            item["type"] = "character"
        elif re.fullmatch(r"-?\d+(?:\.\d+)?", expression):
            item["type"] = "numeric"
        else:
            item["refs"] = expression_refs(expression)
        if "name" in item:
            items.append(item)

    references: dict[str, set[str]] = {}
    for match in _QUALIFIED.finditer(masked):
        dataset = aliases.get(match.group(1).casefold())
        if dataset:
            references.setdefault(dataset, set()).add(match.group(2))
    if len({name for name, _ in tables}) == 1:
        only = tables[0][0]
        for item in items:
            if item.get("source_column") and not item.get("qualifier"):
                references.setdefault(only, set()).add(item["source_column"])
    return {
        "tables": tables,
        "aliases": aliases,
        "items": items,
        "references": references,
    }


def _copy_column(column: Mapping[str, Any], origin: str, source: str | None = None) -> dict[str, Any]:
    copied = {key: value for key, value in column.items() if key != "evidence"}
    copied["origin"] = origin
    if source:
        copied["inherited_from"] = source
    return copied


def infer_schemas(
    declared: Mapping[str, list[dict[str, Any]]],
    code_columns: Mapping[str, Mapping[str, dict[str, Any]]],
    sql_creates: list[dict[str, Any]],
    data_lineage: Mapping[str, list[str]],
    references: Mapping[str, Mapping[str, str]],
    data_steps: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Merge declared, code-declared, inherited and referenced columns per dataset.

    Declared (asset) schemas are final and never extended. Result per dataset:
    ``{"status": "complete"|"inferred"|"partial", "columns": {key: column}}``.
    """
    final: dict[str, dict[str, dict[str, Any]]] = {}
    complete: set[str] = set()
    for name, columns in declared.items():
        final[name] = {
            column["name"].casefold(): {**column, "origin": "asset"} for column in columns
        }
        complete.add(name)
    for name, columns in code_columns.items():
        if name in complete:
            continue
        target = final.setdefault(name, {})
        for key, column in columns.items():
            target.setdefault(key, {**column, "origin": "code"})
    for step in data_steps or []:
        for output in step["outputs"]:
            if output in complete:
                continue
            target = final.setdefault(output, {})
            for assignment in step["assignments"]:
                expression = assignment["expression"].strip()
                column_type = (
                    "character"
                    if _STRING_LITERAL.fullmatch(expression)
                    else "numeric"
                    if re.fullmatch(r"-?\d+(?:\.\d+)?", expression)
                    else "unknown"
                )
                target.setdefault(
                    assignment["column"].casefold(),
                    {"name": assignment["column"], "type": column_type, "origin": "assigned"},
                )

    def lookup(sources: list[str], key: str) -> dict[str, Any] | None:
        for source in sources:
            found = final.get(source, {}).get(key)
            if found:
                return found
        return None

    for _ in range(10):
        changed = False
        for create in sql_creates:
            target = create["target"]
            if target in complete:
                continue
            sources = [name for name, _alias in create["tables"] if name != target]
            columns = final.setdefault(target, {})
            for item in create["items"]:
                if item.get("star"):
                    qualifier = item.get("qualifier")
                    scope = (
                        [create["aliases"][qualifier]]
                        if qualifier in create["aliases"]
                        else ([] if qualifier else sources)
                    )
                    for source in scope:
                        for key, column in list(final.get(source, {}).items()):
                            if key not in columns:
                                columns[key] = _copy_column(column, "inherited", source)
                                changed = True
                    continue
                key = item["name"].casefold()
                existing = columns.get(key)
                scope = (
                    [create["aliases"][item["qualifier"]]]
                    if item.get("qualifier") in create["aliases"]
                    else sources
                )
                source_column = (
                    lookup(scope, item["source_column"].casefold())
                    if item.get("source_column")
                    else None
                )
                column_type = item.get("type") or (
                    source_column.get("type") if source_column else None
                ) or "unknown"
                if existing is None:
                    new_column: dict[str, Any] = {
                        "name": item["name"],
                        "type": column_type,
                        "origin": "inferred",
                    }
                    for field in ("length", "format", "label", "informat"):
                        value = item.get(field) or (source_column or {}).get(field)
                        if value:
                            new_column[field] = value
                    columns[key] = new_column
                    changed = True
                elif existing.get("type", "unknown") == "unknown" and column_type != "unknown":
                    existing["type"] = column_type
                    changed = True
        for output, inputs in data_lineage.items():
            if output in complete:
                continue
            columns = final.setdefault(output, {})
            for source in inputs:
                if source == output:
                    continue
                for key, column in list(final.get(source, {}).items()):
                    if key not in columns:
                        columns[key] = _copy_column(column, "inherited", source)
                        changed = True
        if not changed:
            break

    for dataset, names in references.items():
        if dataset in complete:
            continue
        columns = final.setdefault(dataset, {})
        for key, name in names.items():
            columns.setdefault(key, {"name": name, "type": "unknown", "origin": "referenced"})

    result: dict[str, dict[str, Any]] = {}
    for name, columns in final.items():
        if not columns:
            continue
        if name in complete:
            status = "complete"
        elif any(column.get("origin") != "code" for column in columns.values()):
            status = "inferred"
        else:
            status = "partial"
        result[name] = {"status": status, "columns": columns}
    return result
