"""Static column-level lineage from PROC SQL select lists and DATA-step statements."""

from __future__ import annotations

import re
from collections import deque
from typing import Any, Mapping

from .schema_inference import expression_refs

_IF_THEN = re.compile(r"^\s*if\s+(.+?)\s+then\s+(.*)$", re.I | re.S)
_ELSE = re.compile(r"^\s*else\s+(.*)$", re.I | re.S)
_ASSIGNMENT = re.compile(r"^\s*([A-Za-z_]\w*)\s*=(?!=)\s*(.+)$", re.S)
_AGGREGATE = re.compile(r"\b(?:count|sum|avg|mean|min|max|std|var|median|n|nmiss)\s*\(", re.I)


def extract_assignments(statements: list[tuple[str, int]]) -> list[dict[str, Any]]:
    """Return ``var = expression`` assignments (with IF/ELSE conditions) from DATA-step statements."""
    assignments: list[dict[str, Any]] = []
    for text, line in statements:
        rest = text.strip()
        conditions: list[str] = []
        for _ in range(3):
            if_match = _IF_THEN.match(rest)
            if if_match:
                conditions.append(if_match.group(1).strip())
                rest = if_match.group(2).strip()
                continue
            else_match = _ELSE.match(rest)
            if else_match:
                conditions.append("else")
                rest = else_match.group(1).strip()
                continue
            break
        match = _ASSIGNMENT.match(rest)
        if not match:
            continue
        assignments.append(
            {
                "column": match.group(1),
                "expression": match.group(2).strip(),
                "condition": " and ".join(conditions) or None,
                "line": line,
            }
        )
    return assignments


def _resolve(
    qualifier: str | None,
    column: str,
    scope: list[str],
    aliases: Mapping[str, str],
    schemas: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    key = column.casefold()
    if qualifier:
        dataset = aliases.get(qualifier)
        return [{"dataset": dataset, "column": column}] if dataset else []
    found = [name for name in scope if key in schemas.get(name, {}).get("columns", {})]
    if found:
        return [{"dataset": name, "column": column} for name in found]
    if len(scope) == 1:
        return [{"dataset": scope[0], "column": column, "assumed": True}]
    return []


def _merge(sources: list[dict[str, Any]], extra: list[dict[str, Any]]) -> None:
    for source in extra:
        if not any(
            known["dataset"] == source["dataset"]
            and known["column"].casefold() == source["column"].casefold()
            for known in sources
        ):
            sources.append(source)


def build_column_lineage(
    schemas: Mapping[str, Mapping[str, Any]],
    sql_creates: list[dict[str, Any]],
    data_steps: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build target-column -> source-column edges.

    Transforms: ``copy``, ``rename``, ``star``, ``constant``, ``expression``,
    ``aggregate``, ``conditional`` and ``inherited`` (DATA-step SET/MERGE pass-through).
    An edge with no sources and a non-constant transform is unresolved; unqualified
    references are only resolved against known schemas or a single input table.
    """
    edges: dict[tuple[Any, ...], dict[str, Any]] = {}

    def add(edge: dict[str, Any]) -> None:
        key = (
            edge["target"],
            edge["column"].casefold(),
            edge["transform"],
            tuple((s["dataset"], s["column"].casefold()) for s in edge["sources"]),
            edge.get("expression"),
            edge.get("condition"),
        )
        edge["resolved"] = bool(edge["sources"]) or edge["transform"] == "constant"
        edges.setdefault(key, edge)

    for create in sql_creates:
        target = create["target"]
        sources = [name for name, _alias in create["tables"] if name != target]
        origin = {"origin": "sql", "source": create.get("source"), "line": create.get("line")}
        for item in create["items"]:
            if item.get("star"):
                qualifier = item.get("qualifier")
                scope = (
                    [create["aliases"][qualifier]]
                    if qualifier in create["aliases"]
                    else ([] if qualifier else sources)
                )
                for dataset in scope:
                    for column in schemas.get(dataset, {}).get("columns", {}).values():
                        add(
                            {
                                "target": target,
                                "column": column["name"],
                                "sources": [{"dataset": dataset, "column": column["name"]}],
                                "transform": "star",
                                **origin,
                            }
                        )
                continue
            name = item["name"]
            if item.get("type") and not item.get("source_column"):
                add({"target": target, "column": name, "sources": [], "transform": "constant", **origin})
                continue
            if item.get("source_column"):
                found = _resolve(
                    item.get("qualifier"), item["source_column"], sources, create["aliases"], schemas
                )
                transform = "copy" if item["source_column"].casefold() == name.casefold() else "rename"
                add(
                    {
                        "target": target,
                        "column": name,
                        "sources": found,
                        "transform": transform,
                        **origin,
                    }
                )
                continue
            found = []
            for qualifier, column in item.get("refs", []):
                _merge(found, _resolve(qualifier, column, sources, create["aliases"], schemas))
            expression = item.get("expression", "")
            if not item.get("refs"):
                add(
                    {
                        "target": target,
                        "column": name,
                        "sources": [],
                        "transform": "constant",
                        "expression": expression,
                        **origin,
                    }
                )
                continue
            add(
                {
                    "target": target,
                    "column": name,
                    "sources": found,
                    "transform": "aggregate" if _AGGREGATE.search(expression) else "expression",
                    "expression": expression,
                    **origin,
                }
            )

    for step in data_steps:
        inputs = [name for name in step["inputs"]]
        origin = {"origin": "data_step", "source": step.get("source"), "line": step.get("line")}
        step_vars: dict[str, list[dict[str, Any]]] = {}

        def read(refs: list[tuple[str | None, str]]) -> list[dict[str, Any]]:
            found: list[dict[str, Any]] = []
            for _qualifier, column in refs:
                _merge(found, step_vars.get(column.casefold(), []))
                _merge(found, _resolve(None, column, inputs, {}, schemas))
            return found

        for assignment in step["assignments"]:
            sources = read(expression_refs(assignment["expression"]))
            has_refs = bool(expression_refs(assignment["expression"]))
            control = read(expression_refs(assignment["condition"])) if assignment["condition"] else []
            if not has_refs:
                transform = "constant"
            elif assignment["condition"]:
                transform = "conditional"
            else:
                transform = "expression"
            known = step_vars.setdefault(assignment["column"].casefold(), [])
            _merge(known, sources)
            for output in step["outputs"]:
                add(
                    {
                        "target": output,
                        "column": assignment["column"],
                        "sources": sources,
                        "control_sources": control,
                        "transform": transform,
                        "expression": assignment["expression"],
                        "condition": assignment["condition"],
                        "origin": "data_step",
                        "source": step.get("source"),
                        "line": assignment["line"],
                    }
                )
        assigned = set(step_vars)
        for output in step["outputs"]:
            for key, column in schemas.get(output, {}).get("columns", {}).items():
                if key in assigned:
                    continue
                found = [
                    {"dataset": name, "column": column["name"]}
                    for name in inputs
                    if name != output and key in schemas.get(name, {}).get("columns", {})
                ]
                if found:
                    add(
                        {
                            "target": output,
                            "column": column["name"],
                            "sources": found,
                            "transform": "inherited",
                            **origin,
                        }
                    )

    return sorted(
        edges.values(),
        key=lambda edge: (edge["target"], edge["column"].casefold(), edge["transform"]),
    )


def trace_column_lineage(
    edges: list[dict[str, Any]], dataset: str, column: str, max_depth: int = 50
) -> dict[str, Any]:
    """Walk upstream from ``dataset.column``; return visited edges and ultimate sources."""
    by_target: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for edge in edges:
        by_target.setdefault((edge["target"], edge["column"].casefold()), []).append(edge)
    start = (dataset.casefold(), column.casefold())
    seen = {start}
    queue: deque[tuple[tuple[str, str], int]] = deque([(start, 0)])
    visited: list[dict[str, Any]] = []
    roots: list[dict[str, str]] = []
    while queue:
        node, depth = queue.popleft()
        incoming = by_target.get(node, [])
        upstream = [source for edge in incoming for source in edge["sources"]]
        if not upstream and not any(edge["transform"] == "constant" for edge in incoming):
            roots.append({"dataset": node[0], "column": node[1]})
        visited.extend(incoming)
        if depth >= max_depth:
            continue
        for edge in incoming:
            for source in [*edge["sources"], *edge.get("control_sources", [])]:
                nxt = (source["dataset"].casefold(), source["column"].casefold())
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append((nxt, depth + 1))
    return {"dataset": dataset, "column": column, "edges": visited, "ultimate_sources": roots}
