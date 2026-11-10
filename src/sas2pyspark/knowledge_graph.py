"""Knowledge graph of jobs, scripts, datasets, files, systems and columns with lineage queries."""

from __future__ import annotations

import json
import re
from collections import defaultdict, deque
from typing import Any, Iterable, Mapping
from xml.sax.saxutils import escape, quoteattr

GRAPH_SCHEMA_VERSION = 1
NODE_TYPES = ("job", "script", "dataset", "file", "system", "column")
# Edge types that carry data in the direction source -> target once reads are reversed.
_FLOW_TYPES = {"reads", "writes", "derived_from", "column_derived_from"}


class KnowledgeGraphError(ValueError):
    pass


def node_id(kind: str, name: str) -> str:
    return f"{kind}:{name}"


def build_knowledge_graph(
    pipeline_report: Mapping[str, Any],
    catalog: Mapping[str, Any],
    *,
    include_columns: bool = True,
) -> dict[str, Any]:
    """Merge the pipeline report and catalog into one typed node/edge graph.

    Edges point in their natural direction (``script reads dataset``,
    ``dataset derived_from dataset``); lineage queries convert to data-flow direction.
    """
    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[tuple[str, str, str], dict[str, Any]] = {}

    def add_node(kind: str, name: str, **attributes: Any) -> str:
        key = node_id(kind, name)
        entry = nodes.setdefault(key, {"id": key, "type": kind, "label": name})
        entry.update({k: v for k, v in attributes.items() if v is not None})
        return key

    def add_edge(source: str, target: str, kind: str, **attributes: Any) -> None:
        entry = edges.setdefault(
            (source, target, kind), {"source": source, "target": target, "type": kind}
        )
        entry.update({k: v for k, v in attributes.items() if v is not None})

    for script in pipeline_report.get("scripts", []):
        add_node("script", script["path"], server=script.get("server"), status=script.get("status"))
    classification = {
        item["path"]: item for item in catalog.get("script_classification", [])
    }
    for path, item in classification.items():
        add_node(
            "script", path, role=item.get("role"), activity=item.get("activity"),
            suspect_name=item.get("suspect_name"),
        )
    for job in pipeline_report.get("jobs", []):
        job_key = add_node("job", job["name"], server=job.get("server"))
        if job.get("entrypoint"):
            add_edge(job_key, add_node("script", job["entrypoint"]), "runs")
    for include in pipeline_report.get("includes", []):
        if include.get("target"):
            add_edge(
                add_node("script", include["source"]),
                add_node("script", include["target"]),
                "includes",
                line=include.get("line"),
                status=include.get("status"),
            )

    for system in catalog.get("systems", []):
        system_key = add_node(
            "system", system["id"], system_kind=system.get("kind"), server=system.get("server"),
            status=system.get("status"),
        )
        for source in system.get("sources", []):
            add_edge(add_node("script", source), system_key, "uses")

    role_by_name = {item["name"]: item for item in catalog.get("resource_roles", [])}
    for resource in catalog.get("resources", []):
        kind = "dataset" if resource["kind"] == "sas_dataset" else "file"
        role = role_by_name.get(resource["name"], {})
        schema = resource.get("schema") or {}
        resource_key = add_node(
            kind,
            resource["name"],
            role=role.get("role"),
            temporary=role.get("temporary"),
            schema_status=schema.get("status"),
            column_count=len(schema.get("columns", [])) if schema else None,
        )
        for evidence in resource.get("evidence", []):
            direction = evidence.get("direction")
            if direction in {"input", "output"}:
                add_edge(
                    add_node("script", evidence["source"]),
                    resource_key,
                    "reads" if direction == "input" else "writes",
                )
        for system in resource.get("systems", []):
            if isinstance(system, str):
                add_edge(resource_key, node_id("system", system), "stored_in")

    for lineage in catalog.get("dataset_lineage", []):
        target = add_node("dataset", lineage["target"])
        for source in lineage["sources"]:
            add_edge(target, add_node("dataset", source), "derived_from")

    if include_columns:
        for edge in catalog.get("column_lineage", []):
            target = add_node("column", f"{edge['target']}.{edge['column']}".casefold())
            add_edge(target, node_id("dataset", edge["target"]), "belongs_to")
            for source in [*edge["sources"], *edge.get("control_sources", [])]:
                name = f"{source['dataset']}.{source['column']}".casefold()
                source_key = add_node("column", name)
                add_edge(source_key, node_id("dataset", source["dataset"]), "belongs_to")
                add_edge(
                    target,
                    source_key,
                    "column_derived_from",
                    transform=edge["transform"],
                    expression=edge.get("expression"),
                )

    counts: dict[str, int] = defaultdict(int)
    for entry in nodes.values():
        counts[entry["type"]] += 1
    edge_counts: dict[str, int] = defaultdict(int)
    for entry in edges.values():
        edge_counts[entry["type"]] += 1
    return {
        "schema_version": GRAPH_SCHEMA_VERSION,
        "nodes": sorted(nodes.values(), key=lambda item: item["id"].casefold()),
        "edges": sorted(
            edges.values(), key=lambda item: (item["type"], item["source"], item["target"])
        ),
        "summary": {
            "node_count": len(nodes),
            "edge_count": len(edges),
            "nodes_by_type": dict(sorted(counts.items())),
            "edges_by_type": dict(sorted(edge_counts.items())),
        },
    }


def load_knowledge_graph(path: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as handle:
            graph = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise KnowledgeGraphError(f"Cannot read graph {path!r}: {error}") from error
    if not isinstance(graph, dict) or graph.get("schema_version") != GRAPH_SCHEMA_VERSION:
        raise KnowledgeGraphError("Graph has an unsupported schema version.")
    return graph


def resolve_node(graph: Mapping[str, Any], reference: str) -> str:
    """Resolve a full id, exact label, or unique case-insensitive substring to a node id."""
    nodes = graph["nodes"]
    ids = {node["id"] for node in nodes}
    if reference in ids:
        return reference
    lowered = reference.casefold()
    for matcher in (
        lambda node: node["id"].casefold() == lowered or node["label"].casefold() == lowered,
        lambda node: node["label"].casefold().endswith(lowered),
        lambda node: lowered in node["label"].casefold(),
    ):
        found = [node["id"] for node in nodes if matcher(node)]
        # Prefer data nodes over columns/belongs_to noise when a bare name is ambiguous.
        if len(found) > 1:
            preferred = [i for i in found if not i.startswith("column:")]
            found = preferred or found
        if len(found) == 1:
            return found[0]
        if len(found) > 1:
            raise KnowledgeGraphError(
                f"{reference!r} is ambiguous: {', '.join(sorted(found)[:8])}"
                + (" ..." if len(found) > 8 else "")
            )
    raise KnowledgeGraphError(f"No node matches {reference!r}.")


def search_nodes(
    graph: Mapping[str, Any], text: str, types: Iterable[str] | None = None
) -> list[dict[str, Any]]:
    lowered = text.casefold()
    wanted = set(types or ())
    return [
        node
        for node in graph["nodes"]
        if lowered in node["label"].casefold() and (not wanted or node["type"] in wanted)
    ]


def _adjacency(graph: Mapping[str, Any]) -> tuple[dict[str, list[tuple[str, dict]]], dict[str, list[tuple[str, dict]]]]:
    """Return (forward, backward) data-flow adjacency: ``reads`` edges are reversed."""
    forward: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    backward: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for edge in graph["edges"]:
        if edge["type"] not in _FLOW_TYPES:
            continue
        if edge["type"] in {"reads", "derived_from", "column_derived_from"}:
            source, target = edge["target"], edge["source"]
        else:
            source, target = edge["source"], edge["target"]
        forward[source].append((target, edge))
        backward[target].append((source, edge))
    return forward, backward


def traverse(
    graph: Mapping[str, Any],
    start: str,
    direction: str,
    *,
    max_depth: int | None = None,
    types: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Breadth-first lineage walk.

    ``direction``: ``upstream`` (what feeds the node), ``downstream`` (what depends on it).
    Returns ``nodes`` (with ``depth``) filtered by ``types`` and the traversed ``edges``.
    Columns are followed only when the start node is a column.
    """
    if direction not in {"upstream", "downstream"}:
        raise KnowledgeGraphError("direction must be 'upstream' or 'downstream'.")
    forward, backward = _adjacency(graph)
    adjacency = backward if direction == "upstream" else forward
    by_id = {node["id"]: node for node in graph["nodes"]}
    if start not in by_id:
        raise KnowledgeGraphError(f"Unknown node {start!r}.")
    column_walk = start.startswith("column:")
    depth_of = {start: 0}
    queue: deque[str] = deque([start])
    walked: list[dict[str, Any]] = []
    while queue:
        current = queue.popleft()
        depth = depth_of[current]
        if max_depth is not None and depth >= max_depth:
            continue
        for neighbour, edge in adjacency.get(current, []):
            is_column_edge = edge["type"] == "column_derived_from"
            if is_column_edge != column_walk:
                continue
            walked.append(edge)
            if neighbour not in depth_of:
                depth_of[neighbour] = depth + 1
                queue.append(neighbour)
    wanted = set(types or ())
    result_nodes = [
        {**by_id[key], "depth": depth}
        for key, depth in sorted(depth_of.items(), key=lambda item: (item[1], item[0]))
        if key != start and (not wanted or by_id[key]["type"] in wanted)
    ]
    return {"start": start, "direction": direction, "nodes": result_nodes, "edges": walked}


def ultimate_nodes(graph: Mapping[str, Any], start: str, direction: str, types: Iterable[str]) -> list[dict[str, Any]]:
    """Return walked nodes of ``types`` that have no further neighbour in ``direction``."""
    forward, backward = _adjacency(graph)
    adjacency = backward if direction == "upstream" else forward
    column_walk = start.startswith("column:")
    walk = traverse(graph, start, direction, types=types)

    def has_next(key: str) -> bool:
        return any(
            (edge["type"] == "column_derived_from") == column_walk
            for _neighbour, edge in adjacency.get(key, [])
        )

    return [node for node in walk["nodes"] if not has_next(node["id"])]


def find_path(graph: Mapping[str, Any], start: str, goal: str) -> list[str] | None:
    """Shortest data-flow path from ``start`` to ``goal``, or None."""
    forward, _ = _adjacency(graph)
    previous: dict[str, str | None] = {start: None}
    queue: deque[str] = deque([start])
    while queue:
        current = queue.popleft()
        if current == goal:
            path = []
            step: str | None = current
            while step is not None:
                path.append(step)
                step = previous[step]
            return path[::-1]
        for neighbour, _edge in forward.get(current, []):
            if neighbour not in previous:
                previous[neighbour] = current
                queue.append(neighbour)
    return None


def neighbours(graph: Mapping[str, Any], start: str) -> list[dict[str, Any]]:
    """Every direct edge touching ``start`` (any type, either direction)."""
    return [
        edge for edge in graph["edges"] if edge["source"] == start or edge["target"] == start
    ]


def to_graphml(graph: Mapping[str, Any]) -> str:
    keys = sorted({key for node in graph["nodes"] for key in node if key not in {"id"}})
    edge_keys = sorted({key for edge in graph["edges"] for key in edge if key not in {"source", "target"}})
    lines = ['<?xml version="1.0" encoding="UTF-8"?>', '<graphml xmlns="http://graphml.graphdrawing.org/xmlns">']
    for key in keys:
        lines.append(f'<key id="n_{key}" for="node" attr.name="{key}" attr.type="string"/>')
    for key in edge_keys:
        lines.append(f'<key id="e_{key}" for="edge" attr.name="{key}" attr.type="string"/>')
    lines.append('<graph id="G" edgedefault="directed">')
    for node in graph["nodes"]:
        lines.append(f"<node id={quoteattr(node['id'])}>")
        for key in keys:
            if key in node and node[key] is not None:
                lines.append(f'<data key="n_{key}">{escape(str(node[key]))}</data>')
        lines.append("</node>")
    for index, edge in enumerate(graph["edges"]):
        lines.append(
            f"<edge id=\"e{index}\" source={quoteattr(edge['source'])} target={quoteattr(edge['target'])}>"
        )
        for key in edge_keys:
            if key in edge and edge[key] is not None:
                lines.append(f'<data key="e_{key}">{escape(str(edge[key]))}</data>')
        lines.append("</edge>")
    lines.extend(["</graph>", "</graphml>", ""])
    return "\n".join(lines)


_SHAPES = {
    "job": ('(["', '"])'),
    "script": ('["', '"]'),
    "dataset": ('[("', '")]'),
    "file": ('>"', '"]'),
    "system": ('{{"', '"}}'),
    "column": ('("', '")'),
}


def to_mermaid(graph: Mapping[str, Any], node_ids: Iterable[str], *, max_nodes: int = 80) -> str:
    """Render the sub-graph induced by ``node_ids`` as a Mermaid flowchart (data-flow direction)."""
    selected = list(dict.fromkeys(node_ids))
    truncated = len(selected) > max_nodes
    selected = selected[:max_nodes]
    chosen = set(selected)
    by_id = {node["id"]: node for node in graph["nodes"]}
    alias = {key: f"n{index}" for index, key in enumerate(selected)}
    lines = ["flowchart LR"]
    for key in selected:
        node = by_id[key]
        opening, closing = _SHAPES.get(node["type"], ('["', '"]'))
        label = re.sub(r'["\[\]{}()<>]', "", node["label"].rsplit("/", 1)[-1])
        lines.append(f"  {alias[key]}{opening}{label}{closing}")
    for edge in graph["edges"]:
        if edge["type"] not in _FLOW_TYPES | {"includes", "runs"}:
            continue
        source, target = edge["source"], edge["target"]
        if edge["type"] in {"reads", "derived_from", "column_derived_from"}:
            source, target = target, source
        if source in chosen and target in chosen:
            lines.append(f"  {alias[source]} -->|{edge['type']}| {alias[target]}")
    if truncated:
        lines.append(f"  %% truncated to {max_nodes} nodes")
    return "\n".join(lines) + "\n"
