"""Offline business-context generation: evidence packets, template descriptions, prompt export.

No network access happens here. Descriptions written by people or by an external AI can be
merged back with ``apply_responses``; they are tied to a hash of the evidence packet so a
stale description is detected when the pipeline changes.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

CONTEXT_SCHEMA_VERSION = 1
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_MAX_HEADER = 600
_MAX_LIST = 12
_BLOCK_COMMENT = re.compile(r"/\*(.*?)\*/", re.S)
_STAR_COMMENT = re.compile(r"^\s*\*(.*?);", re.S | re.M)
PROMPT_INSTRUCTIONS = (
    "You document a SAS pipeline for a migration project. Use ONLY the evidence packet below. "
    "Do not invent systems, business rules or names. If the evidence does not support a field, "
    'write "unknown". Respond with JSON: {"purpose": str, "business_context": str, '
    '"inputs": [str], "outputs": [str], "business_rules": [str], "open_questions": [str]}.'
)


class BusinessContextError(ValueError):
    pass


def _packet_hash(packet: Mapping[str, Any]) -> str:
    text = json.dumps(packet, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _clean_comment(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = " ".join(re.sub(r"[*=_\-#]{3,}", " ", text).split())
    cleaned = _EMAIL.sub("[email]", cleaned)[:_MAX_HEADER]
    return cleaned if re.search(r"[A-Za-z0-9]", cleaned) else None


def _header_comment(root: Path, relative: str) -> str | None:
    try:
        text = (root / relative).read_bytes().decode("utf-8", errors="replace")
    except OSError:
        return None
    head = text[:4000].lstrip()
    match = _BLOCK_COMMENT.match(head) or _STAR_COMMENT.match(head)
    return _clean_comment(match.group(1)) if match else None


def _capped(values: list[str]) -> dict[str, Any]:
    ordered = sorted(dict.fromkeys(values), key=str.casefold)
    return {"items": ordered[:_MAX_LIST], "total": len(ordered)}


def _persistent(names: list[str]) -> list[str]:
    return [name for name in names if not name.casefold().startswith("work.")]


def _describe_list(entry: Mapping[str, Any]) -> str:
    items = entry["items"]
    if not items:
        return "none detected"
    extra = entry["total"] - len(items)
    return ", ".join(items) + (f" (+{extra} more)" if extra > 0 else "")


def build_context_packets(
    pipeline_report: Mapping[str, Any], catalog: Mapping[str, Any], root: str | Path | None = None
) -> list[dict[str, Any]]:
    """Return one evidence packet per job, reachable script and persistent final-output dataset."""
    base = Path(root or catalog.get("repository_root") or ".")
    scripts = {item["path"]: item for item in catalog.get("script_classification", [])}
    roles = {item["name"]: item for item in catalog.get("resource_roles", [])}
    includes: dict[str, list[str]] = defaultdict(list)
    for include in pipeline_report.get("includes", []):
        if include.get("target"):
            includes[include["source"]].append(include["target"])
    systems = {item["id"]: item for item in catalog.get("systems", [])}
    systems_by_script: dict[str, list[str]] = defaultdict(list)
    for system in systems.values():
        for source in system.get("sources", []):
            systems_by_script[source].append(system["id"])
    writers: dict[str, list[str]] = defaultdict(list)
    consumers: dict[str, list[str]] = defaultdict(list)
    for path, item in scripts.items():
        for name in item.get("outputs", []):
            writers[name].append(path)
        for name in item.get("inputs", []):
            consumers[name].append(path)
    column_edges: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for edge in catalog.get("column_lineage", []):
        column_edges[edge["target"]].append(edge)
    schemas = {item["dataset"]: item for item in catalog.get("schemas", [])}

    def closure(entrypoint: str) -> list[str]:
        seen = {entrypoint}
        stack = [entrypoint]
        while stack:
            for child in includes.get(stack.pop(), []):
                if child not in seen:
                    seen.add(child)
                    stack.append(child)
        return sorted(seen)

    packets: list[dict[str, Any]] = []
    for job in pipeline_report.get("jobs", []):
        members = closure(job["entrypoint"]) if job.get("entrypoint") else []
        outputs = sorted(
            {
                name
                for path in members
                for name in _persistent(scripts.get(path, {}).get("outputs", []))
            },
            key=str.casefold,
        )
        final = [name for name in outputs if roles.get(name, {}).get("role") == "final_output"]
        sources = sorted(
            {
                source
                for name in outputs
                for source in roles.get(name, {}).get("ultimate_sources", [])
                if not source.casefold().startswith("work.")
            },
            key=str.casefold,
        )
        jams = job.get("jams") or {}
        packets.append(
            {
                "id": f"job:{job['name']}",
                "type": "job",
                "name": job["name"],
                "server": job.get("server"),
                "entrypoint": job.get("entrypoint"),
                "header_comment": _clean_comment(job.get("header_comment")),
                "jams": {key: jams[key] for key in sorted(jams) if jams[key] not in (None, "", [])} or None,
                "script_count": len(members),
                "scripts": _capped([Path(path).name for path in members]),
                "persistent_outputs": _capped(outputs),
                "final_outputs": _capped(final),
                "ultimate_sources": _capped(sources),
                "systems": _capped(
                    [sid for path in members for sid in systems_by_script.get(path, [])]
                ),
            }
        )

    for path, item in sorted(scripts.items()):
        if not item.get("reachable"):
            continue
        outputs = _persistent(item.get("outputs", []))
        packets.append(
            {
                "id": f"script:{path}",
                "type": "script",
                "name": Path(path).name,
                "path": path,
                "server": item.get("server"),
                "role": item.get("role"),
                "activity": item.get("activity"),
                "header_comment": _header_comment(base, path),
                "included_by": _capped([Path(p).name for p in item.get("included_by", [])]),
                "includes": _capped([Path(p).name for p in includes.get(path, [])]),
                "persistent_inputs": _capped(_persistent(item.get("inputs", []))),
                "persistent_outputs": _capped(outputs),
                "work_dataset_count": len(item.get("outputs", [])) - len(outputs),
                "systems": _capped(systems_by_script.get(path, [])),
            }
        )

    for name, role in sorted(roles.items(), key=lambda pair: pair[0].casefold()):
        if role.get("role") != "final_output" or role.get("temporary") or role["kind"] != "sas_dataset":
            continue
        transforms: dict[str, int] = defaultdict(int)
        for edge in column_edges.get(name, []):
            transforms[edge["transform"]] += 1
        schema = schemas.get(name, {})
        packets.append(
            {
                "id": f"dataset:{name}",
                "type": "dataset",
                "name": name,
                "produced_by": _capped([Path(p).name for p in role.get("produced_by", [])]),
                "consumed_by": _capped([Path(p).name for p in consumers.get(name, [])]),
                "ultimate_sources": _capped(
                    [s for s in role.get("ultimate_sources", []) if not s.casefold().startswith("work.")]
                ),
                "schema_status": schema.get("status", "unknown"),
                "column_count": len(schema.get("columns", [])),
                "columns": _capped([c["name"] for c in schema.get("columns", [])]),
                "column_transforms": dict(sorted(transforms.items())),
            }
        )

    for packet in packets:
        packet["packet_hash"] = _packet_hash({k: v for k, v in packet.items() if k != "packet_hash"})
    return packets


def template_description(packet: Mapping[str, Any]) -> dict[str, Any]:
    """Rule-based factual description; every statement comes directly from the packet."""
    kind = packet["type"]
    if kind == "job":
        purpose = (
            f"Job {packet['name']} on {packet.get('server') or 'unknown server'} runs "
            f"{packet['script_count']} script(s) from {Path(packet['entrypoint'] or '').name or 'an unknown entrypoint'}."
        )
        outputs = packet["persistent_outputs"]
        inputs = packet["ultimate_sources"]
    elif kind == "script":
        activity = {
            "producer": "writes persistent data",
            "work_only": "only builds temporary WORK datasets",
            "read_only": "only reads data",
            "no_io": "has no detected data I/O (setup, macros or control logic)",
        }.get(str(packet.get("activity")), "has unclassified activity")
        purpose = f"{packet['role'].capitalize()} script {packet['name']} {activity}."
        outputs = packet["persistent_outputs"]
        inputs = packet["persistent_inputs"]
    else:
        transforms = packet["column_transforms"]
        detail = (
            "; column derivations: " + ", ".join(f"{count} {name}" for name, count in transforms.items())
            if transforms
            else ""
        )
        purpose = (
            f"Dataset {packet['name']} is a final output built by {_describe_list(packet['produced_by'])} "
            f"({packet['column_count']} known column(s), schema {packet['schema_status']}{detail})."
        )
        outputs = {"items": [packet["name"]], "total": 1}
        inputs = packet["ultimate_sources"]
    context = packet.get("header_comment")
    open_questions = []
    if not context and kind != "dataset":
        open_questions.append("No header comment describes the business purpose; confirm with the owner.")
    if kind == "dataset" and packet["schema_status"] != "complete":
        open_questions.append("Schema is not authoritative; supply PROC CONTENTS or DDL.")
    if kind == "job" and not packet.get("jams"):
        open_questions.append("No JAMS metadata (schedule, owner) was supplied for this job.")
    return {
        "purpose": purpose,
        "business_context": context or "unknown",
        "inputs": inputs["items"],
        "inputs_total": inputs["total"],
        "outputs": outputs["items"],
        "outputs_total": outputs["total"],
        "business_rules": [],
        "open_questions": open_questions,
    }


def build_prompts(packets: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "id": packet["id"],
            "packet_hash": packet["packet_hash"],
            "prompt": f"{PROMPT_INSTRUCTIONS}\n\nEVIDENCE PACKET:\n{json.dumps(packet, indent=2, sort_keys=True)}",
        }
        for packet in packets
    ]


def load_responses(path: str | Path) -> list[dict[str, Any]]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BusinessContextError(f"Cannot read responses {str(path)!r}: {error}") from error
    entries = data.get("responses") if isinstance(data, dict) else data
    if not isinstance(entries, list):
        raise BusinessContextError('Responses must be a list or {"responses": [...]}.')
    for entry in entries:
        if not isinstance(entry, dict) or not entry.get("id") or not isinstance(entry.get("description"), dict):
            raise BusinessContextError("Every response needs an 'id' and a 'description' object.")
        if entry.get("source", "ai") not in {"ai", "human"}:
            raise BusinessContextError("Response 'source' must be 'ai' or 'human'.")
    return entries


def apply_responses(
    packets: list[Mapping[str, Any]], responses: list[Mapping[str, Any]]
) -> dict[str, Any]:
    """Return ``{id: record}`` merging template text with AI/human responses.

    Human responses always win and are never marked stale. AI responses are ``stale`` when
    their ``packet_hash`` differs from the current packet, and always carry ``needs_review``.
    """
    by_id = {packet["id"]: packet for packet in packets}
    records: dict[str, Any] = {}
    for packet in packets:
        records[packet["id"]] = {
            "id": packet["id"],
            "type": packet["type"],
            "name": packet["name"],
            "packet_hash": packet["packet_hash"],
            "source": "template",
            "needs_review": False,
            "stale": False,
            "description": template_description(packet),
        }
    unmatched = []
    for response in responses:
        packet = by_id.get(response["id"])
        if packet is None:
            unmatched.append(response["id"])
            continue
        source = response.get("source", "ai")
        record = records[packet["id"]]
        if source == "ai" and record["source"] == "human":
            continue
        merged = {**record["description"], **response["description"]}
        record.update(
            description=merged,
            source=source,
            needs_review=source == "ai",
            stale=source == "ai" and response.get("packet_hash") not in (None, packet["packet_hash"]),
            model=response.get("model"),
        )
    return {"records": records, "unmatched_response_ids": sorted(unmatched)}


def build_business_context(
    pipeline_report: Mapping[str, Any],
    catalog: Mapping[str, Any],
    *,
    root: str | Path | None = None,
    responses: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    packets = build_context_packets(pipeline_report, catalog, root)
    merged = apply_responses(packets, responses or [])
    records = sorted(merged["records"].values(), key=lambda item: (item["type"], item["name"].casefold()))
    by_source: dict[str, int] = defaultdict(int)
    for record in records:
        by_source[record["source"]] += 1
    return {
        "schema_version": CONTEXT_SCHEMA_VERSION,
        "project_name": pipeline_report.get("project_name"),
        "records": records,
        "packets": packets,
        "unmatched_response_ids": merged["unmatched_response_ids"],
        "summary": {
            "record_count": len(records),
            "by_source": dict(sorted(by_source.items())),
            "stale_count": sum(record["stale"] for record in records),
            "needs_review_count": sum(record["needs_review"] for record in records),
        },
        "limitations": [
            "Template descriptions restate detected facts; they do not explain business meaning.",
            "AI or human text is only merged from supplied response files; nothing is sent anywhere.",
            "Business rules are empty unless a response supplies them.",
        ],
    }


def render_markdown(context: Mapping[str, Any]) -> str:
    def block(record: Mapping[str, Any]) -> list[str]:
        description = record["description"]
        flag = (
            f" _(source: {record['source']}"
            + (", stale" if record["stale"] else "")
            + (", needs review" if record["needs_review"] else "")
            + ")_"
        )
        lines = [f"### {record['name']}{flag}", "", description["purpose"], ""]
        if description.get("business_context") not in (None, "unknown"):
            lines += [f"**Business context:** {description['business_context']}", ""]
        for title, key in (("Inputs", "inputs"), ("Outputs", "outputs"), ("Business rules", "business_rules"), ("Open questions", "open_questions")):
            values = description.get(key) or []
            if values:
                total = description.get(f"{key}_total")
                suffix = f" (showing {len(values)} of {total})" if total and total > len(values) else ""
                lines.append(f"**{title}**{suffix}")
                lines += [f"- {value}" for value in values]
                lines.append("")
        return lines

    lines = ["# Business context", "", "_Generated from static analysis; review before relying on it._", ""]
    for kind, title in (("job", "Jobs"), ("script", "Scripts"), ("dataset", "Final-output datasets")):
        selected = [record for record in context["records"] if record["type"] == kind]
        if not selected:
            continue
        lines += [f"## {title}", ""]
        for record in selected:
            lines += block(record)
    return "\n".join(lines).rstrip() + "\n"
