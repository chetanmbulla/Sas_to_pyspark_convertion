"""Classify scripts (used/unused) and resources (ultimate source, intermediate, final output)."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Iterable, Mapping

_SUSPECT_NAME = re.compile(
    r"(?:\b|_)(?:old|bak|backup|copy|test|tmp|temp|draft|obsolete|archive)(?:\b|_)"
    r"|\b(?:19|20)\d{2}[-_]?\d{2}[-_]?\d{2}\b|\(\d+\)",
    re.I,
)


def _keyed(path: str) -> str:
    return path.casefold()


def classify_scripts(
    pipeline_report: Mapping[str, Any], resources: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Return one record per discovered script.

    ``role``: ``entrypoint`` | ``included`` | ``unreached``. Reachable scripts also get an
    ``activity`` of ``producer`` (writes a non-WORK dataset or file), ``work_only``,
    ``read_only`` or ``no_io``; unreached scripts have activity ``unreached`` and are not
    scanned. ``suspect_name`` flags backup/test/dated file names, which usually indicate
    obsolete code rather than a missing job.
    """
    entrypoints = {_keyed(str(item["path"])) for item in pipeline_report.get("entrypoints", [])}
    reads: dict[str, set[str]] = defaultdict(set)
    writes: dict[str, set[str]] = defaultdict(set)
    persistent_writes: dict[str, set[str]] = defaultdict(set)
    for resource in resources:
        is_work = str(resource["name"]).casefold().startswith("work.")
        for evidence in resource.get("evidence", []):
            source = _keyed(str(evidence["source"]))
            direction = evidence.get("direction")
            if direction == "input":
                reads[source].add(resource["name"])
            elif direction == "output":
                writes[source].add(resource["name"])
                if not is_work:
                    persistent_writes[source].add(resource["name"])

    included_by: dict[str, set[str]] = defaultdict(set)
    for include in pipeline_report.get("includes", []):
        if include.get("status") in {"resolved", "waived"} and include.get("target"):
            included_by[_keyed(str(include["target"]))].add(_keyed(str(include["source"])))

    records = []
    for script in pipeline_report.get("scripts", []):
        path = str(script["path"])
        key = _keyed(path)
        reachable = script.get("status") == "reachable"
        record: dict[str, Any] = {
            "path": path,
            "server": script.get("server"),
            "reachable": reachable,
            "included_by": sorted(included_by.get(key, ())),
            "suspect_name": bool(_SUSPECT_NAME.search(path.rsplit("/", 1)[-1])),
        }
        if key in entrypoints:
            record["role"] = "entrypoint"
        elif reachable:
            record["role"] = "included"
        else:
            record["role"] = "unreached"
        if reachable:
            if persistent_writes.get(key):
                activity = "producer"
            elif writes.get(key):
                activity = "work_only"
            elif reads.get(key):
                activity = "read_only"
            else:
                activity = "no_io"
            record.update(
                activity=activity,
                inputs=sorted(reads.get(key, ())),
                outputs=sorted(writes.get(key, ())),
            )
        else:
            record["activity"] = "unreached"
        records.append(record)
    return sorted(records, key=lambda item: item["path"].casefold())


def classify_resources(
    resources: Iterable[Mapping[str, Any]],
    dataset_edges: Mapping[str, Iterable[str]],
) -> list[dict[str, Any]]:
    """Classify datasets/files as ultimate source, intermediate or final output.

    ``dataset_edges`` maps a target dataset to the datasets it is built from. Roles use
    only reachable-script evidence: ``ultimate_source`` is read and never written,
    ``final_output`` is written and never read, ``intermediate`` is both. WORK datasets are
    flagged ``temporary``. Written datasets list the ultimate sources upstream of them.
    """
    resources = list(resources)
    written: dict[str, list[str]] = defaultdict(list)
    read: set[str] = set()
    for resource in resources:
        for evidence in resource.get("evidence", []):
            direction = evidence.get("direction")
            if direction == "output":
                written[resource["name"]].append(evidence["source"])
            elif direction == "input":
                read.add(resource["name"])

    upstream = {target: set(sources) - {target} for target, sources in dataset_edges.items()}
    source_names = {
        name for name in (r["name"] for r in resources) if name not in written
    }

    def ultimate(name: str) -> list[str]:
        found: set[str] = set()
        seen = {name}
        stack = [name]
        while stack:
            for parent in upstream.get(stack.pop(), ()):
                if parent in seen:
                    continue
                seen.add(parent)
                if parent in source_names:
                    found.add(parent)
                stack.append(parent)
        return sorted(found)

    records = []
    for resource in resources:
        name = resource["name"]
        is_written = name in written
        is_read = name in read or any(
            name in sources for sources in upstream.values()
        )
        if not is_written:
            role = "ultimate_source"
        elif is_read:
            role = "intermediate"
        else:
            role = "final_output"
        record: dict[str, Any] = {
            "name": name,
            "kind": resource["kind"],
            "role": role,
            "temporary": resource["kind"] == "sas_dataset" and name.casefold().startswith("work."),
            "produced_by": sorted(set(written.get(name, ()))),
        }
        if is_written:
            record["ultimate_sources"] = ultimate(name)
        records.append(record)
    return sorted(records, key=lambda item: (item["kind"], item["name"].casefold()))


def summarize_roles(
    scripts: list[Mapping[str, Any]], resources: list[Mapping[str, Any]]
) -> dict[str, Any]:
    def count(items: list[Mapping[str, Any]], field: str) -> dict[str, int]:
        totals: dict[str, int] = defaultdict(int)
        for item in items:
            totals[str(item[field])] += 1
        return dict(sorted(totals.items()))

    persistent = [r for r in resources if not r["temporary"]]
    return {
        "script_roles": count(list(scripts), "role"),
        "script_activity": count(list(scripts), "activity"),
        "unused_script_count": sum(not s["reachable"] for s in scripts),
        "suspect_unused_script_count": sum(
            not s["reachable"] and s["suspect_name"] for s in scripts
        ),
        "resource_roles": count(persistent, "role"),
        "temporary_dataset_count": len(resources) - len(persistent),
    }
