"""Project-level Markdown reporting from resolver, catalog, plan, and conversion data."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

_SENSITIVE_VALUE = re.compile(
    r"(?i)(password|passwd|pwd|user(name)?|uid|token|secret|credential|"
    r"access[._-]?key)\s*=\s*('[^']*'|\"[^\"]*\"|[^;\s,)]+)"
)
_URI_USERINFO = re.compile(r"(?i)(://)[^/@\s:]+(?::[^/@\s]*)?@")


class ProjectReportError(ValueError):
    """Raised when an input report is invalid or incompatible."""


def _read_report(path: str | Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ProjectReportError(f"Cannot read {label} {str(path)!r}: {error}") from error
    if not isinstance(value, dict):
        raise ProjectReportError(f"{label} must be a JSON object.")
    if value.get("schema_version") != 1:
        raise ProjectReportError(f"{label} has an unsupported schema version.")
    return value


def _path_key(value: Any) -> str:
    return str(value or "").replace("\\", "/").casefold()


def _complexity_label(score: int) -> str:
    return "low" if score <= 2 else "medium" if score <= 6 else "high"


def _mermaid_label(value: str) -> str:
    clean = re.sub(r"[\r\n]+", " ", value).replace('"', "'")
    return clean.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _markdown_cell(value: str) -> str:
    return (
        _mermaid_label(value)
        .replace("|", r"\|")
        .replace("`", "'")
    )


def _safe_error(value: str) -> str:
    value = _SENSITIVE_VALUE.sub(
        lambda match: f"{match.group(1)}=<redacted>", value
    )
    return _URI_USERINFO.sub(r"\1<redacted>@", value)


def build_project_report(
    pipeline_report: Mapping[str, Any],
    catalog_report: Mapping[str, Any],
    plan_report: Mapping[str, Any],
    conversion_report: Mapping[str, Any],
) -> dict[str, Any]:
    """Combine the existing project reports into a Markdown report and score."""
    for label, report in (
        ("pipeline", pipeline_report),
        ("catalog", catalog_report),
        ("plan", plan_report),
        ("conversion", conversion_report),
    ):
        if report.get("schema_version") != 1:
            raise ProjectReportError(f"{label.title()} report has an unsupported schema version.")
    project_name = str(
        conversion_report.get("project_name")
        or plan_report.get("project_name")
        or "SAS migration project"
    )
    if plan_report.get("project_id") != conversion_report.get("project_id"):
        raise ProjectReportError("Plan and conversion reports refer to different projects.")

    plans = {
        _path_key(program.get("path")): program
        for program in plan_report.get("programs", [])
    }
    conversions = {
        _path_key(outcome.get("source_path")): outcome
        for outcome in conversion_report.get("outcomes", [])
    }
    catalog_resources = catalog_report.get("resources", [])
    resources_by_source: dict[str, list[dict[str, Any]]] = {}
    for resource in catalog_resources:
        for evidence in resource.get("evidence", []):
            resources_by_source.setdefault(
                _path_key(evidence.get("source")), []
            ).append(resource)

    programs: list[dict[str, Any]] = []
    for script in pipeline_report.get("scripts", []):
        path = str(script.get("path", ""))
        plan = plans.get(_path_key(path), {})
        conversion = conversions.get(_path_key(path), {})
        outcome = str(conversion.get("status", "not_run"))
        base_score = int(plan.get("complexity_score", 0))
        warnings = conversion.get("warnings", [])
        if outcome == "failed":
            conversion_adjustment = 10
        elif outcome in {"pending", "not_run"}:
            conversion_adjustment = 5
        elif outcome == "succeeded":
            conversion_adjustment = min(len(warnings), 5)
        else:
            raise ProjectReportError(
                f"Unknown conversion status {outcome!r} for {path!r}."
            )
        score = min(100, base_score + conversion_adjustment)
        programs.append(
            {
                "path": path,
                "server": script.get("server"),
                "reachability": script.get("status", "unknown"),
                "plan_complexity": plan.get("complexity", "unscored"),
                "plan_score": base_score,
                "conversion_status": outcome,
                "conversion_warning_count": len(warnings),
                "conversion_adjustment": conversion_adjustment,
                "complexity_score": score,
                "complexity": _complexity_label(score),
                "resources": sorted(
                    {
                        item["name"]
                        for item in resources_by_source.get(_path_key(path), [])
                    },
                    key=str.casefold,
                ),
            }
        )
    programs.sort(key=lambda item: item["path"].casefold())
    scored_programs = [
        item for item in programs if item["reachability"] == "reachable"
    ]
    project_score = (
        round(
            sum(item["complexity_score"] for item in scored_programs)
            / len(scored_programs)
        )
        if scored_programs
        else 0
    )

    includes = pipeline_report.get("includes", [])
    dependencies = plan_report.get("dependencies", [])
    datasets = catalog_resources
    unresolved = (
        list(pipeline_report.get("issues", []))
        + list(catalog_report.get("issues", []))
        + list(plan_report.get("unresolved_references", []))
    )
    for path, conversion in conversions.items():
        if conversion.get("status") == "failed":
            unresolved.append(
                {
                    "source": conversion.get("source_path", path),
                    "line": None,
                    "kind": "conversion_failed",
                    "reference": _safe_error(str(conversion.get("error") or "Unknown conversion error.")),
                }
            )

    lines = [
        f"# {project_name} — SAS migration report",
        "",
        "## Executive summary",
        "",
        f"- **Reachable scripts:** {sum(item['reachability'] == 'reachable' for item in programs)} / {len(programs)}",
        f"- **Conversion outcomes:** {conversion_report.get('summary', {}).get('succeeded', 0)} succeeded; {conversion_report.get('summary', {}).get('failed', 0)} failed",
        f"- **Catalogued resources:** {len(datasets)}",
        f"- **External and physical systems:** {len(catalog_report.get('systems', []))}",
        f"- **Unresolved findings:** {len(unresolved)}",
        f"- **Project complexity:** {project_score}/100 — {_complexity_label(project_score)}",
        "",
        "Complexity is a prioritization heuristic, not a delivery estimate. Each reachable script starts with the static plan score; failed conversions add 10 points, conversions not yet run add 5, and successful conversions add up to 5 points for warnings. Scores are not evidence of runtime equivalence.",
        "",
        "## Conversion and complexity by script",
        "",
        "| Script | Server | Reachability | Conversion | Static score | Conversion adjustment | Complexity |",
        "|---|---|---|---|---:|---:|---|",
    ]
    for item in programs:
        lines.append(
            f"| `{_markdown_cell(item['path'])}` | {_markdown_cell(str(item['server'] or 'unknown'))} | "
            f"{item['reachability']} | {item['conversion_status']} "
            f"({item['conversion_warning_count']} warning(s)) | {item['plan_score']} | "
            f"+{item['conversion_adjustment']} | {item['complexity']} ({item['complexity_score']}) |"
        )

    lines.extend(
        [
            "",
            "## Pipeline graph",
            "",
            "```mermaid",
            "flowchart LR",
        ]
    )
    script_ids: dict[str, str] = {}
    for index, item in enumerate(programs):
        script_ids[_path_key(item["path"])] = f"script{index}"
        lines.append(
            f'  script{index}["{_mermaid_label(item["path"])}"]'
        )
    resource_ids: dict[str, str] = {}
    plan_program_by_id = {
        program.get("program_id"): _path_key(program.get("path"))
        for program in plan_report.get("programs", [])
    }
    plan_dataset_by_id = {
        dataset.get("dataset_id"): str(dataset.get("name", ""))
        for dataset in plan_report.get("datasets", [])
    }
    for index, resource in enumerate(datasets):
        resource_ids[str(resource.get("name", ""))] = f"resource{index}"
        lines.append(
            f'  resource{index}[("{_mermaid_label(str(resource.get("name", "")))}")]'
        )
    system_ids: dict[str, str] = {}
    for index, system in enumerate(catalog_report.get("systems", [])):
        system_id = str(system.get("id", ""))
        system_ids[system_id] = f"system{index}"
        label = " / ".join(
            str(value)
            for value in (system.get("server"), system.get("kind"), system.get("system"))
            if value
        )
        lines.append(f'  system{index}{{"{_mermaid_label(label)}"}}')
    for resource in datasets:
        resource_id = resource_ids.get(str(resource.get("name", "")))
        for system_id in resource.get("systems", []):
            target_id = system_ids.get(str(system_id))
            if resource_id and target_id:
                lines.append(f"  {resource_id} --> {target_id}")
    for include in includes:
        source_id = script_ids.get(_path_key(include.get("source")))
        target_id = script_ids.get(_path_key(include.get("target")))
        if source_id and target_id and include.get("status") == "resolved":
            lines.append(f"  {source_id} -. include .-> {target_id}")
    for connection in pipeline_report.get("external_connections", []):
        source_id = script_ids.get(_path_key(connection.get("source")))
        matched_system = next(
            (
                system
                for system in catalog_report.get("systems", [])
                if system.get("kind") == connection.get("mode")
                and str(system.get("system", "")).casefold()
                == str(connection.get("system", "")).casefold()
                and str(system.get("server", "")).casefold()
                == str(connection.get("server", "")).casefold()
            ),
            None,
        )
        target_id = (
            system_ids.get(str(matched_system.get("id")))
            if matched_system
            else None
        )
        if source_id and target_id:
            lines.append(f"  {source_id} -. connection .-> {target_id}")
    for remote in pipeline_report.get("remote_references", []):
        source_id = script_ids.get(_path_key(remote.get("source")))
        matched_system = next(
            (
                system
                for system in catalog_report.get("systems", [])
                if system.get("kind") == "sas_remote_submit"
                and str(system.get("system", "")).casefold()
                == str(remote.get("remote_alias") or "unknown").casefold()
                and str(system.get("server", "")).casefold()
                == str(remote.get("server") or "").casefold()
            ),
            None,
        )
        target_id = (
            system_ids.get(str(matched_system.get("id")))
            if matched_system
            else None
        )
        if source_id and target_id:
            lines.append(f"  {source_id} -. remote submit .-> {target_id}")
    for edge in dependencies:
        source_id = edge.get("source_id")
        target_id = edge.get("target_id")
        kind = edge.get("kind")
        if kind not in {"produces", "consumes"}:
            continue
        program_key = plan_program_by_id.get(source_id)
        dataset_name = plan_dataset_by_id.get(target_id)
        if kind == "consumes":
            dataset_name = plan_dataset_by_id.get(source_id)
            program_key = plan_program_by_id.get(target_id)
        program_id = script_ids.get(program_key or "")
        dataset_id = resource_ids.get(dataset_name or "")
        if program_id and dataset_id:
            arrow = "-->" if kind == "produces" else "<--"
            lines.append(f"  {program_id} {arrow} {dataset_id}")
    lines.extend(["```", "", "## Resources"])
    lines.extend(
        [
            "",
            "| Resource | Kind | Direction | Schema | System mapping |",
            "|---|---|---|---|---|",
        ]
    )
    for resource in datasets:
        schema = resource.get("schema", {})
        system_value = resource.get("systems", [])
        lines.append(
            f"| `{_markdown_cell(str(resource.get('name', '')))}` | "
            f"{resource.get('kind', 'unknown')} | {resource.get('direction', 'unknown')} | "
            f"{schema.get('status', 'unknown')} | {_markdown_cell(', '.join(map(str, system_value)) or 'unknown')} |"
        )
    lines.extend(["", "## Unresolved findings", ""])
    if unresolved:
        lines.extend(["| Source | Line | Finding | Reference |", "|---|---:|---|---|"])
        for issue in unresolved:
            lines.append(
                f"| `{_markdown_cell(str(issue.get('source', '')))}` | "
                f"{issue.get('line') or ''} | {_markdown_cell(str(issue.get('kind', issue.get('category', 'unresolved'))))} | "
                f"`{_markdown_cell(str(issue.get('reference', issue.get('name', ''))) )}` |"
            )
    else:
        lines.append("No unresolved findings were recorded in the supplied reports.")
    lines.extend(
        [
            "",
            "## Scope and limitations",
            "",
            "- The report combines static analysis and conversion outcomes; it does not execute SAS or verify converted results.",
            "- Unreached scripts are not proven unused outside the configured entrypoints.",
            "- Schema metadata is partial unless separately enriched with authoritative table metadata.",
            "- Conversion success means code generation succeeded, not that business behavior is equivalent.",
            "",
        ]
    )
    return {
        "schema_version": 1,
        "project_name": project_name,
        "complexity_score": project_score,
        "complexity": _complexity_label(project_score),
        "programs": programs,
        "markdown": "\n".join(lines),
        "summary": {
            "script_count": len(programs),
            "reachable_script_count": len(scored_programs),
            "resource_count": len(datasets),
            "unresolved_finding_count": len(unresolved),
            "complexity_score": project_score,
        },
    }


def build_project_report_from_files(
    *,
    pipeline_report_path: str | Path,
    catalog_report_path: str | Path,
    plan_report_path: str | Path,
    conversion_report_path: str | Path,
) -> dict[str, Any]:
    return build_project_report(
        _read_report(pipeline_report_path, "Pipeline report"),
        _read_report(catalog_report_path, "Catalog report"),
        _read_report(plan_report_path, "Plan report"),
        _read_report(conversion_report_path, "Conversion report"),
    )
