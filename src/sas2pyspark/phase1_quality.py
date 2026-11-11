"""Evidence-based completeness gates for Phase 1 project analysis."""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


def build_phase1_quality_report(
    *,
    configuration: str,
    pipeline_config: Mapping[str, Any],
    pipeline_report: Mapping[str, Any],
    catalog_report: Mapping[str, Any],
    phase1_index: Mapping[str, Any],
) -> dict[str, Any]:
    """Summarize unresolved evidence gaps without claiming semantic accuracy."""
    pipeline_summary = pipeline_report.get("summary", {})
    catalog_summary = catalog_report.get("summary", {})
    resources = [
        item for item in catalog_report.get("resources", [])
        if item.get("kind") == "sas_dataset"
    ]
    schema_statuses = Counter(
        str((item.get("schema") or {}).get("status", "unknown"))
        for item in resources
    )
    unknown_columns = sum(
        str(column.get("type", "unknown")).casefold() not in {"numeric", "character"}
        for item in resources
        for column in ((item.get("schema") or {}).get("columns") or [])
    )
    includes = pipeline_report.get("includes", [])
    include_statuses = Counter(str(item.get("status", "unknown")) for item in includes)
    libraries = pipeline_report.get("libraries", [])
    library_statuses = Counter(str(item.get("status", "unknown")) for item in libraries)
    systems = catalog_report.get("systems", [])
    system_statuses = Counter(str(item.get("status", "unknown")) for item in systems)
    jobs = pipeline_report.get("jobs", [])
    jobs_with_jams = sum(isinstance(item.get("jams"), Mapping) for item in jobs)
    declared_jams_jobs = pipeline_config.get("jams_jobs", [])
    if not isinstance(declared_jams_jobs, list):
        declared_jams_jobs = []
    context_sources = (
        phase1_index.get("summaries", {})
        .get("business_context", {})
        .get("by_source", {})
    )
    template_context_count = int(context_sources.get("template", 0))
    dynamic_reference_count = int(catalog_summary.get("dynamic_reference_count", 0))
    unresolved_lineage_count = int(
        catalog_summary.get("unresolved_column_lineage_count", 0)
    )
    unresolved_libraries = int(pipeline_summary.get("unresolved_library_count", 0))
    unresolved_includes = include_statuses.get("unresolved", 0)
    opaque_includes = include_statuses.get("runtime_generated", 0) + include_statuses.get("waived", 0)
    unresolved_systems = sum(
        count for status, count in system_statuses.items()
        if status.casefold() not in {"mapped", "known"}
    )
    external_connections = pipeline_report.get("external_connections", [])
    valid_directions = {"input", "output", "both", "input/output"}
    incomplete_connection_specs = sum(
        not item.get("mode")
        or not item.get("server")
        or not item.get("system")
        or str(item.get("direction", "")).casefold() not in valid_directions
        for item in external_connections
    )
    unmapped_drives = pipeline_summary.get("unmapped_drives", [])

    gates: list[dict[str, Any]] = []

    def gate(
        key: str,
        title: str,
        status: str,
        severity: str,
        evidence: str,
        required_action: str,
        assumption: str,
        unavailable_fields: tuple[str, ...] = (),
    ) -> None:
        gates.append(
            {
                "id": key,
                "title": title,
                "status": status,
                "severity": severity,
                "evidence": evidence,
                "required_action": required_action,
                "poc_assumption": assumption,
                "unavailable_fields": [
                    {"field": field, "value": "NA"}
                    for field in unavailable_fields
                ],
            }
        )

    if configuration == "all_scripts_fallback":
        gate(
            "pipeline_configuration",
            "Authoritative job entrypoints and server mappings",
            "blocked",
            "blocker",
            "All discovered SAS files were treated as entrypoints on a placeholder server.",
            "Provide confirmed JAMS jobs, server names, drive mappings, and remote aliases.",
            "Treat discovered files as candidate entrypoints; placeholder server mappings are not authoritative.",
            ("authoritative_entrypoints", "verified_server_mappings", "remote_aliases"),
        )
    else:
        gate(
            "pipeline_configuration",
            "Authoritative job entrypoints and server mappings",
            "review_required",
            "blocker",
            f"An uploaded pipeline configuration declares {len(pipeline_config.get('entrypoints', []))} entrypoints; its correctness is not independently verified.",
            "Compare entrypoints and server/drive mappings with the JAMS export and platform configuration.",
            "Use the supplied configuration as a working input until independently verified.",
        )

    if not declared_jams_jobs:
        gate(
            "jams_metadata",
            "JAMS schedule and dependency metadata",
            "blocked",
            "blocker",
            f"0 JAMS job definitions were supplied; {len(jobs)} source-header job records were analyzed.",
            "Supply a JAMS export or validate and enter job names, schedules, dependencies, and parameters.",
            "Use source-header job markers only; do not infer scheduler behavior.",
            ("jams_schedules", "job_dependencies", "calendars", "triggers", "runtime_parameters", "retry_policy"),
        )
    else:
        gate(
            "jams_metadata",
            "JAMS schedule and dependency metadata",
            "review_required",
            "blocker",
            f"{len(declared_jams_jobs)} JAMS records are configured; {jobs_with_jams} analyzed jobs carry attached JAMS metadata.",
            "Verify configured job metadata against an authoritative JAMS export.",
            "Treat configured JAMS records as unverified until matched against an authoritative export.",
        )

    gate(
        "include_resolution",
        "Recursive include coverage",
        "blocked" if unresolved_includes or opaque_includes else "review_required",
        "blocker" if unresolved_includes or opaque_includes else "review",
        f"{include_statuses.get('resolved', 0)} resolved, {unresolved_includes} unresolved, "
        f"{include_statuses.get('runtime_generated', 0)} runtime-generated, "
        f"{include_statuses.get('waived', 0)} waived includes.",
        "Supply missing include files and macro values; inspect runtime-generated or waived content and confirm include resolution.",
        "Resolve static includes from supplied files and macro values; do not invent unavailable include contents.",
        tuple(
            field for condition, field in (
                (unresolved_includes > 0, "unresolved_include_contents"),
                (include_statuses.get("runtime_generated", 0) > 0, "runtime_generated_include_contents"),
                (include_statuses.get("waived", 0) > 0, "waived_include_contents"),
            ) if condition
        ),
    )

    gate(
        "library_mapping",
        "SAS library physical paths",
        "blocked" if unresolved_libraries else "review_required",
        "blocker" if unresolved_libraries else "review",
        f"{library_statuses.get('mapped', 0)} mapped and {unresolved_libraries} unresolved library references; "
        f"{len(unmapped_drives)} drive mappings remain unresolved.",
        "Confirm every library's server, physical path, and access permissions; resolve each unmapped drive.",
        "Treat source LIBNAME statements as provisional; leave unresolved physical mappings unavailable.",
        tuple(
            field for condition, field in (
                (unresolved_libraries > 0, "unresolved_library_physical_paths"),
                (bool(unmapped_drives), "unmapped_drive_targets"),
            ) if condition
        ),
    )

    incomplete_schema_count = (
        schema_statuses.get("unknown", 0)
        + schema_statuses.get("partial", 0)
        + schema_statuses.get("inferred", 0)
    )
    gate(
        "authoritative_schemas",
        "Authoritative table schemas",
        "blocked" if schema_statuses.get("unknown", 0) or unknown_columns else "review_required",
        "blocker" if schema_statuses.get("unknown", 0) or unknown_columns else "review",
        f"{schema_statuses.get('complete', 0)} complete, {schema_statuses.get('inferred', 0)} inferred, "
        f"{schema_statuses.get('partial', 0)} partial, {schema_statuses.get('unknown', 0)} unknown datasets; "
        f"{unknown_columns} columns have unknown/unsupported types.",
        "Provide matched PROC CONTENTS, DDL, or equivalent authoritative metadata and resolve unknown columns.",
        "Use supplied schema assets and explicit source declarations; do not guess missing attributes.",
        tuple(
            field for condition, field in (
                (incomplete_schema_count > 0, "authoritative_schema_attributes"),
                (unknown_columns > 0, "unsupported_column_types"),
            ) if condition
        ),
    )

    gate(
        "external_systems",
        "External system endpoints and direction",
        "blocked" if unresolved_systems else "review_required",
        "blocker" if unresolved_systems else "review",
        f"{len(systems)} systems identified; {unresolved_systems} are unresolved or have unknown endpoints. "
        f"{incomplete_connection_specs} of {len(external_connections)} external connection records lack a mode, "
        "server, system, or recognized direction.",
        "Confirm external system, connection mode, endpoint/server, and input/output direction with platform owners.",
        "Treat connection syntax as an observed hint, not proof of a reachable or verified system.",
        tuple(
            field for condition, field in (
                (unresolved_systems > 0, "verified_external_system_endpoints"),
                (incomplete_connection_specs > 0, "complete_connection_mode_server_system_and_direction"),
            ) if condition
        ),
    )

    gate(
        "dynamic_lineage",
        "Dynamic references and column lineage",
        "blocked" if dynamic_reference_count or unresolved_lineage_count else "review_required",
        "blocker" if dynamic_reference_count or unresolved_lineage_count else "review",
        f"{dynamic_reference_count} dynamic references and {unresolved_lineage_count} unresolved column-lineage edges.",
        "Resolve macro-generated references and review unresolved lineage against SAS source and business rules.",
        "Use statically visible references only; leave runtime-selected lineage unavailable.",
        tuple(
            field for condition, field in (
                (dynamic_reference_count > 0, "runtime_resolved_dynamic_references"),
                (unresolved_lineage_count > 0, "unresolved_column_lineage"),
            ) if condition
        ),
    )

    gate(
        "business_context",
        "Business context validation",
        "review_required" if template_context_count else "awaiting_review",
        "review",
        f"{template_context_count} business-context records originate from templates.",
        "Have process owners validate business definitions, inputs, outputs, and critical transformations.",
        "Treat generated business context as draft until reviewed by process owners.",
        ("owner_validated_business_definitions",) if template_context_count else (),
    )

    blocker_count = sum(item["severity"] == "blocker" for item in gates)
    blocked_count = sum(item["status"] == "blocked" for item in gates)
    review_count = sum(item["status"] in {"review_required", "awaiting_review"} for item in gates)
    return {
        "schema_version": 1,
        "assessment": "not_certified",
        "missing_value_convention": (
            "NA means the required information was not available from supplied source "
            "files or configuration. It is not an inferred value or a confirmed absence."
        ),
        "poc_assumptions": True,
        "assessment_note": (
            "This report measures evidence gaps; it cannot certify that static analysis "
            "matches actual SAS runtime behavior or business intent. PoC assumptions are "
            "explicit, and unavailable evidence is marked NA."
        ),
        "summary": {
            "gate_count": len(gates),
            "blocked_count": blocked_count,
            "review_required_count": review_count,
            "blocker_gate_count": blocker_count,
            "dataset_count": len(resources),
            "schema_status_counts": dict(sorted(schema_statuses.items())),
            "unknown_or_unsupported_column_count": unknown_columns,
            "incomplete_schema_dataset_count": incomplete_schema_count,
        },
        "gates": gates,
    }
