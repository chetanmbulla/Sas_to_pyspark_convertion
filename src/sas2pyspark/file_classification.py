"""Evidence-linked, static classification of discovered SAS programs."""

from __future__ import annotations

import hashlib
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from .discovery import SasProgramInventory, _decode_source
from .sas_parser import SasParseError, parse_program


CLASSIFICATION_SCHEMA_VERSION = 1
_DATASET = re.compile(r"^\s*data\s+([A-Za-z_][\w.]*)", re.I)
_PROC = re.compile(r"^\s*proc\s+([A-Za-z_]\w*)", re.I)
_MACRO_DEFINITION = re.compile(r"^\s*%macro\s+([A-Za-z_]\w*)", re.I)
_MACRO_REFERENCE = re.compile(
    r"%(?!macro\b|mend\b|let\b|include\b|inc\b|if\b|then\b|else\b|do\b|end\b|"
    r"sysfunc\b|put\b|eval\b|scan\b|substr\b|upcase\b|lowcase\b|global\b|"
    r"local\b|return\b|goto\b|abort\b|while\b|until\b|to\b|by\b|str\b|"
    r"nrstr\b|quote\b|unquote\b|symput\b|symget\b|sysget\b|syslput\b|"
    r"sysrput\b)([A-Za-z_]\w*)\b(?=\s*(?:\(|;))",
    re.I,
)
_SENSITIVE_VALUE = re.compile(
    r"(?i)(password|passwd|pwd|user(name)?|uid|token|secret|credential|"
    r"access[._-]?key)\s*=\s*('[^']*'|\"[^\"]*\"|[^;\s,)]+)"
)
_DOMAIN_TERMS: dict[str, tuple[str, ...]] = {
    "Inventory / Stock Management": (
        "inventory", "stock", "on hand", "stock fip", "stock level",
        "stock management", "stock quantity",
    ),
    "Supply Chain / Logistics": (
        "supply chain", "logistics", "shipment", "shipping", "delivery",
        "freight", "transport",
    ),
    "Manufacturing / Production": (
        "manufacturing", "production", "work order", "bill of material", "plant",
    ),
    "Procurement / Purchasing": (
        "procurement", "purchasing", "purchase order", "supplier", "vendor",
    ),
    "Sales / Commercial": (
        "sales", "revenue", "commercial", "order value", "sales order",
    ),
    "Finance / Accounting": (
        "finance", "accounting", "ledger", "invoice", "cost center", "currency",
    ),
    "Customer Management": ("customer", "client", "account holder"),
    "Product / Material Management": (
        "product", "material", "sku", "article", "item master",
    ),
    "Risk / Compliance": (
        "risk", "compliance", "audit", "regulatory", "control finding",
    ),
    "Enterprise Shared Services": (
        "shared service", "enterprise service", "master data",
    ),
    "Technical Administration": (
        "autoexec", "configuration", "initialization", "system option",
    ),
}
_REPORT_PROCS = frozenset({"PRINT", "REPORT", "TABULATE"})
_AGGREGATE_PROCS = frozenset({"MEANS", "SUMMARY", "FREQ"})
_STATISTICAL_PROCS = frozenset(
    {"ANOVA", "CLUSTER", "CORR", "FACTOR", "GLM", "LOGISTIC", "REG", "TTEST"}
)
_QUALITY_PROCS = frozenset({"COMPARE"})
_ADMIN_PROCS = frozenset({"CONTENTS", "DATASETS", "FORMAT"})


def _key(path: str) -> str:
    return path.replace("\\", "/").casefold()


def _distribution(values: Iterable[str], total: int) -> dict[str, dict[str, int | float]]:
    counts = Counter(values)
    return {
        name: {
            "count": count,
            "percentage": round(100 * count / total, 1) if total else 0.0,
        }
        for name, count in sorted(counts.items(), key=lambda item: item[0].casefold())
    }


def _safe_statement(text: str) -> str:
    return _SENSITIVE_VALUE.sub(lambda match: f"{match.group(1)}=<redacted>", text)


def _evidence(statement: str, line: int, category: str) -> dict[str, Any]:
    return {
        "category": category,
        "line": line,
        "statement": _safe_statement(statement.strip())[:240],
        "source_type": "static_source_analysis",
    }


def _transitive_includes(
    start: str, direct: Mapping[str, set[str]]
) -> list[str]:
    seen: set[str] = set()
    start_key = _key(start)
    stack = list(direct.get(_key(start), ()))
    while stack:
        target = stack.pop()
        normalized = _key(target)
        if normalized == start_key or normalized in seen:
            continue
        seen.add(normalized)
        stack.extend(direct.get(normalized, ()))
    return sorted(seen, key=str.casefold)


def _business_domain(
    source: str, relative_path: str, resources: list[Mapping[str, Any]]
) -> dict[str, Any]:
    domain_evidence: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_lines = source.splitlines()
    for line_number, line in enumerate(source_lines, start=1):
        lowered = line.casefold()
        for domain, terms in _DOMAIN_TERMS.items():
            matches = [term for term in terms if term in lowered]
            if matches:
                domain_evidence[domain].append(
                    {
                        "line": line_number,
                        "terms": sorted(set(matches)),
                        "source_type": "source_text",
                    }
                )
    for resource in resources:
        if not any(
            _key(str(item.get("source", ""))) == _key(relative_path)
            for item in resource.get("evidence", [])
        ):
            continue
        name = str(resource.get("name", "")).casefold()
        for domain, terms in _DOMAIN_TERMS.items():
            matches = [term for term in terms if term in name]
            if matches:
                line = next(
                    (
                        item.get("line")
                        for item in resource.get("evidence", [])
                        if _key(str(item.get("source", ""))) == _key(relative_path)
                    ),
                    None,
                )
                domain_evidence[domain].append(
                    {
                        "line": line,
                        "terms": sorted(set(matches)),
                        "source_type": "dataset_name",
                        "dataset": resource.get("name"),
                    }
                )

    ordered = sorted(
        domain_evidence,
        key=lambda name: (-len(domain_evidence[name]), name.casefold()),
    )
    primary = ordered[0] if ordered else "Unknown / Unclassified"
    confidence = (
        "medium"
        if primary != "Unknown / Unclassified"
        and any(item["source_type"] == "source_text" for item in domain_evidence[primary])
        else "low"
    )
    datasets = sorted(
        {
            str(resource.get("name"))
            for resource in resources
            if any(
                _key(str(item.get("source", ""))) == _key(relative_path)
                for item in resource.get("evidence", [])
            )
        },
        key=str.casefold,
    )
    return {
        "primary_business_domain": primary,
        "secondary_business_domains": ordered[1:],
        "business_purpose_summary": (
            f"Source terminology supports {primary}."
            if ordered
            else "Business purpose cannot be established from available source evidence."
        ),
        "key_business_entities": sorted(
            {
                term
                for items in domain_evidence.values()
                for item in items
                for term in item["terms"]
            },
            key=str.casefold,
        ),
        "datasets_examined": datasets,
        "key_business_measures": [],
        "identified_business_rules": [],
        "business_domain_evidence": [
            {"domain": domain, **item}
            for domain in ordered
            for item in domain_evidence[domain]
        ],
        "business_domain_confidence": confidence,
        "unresolved_business_questions": (
            []
            if ordered
            else ["Which business process and data owner does this program support?"]
        ),
    }


def _technical_classification(
    statements: list[Any],
    output_datasets: list[str],
    macro_definitions: list[str],
) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    flags: set[str] = set()
    data_steps = 0
    has_sql_join = False
    has_sql_aggregate = False
    has_sql_write = False
    if output_datasets:
        flags.add("publishing")
    for statement in statements:
        text = statement.text
        lowered = text.casefold()
        if re.search(r"\bods\s+(pdf|excel|html|rtf)\b", lowered):
            flags.add("reporting")
            evidence.append(_evidence(text, statement.span.start_line, "report output"))
        procedure = _PROC.match(text)
        if procedure:
            name = procedure.group(1).upper()
            evidence.append(_evidence(text, statement.span.start_line, f"PROC {name}"))
            if name in _REPORT_PROCS:
                flags.add("reporting")
            if name in _AGGREGATE_PROCS or re.search(r"\bgroup\s+by\b", lowered):
                flags.add("aggregation")
            if name in _STATISTICAL_PROCS:
                flags.add("statistical")
            if name in _QUALITY_PROCS:
                flags.add("quality")
            if name in _ADMIN_PROCS:
                flags.add("administration")
            if name == "SQL":
                flags.add("sql")
                has_sql_join |= bool(re.search(r"\bjoin\b", lowered))
                has_sql_aggregate |= bool(re.search(r"\bgroup\s+by\b|\b(count|sum|avg|min|max)\s*\(", lowered))
                has_sql_write |= bool(re.search(r"\b(insert|update|delete)\b", lowered))
            if name == "IMPORT":
                flags.add("ingestion")
            if name == "EXPORT":
                flags.add("publishing")
                flags.add("external_file")
            continue

        dataset_match = _DATASET.match(text)
        if dataset_match:
            data_steps += 1
            flags.add("transformation")
            evidence.append(_evidence(text, statement.span.start_line, "DATA step"))
        for pattern, category in (
            (r"^\s*merge\b", "integration"),
            (r"^\s*update\b", "integration"),
            (r"^\s*modify\b", "data_management"),
            (r"^\s*libname\b", "database_or_library"),
            (r"^\s*filename\b", "external_file"),
            (r"^\s*(?:infile|file)\b", "external_file"),
            (r"^\s*%macro\b", "macro_definition"),
            (r"^\s*%include\b|^\s*%inc\b", "include"),
            (r"^\s*(?:options|%let)\b", "initialization"),
            (r"\b(?:syscc|syserr|abort|endsas)\b", "error_control"),
            (r"\b(?:proc\s+sql|connect\s+to|execute\s+by)\b", "database_connectivity"),
            (r"\b(?:test\s+(?:program|data|case)|debug(?:ging)?\b)", "testing"),
        ):
            if re.search(pattern, text, re.I):
                flags.add(category)
                evidence.append(_evidence(text, statement.span.start_line, category))
        if re.search(r"^\s*merge\b|^\s*update\b", text, re.I):
            flags.add("integration")
        if re.search(r"\bset\b", text, re.I) and data_steps:
            flags.add("ingestion")
        if re.search(r"\b(?:join|union)\b", text, re.I):
            has_sql_join |= bool(re.search(r"\b(?:proc\s+sql|join|union)\b", text, re.I))
        if re.search(r"\b(?:group\s+by|summary|means)\b", text, re.I):
            flags.add("aggregation")
        if re.search(r"\b(?:proc\s+import|infile)\b", text, re.I):
            flags.add("ingestion")
        if re.search(r"\b(?:proc\s+export|file|put)\b", text, re.I):
            flags.add("external_file")
        if re.search(r"\b(?:insert|update|delete)\b", text, re.I):
            has_sql_write = True
        if re.search(r"\b(?:libname|connect\s+to|execute\s+by)\b", text, re.I):
            flags.add("database_connectivity")
        if re.search(
            r"\blibname\s+[A-Za-z_]\w*\s+(?:odbc|oledb|oracle|db2|sybase|"
            r"teradata|postgres(?:ql)?|mysql|sqlsrv|netezza|redshift)\b",
            text,
            re.I,
        ):
            flags.add("database_connectivity")

    categories: list[str] = []
    category_map = (
        ("Initialization / Environment Configuration", "initialization"),
        ("Data Ingestion / Extraction", "ingestion"),
        ("Data Transformation", "transformation"),
        ("Data Integration / Merging", "integration"),
        ("Data Aggregation", "aggregation"),
        ("Data Loading / Publishing", "publishing"),
        ("SQL Processing", "sql"),
        ("Macro Processing / Reusable Logic", "macro_definition"),
        ("Reporting / Business Intelligence", "reporting"),
        ("Statistical / Analytical Processing", "statistical"),
        ("Data Quality / Validation", "quality"),
        ("Data Management / Administration", "administration"),
        ("Database Connectivity / Integration", "database_connectivity"),
        ("File Handling / External Interfaces", "external_file"),
        ("Error Handling / Execution Control", "error_control"),
        ("Testing / Debugging", "testing"),
    )
    for category, flag in category_map:
        if flag in flags:
            categories.append(category)
    if "database_or_library" in flags and "Database Connectivity / Integration" not in categories:
        categories.append("Initialization / Environment Configuration")
    if has_sql_join:
        categories.append("Data Integration / Merging")
    if has_sql_aggregate:
        categories.append("Data Aggregation")
    if has_sql_write:
        categories.append("Data Loading / Publishing")
    categories = list(dict.fromkeys(categories))

    if "integration" in flags or has_sql_join:
        primary = "Data Integration / Merging"
    elif "aggregation" in flags or has_sql_aggregate:
        primary = "Data Aggregation"
    elif "reporting" in flags:
        primary = "Reporting / Business Intelligence"
    elif "quality" in flags:
        primary = "Data Quality / Validation"
    elif "statistical" in flags:
        primary = "Statistical / Analytical Processing"
    elif data_steps:
        primary = "Data Transformation"
    elif "ingestion" in flags and not output_datasets:
        primary = "Data Ingestion / Extraction"
    elif "publishing" in flags or has_sql_write or output_datasets:
        primary = "Data Loading / Publishing"
    elif "sql" in flags:
        primary = "SQL Processing"
    elif macro_definitions:
        primary = "Macro Processing / Reusable Logic"
    elif "database_connectivity" in flags:
        primary = "Database Connectivity / Integration"
    elif "external_file" in flags:
        primary = "File Handling / External Interfaces"
    elif "initialization" in flags or "database_or_library" in flags:
        primary = "Initialization / Environment Configuration"
    elif "administration" in flags:
        primary = "Data Management / Administration"
    elif "error_control" in flags:
        primary = "Error Handling / Execution Control"
    else:
        primary = "Unknown / Unclassified"
    if primary == "Unknown / Unclassified":
        confidence = "low"
        categories.append(primary)
    elif evidence:
        confidence = "medium"
    else:
        confidence = "low"
    secondary = [category for category in categories if category != primary]
    return {
        "primary_technical_category": primary,
        "secondary_technical_categories": secondary,
        "technical_evidence": evidence,
        "technical_confidence": confidence,
        "unresolved_technical_questions": (
            ["Primary technical responsibility is not clear from static source analysis."]
            if primary == "Unknown / Unclassified"
            else []
        ),
    }


def build_file_classification(
    repository_root: str | Path,
    programs: Iterable[SasProgramInventory],
    pipeline_report: Mapping[str, Any],
    catalog_report: Mapping[str, Any],
    *,
    configuration_source: str = "provided",
) -> dict[str, Any]:
    """Classify every discovered SAS file without treating static evidence as runtime fact."""
    root = Path(repository_root).resolve(strict=True)
    scripts = pipeline_report.get("scripts", [])
    server_by_path = {_key(str(item["path"])): item.get("server") for item in scripts}
    entrypoints = {
        _key(str(item.get("path", ""))) for item in pipeline_report.get("entrypoints", [])
    }
    direct_includes: dict[str, set[str]] = defaultdict(set)
    includes_by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    included_by: dict[str, set[str]] = defaultdict(set)
    for item in pipeline_report.get("includes", []):
        source = str(item.get("source", ""))
        target = item.get("target")
        includes_by_source[_key(source)].append(dict(item))
        if target and item.get("status") in {"resolved", "waived"}:
            direct_includes[_key(source)].add(str(target))
            included_by[_key(str(target))].add(source)

    resources = list(catalog_report.get("resources", []))
    systems = list(catalog_report.get("systems", []))
    jobs_by_entrypoint: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for job in pipeline_report.get("jobs", []):
        entrypoint = str(job.get("entrypoint", ""))
        jobs_by_entrypoint[_key(entrypoint)].append(dict(job))
    definitions_by_name: dict[str, list[str]] = defaultdict(list)
    parsed_by_path: dict[str, tuple[str, list[Any], str | None]] = {}
    for program in programs:
        path = str(program.path)
        physical = root / Path(path)
        try:
            contents = physical.read_bytes()
        except OSError as error:
            parsed_by_path[_key(path)] = ("", [], f"Cannot read source: {error}")
            continue
        source, _, decode_error = _decode_source(contents)
        if source is None:
            parsed_by_path[_key(path)] = ("", [], decode_error or "Cannot decode source.")
            continue
        try:
            statements = list(parse_program(source).statements)
        except SasParseError as error:
            parsed_by_path[_key(path)] = (source, [], str(error))
            continue
        parsed_by_path[_key(path)] = (source, statements, None)
        for statement in statements:
            definition = _MACRO_DEFINITION.match(statement.text)
            if definition:
                definitions_by_name[definition.group(1).casefold()].append(path)
    invocations_by_path = {
        path: {
            match.group(1).casefold()
            for statement in parsed[1]
            for match in _MACRO_REFERENCE.finditer(statement.text)
        }
        for path, parsed in parsed_by_path.items()
    }

    records: list[dict[str, Any]] = []
    for program in programs:
        relative = str(program.path)
        normalized = _key(relative)
        source, statements, parse_error = parsed_by_path.get(
            normalized, ("", [], "Source was not available for classification.")
        )
        file_name = relative.replace("\\", "/").rsplit("/", 1)[-1]
        path = root / Path(relative)
        scoped_resources = [
            item
            for item in resources
            if any(
                _key(str(evidence.get("source", ""))) == normalized
                for evidence in item.get("evidence", [])
            )
        ]
        inputs = sorted(
            {
                str(item.get("name"))
                for item in scoped_resources
                if any(
                    _key(str(evidence.get("source", ""))) == normalized
                    and evidence.get("direction") == "input"
                    for evidence in item.get("evidence", [])
                )
            },
            key=str.casefold,
        )
        outputs = sorted(
            {
                str(item.get("name"))
                for item in scoped_resources
                if any(
                    _key(str(evidence.get("source", ""))) == normalized
                    and evidence.get("direction") in {"output", "update"}
                    for evidence in item.get("evidence", [])
                )
            },
            key=str.casefold,
        )
        macro_definitions = sorted(
            {
                match.group(1)
                for statement in statements
                if (match := _MACRO_DEFINITION.match(statement.text))
            },
            key=str.casefold,
        )
        macro_definition_evidence = [
            {
                "name": match.group(1),
                "line": statement.span.start_line,
                "source_type": "static_source_analysis",
            }
            for statement in statements
            if (match := _MACRO_DEFINITION.match(statement.text))
        ]
        macro_invocations: set[str] = set()
        macro_invocation_evidence: list[dict[str, Any]] = []
        for statement in statements:
            for match in _MACRO_REFERENCE.finditer(statement.text):
                macro_invocations.add(match.group(1))
                macro_invocation_evidence.append(
                    {
                        "name": match.group(1),
                        "line": statement.span.start_line,
                        "source_type": "static_source_analysis",
                    }
                )
        macro_callers = sorted(
            (
                caller
                for caller, names in invocations_by_path.items()
                if caller != normalized
                and any(name.casefold() in names for name in macro_definitions)
            ),
            key=str.casefold,
        )
        macro_dependencies = [
            {
                "name": name,
                "status": (
                    "defined_in_project"
                    if name.casefold() in definitions_by_name
                    else "unresolved"
                ),
                "defined_in": sorted(
                    definitions_by_name.get(name.casefold(), []), key=str.casefold
                ),
            }
            for name in sorted(macro_invocations, key=str.casefold)
        ]
        technical = _technical_classification(
            statements, outputs, macro_definitions
        )
        business = _business_domain(source, relative, scoped_resources)
        called_programs = sorted(
            direct_includes.get(normalized, ()), key=str.casefold
        )
        calling_programs = sorted(included_by.get(normalized, ()), key=str.casefold)
        transitive_includes = _transitive_includes(relative, direct_includes)
        jobs = jobs_by_entrypoint.get(normalized, [])
        is_entrypoint = normalized in entrypoints
        fallback_entrypoint = (
            configuration_source == "all_scripts_fallback" and is_entrypoint
        )
        if fallback_entrypoint:
            primary_program_type = "Unknown / Unclassified"
            program_confidence = "low"
            program_evidence = ["All-scripts fallback entrypoints are not authoritative."]
        elif jobs:
            primary_program_type = "Main / Entry-Point Program"
            program_confidence = "high"
            program_evidence = ["Configured JAMS job maps to this entrypoint."]
        elif is_entrypoint:
            primary_program_type = "Main / Entry-Point Program"
            program_confidence = "high"
            program_evidence = ["Configured pipeline entrypoint."]
        elif macro_definitions and macro_callers:
            primary_program_type = "Reusable Macro Library"
            program_confidence = "high"
            program_evidence = [
                "Contains macro definitions invoked from another discovered SAS file."
            ]
        elif calling_programs:
            primary_program_type = "Included SAS Component"
            program_confidence = "high"
            program_evidence = ["Referenced by a statically resolved %INCLUDE."]
        else:
            primary_program_type = "Unknown / Unclassified"
            program_confidence = "low"
            program_evidence = [
                "No authoritative entrypoint or caller relationship was found."
            ]
        secondary_roles: list[str] = []
        if jobs:
            secondary_roles.append("Scheduled Batch Program")
        if macro_definitions and primary_program_type != "Reusable Macro Library":
            secondary_roles.append("Local Macro Definitions")
        if any(re.search(r"^\s*proc\s+(?:report|print|tabulate)\b", item.text, re.I) for item in statements):
            secondary_roles.append("Reporting / Presentation Program")
        include_dependencies = [
            {
                "target": item.get("target"),
                "reference": item.get("reference"),
                "line": item.get("line"),
                "status": item.get("status"),
                "relationship": "direct",
            }
            for item in includes_by_source.get(normalized, [])
        ]
        include_dependencies.extend(
            {
                "target": target,
                "reference": None,
                "line": None,
                "status": "resolved_transitively",
                "relationship": "transitive",
            }
            for target in transitive_includes
            if _key(target) not in {_key(item.get("target") or "") for item in include_dependencies}
        )
        external_systems = sorted(
            {
                str(system.get("id") or system.get("name"))
                for system in systems
                if any(
                    _key(str(item)) == normalized
                    for item in system.get("sources", [])
                )
            },
            key=str.casefold,
        )
        external_system_evidence = [
            {
                key: item.get(key)
                for key in (
                    "line",
                    "mode",
                    "system",
                    "direction",
                    "target_server",
                )
            }
            for item in pipeline_report.get("external_connections", [])
            if _key(str(item.get("source", ""))) == normalized
        ]
        dataset_evidence = [
            {
                "name": item.get("name"),
                "kind": item.get("kind"),
                "direction": evidence_item.get("direction"),
                "operation": evidence_item.get("operation"),
                "line": evidence_item.get("line"),
                "systems": item.get("systems", []),
                "source_type": "catalog_static_analysis",
            }
            for item in scoped_resources
            for evidence_item in item.get("evidence", [])
            if _key(str(evidence_item.get("source", ""))) == normalized
        ]
        jobs_info = [
            {
                "name": item.get("name"),
                "schedule": item.get("schedule"),
                "depends_on": item.get("depends_on", []),
            }
            for item in jobs
        ]
        if fallback_entrypoint:
            entrypoint_status = "fallback_assigned_not_authoritative"
        elif is_entrypoint:
            entrypoint_status = "configured_entrypoint"
        else:
            entrypoint_status = "not_configured"
        referenced = bool(
            jobs
            or (is_entrypoint and not fallback_entrypoint)
            or calling_programs
            or called_programs
            or macro_callers
        )
        execution = {
            "execution_mode": "scheduled" if any(item.get("schedule") for item in jobs) else "unknown",
            "execution_frequency": sorted(
                {str(item["schedule"]) for item in jobs if item.get("schedule")},
                key=str.casefold,
            ),
            "scheduler_name": "JAMS" if jobs else None,
            "jams_job_id": [str(item.get("name")) for item in jobs],
            "trigger_mechanism": (
                "configured JAMS schedule"
                if any(item.get("schedule") for item in jobs)
                else "configured pipeline entrypoint"
                if is_entrypoint and not fallback_entrypoint
                else "unknown"
            ),
            "upstream_job_dependencies": sorted(
                {str(dependency) for item in jobs for dependency in item.get("depends_on", [])},
                key=str.casefold,
            ),
            "downstream_job_dependencies": sorted(
                {
                    str(other.get("name"))
                    for other in pipeline_report.get("jobs", [])
                    if any(
                        str(dependency).casefold()
                        in {str(job.get("name", "")).casefold() for job in jobs}
                        for dependency in other.get("depends_on", [])
                    )
                },
                key=str.casefold,
            ),
            "execution_server": server_by_path.get(normalized),
            "environment": "unknown",
            "execution_history": "unavailable",
            "last_execution_date": None,
            "last_successful_execution": None,
            "execution_failure_history": "unavailable",
            "runtime_duration": None,
            "execution_status": "unknown",
            "usage_classification": (
                "referenced_but_not_observed" if referenced else "orphan_candidate"
            ),
            "execution_confidence": "low",
            "execution_evidence": [
                "Static configuration and include relationships only; no runtime history was supplied."
            ],
            "execution_unresolved_questions": [
                "What is the authoritative execution schedule and observation period?",
                "Can JAMS execution history and runtime metrics be supplied?",
            ],
        }
        evidence = [
            *technical["technical_evidence"],
            *business["business_domain_evidence"],
            *(
                {
                    "category": "program_type",
                    "line": None,
                    "statement": item,
                    "source_type": "pipeline_configuration",
                }
                for item in program_evidence
            ),
        ]
        if parse_error:
            evidence.append(
                {
                    "category": "analysis_limit",
                    "line": None,
                    "statement": parse_error[:240],
                    "source_type": "static_analysis",
                }
            )
            technical["primary_technical_category"] = "Unknown / Unclassified"
            technical["technical_confidence"] = "low"
            technical["unresolved_technical_questions"] = [parse_error]
            business["primary_business_domain"] = "Unknown / Unclassified"
            business["secondary_business_domains"] = []
            business["business_domain_confidence"] = "low"
            business["unresolved_business_questions"] = [
                "Source could not be parsed; what business process does it support?"
            ]
            primary_program_type = "Unknown / Unclassified"
            program_confidence = "low"
            program_evidence = ["Source could not be parsed."]
        review_status = (
            "review_required"
            if parse_error
            or execution["execution_status"] == "unknown"
            or technical["primary_technical_category"] == "Unknown / Unclassified"
            or business["primary_business_domain"] == "Unknown / Unclassified"
            or primary_program_type == "Unknown / Unclassified"
            or any(item.get("status") in {"unresolved", "runtime_generated"} for item in include_dependencies)
            or any(item["status"] == "unresolved" for item in macro_dependencies)
            else "initial_classification"
        )
        records.append(
            {
                "file_id": hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16],
                "file_name": file_name,
                "server": server_by_path.get(normalized),
                "full_path": str(path),
                "file_hash": program.sha256,
                "primary_technical_category": technical["primary_technical_category"],
                "secondary_technical_categories": technical["secondary_technical_categories"],
                "primary_business_domain": business["primary_business_domain"],
                "secondary_business_domains": business["secondary_business_domains"],
                "primary_program_type": primary_program_type,
                "secondary_program_roles": secondary_roles,
                **execution,
                "input_datasets": inputs,
                "output_datasets": outputs,
                "dataset_evidence": dataset_evidence,
                "include_dependencies": include_dependencies,
                "macro_dependencies": macro_dependencies,
                "macro_definitions": macro_definitions,
                "macro_definition_evidence": macro_definition_evidence,
                "macro_invocations": sorted(macro_invocations, key=str.casefold),
                "macro_invocation_evidence": macro_invocation_evidence,
                "macro_callers": macro_callers,
                "called_programs": called_programs,
                "calling_programs": calling_programs,
                "transitive_include_dependencies": transitive_includes,
                "external_systems": external_systems,
                "external_system_evidence": external_system_evidence,
                "classification_evidence": evidence,
                "technical_evidence": technical["technical_evidence"],
                "technical_confidence": technical["technical_confidence"],
                "unresolved_technical_questions": technical[
                    "unresolved_technical_questions"
                ],
                **business,
                "program_type_confidence": program_confidence,
                "program_type_evidence": program_evidence,
                "program_type_unresolved_questions": (
                    []
                    if primary_program_type != "Unknown / Unclassified"
                    else ["Is this file an independent entrypoint or invoked by another asset?"]
                ),
                "execution_characteristics": execution,
                "jams_relationship": jobs_info,
                "entrypoint_status": entrypoint_status,
                "review_status": review_status,
                "analysis_error": parse_error,
            }
        )
    records.sort(key=lambda item: item["full_path"].casefold())
    total = len(records)

    def confidence_distribution(field: str) -> dict[str, dict[str, int | float]]:
        return _distribution((str(item[field]) for item in records), total)

    summary = {
        "file_count": total,
        "technical_category": _distribution(
            (str(item["primary_technical_category"]) for item in records), total
        ),
        "business_domain": _distribution(
            (str(item["primary_business_domain"]) for item in records), total
        ),
        "program_type": _distribution(
            (str(item["primary_program_type"]) for item in records), total
        ),
        "execution_mode": _distribution(
            (str(item["execution_mode"]) for item in records), total
        ),
        "execution_status": _distribution(
            (str(item["execution_status"]) for item in records), total
        ),
        "technical_confidence": confidence_distribution("technical_confidence"),
        "business_domain_confidence": confidence_distribution("business_domain_confidence"),
        "program_type_confidence": confidence_distribution("program_type_confidence"),
        "execution_confidence": confidence_distribution("execution_confidence"),
        "server": _distribution(
            (str(item["server"] or "Unknown") for item in records), total
        ),
        "review_status": _distribution(
            (str(item["review_status"]) for item in records), total
        ),
        "review_required_count": sum(
            item["review_status"] == "review_required" for item in records
        ),
        "unresolved_include_count": sum(
            dependency.get("status") in {"unresolved", "runtime_generated"}
            for item in records
            for dependency in item["include_dependencies"]
        ),
        "unresolved_macro_count": sum(
            dependency.get("status") == "unresolved"
            for item in records
            for dependency in item["macro_dependencies"]
        ),
        "runtime_observation_period": None,
    }
    return {
        "schema_version": CLASSIFICATION_SCHEMA_VERSION,
        "evidence_basis": "static source, pipeline configuration, and catalog evidence",
        "configuration_source": configuration_source,
        "runtime_history_available": False,
        "summary": summary,
        "files": records,
        "limitations": [
            "This is deterministic static classification, not a human-validated business assessment.",
            "No SAS execution history was supplied; execution status, last run, and runtime are unknown.",
            "Business domains are inferred only from explicit source terminology and dataset names; absence is classified as Unknown.",
            "Dynamically generated macro calls, includes, and runtime control flow may not be resolved.",
        ],
    }
