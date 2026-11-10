"""Conservative source, sink, and schema catalog from a pipeline report."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .discovery import _decode_source
from .sas_parser import SasParseError, parse_program


CATALOG_SCHEMA_VERSION = 1
_DATASET = re.compile(r"^[A-Za-z_&][A-Za-z0-9_&]*(?:\.[A-Za-z_&][A-Za-z0-9_&]*)?$")
_DATASET_TOKEN = re.compile(r"[A-Za-z_&][A-Za-z0-9_&]*(?:\.[A-Za-z_&][A-Za-z0-9_&]*)?")
_OPTION = re.compile(
    r"\b(data|out|base|file|datafile|outfile|table|dbms)\s*=\s*"
    r"(?:(['\"])(.*?)\2|([^\s;,)]+))",
    re.I,
)
_SQL_REFERENCE = re.compile(
    r"\b(?:from|join|update|into)\s+"
    r"(?!connection\s+to\b)"
    r"([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)",
    re.I,
)
_SQL_CREATE = re.compile(
    r"\bcreate\s+(?:table|view)\s+"
    r"([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)",
    re.I,
)
_ASSIGNMENT = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=(?!=)")
_LENGTH_DECL = re.compile(
    r"([A-Za-z_][A-Za-z0-9_]*)\s+(\$\s*\d+|\$\d+|\d+)(?=\s|$)",
    re.I,
)
_FORMAT_DECL = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z_]\w*\d*\.?)")
_SENSITIVE = re.compile(
    r"(?i)(password|passwd|pwd|user(name)?|uid|token|secret|credential|"
    r"access[._-]?key)\s*=\s*('[^']*'|\"[^\"]*\"|[^;\s,)]+)"
)
_URI_USERINFO = re.compile(r"(?i)(://)[^/@\s:]+(?::[^/@\s]*)?@")


class SourceSinkCatalogError(ValueError):
    """Raised when a pipeline report cannot be catalogued safely."""


@dataclass(frozen=True)
class ResourceEvidence:
    source: str
    line: int
    operation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "line": self.line,
            "operation": self.operation,
        }


def _safe_text(value: str) -> str:
    redacted = _SENSITIVE.sub(lambda match: f"{match.group(1)}=<redacted>", value)
    return _URI_USERINFO.sub(r"\1<redacted>@", redacted)


def _dataset_name(value: str) -> str:
    normalized = value.strip().strip("'\"")
    if normalized.startswith("&") or not _DATASET.fullmatch(normalized):
        raise ValueError(f"Dynamic or invalid dataset name: {normalized!r}.")
    if "." not in normalized:
        normalized = f"WORK.{normalized}"
    return normalized.casefold()


def _statement_datasets(text: str, keyword: str) -> tuple[list[str], list[str]]:
    match = re.match(rf"^\s*{keyword}\s+(.+)$", text, re.I | re.S)
    if match is None:
        return [], []
    names: list[str] = []
    unknowns: list[str] = []
    value = match.group(1).lstrip()
    depth = 0
    for token_match in _DATASET_TOKEN.finditer(value):
        token = token_match.group()
        prefix = value[: token_match.start()]
        depth = prefix.count("(") - prefix.count(")")
        if depth:
            continue
        if token.casefold() in {"_null_", "_all_"}:
            continue
        try:
            names.append(_dataset_name(token))
        except ValueError:
            unknowns.append(token)
    return names, unknowns


def _option_values(text: str) -> dict[str, list[str]]:
    values: dict[str, list[str]] = {}
    for match in _OPTION.finditer(text):
        key = match.group(1).casefold()
        value = match.group(3) if match.group(2) else match.group(4)
        if value is not None:
            values.setdefault(key, []).append(value)
    return values


def _explicit_columns(
    data_statements: list[Any],
) -> dict[str, dict[str, Any]]:
    columns: dict[str, dict[str, Any]] = {}
    for statement in data_statements:
        text = statement.text.strip()
        lower = text.casefold()
        if lower.startswith("length "):
            body = re.sub(r"^\s*length\s+", "", text, flags=re.I)
            for match in _LENGTH_DECL.finditer(body):
                length_text = match.group(2).replace(" ", "")
                column = columns.setdefault(
                    match.group(1).casefold(),
                    {"name": match.group(1), "type": "unknown", "evidence": []},
                )
                if length_text.startswith("$"):
                    column["type"] = "character"
                    column["length"] = int(length_text[1:])
                else:
                    column["type"] = "numeric"
                    column["length"] = int(length_text)
                column["evidence"].append(
                    {"line": statement.span.start_line, "statement": "LENGTH"}
                )
        elif lower.startswith("format "):
            body = re.sub(r"^\s*format\s+", "", text, flags=re.I)
            for match in _FORMAT_DECL.finditer(body):
                column = columns.setdefault(
                    match.group(1).casefold(),
                    {"name": match.group(1), "type": "unknown", "evidence": []},
                )
                column["format"] = match.group(2)
                column["evidence"].append(
                    {"line": statement.span.start_line, "statement": "FORMAT"}
                )
        elif lower.startswith("attrib "):
            body = re.sub(r"^\s*attrib\s+", "", text, flags=re.I)
            for match in re.finditer(
                r"\b([A-Za-z_]\w*)\s+length\s*=\s*(\$\s*\d+|\$\d+|\d+)",
                body,
                re.I,
            ):
                length_text = re.sub(r"\s+", "", match.group(2))
                column = columns.setdefault(
                    match.group(1).casefold(),
                    {"name": match.group(1), "type": "unknown", "evidence": []},
                )
                if length_text.startswith("$"):
                    column["type"] = "character"
                    column["length"] = int(length_text[1:])
                else:
                    column["type"] = "numeric"
                    column["length"] = int(length_text)
                column["evidence"].append(
                    {"line": statement.span.start_line, "statement": "ATTRIB"}
                )
    return columns


def _source_systems(
    pipeline_report: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    systems: dict[str, dict[str, Any]] = {}
    issues: list[dict[str, Any]] = []

    def add_system(key: str, record: dict[str, Any]) -> None:
        existing = systems.get(key)
        if existing is None:
            systems[key] = record
            return
        for field in (
            "physical_paths",
            "servers",
            "sources",
            "librefs",
            "operations",
        ):
            existing[field] = sorted(
                set(existing.get(field, ())) | set(record.get(field, ())),
                key=str.casefold,
            )

    for library in pipeline_report.get("libraries", []):
        physical_path = _safe_text(str(library.get("physical_path", "")))
        path_key = str(library.get("repository_path") or physical_path).casefold()
        key = f"sas_library:{library.get('server', '').casefold()}:{path_key}"
        add_system(
            key,
            {
                "id": key,
                "kind": "sas_library",
                "server": library.get("server"),
                "physical_paths": [physical_path] if physical_path else [],
                "repository_paths": (
                    [library["repository_path"]]
                    if library.get("repository_path")
                    else []
                ),
                "librefs": [str(library.get("libref", "")).casefold()],
                "sources": [str(library.get("source", ""))],
                "status": library.get("status", "unknown"),
            },
        )
        if library.get("status") != "mapped":
            issues.append(
                {
                    "kind": "unresolved_library",
                    "source": library.get("source"),
                    "line": library.get("line"),
                    "reference": physical_path,
                    "reason": "LIBNAME location is not mapped to a repository path.",
                }
            )

    for connection in pipeline_report.get("external_connections", []):
        system_name = _safe_text(str(connection.get("system", "unknown")))
        mode = str(connection.get("mode", "unknown"))
        server = str(connection.get("server") or "").casefold()
        key = f"{mode}:{server}:{system_name.casefold()}"
        add_system(
            key,
            {
                "id": key,
                "kind": mode,
                "server": connection.get("server"),
                "target_server": connection.get("target_server"),
                "system": system_name,
                "physical_paths": [],
                "repository_paths": [],
                "librefs": (
                    [connection["local_libref"]]
                    if connection.get("local_libref")
                    else []
                ),
                "sources": [str(connection.get("source", ""))],
                "status": (
                    "mapped"
                    if connection.get("target_server")
                    else "unknown_endpoint"
                ),
            },
        )
        if not connection.get("target_server") and mode == "sas_connect_libref":
            issues.append(
                {
                    "kind": "unknown_remote_server",
                    "source": connection.get("source"),
                    "line": connection.get("line"),
                    "reference": system_name,
                    "reason": "Remote alias is not mapped to a configured server.",
                }
            )
        if mode == "sql_pass_through":
            issues.append(
                {
                    "kind": "unknown_connection_endpoint",
                    "source": connection.get("source"),
                    "line": connection.get("line"),
                    "reference": system_name,
                    "reason": "Connection engine is known; host, database, and direction are not statically established.",
                }
            )
    for reference in pipeline_report.get("remote_references", []):
        alias = str(reference.get("remote_alias") or "unknown")
        server = str(reference.get("server") or "")
        target_server = reference.get("target_server")
        key = f"sas_remote_submit:{server.casefold()}:{alias.casefold()}"
        add_system(
            key,
            {
                "id": key,
                "kind": "sas_remote_submit",
                "server": server or None,
                "target_server": target_server,
                "system": alias,
                "physical_paths": [],
                "repository_paths": [],
                "librefs": [],
                "sources": [str(reference.get("source", ""))],
                "operations": [str(reference.get("operation", ""))],
                "status": "mapped" if target_server else "unknown_endpoint",
            },
        )
        if not target_server:
            issues.append(
                {
                    "kind": "unknown_remote_server",
                    "source": reference.get("source"),
                    "line": reference.get("line"),
                    "reference": alias,
                    "reason": "Remote-submit alias is not mapped to a configured server; remote code is not executed.",
                }
            )
    return systems, issues


def build_source_sink_catalog(
    repository_root: str | Path, pipeline_report: Mapping[str, Any]
) -> dict[str, Any]:
    """Build a catalog of statically observed SAS datasets and their systems."""
    if pipeline_report.get("schema_version") != 1:
        raise SourceSinkCatalogError(
            "Pipeline report has an unsupported schema version."
        )
    try:
        root = Path(repository_root).resolve(strict=True)
    except OSError as error:
        raise SourceSinkCatalogError(
            f"Cannot access repository root {str(repository_root)!r}: {error}"
        ) from error
    if not root.is_dir():
        raise SourceSinkCatalogError(
            f"Repository root is not a directory: {str(root)!r}."
        )

    systems, issues = _source_systems(pipeline_report)
    datasets: dict[str, dict[str, Any]] = {}
    files: dict[str, dict[str, Any]] = {}
    dynamic_references: list[dict[str, Any]] = []
    schemas: dict[str, dict[str, Any]] = {}
    scripts = [
        item
        for item in pipeline_report.get("scripts", [])
        if item.get("status") == "reachable"
    ]
    script_servers = {
        str(item["path"]).casefold(): item.get("server")
        for item in scripts
    }
    remote_blocks_by_source: dict[str, list[dict[str, Any]]] = {}
    for block in pipeline_report.get("remote_blocks", []):
        remote_blocks_by_source.setdefault(
            str(block.get("source", "")).casefold(), []
        ).append(block)

    for script in scripts:
        source_path = str(script["path"])
        physical_source = (root / Path(source_path)).resolve(strict=False)
        try:
            physical_source.relative_to(root)
        except ValueError:
            raise SourceSinkCatalogError(
                f"Pipeline source escapes repository root: {source_path!r}."
            ) from None
        try:
            contents = physical_source.read_bytes()
        except OSError as error:
            issues.append(
                {
                    "kind": "source_read_error",
                    "source": source_path,
                    "line": None,
                    "reference": source_path,
                    "reason": str(error),
                }
            )
            continue
        source, _, decode_error = _decode_source(contents)
        if source is None:
            issues.append(
                {
                    "kind": "source_decode_error",
                    "source": source_path,
                    "line": None,
                    "reference": source_path,
                    "reason": decode_error or "Cannot decode SAS source.",
                }
            )
            continue
        try:
            statements = list(parse_program(source).statements)
        except SasParseError as error:
            issues.append(
                {
                    "kind": "source_parse_error",
                    "source": source_path,
                    "line": error.line,
                    "reference": source_path,
                    "reason": error.message,
                }
            )
            continue

        data_step_outputs: list[str] = []
        data_step_statements: list[Any] = []
        in_data_step = False
        filename_refs_by_server: dict[str, dict[str, str | None]] = {}
        execution_server: str | None = script_servers.get(source_path.casefold())

        def server_for_line(line_number: int) -> str | None:
            for block in remote_blocks_by_source.get(source_path.casefold(), []):
                end_line = block.get("end_line")
                if line_number >= block.get("start_line", 0) and (
                    end_line is None or line_number <= end_line
                ):
                    return block.get("target_server")
            return script_servers.get(source_path.casefold())

        def record_dataset(name: str, direction: str, line: int, operation: str) -> None:
            entry = datasets.setdefault(
                name,
                {
                    "name": name,
                    "kind": "sas_dataset",
                    "direction": "unknown",
                    "systems": [],
                    "evidence": [],
                },
            )
            directions: set[str] = set(
                entry["direction"].split("/")
                if entry["direction"] != "unknown"
                else ()
            )
            directions.add(direction)
            entry["direction"] = (
                "input/output"
                if directions == {"input", "output"}
                else next(iter(directions))
            )
            entry["evidence"].append(
                {
                    **ResourceEvidence(source_path, line, operation).to_dict(),
                    "server": execution_server,
                }
            )

        def record_file(
            name: str,
            direction: str,
            line: int,
            operation: str,
            *,
            location_known: bool = True,
        ) -> None:
            safe_name = _safe_text(name)
            entry = files.setdefault(
                safe_name,
                {
                    "name": safe_name,
                    "kind": "file",
                    "direction": direction,
                    "systems": [],
                    "evidence": [],
                },
            )
            directions: set[str] = set(
                entry["direction"].split("/")
                if entry["direction"] != "unknown"
                else ()
            )
            directions.add(direction)
            entry["direction"] = (
                "input/output"
                if directions == {"input", "output"}
                else next(iter(directions))
            )
            entry["evidence"].append(
                ResourceEvidence(source_path, line, operation).to_dict()
            )
            if not location_known or re.search(r"&|%|^temp$", safe_name, re.I):
                status = "unknown"
                location = None
            else:
                status = "known"
                location = safe_name
            server = str(execution_server or "").casefold()
            system_id = f"filesystem:{server}:{safe_name.casefold()}"
            if system_id not in systems:
                systems[system_id] = {
                    "id": system_id,
                    "kind": "filesystem",
                    "server": execution_server,
                    "system": "file",
                    "physical_paths": [location] if location else [],
                    "repository_paths": [],
                    "librefs": [],
                    "sources": [source_path],
                    "status": status,
                }
            elif source_path not in systems[system_id]["sources"]:
                systems[system_id]["sources"].append(source_path)
            if system_id not in entry["systems"]:
                entry["systems"].append(system_id)
            if status == "unknown":
                issues.append(
                    {
                        "kind": "unknown_file_location",
                        "source": source_path,
                        "line": line,
                        "reference": safe_name,
                        "reason": "File location is dynamic, temporary, or not statically established.",
                    }
                )

        for statement in statements:
            text = statement.text.strip()
            lower = text.casefold()
            line = statement.span.start_line
            execution_server = server_for_line(line)
            filename_refs = filename_refs_by_server.setdefault(
                str(execution_server or "").casefold(), {}
            )

            filename = re.match(
                r"""^\s*filename\s+([A-Za-z_]\w*)\s+(?:(['"])(.*?)\2|(\S+))""",
                text,
                re.I | re.S,
            )
            if filename:
                fileref = filename.group(1).casefold()
                path = filename.group(3) if filename.group(2) else filename.group(4)
                filename_refs[fileref] = path if path and path.casefold() != "temp" else None
                continue

            data_match = re.match(r"^data\s+(.+)$", text, re.I | re.S)
            if data_match:
                data_step_outputs, dynamic = _statement_datasets(text, "data")
                for name in data_step_outputs:
                    record_dataset(name, "output", line, "DATA target")
                for reference in dynamic:
                    dynamic_references.append(
                        {
                            "source": source_path,
                            "line": line,
                            "reference": reference,
                            "reason": "DATA target is dynamic or macro-generated.",
                        }
                    )
                data_step_statements = [statement]
                in_data_step = True
                continue

            if in_data_step:
                data_step_statements.append(statement)
                for keyword in ("set", "merge"):
                    inputs, dynamic = _statement_datasets(text, keyword)
                    for name in inputs:
                        record_dataset(name, "input", line, keyword.upper())
                    for reference in dynamic:
                        dynamic_references.append(
                            {
                                "source": source_path,
                                "line": line,
                                "reference": reference,
                                "reason": f"{keyword.upper()} input is dynamic or macro-generated.",
                            }
                        )
                for keyword, direction in (("infile", "input"), ("file", "output")):
                    file_match = re.match(
                        rf"^\s*{keyword}\s+([A-Za-z_]\w*)\b", text, re.I
                    )
                    if file_match:
                        fileref = file_match.group(1).casefold()
                        location = filename_refs.get(fileref)
                        record_file(
                            location or fileref,
                            direction,
                            line,
                            f"{keyword.upper()} fileref",
                            location_known=location is not None,
                        )
                if lower in {"run", "quit"}:
                    explicit = _explicit_columns(data_step_statements)
                    if explicit:
                        declaration_sources = [
                            {
                                "source": source_path,
                                "line": item.span.start_line,
                            }
                            for item in data_step_statements
                            if item.text.strip().casefold().startswith(
                                ("length ", "attrib ", "format ")
                            )
                        ]
                        for output in data_step_outputs:
                            schema = schemas.setdefault(
                                output,
                                {
                                    "dataset": output,
                                    "status": "partial",
                                    "columns": {},
                                    "evidence": [],
                                },
                            )
                            for column_name, details in explicit.items():
                                schema["columns"][column_name] = details
                            schema["evidence"].extend(declaration_sources)
                    in_data_step = False
                    data_step_outputs = []
                    data_step_statements = []
                else:
                    assignment = _ASSIGNMENT.match(text)
                    if assignment and data_step_outputs:
                        for output in data_step_outputs:
                            schema = schemas.setdefault(
                                output,
                                {
                                    "dataset": output,
                                    "status": "partial",
                                    "columns": {},
                                    "evidence": [],
                                },
                            )
                            name = assignment.group(1)
                            schema["columns"].setdefault(
                                name.casefold(),
                                {
                                    "name": name,
                                    "type": "unknown",
                                    "evidence": [
                                        {
                                            "source": source_path,
                                            "line": line,
                                            "statement": "assignment",
                                        }
                                    ],
                                },
                            )
                continue

            if lower.startswith("proc sql"):
                continue
            sql_create = _SQL_CREATE.search(text)
            if sql_create:
                try:
                    name = _dataset_name(sql_create.group(1))
                except ValueError:
                    dynamic_references.append(
                        {
                            "source": source_path,
                            "line": line,
                            "reference": sql_create.group(1),
                            "reason": "SQL target is dynamic or invalid.",
                        }
                    )
                else:
                    record_dataset(name, "output", line, "PROC SQL CREATE")
            if re.match(r"^\s*(proc\s+sql|select|create|insert|update|delete)\b", text, re.I):
                for sql_match in _SQL_REFERENCE.finditer(text):
                    try:
                        name = _dataset_name(sql_match.group(1))
                    except ValueError:
                        dynamic_references.append(
                            {
                                "source": source_path,
                                "line": line,
                                "reference": sql_match.group(1),
                                "reason": "SQL dataset reference is dynamic or invalid.",
                            }
                        )
                    else:
                        operation = sql_match.group(0).strip().split()[0].casefold()
                        is_delete_target = (
                            lower.startswith("delete")
                            and operation == "from"
                        )
                        if operation == "update" or is_delete_target:
                            record_dataset(
                                name, "input", line, f"PROC SQL {operation.upper()}"
                            )
                            record_dataset(
                                name, "output", line, f"PROC SQL {operation.upper()}"
                            )
                        else:
                            record_dataset(
                                name,
                                "output" if operation == "into" else "input",
                                line,
                                f"PROC SQL {operation.upper()}",
                            )

            if lower.startswith("proc "):
                proc_name = lower.split(None, 1)[1].split(None, 1)[0]
                options = _option_values(text)
                input_option = options.get("data", [])
                if proc_name in {"sort", "freq", "means", "summary", "transpose", "print", "contents", "append", "univariate", "export"}:
                    for value in input_option:
                        try:
                            record_dataset(_dataset_name(value), "input", line, f"PROC {proc_name.upper()} DATA=")
                            if proc_name == "sort" and not options.get("out"):
                                record_dataset(
                                    _dataset_name(value),
                                    "output",
                                    line,
                                    "PROC SORT in-place",
                                )
                        except ValueError:
                            dynamic_references.append(
                                {
                                    "source": source_path,
                                    "line": line,
                                    "reference": value,
                                    "reason": f"PROC {proc_name.upper()} DATA= is dynamic or invalid.",
                                }
                            )
                for option_name in ("out", "base"):
                    for value in options.get(option_name, []):
                        try:
                            record_dataset(
                                _dataset_name(value),
                                "output",
                                line,
                                f"PROC {proc_name.upper()} {option_name.upper()}=",
                            )
                        except ValueError:
                            dynamic_references.append(
                                {
                                    "source": source_path,
                                    "line": line,
                                    "reference": value,
                                    "reason": f"PROC {proc_name.upper()} {option_name.upper()}= is dynamic or invalid.",
                                }
                            )
                for option_name, direction in (
                    ("datafile", "input"),
                    ("outfile", "output"),
                    ("file", "output"),
                ):
                    for value in options.get(option_name, []):
                        record_file(
                            value,
                            direction,
                            line,
                            f"PROC {proc_name.upper()} {option_name.upper()}=",
                        )

        if in_data_step and data_step_outputs:
            explicit = _explicit_columns(data_step_statements)
            for output in data_step_outputs:
                if explicit:
                    schemas.setdefault(
                        output,
                        {
                            "dataset": output,
                            "status": "partial",
                            "columns": explicit,
                            "evidence": [
                                {"source": source_path, "line": line}
                                for line in (
                                    item.span.start_line
                                    for item in data_step_statements
                                )
                            ],
                        },
                    )

    system_by_libref: dict[tuple[str, str], list[str]] = {}
    for system_id, system in systems.items():
        if system.get("kind") not in {"sas_library", "sas_connect_libref", "sas_access"}:
            continue
        for source in system.get("sources", []):
            server = str(system.get("server") or "").casefold()
            for libref in system.get("librefs", []):
                system_by_libref.setdefault((server, libref.casefold()), []).append(system_id)

    for dataset in datasets.values():
        parts = dataset["name"].split(".", maxsplit=1)
        libref = parts[0]
        if libref == "work":
            dataset["systems"] = [
                {
                    "kind": "sas_work",
                    "server": None,
                    "location": "session-local",
                    "status": "known",
                }
            ]
        else:
            system_ids = {
                system_id
                for evidence in dataset["evidence"]
                for system_id in system_by_libref.get(
                    (
                        str(evidence.get("server") or "").casefold(),
                        libref,
                    ),
                    [],
                )
            }
            dataset["systems"] = sorted(system_ids)
            if not system_ids:
                issues.append(
                    {
                        "kind": "unknown_dataset_system",
                        "source": dataset["evidence"][0]["source"],
                        "line": dataset["evidence"][0]["line"],
                        "reference": dataset["name"],
                        "reason": f"No LIBNAME or external-system mapping was found for libref {libref!r}.",
                    }
                )
            elif len(system_ids) > 1:
                issues.append(
                    {
                        "kind": "ambiguous_libref_mapping",
                        "source": dataset["evidence"][0]["source"],
                        "line": dataset["evidence"][0]["line"],
                        "reference": dataset["name"],
                        "reason": f"Libref {libref!r} maps to multiple systems on this server.",
                    }
                )
        schema = schemas.get(dataset["name"])
        if schema is None:
            dataset["schema"] = {"status": "unknown", "columns": []}
            issues.append(
                {
                    "kind": "unknown_schema",
                    "source": dataset["evidence"][0]["source"],
                    "line": dataset["evidence"][0]["line"],
                    "reference": dataset["name"],
                    "reason": "No complete table schema was available from the source code or supplied assets.",
                }
            )
        else:
            dataset["schema"] = {
                "status": "partial",
                "columns": sorted(
                    schema["columns"].values(), key=lambda item: item["name"].casefold()
                ),
            }

    catalog_schemas = []
    for dataset_name, dataset in datasets.items():
        schema = schemas.get(dataset_name)
        catalog_schemas.append(
            {
                "dataset": dataset_name,
                "status": "partial" if schema else "unknown",
                "columns": (
                    sorted(
                        schema["columns"].values(),
                        key=lambda item: item["name"].casefold(),
                    )
                    if schema
                    else []
                ),
                "evidence": schema["evidence"] if schema else [],
            }
        )

    return {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "project_name": pipeline_report.get("project_name"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repository_root": str(root),
        "pipeline_report_generated_at": pipeline_report.get("generated_at"),
        "systems": sorted(systems.values(), key=lambda item: item["id"].casefold()),
        "resources": sorted(
            [*datasets.values(), *files.values()],
            key=lambda item: (item["kind"], item["name"].casefold()),
        ),
        "schemas": sorted(catalog_schemas, key=lambda item: item["dataset"]),
        "dynamic_references": sorted(
            dynamic_references,
            key=lambda item: (
                item["source"].casefold(),
                item["line"],
                item["reference"].casefold(),
            ),
        ),
        "issues": sorted(
            issues,
            key=lambda item: (
                str(item.get("source", "")).casefold(),
                item.get("line") or 0,
                item["kind"],
                item["reference"].casefold(),
            ),
        ),
        "summary": {
            "dataset_count": len(datasets),
            "file_resource_count": len(files),
            "system_count": len(systems),
            "unknown_schema_count": sum(
                item.get("schema", {}).get("status") == "unknown"
                for item in datasets.values()
            ),
            "partial_schema_count": len(schemas),
            "dynamic_reference_count": len(dynamic_references),
            "issue_count": len(issues),
        },
        "limitations": [
            "Dataset usage and direction are inferred statically from SAS statements.",
            "Schema fields are partial source declarations, not authoritative PROC CONTENTS metadata.",
            "External connection engines may be identified without a host, database, or verified direction.",
            "No dataset contents are read; generated files are not treated as production metadata.",
        ],
    }


def load_pipeline_report(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SourceSinkCatalogError(
            f"Cannot read pipeline report {str(path)!r}: {error}"
        ) from error
    if not isinstance(value, dict):
        raise SourceSinkCatalogError("Pipeline report must be a JSON object.")
    if value.get("schema_version") != 1:
        raise SourceSinkCatalogError("Pipeline report has an unsupported schema version.")
    return value
