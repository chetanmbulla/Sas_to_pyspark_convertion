"""Resolve statically discoverable SAS include pipelines from configured roots."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Mapping

from .discovery import _decode_source
from .sas_parser import SasParseError, parse_program


PIPELINE_REPORT_SCHEMA_VERSION = 1
_DRIVE = re.compile(r"^[A-Za-z]$")
_DIRECT_INCLUDE = re.compile(
    r"^\s*%inc(?:lude)?\s+(['\"])(.*?)\1(?:\s+.*)?$", re.I | re.S
)
_FILEREF_INCLUDE = re.compile(
    r"^\s*%inc(?:lude)?\s+([A-Za-z_]\w*)\s*\(\s*([^)]+?)\s*\)\s*$",
    re.I | re.S,
)
_FILENAME = re.compile(
    r"^\s*filename\s+([A-Za-z_]\w*)(?:\s+(?:(['\"])(.*?)\2(?:\s+.*)?|clear\b))?\s*$",
    re.I | re.S,
)
_LIBNAME = re.compile(
    r"^\s*libname\s+([A-Za-z_]\w*)(?:\s+(?:(['\"])(.*?)\2(?:\s+.*)?|clear\b))?\s*$",
    re.I | re.S,
)
_X_CD = re.compile(r"^\s*x\s+(['\"])(.*?)\1\s*$", re.I | re.S)
_REMOTE_COMMANDS = re.compile(
    r"^\s*(signon|signoff|rsubmit|endrsubmit)\b\s*([A-Za-z_]\w*)?", re.I
)
_REMOTE_OPTION = re.compile(r"\bremote\s*=\s*([A-Za-z_]\w*)", re.I)
_REMOTE_LIBNAME = re.compile(
    r"^\s*libname\s+([A-Za-z_]\w*)\b(.*)$", re.I | re.S
)
_LIBNAME_SERVER = re.compile(r"\bserver\s*=\s*([A-Za-z_]\w*)", re.I)
_SLIBREF = re.compile(r"\bslibref\s*=\s*([A-Za-z_]\w*)", re.I)
_LIBNAME_ENGINE = re.compile(
    r"^\s*libname\s+[A-Za-z_]\w*\s+([A-Za-z_]\w*)\b", re.I
)
_SQL_CONNECT = re.compile(r"^\s*connect\s+to\s+([A-Za-z_]\w*)\b", re.I)
_LET = re.compile(r"^\s*%let\s+([A-Za-z_]\w*)\s*=\s*(.*?)\s*$", re.I | re.S)
_MACRO_VARIABLE = re.compile(r"&([A-Za-z_]\w*)\.?")
_FILEREF_INCLUDE_FILE = re.compile(r"^\s*%inc(?:lude)?\s+([A-Za-z_]\w*)\s*$", re.I)
_ANY_INCLUDE = re.compile(r"^\s*%inc(?:lude)?\b", re.I)
_COMMENTED_INCLUDE = re.compile(r"%inc(?:lude)?\s+[^;]+", re.I)
_COMMENT_BLOCK = re.compile(r"/\*.*?\*/|^[ \t]*\*[^;]*;", re.S | re.M)
_ENDPOINT = re.compile(r"^\s*([A-Za-z0-9_.\-]+)(?:\s+(\d{1,5}))?\s*$")
_JOB_NAME = re.compile(r"\bjobname\s*=\s*(['\"])(.*?)\1", re.I)
_JOB_SOURCE = re.compile(r"\bsource\s*=\s*(['\"])(.*?)\1", re.I)
_JAMS_JOB_FIELDS = frozenset(
    {"name", "entrypoint", "schedule", "depends_on", "parameters"}
)
_SENSITIVE_NAME = re.compile(r"(?i)pass|pwd|secret|token|tcpsec|credential|key")
_FILE_OUTPUT = re.compile(r"^\s*file\s+([A-Za-z_]\w*)\b", re.I)
_PROC_IMPORT_EXPORT = re.compile(r"^\s*proc\s+(import|export)\b(.*)$", re.I | re.S)
_IO_PATH = re.compile(
    r"\b(?:outfile|datafile|file)\s*=\s*('[^']*'|\"[^\"]*\"|[^\s;]+)", re.I
)
_IO_DBMS = re.compile(r"\bdbms\s*=\s*([A-Za-z0-9_]+)", re.I)
_FILENAME_DEVICE = re.compile(
    r"^\s*filename\s+([A-Za-z_]\w*)\s+(email|ftp|url|pipe|socket)\b(.*)$",
    re.I | re.S,
)
_FILENAME_DEVICE_DIRECTION = {
    "email": "output",
    "ftp": "both",
    "url": "input",
    "pipe": "both",
    "socket": "both",
}
_ODS_OUTPUT = re.compile(
    r"^\s*ods\s+(?!listing\b|results\b|graphics\b|select\b|exclude\b|trace\b|"
    r"output\b|path\b|noproctitle\b|escapechar\b)([A-Za-z_][\w.]*)\b(.*)$",
    re.I | re.S,
)
_ODS_PATH = re.compile(
    r"\b(?:body|file|path)\s*=\s*('[^']*'|\"[^\"]*\"|[^\s;()]+)", re.I
)
_HOST_COMMAND = re.compile(r"^\s*(?:x|%sysexec)\s+(.+)$", re.I | re.S)
_INFILE_FILE = re.compile(
    r"^\s*(infile|file)\s+('[^']*'|\"[^\"]*\"|[A-Za-z_]\w*)", re.I
)
_DRIVE_RELATIVE_POLICIES = ("reject", "drive_root")
_SENSITIVE_OPTION = re.compile(
    r"(?i)(password|passwd|pwd|user(name)?|uid|token|secret|credential|"
    r"access[._-]?key)\s*=\s*('[^']*'|\"[^\"]*\"|[^;\s,)]+)"
)
_URI_USERINFO = re.compile(r"(?i)(://)[^/@\s:]+(?::[^/@\s]*)?@")


_FILENAME_DEVICE_TARGET = re.compile(r"('[^']*'|\"[^\"]*\")")


def _quoted_value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        return value[1:-1]
    return value


def _safe_report_value(value: str) -> str:
    value = _SENSITIVE_OPTION.sub(
        lambda match: f"{match.group(1)}=<redacted>", value
    )
    return _URI_USERINFO.sub(r"\1<redacted>@", value)


class PipelineResolverError(ValueError):
    """Raised when pipeline resolver configuration or root paths are invalid."""


@dataclass(frozen=True)
class PipelineScript:
    path: str
    server: str | None
    sha256: str
    status: str
    reachable_from: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "server": self.server,
            "sha256": self.sha256,
            "status": self.status,
            "reachable_from": list(self.reachable_from),
        }


@dataclass(frozen=True)
class PipelineInclude:
    source: str
    line: int
    reference: str
    target: str | None
    status: str
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "line": self.line,
            "reference": self.reference,
            "target": self.target,
            "status": self.status,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class PipelineLibrary:
    source: str
    line: int
    server: str
    libref: str
    physical_path: str
    repository_path: str | None
    status: str
    reason_code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "line": self.line,
            "server": self.server,
            "libref": self.libref,
            "physical_path": self.physical_path,
            "repository_path": self.repository_path,
            "status": self.status,
            "reason_code": self.reason_code,
        }


def _reason_code(message: str) -> str:
    lowered = message.casefold()
    if "is not mapped for server" in lowered:
        return "drive_not_mapped"
    if "statically known value" in lowered or lowered.startswith("macro variable"):
        return "macro_variable_unknown"
    if "remote server is unknown" in lowered:
        return "remote_server_unknown"
    if "drive-relative" in lowered:
        return "drive_relative_unsupported"
    if "unc or unmapped" in lowered:
        return "unc_unsupported"
    if "working directory" in lowered:
        return "no_working_directory"
    return "unresolved"


@dataclass(frozen=True)
class PipelineRemoteEndpoint:
    source: str
    line: int
    alias: str
    host: str
    port: int | None
    target_server: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "line": self.line,
            "alias": self.alias,
            "host": self.host,
            "port": self.port,
            "target_server": self.target_server,
        }


@dataclass(frozen=True)
class PipelineRemoteMacroVariable:
    server: str
    name: str
    value: str
    source: str
    line: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "server": self.server,
            "name": self.name,
            "value": self.value,
            "source": self.source,
            "line": self.line,
        }


@dataclass(frozen=True)
class PipelineRemoteReference:
    source: str
    line: int
    server: str
    operation: str
    remote_alias: str | None
    target_server: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "line": self.line,
            "server": self.server,
            "operation": self.operation,
            "remote_alias": self.remote_alias,
            "target_server": self.target_server,
        }


@dataclass(frozen=True)
class PipelineRemoteBlock:
    source: str
    start_line: int
    end_line: int | None
    remote_alias: str | None
    source_server: str
    target_server: str | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "start_line": self.start_line,
            "end_line": self.end_line,
            "remote_alias": self.remote_alias,
            "source_server": self.source_server,
            "target_server": self.target_server,
            "status": self.status,
        }


@dataclass(frozen=True)
class PipelineExternalConnection:
    source: str
    line: int
    server: str
    mode: str
    system: str
    local_libref: str | None
    remote_libref: str | None
    target_server: str | None
    direction: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "line": self.line,
            "server": self.server,
            "mode": self.mode,
            "system": self.system,
            "local_libref": self.local_libref,
            "remote_libref": self.remote_libref,
            "target_server": self.target_server,
            "direction": self.direction,
        }


@dataclass(frozen=True)
class PipelineIssue:
    source: str
    line: int | None
    kind: str
    reference: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "line": self.line,
            "kind": self.kind,
            "reference": self.reference,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class PipelineReport:
    repository_root: str
    generated_at: str
    entrypoints: tuple[dict[str, str], ...]
    scripts: tuple[PipelineScript, ...]
    includes: tuple[PipelineInclude, ...]
    libraries: tuple[PipelineLibrary, ...]
    remote_references: tuple[PipelineRemoteReference, ...]
    remote_blocks: tuple[PipelineRemoteBlock, ...]
    external_connections: tuple[PipelineExternalConnection, ...]
    issues: tuple[PipelineIssue, ...]
    remote_endpoints: tuple[PipelineRemoteEndpoint, ...] = ()
    remote_macro_variables: tuple[PipelineRemoteMacroVariable, ...] = ()
    jobs: tuple[dict[str, Any], ...] = ()
    schema_version: int = PIPELINE_REPORT_SCHEMA_VERSION

    def _unmapped_drives(self) -> list[dict[str, Any]]:
        counts: dict[tuple[str, str], int] = {}
        for library in self.libraries:
            if library.reason_code != "drive_not_mapped":
                continue
            drive = PureWindowsPath(library.physical_path).drive.rstrip(":").upper()
            counts[(library.server, drive)] = counts.get((library.server, drive), 0) + 1
        return [
            {"server": server, "drive": drive, "library_count": count}
            for (server, drive), count in sorted(counts.items())
        ]

    def to_dict(self) -> dict[str, Any]:
        reachable = sum(item.status == "reachable" for item in self.scripts)
        return {
            "schema_version": self.schema_version,
            "repository_root": self.repository_root,
            "generated_at": self.generated_at,
            "entrypoints": [dict(item) for item in self.entrypoints],
            "jobs": [dict(item) for item in self.jobs],
            "scripts": [item.to_dict() for item in self.scripts],
            "includes": [item.to_dict() for item in self.includes],
            "libraries": [item.to_dict() for item in self.libraries],
            "remote_references": [
                item.to_dict() for item in self.remote_references
            ],
            "remote_blocks": [item.to_dict() for item in self.remote_blocks],
            "remote_endpoints": [item.to_dict() for item in self.remote_endpoints],
            "remote_macro_variables": [
                item.to_dict() for item in self.remote_macro_variables
            ],
            "external_connections": [
                item.to_dict() for item in self.external_connections
            ],
            "issues": [item.to_dict() for item in self.issues],
            "summary": {
                "script_count": len(self.scripts),
                "job_count": len(self.jobs),
                "reachable_script_count": reachable,
                "unreached_script_count": len(self.scripts) - reachable,
                "resolved_include_count": sum(
                    item.status == "resolved" for item in self.includes
                ),
                "unresolved_include_count": sum(
                    item.status == "unresolved" for item in self.includes
                ),
                "runtime_generated_include_count": sum(
                    item.status == "runtime_generated" for item in self.includes
                ),
                "waived_include_count": sum(
                    item.status == "waived" for item in self.includes
                ),
                "library_count": len(self.libraries),
                "unresolved_library_count": sum(
                    item.status != "mapped" for item in self.libraries
                ),
                "unmapped_drives": self._unmapped_drives(),
                "remote_reference_count": len(self.remote_references),
                "remote_block_count": len(self.remote_blocks),
                "resolved_remote_block_count": sum(
                    item.status == "resolved" for item in self.remote_blocks
                ),
                "external_connection_count": len(self.external_connections),
                "issue_count": len(self.issues),
            },
            "limitations": [
                "Reachability is static potential reachability; macro conditions "
                "and runtime control flow are not evaluated.",
                "Remote submit references are reported, not executed or inferred "
                "as a mapping between server names.",
                "Dynamic macro-generated paths and unsupported FILENAME forms "
                "remain unresolved.",
                "Includes of files written by the same script at run time "
                "(runtime_generated) and includes waived in the configuration "
                "(waived) are not analyzed; their contents are unknown.",
            ],
        }

    def save(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


@dataclass(frozen=True)
class _ServerConfig:
    name: str
    drives: Mapping[str, str]
    initialization_scripts: tuple[str, ...]


def _relative_path(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PipelineResolverError(f"{field} must be a non-empty relative path.")
    normalized = value.replace("\\", "/").strip("/")
    windows = PureWindowsPath(value)
    posix = PurePosixPath(value)
    if (
        windows.is_absolute()
        or windows.drive
        or posix.is_absolute()
        or not normalized
        or any(part in {"", ".", ".."} for part in normalized.split("/"))
    ):
        raise PipelineResolverError(f"{field} must stay within the repository root.")
    return normalized


@dataclass(frozen=True)
class _ResolverOptions:
    macro_variables: Mapping[str, str]
    waived_includes: tuple[tuple[str, str], ...]
    drive_relative_root: bool
    remote_hosts: Mapping[str, str] = field(default_factory=dict)
    seed_remote_macro_variables: bool = False
    server_macro_variables: Mapping[str, Mapping[str, str]] = field(
        default_factory=dict
    )
    jams_jobs: tuple[dict[str, Any], ...] = ()


def _normalize_reference(value: str) -> str:
    return value.strip().strip("'\"").replace("/", "\\").casefold()


def _load_options(config: Mapping[str, Any]) -> _ResolverOptions:
    raw_macros = config.get("macro_variables", {})
    if not isinstance(raw_macros, dict):
        raise PipelineResolverError("macro_variables must be an object.")
    macros: dict[str, str] = {}
    for name, value in raw_macros.items():
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_]\w*", name):
            raise PipelineResolverError(f"Invalid macro variable name {name!r}.")
        if not isinstance(value, str):
            raise PipelineResolverError(
                f"Macro variable {name!r} must have a string value."
            )
        macros[name.casefold()] = value

    raw_waivers = config.get("waived_includes", [])
    if not isinstance(raw_waivers, list):
        raise PipelineResolverError("waived_includes must be a list.")
    waivers: list[tuple[str, str]] = []
    for item in raw_waivers:
        if (
            not isinstance(item, dict)
            or set(item) != {"reference", "reason"}
            or not all(isinstance(item[field], str) and item[field].strip() for field in item)
        ):
            raise PipelineResolverError(
                "Each waived include requires a non-empty reference and reason."
            )
        waivers.append((_normalize_reference(item["reference"]), item["reason"].strip()))

    policy = config.get("drive_relative_paths", "reject")
    if policy not in _DRIVE_RELATIVE_POLICIES:
        raise PipelineResolverError(
            f"drive_relative_paths must be one of {list(_DRIVE_RELATIVE_POLICIES)}."
        )
    raw_hosts = config.get("remote_hosts", {})
    if not isinstance(raw_hosts, dict) or not all(
        isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip()
        for k, v in raw_hosts.items()
    ):
        raise PipelineResolverError(
            "remote_hosts must map host names to configured server names."
        )
    hosts = {k.strip().casefold(): v for k, v in raw_hosts.items()}

    seeding = config.get("seed_remote_macro_variables", False)
    if not isinstance(seeding, bool):
        raise PipelineResolverError("seed_remote_macro_variables must be a boolean.")

    raw_server_macros = config.get("server_macro_variables", {})
    if not isinstance(raw_server_macros, dict):
        raise PipelineResolverError("server_macro_variables must be an object.")
    server_macros: dict[str, dict[str, str]] = {}
    for server, values in raw_server_macros.items():
        if not isinstance(values, dict) or not all(
            isinstance(n, str)
            and re.fullmatch(r"[A-Za-z_]\w*", n)
            and isinstance(v, str)
            for n, v in values.items()
        ):
            raise PipelineResolverError(
                f"server_macro_variables.{server} must map macro names to strings."
            )
        server_macros[str(server).casefold()] = {
            n.casefold(): v for n, v in values.items()
        }

    raw_jobs = config.get("jams_jobs", [])
    if not isinstance(raw_jobs, list):
        raise PipelineResolverError("jams_jobs must be a list.")
    jobs: list[dict[str, Any]] = []
    for item in raw_jobs:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("name"), str)
            or not item["name"].strip()
            or not isinstance(item.get("entrypoint"), str)
            or set(item).difference(_JAMS_JOB_FIELDS)
        ):
            raise PipelineResolverError(
                "Each jams_jobs item requires name and entrypoint; optional fields: "
                + ", ".join(sorted(_JAMS_JOB_FIELDS - {"name", "entrypoint"}))
                + "."
            )
        depends = item.get("depends_on", [])
        params = item.get("parameters", {})
        if not isinstance(depends, list) or not all(isinstance(d, str) for d in depends):
            raise PipelineResolverError("jams_jobs depends_on must be a list of names.")
        if not isinstance(params, dict):
            raise PipelineResolverError("jams_jobs parameters must be an object.")
        jobs.append(
            {
                "name": item["name"].strip(),
                "entrypoint": _normalize_reference(item["entrypoint"]),
                "schedule": item.get("schedule"),
                "depends_on": list(depends),
                "parameters": {
                    str(k): "<redacted>" if _SENSITIVE_NAME.search(str(k)) else str(v)
                    for k, v in params.items()
                },
            }
        )
    names = [job["name"].casefold() for job in jobs]
    if len(set(names)) != len(names):
        raise PipelineResolverError("jams_jobs names must be unique.")
    for job in jobs:
        for dependency in job["depends_on"]:
            if dependency.casefold() not in names:
                raise PipelineResolverError(
                    f"JAMS job {job['name']!r} depends on unknown job {dependency!r}."
                )
    return _ResolverOptions(
        macros,
        tuple(waivers),
        policy == "drive_root",
        hosts,
        seeding,
        server_macros,
        tuple(jobs),
    )


def _load_config(config: Mapping[str, Any]) -> tuple[
    tuple[_ServerConfig, ...], tuple[dict[str, str], ...], dict[str, str]
]:
    expected = {"schema_version", "servers", "entrypoints"}
    optional = {
        "remote_aliases",
        "macro_variables",
        "waived_includes",
        "drive_relative_paths",
        "remote_hosts",
        "seed_remote_macro_variables",
        "server_macro_variables",
        "jams_jobs",
    }
    if not isinstance(config, Mapping):
        raise PipelineResolverError("Pipeline config must be a JSON object.")
    if set(config).difference(expected | optional) or expected.difference(config):
        raise PipelineResolverError(
            "Pipeline config requires schema_version, servers, and entrypoints; "
            "only remote_aliases, remote_hosts, macro_variables, "
            "server_macro_variables, seed_remote_macro_variables, "
            "waived_includes, drive_relative_paths, and jams_jobs are optional."
        )
    if type(config["schema_version"]) is not int or config["schema_version"] != 1:
        raise PipelineResolverError(
            f"Unsupported pipeline config schema version: {config['schema_version']!r}."
        )
    raw_servers = config["servers"]
    if not isinstance(raw_servers, dict) or not raw_servers:
        raise PipelineResolverError("Pipeline config servers must be a non-empty object.")
    servers: list[_ServerConfig] = []
    for name, value in raw_servers.items():
        if not isinstance(name, str) or not name.strip():
            raise PipelineResolverError("Server names must be non-empty strings.")
        if (
            not isinstance(value, dict)
            or "drives" not in value
            or set(value).difference({"drives", "initialization_scripts"})
        ):
            raise PipelineResolverError(
                f"Server {name!r} requires drives and optionally "
                "initialization_scripts."
            )
        raw_drives = value["drives"]
        if not isinstance(raw_drives, dict) or not raw_drives:
            raise PipelineResolverError(
                f"Server {name!r} must define at least one drive mapping."
            )
        drives: dict[str, str] = {}
        for raw_drive, raw_root in raw_drives.items():
            drive = str(raw_drive).rstrip(":").upper()
            if not _DRIVE.fullmatch(drive):
                raise PipelineResolverError(
                    f"Invalid drive {raw_drive!r} for server {name!r}."
                )
            if drive in drives:
                raise PipelineResolverError(
                    f"Duplicate drive mapping {drive!r} for server {name!r}."
                )
            drives[drive] = _relative_path(raw_root, f"servers.{name}.drives.{drive}")
        raw_initializers = value.get("initialization_scripts", [])
        if not isinstance(raw_initializers, list):
            raise PipelineResolverError(
                f"Server {name!r} initialization_scripts must be a list."
            )
        initialization_scripts = tuple(
            _relative_path(item, f"servers.{name}.initialization_scripts")
            for item in raw_initializers
        )
        servers.append(_ServerConfig(name, drives, initialization_scripts))

    raw_entrypoints = config["entrypoints"]
    if not isinstance(raw_entrypoints, list) or not raw_entrypoints:
        raise PipelineResolverError("Pipeline config entrypoints must be a non-empty list.")
    entrypoints: list[dict[str, str]] = []
    for item in raw_entrypoints:
        if not isinstance(item, dict) or set(item) != {"server", "path"}:
            raise PipelineResolverError(
                "Each entrypoint must contain exactly server and path."
            )
        server = item["server"]
        if not isinstance(server, str) or server not in {item.name for item in servers}:
            raise PipelineResolverError(
                f"Entrypoint references an unknown server: {server!r}."
            )
        entrypoints.append(
            {
                "server": server,
                "path": _relative_path(item["path"], "entrypoints.path"),
            }
        )

    aliases = config.get("remote_aliases", {})
    if not isinstance(aliases, dict):
        raise PipelineResolverError("remote_aliases must be an object.")
    known_servers = {item.name.casefold(): item.name for item in servers}
    normalized_aliases: dict[str, str] = {}
    for alias, server in aliases.items():
        if not isinstance(alias, str) or not alias.strip():
            raise PipelineResolverError("Remote aliases must be non-empty strings.")
        if not isinstance(server, str) or server.casefold() not in known_servers:
            raise PipelineResolverError(
                f"Remote alias {alias!r} maps to an unknown configured server."
            )
        normalized_aliases[alias.casefold()] = known_servers[server.casefold()]
    return tuple(servers), tuple(entrypoints), normalized_aliases


class _PathResolver:
    def __init__(
        self,
        root: Path,
        servers: tuple[_ServerConfig, ...],
        *,
        drive_relative_root: bool = False,
    ) -> None:
        self.root = root
        self.drive_relative_root = drive_relative_root
        self.drives: dict[str, dict[str, Path]] = {}
        self.mapped_roots: list[tuple[str, str, Path]] = []
        for server in servers:
            self.drives[server.name] = {}
            for drive, relative in server.drives.items():
                mapped = (root / Path(relative)).resolve(strict=False)
                self._inside_root(mapped, f"drive mapping {drive}: on {server.name}")
                self.drives[server.name][drive] = mapped
                self.mapped_roots.append((server.name, drive, mapped))
        self.mapped_roots.sort(key=lambda item: len(item[2].parts), reverse=True)

    def _inside_root(self, path: Path, field: str) -> None:
        try:
            path.relative_to(self.root)
        except ValueError:
            raise PipelineResolverError(
                f"{field} resolves outside the repository root."
            ) from None

    def resolve(self, server: str, raw_path: str, cwd: str | None) -> Path:
        value = raw_path.strip().strip("'\"")
        windows = PureWindowsPath(value)
        if windows.drive:
            drive = windows.drive.rstrip(":").upper()
            base = self.drives.get(server, {}).get(drive)
            if base is None:
                raise PipelineResolverError(
                    f"Drive {drive}: is not mapped for server {server!r}."
                )
            if not windows.is_absolute() and not self.drive_relative_root:
                raise PipelineResolverError(
                    f"Drive-relative path {value!r} is not supported."
                )
            relative = Path(*windows.parts[1:])
            target = (base / relative).resolve(strict=False)
        else:
            if windows.is_absolute() or value.startswith(("\\\\", "//")):
                raise PipelineResolverError(
                    f"UNC or unmapped absolute path {value!r} is not supported."
                )
            if cwd is None:
                raise PipelineResolverError(
                    f"Relative path {value!r} has no statically known working directory."
                )
            target = self.resolve(server, cwd, None) / Path(
                *PureWindowsPath(value).parts
            )
            target = target.resolve(strict=False)
        self._inside_root(target, f"physical path {value!r}")
        return target

    def server_for(self, path: Path) -> str | None:
        matches: list[tuple[int, str]] = []
        for server, _, mapped_root in self.mapped_roots:
            try:
                path.relative_to(mapped_root)
            except ValueError:
                continue
            matches.append((len(mapped_root.parts), server))
        if not matches:
            return None
        longest = max(item[0] for item in matches)
        candidates = {server for depth, server in matches if depth == longest}
        return next(iter(candidates)) if len(candidates) == 1 else None


def _scan_sas_files(root: Path) -> dict[str, Path]:
    result: dict[str, Path] = {}
    def raise_walk_error(error: OSError) -> None:
        raise PipelineResolverError(
            f"Cannot inventory SAS files under {str(error.filename or root)!r}: {error}"
        ) from error

    for current, directories, filenames in os.walk(
        root, followlinks=False, onerror=raise_walk_error
    ):
        current_path = Path(current)
        directories[:] = sorted(
            directory
            for directory in directories
            if not (current_path / directory).is_symlink()
        )
        for filename in filenames:
            candidate = current_path / filename
            if candidate.suffix.casefold() != ".sas" or candidate.is_symlink():
                continue
            relative = candidate.relative_to(root).as_posix()
            key = relative.casefold()
            if key in result:
                raise PipelineResolverError(
                    f"Case-insensitive SAS path collision: {result[key]} and {candidate}."
                )
            result[key] = candidate
    return result


def _expand_macro_variables(value: str, variables: Mapping[str, str]) -> str:
    expanded = value
    for _ in range(10):
        matches = list(_MACRO_VARIABLE.finditer(expanded))
        if not matches:
            return expanded
        for match in reversed(matches):
            name = match.group(1).casefold()
            if name not in variables:
                raise PipelineResolverError(
                    f"Macro variable &{match.group(1)} has no statically known value."
                )
            expanded = (
                expanded[: match.start()]
                + variables[name]
                + expanded[match.end() :]
            )
    if _MACRO_VARIABLE.search(expanded):
        raise PipelineResolverError("Macro variable expansion exceeded 10 passes.")
    return expanded


def resolve_pipeline(
    repository_root: str | Path, config: Mapping[str, Any]
) -> PipelineReport:
    """Resolve include reachability from configured server-specific entrypoints."""
    report = _resolve_pipeline_once(repository_root, config, {})
    options = _load_options(config)
    if not options.seed_remote_macro_variables or not report.remote_macro_variables:
        return report
    seed: dict[str, dict[str, str]] = {}
    for variable in report.remote_macro_variables:
        seed.setdefault(variable.server, {}).setdefault(
            variable.name.casefold(), variable.value
        )
    second = _resolve_pipeline_once(repository_root, config, seed)
    notes = tuple(
        PipelineIssue(
            variable.source,
            variable.line,
            "remote_macro_variable_seeded",
            variable.name,
            f"Assumed &{variable.name}={variable.value} from a remote session "
            f"on server {variable.server!r} when resolving that server's scripts.",
        )
        for variable in report.remote_macro_variables
    )
    return replace(second, issues=tuple(second.issues) + notes)


def _resolve_pipeline_once(
    repository_root: str | Path,
    config: Mapping[str, Any],
    remote_seed: Mapping[str, Mapping[str, str]],
) -> PipelineReport:
    try:
        root = Path(repository_root).resolve(strict=True)
    except OSError as error:
        raise PipelineResolverError(
            f"Cannot access repository root {str(repository_root)!r}: {error}"
        ) from error
    if not root.is_dir():
        raise PipelineResolverError(
            f"Repository root is not a directory: {str(root)!r}."
        )

    servers, entrypoints, remote_aliases = _load_config(config)
    resolution_options = _load_options(config)
    known_server_names = {item.name.casefold(): item.name for item in servers}
    remote_hosts: dict[str, str] = {}
    for host, server_name in resolution_options.remote_hosts.items():
        if server_name.casefold() not in known_server_names:
            raise PipelineResolverError(
                f"remote_hosts maps {host!r} to an unknown configured server."
            )
        remote_hosts[host] = known_server_names[server_name.casefold()]
    remote_endpoints: dict[tuple[str, int, str], PipelineRemoteEndpoint] = {}
    remote_variables: dict[tuple[str, str], PipelineRemoteMacroVariable] = {}
    learned_alias_servers: dict[str, str] = {}
    resolver = _PathResolver(
        root, servers, drive_relative_root=resolution_options.drive_relative_root
    )
    files = _scan_sas_files(root)
    actual_paths = {
        key: path.relative_to(root).as_posix() for key, path in files.items()
    }
    reachable_from: dict[str, set[str]] = {}
    executing_servers: dict[str, set[str]] = {}
    includes: dict[tuple[str, int, str], PipelineInclude] = {}
    libraries: dict[tuple[str, int, str, str], PipelineLibrary] = {}
    remotes: dict[tuple[str, int, str, str | None], PipelineRemoteReference] = {}
    remote_blocks: dict[tuple[str, int], PipelineRemoteBlock] = {}
    external_connections: dict[
        tuple[str, int, str, str, str | None], PipelineExternalConnection
    ] = {}
    issues: dict[tuple[str, int | None, str, str], PipelineIssue] = {}
    entrypoint_rows: list[dict[str, str]] = []
    server_configs = {item.name.casefold(): item for item in servers}

    def add_issue(
        source: str, line: int | None, kind: str, reference: str, reason: str
    ) -> None:
        issue = PipelineIssue(source, line, kind, reference, reason)
        issues[(source, line, kind, reference)] = issue

    def record_failed_include(
        source_key: str, line_number: int, reference: str, error: object
    ) -> None:
        waiver = next(
            (
                reason
                for pattern, reason in resolution_options.waived_includes
                if pattern == _normalize_reference(reference)
            ),
            None,
        )
        if waiver is not None:
            includes[(source_key, line_number, reference)] = PipelineInclude(
                actual_paths[source_key], line_number, reference, None, "waived", waiver
            )
            return
        includes[(source_key, line_number, reference)] = PipelineInclude(
            actual_paths[source_key], line_number, reference, None, "unresolved", str(error)
        )
        add_issue(actual_paths[source_key], line_number, "include", reference, str(error))

    for entrypoint in entrypoints:
        key = entrypoint["path"].casefold()
        path = files.get(key)
        if path is None:
            raise PipelineResolverError(
                f"Entrypoint SAS file does not exist: {entrypoint['path']!r}."
            )
        physical_server = resolver.server_for(path)
        if physical_server is None:
            raise PipelineResolverError(
                f"Entrypoint {entrypoint['path']!r} is not under a configured "
                "server drive mapping."
            )
        if physical_server.casefold() != entrypoint["server"].casefold():
            raise PipelineResolverError(
                f"Entrypoint {entrypoint['path']!r} is under server "
                f"{physical_server!r}, not configured server {entrypoint['server']!r}."
            )
        canonical = actual_paths[key]
        entrypoint_rows.append({"server": entrypoint["server"], "path": canonical})

    max_depth = max(50, len(files) + 1)
    visit_count = 0
    max_visits = max(10_000, len(files) * 100)

    def visit(
        key: str,
        executing_server: str,
        entrypoint_path: str,
        stack: tuple[str, ...],
        depth: int,
        filename_refs: dict[str, str],
        macro_vars: dict[str, str],
        cwd: str | None,
    ) -> None:
        nonlocal visit_count
        visit_count += 1
        if visit_count > max_visits:
            add_issue(
                actual_paths[key],
                None,
                "include_expansion_limit",
                actual_paths[key],
                f"Static include expansion exceeded the safety limit of {max_visits}.",
            )
            return
        if depth > max_depth:
            add_issue(
                actual_paths[key],
                None,
                "include_depth",
                actual_paths[key],
                f"Include nesting exceeded the safety limit of {max_depth}.",
            )
            return
        if key in stack:
            add_issue(
                actual_paths[key],
                None,
                "include_cycle",
                actual_paths[key],
                "Recursive include cycle detected.",
            )
            return
        reachable_from.setdefault(key, set()).add(entrypoint_path)
        path = files[key]
        physical_server = resolver.server_for(path)
        executing_servers.setdefault(key, set()).add(
            physical_server or executing_server
        )
        try:
            content = path.read_bytes()
        except OSError as error:
            add_issue(
                actual_paths[key],
                None,
                "read_error",
                actual_paths[key],
                f"Cannot read SAS source: {error}",
            )
            return
        source, _, decode_error = _decode_source(content)
        if source is None:
            add_issue(
                actual_paths[key],
                None,
                "decode_error",
                actual_paths[key],
                decode_error or "SAS source could not be decoded.",
            )
            return
        try:
            statements = parse_program(source).statements
        except SasParseError as error:
            add_issue(
                actual_paths[key],
                error.line,
                "parse_error",
                actual_paths[key],
                error.message,
            )
            return

        current_server: str | None = executing_server
        written_filerefs: set[str] = set()
        active_remote_alias: str | None = None
        remote_context_stack: list[
            tuple[str | None, dict[str, str], dict[str, str], str | None, int, str | None, str | None]
        ] = []

        def record_remote(operation: str, alias: str | None, line_number: int) -> str | None:
            mapped_server = (
                remote_aliases.get(alias.casefold()) if alias else None
            )
            endpoint_value = macro_vars.get(alias.casefold()) if alias else None
            if alias and mapped_server is None and endpoint_value is None:
                mapped_server = learned_alias_servers.get(alias.casefold())
            endpoint = _ENDPOINT.match(endpoint_value) if endpoint_value else None
            if alias and endpoint:
                host = endpoint.group(1)
                if mapped_server is None:
                    mapped_server = remote_hosts.get(host.casefold())
                if mapped_server is not None:
                    learned_alias_servers[alias.casefold()] = mapped_server
                remote_endpoints[(key, line_number, alias.casefold())] = (
                    PipelineRemoteEndpoint(
                        actual_paths[key],
                        line_number,
                        alias,
                        host,
                        int(endpoint.group(2)) if endpoint.group(2) else None,
                        mapped_server,
                    )
                )
            remotes[
                (key, line_number, operation, alias.casefold() if alias else None)
            ] = PipelineRemoteReference(
                actual_paths[key],
                line_number,
                executing_server,
                operation,
                alias,
                mapped_server,
            )
            if alias and mapped_server is None:
                add_issue(
                    actual_paths[key],
                    line_number,
                    "remote_server",
                    alias,
                    "Remote alias is not mapped to a configured server.",
                )
            elif operation in {"signon", "rsubmit"} and not alias:
                add_issue(
                    actual_paths[key],
                    line_number,
                    "remote_alias_missing",
                    operation.upper(),
                    f"{operation.upper()} does not identify a remote alias.",
                )
            return mapped_server

        def record_io(
            mode: str,
            target: str,
            direction: str | None,
            line_number: int,
            *,
            local_libref: str | None = None,
            dbms: str | None = None,
        ) -> None:
            try:
                target = _expand_macro_variables(target, macro_vars)
            except PipelineResolverError:
                pass
            system = _safe_report_value(target)
            external_connections[
                (key, line_number, mode, system.casefold(), local_libref)
            ] = PipelineExternalConnection(
                actual_paths[key],
                line_number,
                current_server or "unknown",
                mode,
                system,
                local_libref,
                dbms,
                None,
                direction,
            )

        def resolve_server_path(reference: str, working_directory: str | None) -> Path:
            if current_server is None:
                raise PipelineResolverError(
                    "Remote server is unknown; path resolution was not guessed."
                )
            return resolver.resolve(current_server, reference, working_directory)

        for statement in statements:
            text = statement.text.strip()
            line = statement.span.start_line
            lowered = text.casefold()

            remote = _REMOTE_COMMANDS.match(text)
            if remote:
                operation = remote.group(1).casefold()
                supplied_alias = remote.group(2)
                alias = supplied_alias or active_remote_alias
                mapped_server = record_remote(operation, alias, line)
                if operation == "signon":
                    active_remote_alias = alias
                if operation == "rsubmit":
                    active_remote_alias = alias
                    if remote_context_stack:
                        add_issue(
                            actual_paths[key],
                            line,
                            "nested_remote_submit",
                            alias or text,
                            "Nested RSUBMIT blocks are not supported by static remote context analysis.",
                        )
                    remote_context_stack.append(
                        (
                            current_server,
                            filename_refs.copy(),
                            macro_vars.copy(),
                            cwd,
                            line,
                            alias,
                            mapped_server,
                        )
                    )
                    current_server = mapped_server
                    filename_refs.clear()
                    macro_vars.clear()
                    cwd = None
                elif operation == "endrsubmit":
                    if not remote_context_stack:
                        add_issue(
                            actual_paths[key],
                            line,
                            "unmatched_endrsubmit",
                            text,
                            "ENDRSUBMIT has no matching RSUBMIT in this source.",
                        )
                    else:
                        (
                            previous_server,
                            previous_filenames,
                            previous_macros,
                            previous_cwd,
                            start_line,
                            block_alias,
                            target_server,
                        ) = remote_context_stack.pop()
                        remote_blocks[(key, start_line)] = PipelineRemoteBlock(
                            actual_paths[key],
                            start_line,
                            line,
                            block_alias,
                            executing_server,
                            target_server,
                            "resolved" if target_server else "unknown_endpoint",
                        )
                        current_server = previous_server
                        filename_refs.clear()
                        filename_refs.update(previous_filenames)
                        macro_vars.clear()
                        macro_vars.update(previous_macros)
                        cwd = previous_cwd
                elif operation == "signoff":
                    active_remote_alias = None
                continue

            remote_option = (
                _REMOTE_OPTION.search(text)
                if lowered.startswith("options ")
                else None
            )
            if remote_option:
                active_remote_alias = remote_option.group(1)
                record_remote("options_remote", active_remote_alias, line)

            macro_assignment = _LET.fullmatch(text)
            file_output = _FILE_OUTPUT.match(text)
            if file_output and file_output.group(1).casefold() not in {"print", "log"}:
                written_filerefs.add(file_output.group(1).casefold())
            if macro_assignment:
                name = macro_assignment.group(1).casefold()
                value = macro_assignment.group(2).strip()
                macro_vars[name] = value
                if remote_context_stack and current_server is not None:
                    remote_variables.setdefault(
                        (current_server, name),
                        PipelineRemoteMacroVariable(
                            current_server,
                            name,
                            _safe_report_value(value),
                            actual_paths[key],
                            line,
                        ),
                    )
                continue

            cd = _X_CD.fullmatch(text)
            if cd:
                try:
                    command = _expand_macro_variables(
                        cd.group(2).strip(), macro_vars
                    )
                except PipelineResolverError as error:
                    add_issue(
                        actual_paths[key],
                        line,
                        "working_directory",
                        cd.group(2),
                        str(error),
                    )
                    continue
                command_match = re.fullmatch(r"(?:cd|chdir)\s+(.+)", command, re.I)
                if command_match:
                    requested = command_match.group(1).strip().strip("'\"")
                    try:
                        resolve_server_path(requested, cwd)
                    except PipelineResolverError as error:
                        add_issue(
                            actual_paths[key],
                            line,
                            "working_directory",
                            requested,
                            str(error),
                        )
                    else:
                        cwd = requested
                else:
                    record_io("host_command", command[:200], None, line)
                continue

            filename = _FILENAME.match(text)
            if filename:
                fileref = filename.group(1).casefold()
                if filename.group(3) is None:
                    filename_refs.pop(fileref, None)
                else:
                    filename_refs[fileref] = filename.group(3)
                continue
            device = _FILENAME_DEVICE.match(text)
            if device:
                device_name = device.group(2).casefold()
                target = _FILENAME_DEVICE_TARGET.search(device.group(3))
                record_io(
                    device_name,
                    _quoted_value(target.group(1)) if target and device_name != "email" else device_name,
                    _FILENAME_DEVICE_DIRECTION[device_name],
                    line,
                    local_libref=device.group(1).casefold(),
                )
                continue
            if lowered.startswith("filename "):
                add_issue(
                    actual_paths[key],
                    line,
                    "filename",
                    text,
                    "FILENAME form is not a statically resolvable quoted path or CLEAR.",
                )
                continue

            remote_libname = _REMOTE_LIBNAME.match(text)
            if remote_libname:
                options = remote_libname.group(2)
                remote_server_match = _LIBNAME_SERVER.search(options)
                if remote_server_match:
                    local_libref = remote_libname.group(1).casefold()
                    remote_server = remote_server_match.group(1)
                    remote_libref_match = _SLIBREF.search(options)
                    mapped_server = remote_aliases.get(remote_server.casefold())
                    external_connections[
                        (
                            key,
                            line,
                            "sas_connect_libref",
                            remote_server.casefold(),
                            local_libref,
                        )
                    ] = PipelineExternalConnection(
                        actual_paths[key],
                        line,
                        current_server or "unknown",
                        "sas_connect_libref",
                        remote_server,
                        local_libref,
                        (
                            remote_libref_match.group(1)
                            if remote_libref_match
                            else None
                        ),
                        mapped_server,
                    )
                    if mapped_server is None:
                        add_issue(
                            actual_paths[key],
                            line,
                            "remote_server",
                            remote_server,
                            "Remote LIBNAME server is not mapped to a configured server.",
                        )
                    continue

            library = _LIBNAME.match(text)
            if library:
                libref = library.group(1).casefold()
                if library.group(3) is not None:
                    physical = library.group(3)
                    reason_code: str | None = None
                    try:
                        physical = _expand_macro_variables(
                            library.group(3), macro_vars
                        )
                        report_physical = _safe_report_value(physical)
                        if current_server is None:
                            raise PipelineResolverError(
                                "Remote server is unknown; library path was not resolved."
                            )
                        mapped = resolve_server_path(physical, cwd)
                        repository_path = mapped.relative_to(root).as_posix()
                        status = "mapped"
                    except PipelineResolverError as error:
                        report_physical = _safe_report_value(physical)
                        repository_path = None
                        status = "unresolved"
                        reason_code = _reason_code(str(error))
                        add_issue(
                            actual_paths[key],
                            line,
                            "library_path",
                            _safe_report_value(physical),
                            _safe_report_value(str(error)),
                        )
                    libraries[(key, line, libref, physical)] = PipelineLibrary(
                        actual_paths[key],
                        line,
                        current_server or "unknown",
                        libref,
                        report_physical,
                        repository_path,
                        status,
                        reason_code,
                    )
                continue
            if lowered.startswith("libname "):
                engine = _LIBNAME_ENGINE.match(text)
                engine_name = engine.group(1).casefold() if engine else ""
                libref_match = re.match(r"^\s*libname\s+([A-Za-z_]\w*)", text, re.I)
                if engine_name and engine_name != "clear" and libref_match:
                    external_connections[
                        (key, line, "sas_access", engine_name, None)
                    ] = PipelineExternalConnection(
                        actual_paths[key],
                        line,
                        current_server or "unknown",
                        "sas_access",
                        engine_name,
                        libref_match.group(1).casefold(),
                        None,
                        None,
                    )
                else:
                    add_issue(
                        actual_paths[key],
                        line,
                        "library_path",
                        text,
                        "LIBNAME form is not a statically resolvable quoted physical path or CLEAR.",
                    )
                continue

            include = _DIRECT_INCLUDE.fullmatch(text)
            if include:
                reference = include.group(2)
                try:
                    if current_server is None:
                        raise PipelineResolverError(
                            "Remote server is unknown; include path was not resolved."
                        )
                    reference = _expand_macro_variables(
                        include.group(2), macro_vars
                    )
                    reference_path = PureWindowsPath(reference)
                    if reference_path.drive or reference_path.is_absolute() or cwd:
                        target = resolve_server_path(reference, cwd)
                    else:
                        if (
                            physical_server
                            and current_server.casefold() != physical_server.casefold()
                        ):
                            raise PipelineResolverError(
                                "Relative include inside RSUBMIT requires a known remote working directory."
                            )
                        target = (
                            path.parent / Path(*reference_path.parts)
                        ).resolve(strict=False)
                        resolver._inside_root(target, f"include path {reference!r}")
                    target_key = target.relative_to(root).as_posix().casefold()
                    if not target.suffix:
                        target = target.with_suffix(".sas")
                        target_key = target.relative_to(root).as_posix().casefold()
                    target_key = target_key if target_key in files else ""
                    if not target_key:
                        raise PipelineResolverError(
                            f"Included file {reference!r} is not in the scanned SAS inventory."
                        )
                except (PipelineResolverError, ValueError) as error:
                    record_failed_include(key, line, reference, error)
                else:
                    target_path = actual_paths[target_key]
                    includes[(key, line, reference)] = PipelineInclude(
                        actual_paths[key], line, reference, target_path, "resolved"
                    )
                    reference_drive = PureWindowsPath(reference)
                    if (
                        resolution_options.drive_relative_root
                        and reference_drive.drive
                        and not reference_drive.is_absolute()
                    ):
                        add_issue(
                            actual_paths[key],
                            line,
                            "drive_relative_assumed_root",
                            reference,
                            "Drive-relative path was resolved against the drive root "
                            "by configuration (drive_relative_paths=drive_root).",
                        )
                    visit(
                        target_key,
                        current_server,
                        entrypoint_path,
                        stack + (key,),
                        depth + 1,
                        filename_refs,
                        macro_vars,
                        cwd,
                    )
                continue

            include_file = _FILEREF_INCLUDE_FILE.fullmatch(text)
            if include_file:
                fileref = include_file.group(1).casefold()
                if fileref in written_filerefs:
                    includes[(key, line, text)] = PipelineInclude(
                        actual_paths[key],
                        line,
                        text,
                        None,
                        "runtime_generated",
                        f"Fileref {fileref!r} is written by this script (FILE {fileref}) "
                        "before it is included; its contents are generated at run time.",
                    )
                    continue
                physical_path = filename_refs.get(fileref)
                try:
                    if current_server is None:
                        raise PipelineResolverError(
                            "Remote server is unknown; FILENAME include was not resolved."
                        )
                    if physical_path is None:
                        raise PipelineResolverError(
                            f"FILENAME fileref {fileref!r} has no known assignment."
                        )
                    physical_path = _expand_macro_variables(
                        physical_path, macro_vars
                    )
                    target = resolve_server_path(physical_path, cwd)
                    if target.suffix.casefold() != ".sas":
                        target = target.with_suffix(".sas")
                    target_key = target.relative_to(root).as_posix().casefold()
                    if target_key not in files:
                        raise PipelineResolverError(
                            f"FILENAME fileref {fileref!r} does not resolve to a "
                            "scanned SAS file."
                        )
                except (PipelineResolverError, ValueError) as error:
                    record_failed_include(key, line, text, error)
                else:
                    includes[(key, line, text)] = PipelineInclude(
                        actual_paths[key],
                        line,
                        text,
                        actual_paths[target_key],
                        "resolved",
                    )
                    visit(
                        target_key,
                        current_server,
                        entrypoint_path,
                        stack + (key,),
                        depth + 1,
                        filename_refs,
                        macro_vars,
                        cwd,
                    )
                continue

            include = _FILEREF_INCLUDE.fullmatch(text)
            if include:
                fileref = include.group(1).casefold()
                member = include.group(2).strip().strip("'\"")
                physical_base = filename_refs.get(fileref)
                if physical_base is None:
                    reason = f"FILENAME fileref {fileref!r} has no known assignment."
                    includes[(key, line, text)] = PipelineInclude(
                        actual_paths[key], line, text, None, "unresolved", reason
                    )
                    add_issue(actual_paths[key], line, "include", text, reason)
                    continue
                try:
                    if current_server is None:
                        raise PipelineResolverError(
                            "Remote server is unknown; FILENAME member include was not resolved."
                        )
                    physical_base = _expand_macro_variables(
                        physical_base, macro_vars
                    )
                    target = resolve_server_path(physical_base, cwd)
                    target = target / Path(*PureWindowsPath(member).parts)
                    if not target.suffix:
                        target = target.with_suffix(".sas")
                    target = target.resolve(strict=False)
                    resolver._inside_root(target, f"included member {member!r}")
                    target_key = target.relative_to(root).as_posix().casefold()
                    if target_key not in files:
                        raise PipelineResolverError(
                            f"Included member {member!r} was not found under "
                            f"FILENAME {fileref} path {physical_base!r}."
                        )
                except (PipelineResolverError, ValueError) as error:
                    record_failed_include(key, line, text, error)
                else:
                    target_path = actual_paths[target_key]
                    includes[(key, line, text)] = PipelineInclude(
                        actual_paths[key], line, text, target_path, "resolved"
                    )
                    visit(
                        target_key,
                        current_server,
                        entrypoint_path,
                        stack + (key,),
                        depth + 1,
                        filename_refs,
                        macro_vars,
                        cwd,
                    )
                continue

            if _ANY_INCLUDE.match(text):
                reference = text
                reason = "Dynamic or unsupported %INCLUDE form."
                record_failed_include(key, line, text, reason)
                continue

            sql_connection = _SQL_CONNECT.search(text)
            proc_io = _PROC_IMPORT_EXPORT.match(text)
            if proc_io:
                io_kind = proc_io.group(1).casefold()
                io_path = _IO_PATH.search(proc_io.group(2))
                io_dbms = _IO_DBMS.search(proc_io.group(2))
                record_io(
                    f"file_{io_kind}",
                    _quoted_value(io_path.group(1))
                    if io_path
                    else (io_dbms.group(1).casefold() if io_dbms else "unspecified"),
                    "input" if io_kind == "import" else "output",
                    line,
                    dbms=io_dbms.group(1).casefold() if io_dbms else None,
                )
            ods = _ODS_OUTPUT.match(text)
            if ods:
                ods_path = _ODS_PATH.search(ods.group(2))
                if ods_path:
                    record_io(
                        "ods_output",
                        _quoted_value(ods_path.group(1)),
                        "output",
                        line,
                        dbms=ods.group(1).casefold(),
                    )
            host_command = _HOST_COMMAND.match(text)
            if host_command and not _X_CD.fullmatch(text):
                record_io(
                    "host_command",
                    " ".join(host_command.group(1).split())[:200],
                    None,
                    line,
                )
            stream = _INFILE_FILE.match(text)
            if stream and stream.group(2).casefold() not in {"print", "log"}:
                stream_kind = stream.group(1).casefold()
                stream_target = _quoted_value(stream.group(2))
                if stream.group(2)[0] not in "'\"":
                    stream_target = filename_refs.get(
                        stream_target.casefold(), f"fileref:{stream_target}"
                    )
                record_io(
                    "file_read" if stream_kind == "infile" else "file_write",
                    stream_target,
                    "input" if stream_kind == "infile" else "output",
                    line,
                )
            if sql_connection:
                engine = sql_connection.group(1).casefold()
                external_connections[
                    (key, line, "sql_pass_through", engine, None)
                ] = PipelineExternalConnection(
                    actual_paths[key],
                    line,
                    current_server or "unknown",
                    "sql_pass_through",
                    engine,
                    None,
                    None,
                    None,
                )

        for context in remote_context_stack:
            start_line = context[4]
            block_alias = context[5]
            target_server = context[6]
            remote_blocks[(key, start_line)] = PipelineRemoteBlock(
                actual_paths[key],
                start_line,
                None,
                block_alias,
                executing_server,
                target_server,
                "unclosed",
            )
            add_issue(
                actual_paths[key],
                start_line,
                "unclosed_rsubmit",
                block_alias or actual_paths[key],
                "RSUBMIT has no matching ENDRSUBMIT in this source.",
            )

    for entrypoint in entrypoint_rows:
        key = entrypoint["path"].casefold()
        filename_refs: dict[str, str] = {}
        macro_vars: dict[str, str] = {
            **resolution_options.macro_variables,
            **resolution_options.server_macro_variables.get(
                entrypoint["server"].casefold(), {}
            ),
        }
        for seeded_name, seeded_value in remote_seed.get(
            entrypoint["server"], {}
        ).items():
            macro_vars.setdefault(seeded_name, seeded_value)
        cwd: str | None = None
        server_config = server_configs[entrypoint["server"].casefold()]
        for initialization_script in server_config.initialization_scripts:
            initialization_key = initialization_script.casefold()
            initialization_path = files.get(initialization_key)
            if initialization_path is None:
                add_issue(
                    initialization_script,
                    None,
                    "initialization_script",
                    initialization_script,
                    "Configured server initialization script is missing from the SAS inventory.",
                )
                continue
            visit(
                initialization_key,
                entrypoint["server"],
                entrypoint["path"],
                (),
                0,
                filename_refs,
                macro_vars,
                cwd,
            )
        visit(
            key,
            entrypoint["server"],
            entrypoint["path"],
            (),
            0,
            filename_refs,
            macro_vars,
            cwd,
        )

    scripts: list[PipelineScript] = []
    for key, path in sorted(files.items(), key=lambda item: item[1].relative_to(root).as_posix().casefold()):
        relative = actual_paths[key]
        try:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as error:
            add_issue(relative, None, "read_error", relative, str(error))
            digest = ""
        roots = tuple(sorted(reachable_from.get(key, ()), key=str.casefold))
        script_servers = executing_servers.get(key, set())
        script_server = (
            next(iter(script_servers))
            if len(script_servers) == 1
            else resolver.server_for(path)
            if not script_servers
            else None
        )
        scripts.append(
            PipelineScript(
                relative,
                script_server,
                digest,
                "reachable" if roots else "unreached",
                roots,
            )
        )
        if script_server is None:
            add_issue(
                relative,
                None,
                "server_mapping",
                relative,
                "Script is outside configured server drive mappings or matches "
                "overlapping mappings for multiple servers.",
            )

    job_rows = _build_jobs(
        files, entrypoint_rows, actual_paths, includes, resolution_options.jams_jobs
    )

    return PipelineReport(
        repository_root=str(root),
        generated_at=datetime.now(timezone.utc).isoformat(),
        jobs=tuple(job_rows),
        remote_endpoints=tuple(
            sorted(
                remote_endpoints.values(),
                key=lambda item: (item.source.casefold(), item.line, item.alias),
            )
        ),
        remote_macro_variables=tuple(
            sorted(remote_variables.values(), key=lambda item: (item.server, item.name))
        ),
        entrypoints=tuple(
            sorted(entrypoint_rows, key=lambda item: (item["server"].casefold(), item["path"].casefold()))
        ),
        scripts=tuple(scripts),
        includes=tuple(
            sorted(includes.values(), key=lambda item: (item.source.casefold(), item.line, item.reference.casefold()))
        ),
        libraries=tuple(
            sorted(libraries.values(), key=lambda item: (item.source.casefold(), item.line, item.libref))
        ),
        remote_references=tuple(
            sorted(remotes.values(), key=lambda item: (item.source.casefold(), item.line, item.operation))
        ),
        remote_blocks=tuple(
            sorted(
                remote_blocks.values(),
                key=lambda item: (item.source.casefold(), item.start_line),
            )
        ),
        external_connections=tuple(
            sorted(
                external_connections.values(),
                key=lambda item: (
                    item.source.casefold(),
                    item.line,
                    item.mode,
                    item.system.casefold(),
                ),
            )
        ),
        issues=tuple(
            sorted(
                issues.values(),
                key=lambda item: (item.source.casefold(), item.line or 0, item.kind, item.reference.casefold()),
            )
        ),
    )


def _build_jobs(
    files: Mapping[str, Path],
    entrypoints: list[dict[str, str]],
    actual_paths: Mapping[str, str],
    includes: Mapping[Any, PipelineInclude],
    configured: tuple[dict[str, Any], ...],
) -> list[dict[str, Any]]:
    by_entrypoint = {job["entrypoint"]: job for job in configured}
    matched: set[str] = set()
    rows: list[dict[str, Any]] = []
    for entrypoint in entrypoints:
        path = entrypoint["path"]
        source_text = ""
        decoded = _decode_source(files[path.casefold()].read_bytes())[0]
        if decoded:
            source_text = decoded
        job_name = _JOB_NAME.search(source_text)
        job_source = _JOB_SOURCE.search(source_text)
        header = re.match(r"\s*/\*(.*?)\*/", source_text, re.S)
        disabled = sorted(
            {
                m.group(0).strip()
                for block in _COMMENT_BLOCK.findall(source_text)
                for m in _COMMENTED_INCLUDE.finditer(block)
            }
        )
        direct = [
            {"line": item.line, "reference": item.reference, "status": item.status}
            for item in sorted(includes.values(), key=lambda i: i.line)
            if item.source == actual_paths[path.casefold()]
        ]
        declared = by_entrypoint.get(_normalize_reference(path))
        if declared:
            matched.add(declared["entrypoint"])
        rows.append(
            {
                "name": declared["name"]
                if declared
                else (job_name.group(2) if job_name else Path(path).stem),
                "entrypoint": path,
                "server": entrypoint["server"],
                "declared_job_name": job_name.group(2) if job_name else None,
                "declared_source": job_source.group(2) if job_source else None,
                "header_comment": " ".join(header.group(1).split())[:500]
                if header
                else None,
                "direct_includes": direct,
                "disabled_includes": disabled,
                "jams": (
                    {
                        "schedule": declared["schedule"],
                        "depends_on": declared["depends_on"],
                        "parameters": declared["parameters"],
                    }
                    if declared
                    else None
                ),
            }
        )
    for entrypoint_key, declared in by_entrypoint.items():
        if entrypoint_key not in matched:
            raise PipelineResolverError(
                f"JAMS job {declared['name']!r} references an entrypoint that is "
                "not configured."
            )
    return rows


def load_pipeline_config(path: str | Path) -> dict[str, Any]:
    """Load pipeline resolver JSON config and surface malformed JSON explicitly."""
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise PipelineResolverError(
            f"Cannot read pipeline configuration {str(path)!r}: {error}"
        ) from error
    if not isinstance(value, dict):
        raise PipelineResolverError("Pipeline configuration must be a JSON object.")
    return value
