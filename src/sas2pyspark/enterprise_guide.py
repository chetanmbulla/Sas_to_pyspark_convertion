"""Non-executing inventory for Enterprise Guide packages and stored processes."""

from __future__ import annotations

import hashlib
import os
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from .discovery import DEFAULT_EXCLUDED_DIRECTORIES
from .sas_parser import SasParseError, parse_program

MAX_PACKAGE_MEMBERS = 10_000
MAX_PACKAGE_ARCHIVE_BYTES = 512 * 1024 * 1024
MAX_PACKAGE_UNCOMPRESSED_BYTES = 256 * 1024 * 1024
MAX_METADATA_XML_BYTES = 2 * 1024 * 1024
MAX_EMBEDDED_SAS_BYTES = 20 * 1024 * 1024
_STP_MARKERS = {
    "%stpbegin": "stored_process_begin",
    "%stpend": "stored_process_end",
    "_webout": "web_output",
    "_odsout": "ods_output",
    "stpsrv_header": "stored_process_header",
}


class EnterpriseGuideError(ValueError):
    """Raised when an Enterprise Guide inventory cannot be created."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stored_process_markers(source: str) -> tuple[str, ...]:
    try:
        statements = parse_program(source).statements
    except SasParseError:
        # Discovery remains best effort for malformed or vendor-specific source.
        statements = ()
    text = "\n".join(statement.text for statement in statements).casefold()
    return tuple(
        marker
        for token, marker in _STP_MARKERS.items()
        if re.search(
            rf"(?<![A-Za-z0-9_]){re.escape(token)}(?![A-Za-z0-9_])",
            text,
        )
    )


def build_enterprise_guide_inventory(root: str | Path) -> dict[str, Any]:
    """List EG package contents and probable stored-process SAS sources.

    EGP archives are never extracted, and SAS code is never executed. Package
    metadata content is not returned; only member paths and package structure.
    """
    try:
        repository = Path(root).resolve(strict=True)
    except OSError as error:
        raise EnterpriseGuideError(f"Cannot access repository root {str(root)!r}: {error}") from error
    if not repository.is_dir():
        raise EnterpriseGuideError(f"Repository root is not a directory: {repository}.")

    packages: list[dict[str, Any]] = []
    stored_processes: list[dict[str, Any]] = []
    issues: list[dict[str, str]] = []
    for current_root, directories, filenames in os.walk(
        repository, topdown=True, followlinks=False
    ):
        base = Path(current_root)
        directories[:] = sorted(
            [
                name
                for name in directories
                if not (base / name).is_symlink()
                and name.casefold()
                not in {excluded.casefold() for excluded in DEFAULT_EXCLUDED_DIRECTORIES}
            ],
            key=str.casefold,
        )
        for filename in sorted(filenames, key=str.casefold):
            path = base / filename
            if path.is_symlink() or not path.is_file():
                continue
            relative = path.relative_to(repository).as_posix()
            suffix = path.suffix.casefold()
            if suffix == ".sas":
                try:
                    source = path.read_text(encoding="utf-8-sig", errors="replace")
                except OSError as error:
                    issues.append({"path": relative, "message": f"Cannot read source: {error}"})
                    continue
                markers = _stored_process_markers(source)
                if markers:
                    explicit = any(
                        marker in {"stored_process_begin", "stored_process_end"}
                        for marker in markers
                    )
                    stored_processes.append(
                        {
                            "path": relative,
                            "classification": (
                                "stored_process_marker" if explicit else "web_output_candidate"
                            ),
                            "confidence": "high" if explicit else "low",
                            "markers": list(markers),
                        }
                    )
                continue
            if suffix != ".egp":
                continue
            try:
                if path.stat().st_size > MAX_PACKAGE_ARCHIVE_BYTES:
                    raise EnterpriseGuideError(
                        "Package exceeds the compressed archive size safety limit."
                    )
                digest = _sha256(path)
            except OSError as error:
                issues.append({"path": relative, "message": f"Cannot hash package: {error}"})
                continue
            except EnterpriseGuideError as error:
                package_issue = str(error)
                packages.append(
                    {
                        "path": relative,
                        "sha256": None,
                        "member_count": 0,
                        "sas_members": [],
                        "metadata_xml_members": [],
                        "xml_root_tags": {},
                        "issues": [package_issue],
                    }
                )
                continue
            package: dict[str, Any] = {
                "path": relative,
                "sha256": digest,
                "member_count": 0,
                "sas_members": [],
                "metadata_xml_members": [],
                "xml_root_tags": {},
                "issues": [],
            }
            try:
                with zipfile.ZipFile(path) as archive:
                    members = archive.infolist()
                    package["member_count"] = len(members)
                    total_size = sum(member.file_size for member in members)
                    if len(members) > MAX_PACKAGE_MEMBERS:
                        raise EnterpriseGuideError(
                            f"Package has more than {MAX_PACKAGE_MEMBERS} members."
                        )
                    if total_size > MAX_PACKAGE_UNCOMPRESSED_BYTES:
                        raise EnterpriseGuideError(
                            "Package exceeds the uncompressed size safety limit."
                        )
                    for member in members:
                        if member.is_dir():
                            continue
                        member_name = member.filename.replace("\\", "/")
                        member_suffix = Path(member_name).suffix.casefold()
                        if member_suffix == ".sas":
                            package["sas_members"].append(member_name)
                            if member.file_size > MAX_EMBEDDED_SAS_BYTES:
                                package["issues"].append(
                                    f"Skipped oversized SAS member {member_name!r}."
                                )
                                continue
                            try:
                                source = archive.read(member).decode(
                                    "utf-8-sig", errors="replace"
                                )
                                markers = _stored_process_markers(source)
                            except (OSError, RuntimeError, zipfile.BadZipFile) as error:
                                package["issues"].append(
                                    f"Cannot inspect {member_name!r}: {error}"
                                )
                                continue
                            if markers:
                                explicit = any(
                                    marker in {
                                        "stored_process_begin",
                                        "stored_process_end",
                                    }
                                    for marker in markers
                                )
                                stored_processes.append(
                                    {
                                        "path": f"{relative}!/{member_name}",
                                        "classification": (
                                            "stored_process_marker"
                                            if explicit
                                            else "web_output_candidate"
                                        ),
                                        "confidence": "high" if explicit else "low",
                                        "markers": list(markers),
                                    }
                                )
                        elif member_suffix == ".xml":
                            package["metadata_xml_members"].append(member_name)
                            if member.file_size > MAX_METADATA_XML_BYTES:
                                package["issues"].append(
                                    f"Skipped oversized XML member {member_name!r}."
                                )
                                continue
                            try:
                                text = archive.read(member).decode(
                                    "utf-8-sig", errors="replace"
                                )
                                if re.search(r"<!DOCTYPE|<!ENTITY", text, re.I):
                                    package["issues"].append(
                                        f"Skipped XML with a DTD/entity declaration: {member_name!r}."
                                    )
                                    continue
                                element = ET.fromstring(text)
                                package["xml_root_tags"][member_name] = element.tag
                            except (
                                OSError,
                                RuntimeError,
                                zipfile.BadZipFile,
                                ET.ParseError,
                            ) as error:
                                package["issues"].append(
                                    f"Cannot parse XML member {member_name!r}: {error}"
                                )
            except (OSError, zipfile.BadZipFile, EnterpriseGuideError) as error:
                package["issues"].append(str(error))
            package["sas_members"].sort(key=str.casefold)
            package["metadata_xml_members"].sort(key=str.casefold)
            package["xml_root_tags"] = dict(
                sorted(package["xml_root_tags"].items(), key=lambda item: item[0].casefold())
            )
            packages.append(package)

    packages.sort(key=lambda item: item["path"].casefold())
    stored_processes.sort(key=lambda item: item["path"].casefold())
    issues.sort(key=lambda item: (item["path"].casefold(), item["message"]))
    return {
        "schema_version": 1,
        "repository_root": str(repository),
        "packages": packages,
        "stored_processes": stored_processes,
        "issues": issues,
        "summary": {
            "package_count": len(packages),
            "embedded_sas_count": sum(len(item["sas_members"]) for item in packages),
            "stored_process_count": sum(
                item["classification"] == "stored_process_marker"
                for item in stored_processes
            ),
            "web_output_candidate_count": sum(
                item["classification"] == "web_output_candidate"
                for item in stored_processes
            ),
            "issue_count": len(issues)
            + sum(len(package["issues"]) for package in packages),
        },
    }
