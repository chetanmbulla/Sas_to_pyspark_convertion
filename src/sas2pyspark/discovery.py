"""Non-executing SAS repository inventory."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DISCOVERY_SCHEMA_VERSION = 1
DEFAULT_EXCLUDED_DIRECTORIES = (
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    "node_modules",
    "target",
    "dist",
)
DEFAULT_ASSET_EXTENSIONS = (
    ".csv",
    ".dat",
    ".dbf",
    ".ini",
    ".json",
    ".parquet",
    ".sas7bdat",
    ".txt",
    ".xml",
    ".xls",
    ".xlsx",
    ".xpt",
)
_WORD = re.compile(r"^[A-Za-z_%][A-Za-z0-9_%]*")


class DiscoveryError(ValueError):
    """Raised when a repository cannot be scanned or report options are invalid."""


@dataclass(frozen=True)
class SasProgramInventory:
    path: str
    size_bytes: int
    sha256: str
    encoding: str | None
    statement_count: int
    data_step_count: int
    macro_statement_count: int
    procedures: dict[str, int]
    decode_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "encoding": self.encoding,
            "statement_count": self.statement_count,
            "data_step_count": self.data_step_count,
            "macro_statement_count": self.macro_statement_count,
            "procedures": dict(sorted(self.procedures.items())),
            "decode_error": self.decode_error,
        }


@dataclass(frozen=True)
class AssetInventory:
    path: str
    extension: str
    size_bytes: int
    sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "extension": self.extension,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
        }


@dataclass(frozen=True)
class DiscoveryIssue:
    path: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "message": self.message}


@dataclass(frozen=True)
class DiscoveryReport:
    root: str
    discovered_at: str
    excluded_directories: tuple[str, ...]
    asset_extensions: tuple[str, ...]
    programs: tuple[SasProgramInventory, ...]
    assets: tuple[AssetInventory, ...]
    issues: tuple[DiscoveryIssue, ...]
    skipped_symlinks: int
    schema_version: int = DISCOVERY_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "root": self.root,
            "discovered_at": self.discovered_at,
            "excluded_directories": list(self.excluded_directories),
            "asset_extensions": list(self.asset_extensions),
            "programs": [item.to_dict() for item in self.programs],
            "assets": [item.to_dict() for item in self.assets],
            "issues": [item.to_dict() for item in self.issues],
            "skipped_symlinks": self.skipped_symlinks,
            "summary": {
                "program_count": len(self.programs),
                "asset_count": len(self.assets),
                "issue_count": len(self.issues),
            },
        }

    def save(self, path: str | Path) -> None:
        destination = Path(path)
        document = json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n"
        destination.write_text(document, encoding="utf-8")


def _matches_exclusion(relative_path: str, patterns: tuple[str, ...]) -> bool:
    parts = relative_path.split("/")
    normalized_path = relative_path.casefold()
    for pattern in patterns:
        normalized_pattern = pattern.casefold()
        if normalized_pattern.endswith("/**"):
            directory_prefix = normalized_pattern[:-3].rstrip("/")
            if normalized_path == directory_prefix or normalized_path.startswith(
                directory_prefix + "/"
            ):
                return True
        if fnmatch.fnmatchcase(normalized_path, normalized_pattern) or any(
            fnmatch.fnmatchcase(part.casefold(), normalized_pattern) for part in parts
        ):
            return True
    return False


def _decode_source(contents: bytes) -> tuple[str | None, str | None, str | None]:
    encodings = (
        (b"\x00\x00\xfe\xff", "utf-32-be"),
        (b"\xff\xfe\x00\x00", "utf-32-le"),
        (b"\xef\xbb\xbf", "utf-8-sig"),
        (b"\xfe\xff", "utf-16-be"),
        (b"\xff\xfe", "utf-16-le"),
    )
    for marker, encoding in encodings:
        if contents.startswith(marker):
            try:
                return contents.decode(encoding), encoding, None
            except UnicodeDecodeError as error:
                return None, encoding, f"Cannot decode SAS source as {encoding}: {error}"
    for encoding in ("utf-8", "cp1252"):
        try:
            return contents.decode(encoding), encoding, None
        except UnicodeDecodeError:
            continue
    return None, None, "SAS source is not valid UTF-8 or Windows-1252 text."


def _sas_statements(source: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    index = 0
    quote: str | None = None
    block_comment = False
    line_comment = False
    star_comment = False
    while index < len(source):
        char = source[index]
        following = source[index + 1] if index + 1 < len(source) else ""
        if block_comment:
            if char == "*" and following == "/":
                block_comment = False
                index += 2
                continue
            index += 1
            continue
        if line_comment:
            if char in "\r\n":
                line_comment = False
            index += 1
            continue
        if star_comment:
            if char == ";":
                star_comment = False
            index += 1
            continue
        if quote is not None:
            current.append(char)
            if char == quote:
                if following == quote:
                    current.append(following)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            current.append(char)
            index += 1
            continue
        if char == "/" and following == "*":
            block_comment = True
            index += 2
            continue
        if char == "*" and following == "/":
            line_comment = True
            index += 2
            continue
        if char == "%" and following == "*" and not "".join(current).strip():
            star_comment = True
            index += 2
            continue
        if char == "*":
            prefix = "".join(current).strip()
            if not prefix or prefix.endswith((";", "\n")):
                star_comment = True
                index += 1
                continue
        if char == ";":
            text = "".join(current).strip()
            if text:
                statements.append(text)
            current.clear()
        else:
            current.append(char)
        index += 1
    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


def _count_source(source: str) -> tuple[int, int, int, dict[str, int]]:
    statements = _sas_statements(source)
    data_steps = 0
    macro_statements = 0
    procedures: dict[str, int] = {}
    for statement in statements:
        word = _WORD.match(statement)
        if word is None:
            continue
        keyword = word.group().casefold()
        if keyword == "data":
            data_steps += 1
        elif keyword == "proc":
            procedure_match = re.match(
                r"^\s*proc\s+([A-Za-z_][A-Za-z0-9_]*)",
                statement,
                re.IGNORECASE,
            )
            if procedure_match:
                procedure = procedure_match.group(1).upper()
                procedures[procedure] = procedures.get(procedure, 0) + 1
        elif keyword.startswith("%"):
            macro_statements += 1
    return len(statements), data_steps, macro_statements, procedures


def discover_repository(
    root: str | Path,
    *,
    excluded_directories: tuple[str, ...] = DEFAULT_EXCLUDED_DIRECTORIES,
    asset_extensions: tuple[str, ...] = DEFAULT_ASSET_EXTENSIONS,
    excluded_paths: tuple[str | Path, ...] = (),
) -> DiscoveryReport:
    repository = Path(root)
    try:
        repository = repository.resolve(strict=True)
    except OSError as error:
        raise DiscoveryError(f"Cannot access repository root {str(root)!r}: {error}") from error
    if not repository.is_dir():
        raise DiscoveryError(f"Repository root is not a directory: {str(repository)!r}.")

    exclusions = tuple(_normalize_exclusion(item) for item in excluded_directories)
    extensions = tuple(sorted({_normalize_extension(item) for item in asset_extensions}))
    ignored_files = set()
    for path in excluded_paths:
        try:
            ignored_files.add(Path(path).resolve(strict=False))
        except OSError as error:
            raise DiscoveryError(f"Cannot normalize excluded file {str(path)!r}: {error}") from error
    programs: list[SasProgramInventory] = []
    assets: list[AssetInventory] = []
    issues: list[DiscoveryIssue] = []
    skipped_symlinks = 0

    def on_walk_error(error: OSError) -> None:
        error_path = getattr(error, "filename", None)
        try:
            relative = Path(error_path).relative_to(repository).as_posix() if error_path else "."
        except ValueError:
            relative = str(error_path or ".")
        issues.append(DiscoveryIssue(relative, str(error)))

    for current_root, directories, filenames in os.walk(
        repository, topdown=True, followlinks=False, onerror=on_walk_error
    ):
        current_path = Path(current_root)
        relative_directory = current_path.relative_to(repository).as_posix()
        if relative_directory == ".":
            relative_directory = ""
        kept_directories: list[str] = []
        for directory in sorted(directories, key=str.casefold):
            candidate = current_path / directory
            relative = (Path(relative_directory) / directory).as_posix()
            if candidate.is_symlink():
                skipped_symlinks += 1
            elif not _matches_exclusion(relative, exclusions):
                kept_directories.append(directory)
        directories[:] = kept_directories

        for filename in sorted(filenames, key=str.casefold):
            candidate = current_path / filename
            relative = candidate.relative_to(repository).as_posix()
            if candidate.is_symlink():
                skipped_symlinks += 1
                continue
            if candidate.resolve(strict=False) in ignored_files:
                continue
            suffix = candidate.suffix.casefold()
            if suffix != ".sas" and suffix not in extensions:
                continue
            try:
                contents = candidate.read_bytes()
            except OSError as error:
                issues.append(DiscoveryIssue(relative, f"Cannot read file: {error}"))
                continue
            digest = hashlib.sha256(contents).hexdigest()
            if suffix == ".sas":
                source, encoding, decode_error = _decode_source(contents)
                counts = _count_source(source) if source is not None else (0, 0, 0, {})
                statement_count, data_steps, macro_statements, procedures = counts
                programs.append(
                    SasProgramInventory(
                        path=relative,
                        size_bytes=len(contents),
                        sha256=digest,
                        encoding=encoding,
                        statement_count=statement_count,
                        data_step_count=data_steps,
                        macro_statement_count=macro_statements,
                        procedures=procedures,
                        decode_error=decode_error,
                    )
                )
                if decode_error:
                    issues.append(DiscoveryIssue(relative, decode_error))
            else:
                assets.append(
                    AssetInventory(
                        path=relative,
                        extension=suffix,
                        size_bytes=len(contents),
                        sha256=digest,
                    )
                )

    programs.sort(key=lambda item: item.path.casefold())
    assets.sort(key=lambda item: item.path.casefold())
    issues.sort(key=lambda item: (item.path.casefold(), item.message))
    return DiscoveryReport(
        root=str(repository),
        discovered_at=datetime.now(timezone.utc).isoformat(),
        excluded_directories=exclusions,
        asset_extensions=extensions,
        programs=tuple(programs),
        assets=tuple(assets),
        issues=tuple(issues),
        skipped_symlinks=skipped_symlinks,
    )


def _normalize_exclusion(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DiscoveryError("Excluded directory patterns must be non-empty.")
    normalized = value.replace("\\", "/").strip("/")
    if not normalized or any(part == ".." for part in normalized.split("/")):
        raise DiscoveryError(f"Invalid excluded directory pattern: {value!r}.")
    return normalized


def _normalize_extension(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DiscoveryError("Asset extensions must be non-empty.")
    normalized = value.strip().casefold()
    if not normalized.startswith("."):
        normalized = "." + normalized
    if not re.fullmatch(r"\.[a-z0-9][a-z0-9._-]*", normalized):
        raise DiscoveryError(f"Invalid asset extension: {value!r}.")
    if normalized == ".sas":
        raise DiscoveryError("SAS programs are inventoried separately, not as assets.")
    return normalized
