"""Batch conversion workflow for registered SAS programs."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from .converter import ConversionError, SasToPySparkConverter
from .discovery import _decode_source
from .project import Conversion, MigrationProject


CONVERSION_REPORT_SCHEMA_VERSION = 1


class BatchConversionError(ValueError):
    """Raised when batch conversion options or repository paths are invalid."""


@dataclass(frozen=True)
class ProgramConversionOutcome:
    program_id: str
    source_path: str
    source_sha256: str | None
    output_path: str
    status: str
    warnings: tuple[str, ...] = ()
    error: str | None = None
    manifest_recorded: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "program_id": self.program_id,
            "source_path": self.source_path,
            "source_sha256": self.source_sha256,
            "output_path": self.output_path,
            "status": self.status,
            "warnings": list(self.warnings),
            "error": self.error,
            "manifest_recorded": self.manifest_recorded,
        }


@dataclass(frozen=True)
class BatchConversionReport:
    project_id: str
    project_name: str
    repository_root: str
    output_directory: str
    generated_at: str
    converter_version: str
    outcomes: tuple[ProgramConversionOutcome, ...]
    schema_version: int = CONVERSION_REPORT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        succeeded = sum(item.status == "succeeded" for item in self.outcomes)
        failed = sum(item.status == "failed" for item in self.outcomes)
        return {
            "schema_version": self.schema_version,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "repository_root": self.repository_root,
            "output_directory": self.output_directory,
            "generated_at": self.generated_at,
            "converter_version": self.converter_version,
            "outcomes": [item.to_dict() for item in self.outcomes],
            "summary": {
                "program_count": len(self.outcomes),
                "succeeded": succeeded,
                "failed": failed,
            },
        }

    def save(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def _output_directory(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BatchConversionError("Output directory must be a non-empty relative path.")
    normalized = value.replace("\\", "/")
    windows_path = PureWindowsPath(value)
    posix_path = PurePosixPath(normalized)
    if (
        windows_path.is_absolute()
        or windows_path.drive
        or posix_path.is_absolute()
        or any(part in {"", ".", ".."} for part in normalized.split("/"))
    ):
        raise BatchConversionError(
            "Output directory must be a normalized path relative to the repository root."
        )
    return normalized


def _write_output(path: Path, code: str, *, force: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        raise FileExistsError(
            f"Generated output already exists: {path}. Use --force to replace it."
        )
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary.write(code)
            temporary_path = Path(temporary.name)
        if force:
            os.replace(temporary_path, path)
        else:
            os.rename(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def convert_project(
    project: MigrationProject,
    repository_root: str | Path,
    *,
    output_directory: str = "pyspark",
    converter_version: str = "0.1.0",
    force: bool = False,
    sas_version: str = "auto",
) -> BatchConversionReport:
    """Convert each registered program independently and record its outcome."""
    project.validate()
    try:
        root = Path(repository_root).resolve(strict=True)
    except OSError as error:
        raise BatchConversionError(
            f"Cannot access repository root {str(repository_root)!r}: {error}"
        ) from error
    if not root.is_dir():
        raise BatchConversionError(
            f"Repository root is not a directory: {str(root)!r}."
        )
    output_relative = _output_directory(output_directory)
    output_root = (root / Path(output_relative)).resolve(strict=False)
    try:
        output_root.relative_to(root)
    except ValueError:
        raise BatchConversionError("Output directory escapes the repository root.") from None

    outcomes: list[ProgramConversionOutcome] = []
    converter = SasToPySparkConverter()
    for program in sorted(project.programs, key=lambda item: item.path.casefold()):
        source_relative = program.path
        source_path = (root / Path(source_relative)).resolve(strict=False)
        try:
            source_path.relative_to(root)
        except ValueError:
            outcomes.append(
                ProgramConversionOutcome(
                    program_id=program.id,
                    source_path=source_relative,
                    source_sha256=None,
                    output_path=(
                        PurePosixPath(output_relative) / PurePosixPath(source_relative)
                    ).with_suffix(".py").as_posix(),
                    status="failed",
                    error="Resolved source path escapes the repository root.",
                )
            )
            continue

        output_relative_path = (
            PurePosixPath(output_relative) / PurePosixPath(source_relative)
        ).with_suffix(".py").as_posix()
        output_path = (root / Path(output_relative_path)).resolve(strict=False)
        try:
            output_path.relative_to(root)
        except ValueError:
            outcomes.append(
                ProgramConversionOutcome(
                    program_id=program.id,
                    source_path=source_relative,
                    source_sha256=None,
                    output_path=output_relative_path,
                    status="failed",
                    error="Resolved generated-output path escapes the repository root.",
                )
            )
            continue
        if output_path.resolve(strict=False) == source_path:
            outcomes.append(
                ProgramConversionOutcome(
                    program_id=program.id,
                    source_path=source_relative,
                    source_sha256=None,
                    output_path=output_relative_path,
                    status="failed",
                    error="Generated output would overwrite the source program.",
                )
            )
            continue

        try:
            source_bytes = source_path.read_bytes()
        except OSError as error:
            outcomes.append(
                ProgramConversionOutcome(
                    program_id=program.id,
                    source_path=source_relative,
                    source_sha256=None,
                    output_path=output_relative_path,
                    status="failed",
                    error=f"Cannot read source program: {error}",
                )
            )
            continue

        source_sha256 = hashlib.sha256(source_bytes).hexdigest()

        def record_failure(error_message: str) -> None:
            conversion = Conversion.create(
                program.id,
                source_sha256,
                converter_version,
                status="failed",
                output_path=output_relative_path,
                error=error_message,
            )
            _upsert_conversion(project, conversion)
            outcomes.append(
                ProgramConversionOutcome(
                    program.id,
                    source_relative,
                    source_sha256,
                    output_relative_path,
                    "failed",
                    error=error_message,
                    manifest_recorded=True,
                )
            )

        source, _, decode_error = _decode_source(source_bytes)
        if source is None:
            record_failure(decode_error or "Could not decode SAS source.")
            continue
        if not source_relative.casefold().endswith(".sas"):
            record_failure("Registered source program must have a .sas extension.")
            continue
        try:
            result = converter.convert(source, sas_version=sas_version)
        except ConversionError as error:
            record_failure(str(error))
            continue

        try:
            _write_output(output_path, result.code, force=force)
        except OSError as error:
            record_failure(f"Cannot write generated output: {error}")
            continue

        conversion = Conversion.create(
            program.id,
            source_sha256,
            converter_version,
            status="succeeded",
            output_path=output_relative_path,
            warnings=result.warnings,
        )
        _upsert_conversion(project, conversion)
        outcomes.append(
            ProgramConversionOutcome(
                program.id,
                source_relative,
                source_sha256,
                output_relative_path,
                "succeeded",
                warnings=result.warnings,
                manifest_recorded=True,
            )
        )

    project.validate()
    return BatchConversionReport(
        project_id=project.project_id,
        project_name=project.name,
        repository_root=str(root),
        output_directory=output_relative,
        generated_at=datetime.now(timezone.utc).isoformat(),
        converter_version=converter_version,
        outcomes=tuple(outcomes),
    )


def _upsert_conversion(project: MigrationProject, conversion: Conversion) -> None:
    for index, existing in enumerate(project.conversions):
        if existing.id == conversion.id:
            project.conversions[index] = conversion
            return
    project.conversions.append(conversion)
