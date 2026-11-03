"""Versioned migration-project metadata and persistence."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import PurePosixPath, PureWindowsPath
from pathlib import Path
from typing import Any


PROJECT_SCHEMA_VERSION = 1
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PROJECT_FIELDS = {
    "schema_version",
    "project_id",
    "name",
    "created_at",
    "programs",
    "datasets",
    "dependencies",
    "conversions",
    "reviews",
}


class ProjectError(ValueError):
    """Raised when a migration-project document is invalid or unreadable."""


def _stable_id(kind: str, *parts: str) -> str:
    value = json.dumps(("sas2pyspark", kind, *parts), separators=(",", ":"))
    return f"{kind}:{uuid.uuid5(uuid.NAMESPACE_URL, value)}"


def _relative_path(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProjectError(f"{field_name} must be a non-empty relative path.")
    normalized = value.replace("\\", "/")
    windows_path = PureWindowsPath(value)
    posix_path = PurePosixPath(normalized)
    if (
        windows_path.is_absolute()
        or windows_path.drive
        or posix_path.is_absolute()
        or any(part in {"", ".", ".."} for part in normalized.split("/"))
    ):
        raise ProjectError(f"{field_name} must be a normalized project-relative path.")
    return normalized


def _required_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProjectError(f"{field_name} must be a non-empty string.")
    return value


def _check_exact_fields(value: dict[str, Any], expected: set[str], entity: str) -> None:
    missing = expected.difference(value)
    extra = set(value).difference(expected)
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing {', '.join(sorted(missing))}")
        if extra:
            details.append(f"unknown {', '.join(sorted(extra))}")
        raise ProjectError(f"Invalid {entity} fields: {'; '.join(details)}.")


@dataclass(frozen=True)
class Program:
    id: str
    path: str

    @classmethod
    def create(cls, path: str) -> Program:
        normalized = _relative_path(path, "program.path")
        return cls(_stable_id("program", normalized.casefold()), normalized)


@dataclass(frozen=True)
class Dataset:
    id: str
    name: str

    @classmethod
    def create(cls, name: str) -> Dataset:
        clean_name = _required_string(name, "dataset.name")
        return cls(_stable_id("dataset", clean_name.casefold()), clean_name)


@dataclass(frozen=True)
class Dependency:
    id: str
    source_id: str
    target_id: str
    kind: str

    @classmethod
    def create(cls, source_id: str, target_id: str, kind: str) -> Dependency:
        source = _required_string(source_id, "dependency.source_id")
        target = _required_string(target_id, "dependency.target_id")
        dependency_kind = _required_string(kind, "dependency.kind").casefold()
        return cls(
            _stable_id("dependency", source, target, dependency_kind),
            source,
            target,
            dependency_kind,
        )


@dataclass(frozen=True)
class Conversion:
    id: str
    program_id: str
    source_sha256: str
    converter_version: str
    status: str = "pending"
    output_path: str | None = None
    warnings: tuple[str, ...] = ()
    error: str | None = None

    @classmethod
    def create(
        cls,
        program_id: str,
        source_sha256: str,
        converter_version: str,
        *,
        status: str = "pending",
        output_path: str | None = None,
        warnings: tuple[str, ...] = (),
        error: str | None = None,
    ) -> Conversion:
        program = _required_string(program_id, "conversion.program_id")
        digest = source_sha256.lower() if isinstance(source_sha256, str) else ""
        version = _required_string(converter_version, "conversion.converter_version")
        normalized_output = (
            _relative_path(output_path, "conversion.output_path")
            if output_path is not None
            else None
        )
        return cls(
            _stable_id("conversion", program, digest, version),
            program,
            digest,
            version,
            status,
            normalized_output,
            tuple(warnings),
            error,
        )


@dataclass(frozen=True)
class Review:
    id: str
    program_id: str
    status: str = "pending"
    reviewer: str | None = None
    updated_at: str | None = None
    notes: str | None = None

    @classmethod
    def create(
        cls,
        program_id: str,
        *,
        status: str = "pending",
        reviewer: str | None = None,
        updated_at: str | None = None,
        notes: str | None = None,
    ) -> Review:
        program = _required_string(program_id, "review.program_id")
        return cls(
            _stable_id("review", program),
            program,
            status,
            reviewer,
            updated_at,
            notes,
        )


@dataclass
class MigrationProject:
    project_id: str
    name: str
    created_at: str
    programs: list[Program] = field(default_factory=list)
    datasets: list[Dataset] = field(default_factory=list)
    dependencies: list[Dependency] = field(default_factory=list)
    conversions: list[Conversion] = field(default_factory=list)
    reviews: list[Review] = field(default_factory=list)
    schema_version: int = PROJECT_SCHEMA_VERSION

    @classmethod
    def create(cls, name: str) -> MigrationProject:
        clean_name = _required_string(name, "project.name")
        return cls(
            project_id=str(uuid.uuid4()),
            name=clean_name,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    def add_program(self, path: str) -> Program:
        program = Program.create(path)
        if any(item.id == program.id for item in self.programs):
            raise ProjectError(f"Program path is already registered: {program.path!r}.")
        self.programs.append(program)
        return program

    def add_dataset(self, name: str) -> Dataset:
        dataset = Dataset.create(name)
        if any(item.id == dataset.id for item in self.datasets):
            raise ProjectError(f"Dataset is already registered: {dataset.name!r}.")
        self.datasets.append(dataset)
        return dataset

    def add_dependency(self, source_id: str, target_id: str, kind: str) -> Dependency:
        dependency = Dependency.create(source_id, target_id, kind)
        if any(item.id == dependency.id for item in self.dependencies):
            raise ProjectError("Dependency is already registered.")
        self.dependencies.append(dependency)
        return dependency

    def add_conversion(self, conversion: Conversion) -> None:
        if any(item.id == conversion.id for item in self.conversions):
            raise ProjectError("Conversion result is already registered.")
        self.conversions.append(conversion)

    def set_review(self, review: Review) -> None:
        for index, existing in enumerate(self.reviews):
            if existing.program_id == review.program_id:
                self.reviews[index] = review
                return
        self.reviews.append(review)

    def validate(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != PROJECT_SCHEMA_VERSION:
            raise ProjectError(
                f"Unsupported project schema version: {self.schema_version!r}."
            )
        try:
            uuid.UUID(self.project_id)
        except (ValueError, TypeError, AttributeError) as error:
            raise ProjectError("project_id must be a UUID.") from error
        _required_string(self.name, "project.name")
        _parse_datetime(self.created_at, "project.created_at")

        program_ids: set[str] = set()
        program_paths: set[str] = set()
        for program in self.programs:
            _required_string(program.id, "program.id")
            normalized = _relative_path(program.path, "program.path")
            if normalized != program.path:
                raise ProjectError(f"Program path is not normalized: {program.path!r}.")
            if program.id != Program.create(program.path).id:
                raise ProjectError(f"Program ID does not match path {program.path!r}.")
            if program.id in program_ids or normalized.casefold() in program_paths:
                raise ProjectError(f"Duplicate program ID or path: {program.path!r}.")
            program_ids.add(program.id)
            program_paths.add(normalized.casefold())

        dataset_ids: set[str] = set()
        for dataset in self.datasets:
            _required_string(dataset.id, "dataset.id")
            _required_string(dataset.name, "dataset.name")
            if dataset.id != Dataset.create(dataset.name).id:
                raise ProjectError(f"Dataset ID does not match name {dataset.name!r}.")
            if dataset.id in dataset_ids:
                raise ProjectError(f"Duplicate dataset: {dataset.name!r}.")
            dataset_ids.add(dataset.id)

        entity_ids = program_ids | dataset_ids
        dependency_ids: set[str] = set()
        for dependency in self.dependencies:
            _required_string(dependency.id, "dependency.id")
            _required_string(dependency.source_id, "dependency.source_id")
            _required_string(dependency.target_id, "dependency.target_id")
            dependency_kind = _required_string(dependency.kind, "dependency.kind")
            if dependency_kind != dependency_kind.casefold():
                raise ProjectError("dependency.kind must be lowercase.")
            if dependency.id != Dependency.create(
                dependency.source_id, dependency.target_id, dependency.kind
            ).id:
                raise ProjectError("Dependency ID does not match its endpoints and kind.")
            if dependency.source_id not in entity_ids or dependency.target_id not in entity_ids:
                raise ProjectError("Dependency references an unknown program or dataset.")
            if dependency.source_id == dependency.target_id:
                raise ProjectError("Dependency cannot reference itself.")
            if dependency.id in dependency_ids:
                raise ProjectError("Duplicate dependency.")
            dependency_ids.add(dependency.id)

        conversion_ids: set[str] = set()
        for conversion in self.conversions:
            _required_string(conversion.id, "conversion.id")
            _required_string(conversion.program_id, "conversion.program_id")
            _required_string(conversion.converter_version, "conversion.converter_version")
            _required_string(conversion.status, "conversion.status")
            if conversion.program_id not in program_ids:
                raise ProjectError("Conversion references an unknown program.")
            if not isinstance(conversion.source_sha256, str) or not _SHA256.fullmatch(
                conversion.source_sha256
            ):
                raise ProjectError("conversion.source_sha256 must be a 64-character SHA-256.")
            if conversion.id != Conversion.create(
                conversion.program_id,
                conversion.source_sha256,
                conversion.converter_version,
            ).id:
                raise ProjectError("Conversion ID does not match its source and version.")
            if conversion.status not in {"pending", "succeeded", "failed"}:
                raise ProjectError(f"Invalid conversion status: {conversion.status!r}.")
            if conversion.output_path is not None:
                if (
                    _relative_path(conversion.output_path, "conversion.output_path")
                    != conversion.output_path
                ):
                    raise ProjectError("conversion.output_path must be normalized.")
            if conversion.status == "succeeded" and conversion.output_path is None:
                raise ProjectError("Successful conversion requires output_path.")
            if conversion.status == "failed" and not conversion.error:
                raise ProjectError("Failed conversion requires an error.")
            if conversion.error is not None and not isinstance(conversion.error, str):
                raise ProjectError("conversion.error must be a string or null.")
            if not isinstance(conversion.warnings, (tuple, list)) or any(
                not isinstance(item, str) for item in conversion.warnings
            ):
                raise ProjectError("Conversion warnings must be strings.")
            if conversion.id in conversion_ids:
                raise ProjectError("Duplicate conversion result.")
            conversion_ids.add(conversion.id)

        review_ids: set[str] = set()
        reviewed_programs: set[str] = set()
        for review in self.reviews:
            _required_string(review.id, "review.id")
            _required_string(review.program_id, "review.program_id")
            _required_string(review.status, "review.status")
            if review.program_id not in program_ids:
                raise ProjectError("Review references an unknown program.")
            if review.id != Review.create(review.program_id).id:
                raise ProjectError("Review ID does not match its program.")
            if review.status not in {
                "pending",
                "approved",
                "changes_requested",
                "rejected",
            }:
                raise ProjectError(f"Invalid review status: {review.status!r}.")
            if review.updated_at is not None:
                _parse_datetime(review.updated_at, "review.updated_at")
            if review.reviewer is not None and not isinstance(review.reviewer, str):
                raise ProjectError("review.reviewer must be a string or null.")
            if review.notes is not None and not isinstance(review.notes, str):
                raise ProjectError("review.notes must be a string or null.")
            if review.id in review_ids or review.program_id in reviewed_programs:
                raise ProjectError("Only one current review status is allowed per program.")
            review_ids.add(review.id)
            reviewed_programs.add(review.program_id)

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "project_id": self.project_id,
            "name": self.name,
            "created_at": self.created_at,
            "programs": [
                {"id": item.id, "path": item.path}
                for item in sorted(self.programs, key=lambda value: value.id)
            ],
            "datasets": [
                {"id": item.id, "name": item.name}
                for item in sorted(self.datasets, key=lambda value: value.id)
            ],
            "dependencies": [
                {
                    "id": item.id,
                    "source_id": item.source_id,
                    "target_id": item.target_id,
                    "kind": item.kind,
                }
                for item in sorted(self.dependencies, key=lambda value: value.id)
            ],
            "conversions": [
                {
                    "id": item.id,
                    "program_id": item.program_id,
                    "source_sha256": item.source_sha256,
                    "converter_version": item.converter_version,
                    "status": item.status,
                    "output_path": item.output_path,
                    "warnings": list(item.warnings),
                    "error": item.error,
                }
                for item in sorted(self.conversions, key=lambda value: value.id)
            ],
            "reviews": [
                {
                    "id": item.id,
                    "program_id": item.program_id,
                    "status": item.status,
                    "reviewer": item.reviewer,
                    "updated_at": item.updated_at,
                    "notes": item.notes,
                }
                for item in sorted(self.reviews, key=lambda value: value.id)
            ],
        }

    def save(self, path: str | Path) -> None:
        destination = Path(path)
        document = json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n"
        destination.write_text(document, encoding="utf-8")

    @classmethod
    def load(cls, path: str | Path) -> MigrationProject:
        source_path = Path(path)
        try:
            raw = json.loads(source_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ProjectError(f"Cannot read project file {str(source_path)!r}: {error}") from error
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: Any) -> MigrationProject:
        if not isinstance(raw, dict):
            raise ProjectError("Project document must be a JSON object.")
        _check_exact_fields(raw, _PROJECT_FIELDS, "project")
        if type(raw["schema_version"]) is not int:
            raise ProjectError("schema_version must be an integer.")
        project = cls(
            schema_version=raw["schema_version"],
            project_id=raw["project_id"],
            name=raw["name"],
            created_at=raw["created_at"],
            programs=[
                Program(**_entity(item, {"id", "path"}, "program"))
                for item in _list(raw["programs"], "programs")
            ],
            datasets=[
                Dataset(**_entity(item, {"id", "name"}, "dataset"))
                for item in _list(raw["datasets"], "datasets")
            ],
            dependencies=[
                Dependency(**_entity(
                    item, {"id", "source_id", "target_id", "kind"}, "dependency"
                ))
                for item in _list(raw["dependencies"], "dependencies")
            ],
            conversions=[
                _conversion_from_dict(item)
                for item in _list(raw["conversions"], "conversions")
            ],
            reviews=[
                Review(**_entity(
                    item,
                    {"id", "program_id", "status", "reviewer", "updated_at", "notes"},
                    "review",
                ))
                for item in _list(raw["reviews"], "reviews")
            ],
        )
        project.validate()
        return project


def _parse_datetime(value: Any, field_name: str) -> None:
    if not isinstance(value, str):
        raise ProjectError(f"{field_name} must be an ISO-8601 datetime.")
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ProjectError(f"{field_name} must be an ISO-8601 datetime.") from error


def _list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ProjectError(f"{field_name} must be an array.")
    return value


def _entity(value: Any, fields: set[str], entity: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProjectError(f"Each {entity} must be a JSON object.")
    _check_exact_fields(value, fields, entity)
    return value


def _conversion_from_dict(value: Any) -> Conversion:
    item = _entity(
        value,
        {
            "id",
            "program_id",
            "source_sha256",
            "converter_version",
            "status",
            "output_path",
            "warnings",
            "error",
        },
        "conversion",
    )
    warnings = item["warnings"]
    if not isinstance(warnings, list):
        raise ProjectError("conversion.warnings must be an array.")
    return Conversion(
        id=item["id"],
        program_id=item["program_id"],
        source_sha256=item["source_sha256"],
        converter_version=item["converter_version"],
        status=item["status"],
        output_path=item["output_path"],
        warnings=tuple(warnings),
        error=item["error"],
    )
