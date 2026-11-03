"""Human review workflow for generated PySpark conversions."""

from __future__ import annotations

import ast
import hashlib
import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from .project import Conversion, MigrationProject


REVIEW_REPORT_SCHEMA_VERSION = 1
REVIEW_DECISIONS = {"accepted", "corrected", "deferred", "rejected"}
ISSUE_KINDS = {"warning", "unsupported_statement", "conversion_error"}


class ReviewWorkflowError(ValueError):
    """Raised when a review report or decision is invalid."""


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReviewWorkflowError(f"{field} must be a non-empty string.")
    return value


def _relative_path(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReviewWorkflowError(f"{field} must be a non-empty project-relative path.")
    normalized = value.replace("\\", "/")
    posix_path = PurePosixPath(normalized)
    windows_path = PureWindowsPath(value)
    if (
        posix_path.is_absolute()
        or windows_path.is_absolute()
        or windows_path.drive
        or any(part in {"", ".", ".."} for part in normalized.split("/"))
    ):
        raise ReviewWorkflowError(f"{field} must be a normalized project-relative path.")
    return normalized


def _issue_id(
    conversion_id: str, kind: str, occurrence: int, message: str
) -> str:
    identity = json.dumps(
        (conversion_id, kind, occurrence, message), separators=(",", ":")
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    return f"issue:{digest}"


@dataclass(frozen=True)
class ReviewDecisionEvent:
    event_id: str
    timestamp: str
    reviewer: str
    decision: str
    notes: str | None = None
    corrected_output_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "reviewer": self.reviewer,
            "decision": self.decision,
            "notes": self.notes,
            "corrected_output_path": self.corrected_output_path,
        }


@dataclass(frozen=True)
class ReviewIssue:
    id: str
    conversion_id: str
    program_id: str
    source_path: str
    source_sha256: str
    converter_version: str
    output_path: str | None
    kind: str
    message: str
    decision: str = "pending"
    history: tuple[ReviewDecisionEvent, ...] = ()
    occurrence: int = 0

    @classmethod
    def create(
        cls,
        conversion: Conversion,
        *,
        source_path: str,
        kind: str,
        message: str,
        occurrence: int,
    ) -> ReviewIssue:
        return cls(
            id=_issue_id(conversion.id, kind, occurrence, message),
            conversion_id=conversion.id,
            program_id=conversion.program_id,
            source_path=source_path,
            source_sha256=conversion.source_sha256,
            converter_version=conversion.converter_version,
            output_path=conversion.output_path,
            kind=kind,
            message=message,
            occurrence=occurrence,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "conversion_id": self.conversion_id,
            "program_id": self.program_id,
            "source_path": self.source_path,
            "source_sha256": self.source_sha256,
            "converter_version": self.converter_version,
            "output_path": self.output_path,
            "kind": self.kind,
            "message": self.message,
            "decision": self.decision,
            "history": [event.to_dict() for event in self.history],
            "occurrence": self.occurrence,
        }


@dataclass(frozen=True)
class ReviewReport:
    project_id: str
    project_name: str
    repository_root: str
    generated_at: str
    issues: tuple[ReviewIssue, ...]
    schema_version: int = REVIEW_REPORT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        counts = {decision: 0 for decision in ("pending", *sorted(REVIEW_DECISIONS))}
        for issue in self.issues:
            counts[issue.decision] += 1
        return {
            "schema_version": self.schema_version,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "repository_root": self.repository_root,
            "generated_at": self.generated_at,
            "issues": [issue.to_dict() for issue in sorted(self.issues, key=lambda x: x.id)],
            "summary": {
                "issue_count": len(self.issues),
                "pending": counts["pending"],
                "accepted": counts["accepted"],
                "corrected": counts["corrected"],
                "deferred": counts["deferred"],
                "rejected": counts["rejected"],
            },
        }

    def save(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @classmethod
    def load(cls, path: str | Path) -> ReviewReport:
        source = Path(path)
        try:
            raw = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ReviewWorkflowError(
                f"Cannot read review report {str(source)!r}: {error}"
            ) from error
        return _review_report_from_dict(raw)


def _review_report_from_dict(raw: Any) -> ReviewReport:
    if not isinstance(raw, dict):
        raise ReviewWorkflowError("Review report must be a JSON object.")
    expected = {
        "schema_version",
        "project_id",
        "project_name",
        "repository_root",
        "generated_at",
        "issues",
        "summary",
    }
    if set(raw) != expected:
        raise ReviewWorkflowError("Review report has missing or unknown fields.")
    if type(raw["schema_version"]) is not int or raw["schema_version"] != REVIEW_REPORT_SCHEMA_VERSION:
        raise ReviewWorkflowError(
            f"Unsupported review report schema version: {raw['schema_version']!r}."
        )
    if not isinstance(raw["issues"], list):
        raise ReviewWorkflowError("Review report issues must be an array.")

    issues: list[ReviewIssue] = []
    seen_ids: set[str] = set()
    issue_fields = {
        "id",
        "conversion_id",
        "program_id",
        "source_path",
        "source_sha256",
        "converter_version",
        "output_path",
        "kind",
        "message",
        "decision",
        "history",
        "occurrence",
    }
    event_fields = {
        "event_id",
        "timestamp",
        "reviewer",
        "decision",
        "notes",
        "corrected_output_path",
    }
    for item in raw["issues"]:
        if not isinstance(item, dict) or set(item) != issue_fields:
            raise ReviewWorkflowError("Review issue has missing or unknown fields.")
        if type(item["occurrence"]) is not int or item["occurrence"] < 0:
            raise ReviewWorkflowError("Review issue occurrence must be a non-negative integer.")
        issue = ReviewIssue(
            id=_required_string(item["id"], "issue.id"),
            conversion_id=_required_string(item["conversion_id"], "issue.conversion_id"),
            program_id=_required_string(item["program_id"], "issue.program_id"),
            source_path=_relative_path(item["source_path"], "issue.source_path"),
            source_sha256=_required_string(item["source_sha256"], "issue.source_sha256"),
            converter_version=_required_string(
                item["converter_version"], "issue.converter_version"
            ),
            output_path=(
                _relative_path(item["output_path"], "issue.output_path")
                if item["output_path"] is not None
                else None
            ),
            kind=_required_string(item["kind"], "issue.kind"),
            message=_required_string(item["message"], "issue.message"),
            decision=_required_string(item["decision"], "issue.decision"),
            occurrence=item["occurrence"],
        )
        if issue.kind not in ISSUE_KINDS:
            raise ReviewWorkflowError(f"Invalid review issue kind: {issue.kind!r}.")
        if issue.decision not in {"pending", *REVIEW_DECISIONS}:
            raise ReviewWorkflowError(f"Invalid review decision: {issue.decision!r}.")
        if (
            len(issue.source_sha256) != 64
            or any(character not in "0123456789abcdef" for character in issue.source_sha256)
        ):
            raise ReviewWorkflowError("issue.source_sha256 must be a lowercase SHA-256.")
        if issue.id != _issue_id(
            issue.conversion_id, issue.kind, issue.occurrence, issue.message
        ):
            raise ReviewWorkflowError("Review issue ID does not match its contents.")
        if issue.id in seen_ids:
            raise ReviewWorkflowError("Duplicate review issue.")
        seen_ids.add(issue.id)

        if not isinstance(item["history"], list):
            raise ReviewWorkflowError("Review issue history must be an array.")
        history: list[ReviewDecisionEvent] = []
        event_ids: set[str] = set()
        for entry in item["history"]:
            if not isinstance(entry, dict) or set(entry) != event_fields:
                raise ReviewWorkflowError("Review decision event has missing or unknown fields.")
            if entry["decision"] not in REVIEW_DECISIONS:
                raise ReviewWorkflowError(
                    f"Invalid review decision: {entry['decision']!r}."
                )
            try:
                datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00"))
            except (AttributeError, TypeError, ValueError) as error:
                raise ReviewWorkflowError(
                    "Review event timestamp must be an ISO-8601 datetime."
                ) from error
            corrected_path = entry["corrected_output_path"]
            if corrected_path is not None:
                corrected_path = _relative_path(
                    corrected_path, "event.corrected_output_path"
                )
            if entry["decision"] == "corrected" and corrected_path is None:
                raise ReviewWorkflowError(
                    "Corrected review events require corrected_output_path."
                )
            if entry["decision"] != "corrected" and corrected_path is not None:
                raise ReviewWorkflowError(
                    "Only corrected review events may have corrected_output_path."
                )
            if entry["notes"] is not None and not isinstance(entry["notes"], str):
                raise ReviewWorkflowError("event.notes must be a string or null.")
            event = ReviewDecisionEvent(
                event_id=_required_string(entry["event_id"], "event.event_id"),
                timestamp=entry["timestamp"],
                reviewer=_required_string(entry["reviewer"], "event.reviewer"),
                decision=entry["decision"],
                notes=entry["notes"],
                corrected_output_path=corrected_path,
            )
            if event.event_id in event_ids:
                raise ReviewWorkflowError("Duplicate review decision event.")
            event_ids.add(event.event_id)
            history.append(event)
        expected_decision = history[-1].decision if history else "pending"
        if issue.decision != expected_decision:
            raise ReviewWorkflowError("Review issue decision does not match its history.")
        issues.append(
            ReviewIssue(
                **{
                    **issue.__dict__,
                    "history": tuple(history),
                }
            )
        )

    report = ReviewReport(
        project_id=_required_string(raw["project_id"], "report.project_id"),
        project_name=_required_string(raw["project_name"], "report.project_name"),
        repository_root=_required_string(raw["repository_root"], "report.repository_root"),
        generated_at=_required_string(raw["generated_at"], "report.generated_at"),
        issues=tuple(issues),
    )
    try:
        datetime.fromisoformat(report.generated_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ReviewWorkflowError("generated_at must be an ISO-8601 datetime.") from error
    expected_summary = report.to_dict()["summary"]
    if not isinstance(raw["summary"], dict) or raw["summary"] != expected_summary:
        raise ReviewWorkflowError("Review report summary does not match its issues.")
    return report


def _issues_from_project(project: MigrationProject) -> list[ReviewIssue]:
    programs = {program.id: program for program in project.programs}
    issues: list[ReviewIssue] = []
    for conversion in sorted(project.conversions, key=lambda item: item.id):
        program = programs[conversion.program_id]
        for index, warning in enumerate(conversion.warnings):
            issues.append(
                ReviewIssue.create(
                    conversion,
                    source_path=program.path,
                    kind="warning",
                    message=warning,
                    occurrence=index,
                )
            )
        if conversion.error:
            kind = (
                "unsupported_statement"
                if "unsupported" in conversion.error.casefold()
                else "conversion_error"
            )
            issues.append(
                ReviewIssue.create(
                    conversion,
                    source_path=program.path,
                    kind=kind,
                    message=conversion.error,
                    occurrence=0,
                )
            )
    return issues


def build_review_report(
    project: MigrationProject,
    repository_root: str | Path,
    *,
    existing_report: ReviewReport | None = None,
) -> ReviewReport:
    project.validate()
    root = Path(repository_root).resolve(strict=True)
    if not root.is_dir():
        raise ReviewWorkflowError(f"Repository root is not a directory: {str(root)!r}.")
    if existing_report is not None and existing_report.project_id != project.project_id:
        raise ReviewWorkflowError("Review report belongs to a different project.")
    by_id = (
        {issue.id: issue for issue in existing_report.issues}
        if existing_report is not None
        else {}
    )
    for issue in _issues_from_project(project):
        by_id.setdefault(issue.id, issue)
    return ReviewReport(
        project_id=project.project_id,
        project_name=project.name,
        repository_root=str(root),
        generated_at=datetime.now(timezone.utc).isoformat(),
        issues=tuple(by_id.values()),
    )


def record_review_decision(
    report: ReviewReport,
    issue_id: str,
    decision: str,
    *,
    reviewer: str,
    repository_root: str | Path,
    notes: str | None = None,
    corrected_code: str | Path | None = None,
) -> ReviewReport:
    if decision not in REVIEW_DECISIONS:
        raise ReviewWorkflowError(
            f"Decision must be one of: {', '.join(sorted(REVIEW_DECISIONS))}."
        )
    reviewer = _required_string(reviewer, "reviewer")
    if notes is not None and not isinstance(notes, str):
        raise ReviewWorkflowError("notes must be a string.")
    if (decision == "corrected") != (corrected_code is not None):
        raise ReviewWorkflowError(
            "Provide --corrected-code only for the corrected decision."
        )
    issue_index = next(
        (index for index, issue in enumerate(report.issues) if issue.id == issue_id),
        None,
    )
    if issue_index is None:
        raise ReviewWorkflowError(f"Review issue not found: {issue_id!r}.")

    issue = report.issues[issue_index]
    corrected_output_path: str | None = None
    if decision == "corrected":
        root = Path(repository_root).resolve(strict=True)
        code_path = Path(corrected_code).resolve(strict=True)
        try:
            code = code_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise ReviewWorkflowError(
                f"Cannot read corrected Python code {str(code_path)!r}: {error}"
            ) from error
        try:
            ast.parse(code, filename=str(code_path))
        except SyntaxError as error:
            raise ReviewWorkflowError(
                f"Corrected code is not valid Python: {error}"
            ) from error

        original = PurePosixPath(issue.output_path or issue.source_path)
        event_id = str(uuid.uuid4())
        corrected_relative = (
            PurePosixPath("reviewed")
            / original.parent
            / f"{original.stem}.review-{event_id}.py"
        ).as_posix()
        corrected_relative = _relative_path(
            corrected_relative, "corrected output path"
        )
        corrected_path = (root / Path(corrected_relative)).resolve(strict=False)
        try:
            corrected_path.relative_to(root)
        except ValueError:
            raise ReviewWorkflowError(
                "Corrected output path escapes the repository root."
            ) from None
        if corrected_path == code_path:
            raise ReviewWorkflowError(
                "Corrected output cannot replace the supplied corrected-code file."
            )
        corrected_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                dir=corrected_path.parent,
                prefix=f".{corrected_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary.write(code)
                temporary_path = Path(temporary.name)
            os.rename(temporary_path, corrected_path)
            temporary_path = None
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        corrected_output_path = corrected_relative
    else:
        event_id = str(uuid.uuid4())

    event = ReviewDecisionEvent(
        event_id=event_id,
        timestamp=datetime.now(timezone.utc).isoformat(),
        reviewer=reviewer,
        decision=decision,
        notes=notes,
        corrected_output_path=corrected_output_path,
    )
    updated_issue = ReviewIssue(
        **{
            **issue.__dict__,
            "decision": decision,
            "history": (*issue.history, event),
        }
    )
    updated_issues = list(report.issues)
    updated_issues[issue_index] = updated_issue
    return ReviewReport(
        project_id=report.project_id,
        project_name=report.project_name,
        repository_root=report.repository_root,
        generated_at=datetime.now(timezone.utc).isoformat(),
        issues=tuple(updated_issues),
    )
