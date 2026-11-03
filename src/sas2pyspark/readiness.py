"""Evidence-linked production readiness assessment."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .governance import (
    ApprovalReport,
    GovernanceError,
    _approval_report_from_dict,
    evaluate_approval,
)
from .project import MigrationProject
from .review_workflow import (
    ReviewReport,
    ReviewWorkflowError,
    _review_report_from_dict,
)


READINESS_REPORT_SCHEMA_VERSION = 1
EVIDENCE_MANIFEST_SCHEMA_VERSION = 1
EVIDENCE_CATEGORIES = (
    "inventory",
    "dependency",
    "conversion",
    "review",
    "execution",
    "reconciliation",
    "approval",
)
_SUMMARY_FIELDS = {
    "inventory": ("program_count", "asset_count", "issue_count"),
    "dependency": (
        "program_count",
        "dataset_count",
        "dependency_count",
        "unresolved_reference_count",
        "cycle_count",
        "blocked_program_count",
    ),
    "conversion": ("program_count", "succeeded", "failed"),
    "review": ("issue_count", "pending", "accepted", "corrected", "deferred", "rejected"),
    "execution": ("program_count", "succeeded", "failed", "timed_out"),
    "reconciliation": ("dataset_count", "pass", "fail", "inconclusive"),
}
_EXPECTED_SCHEMA_VERSIONS = {
    "inventory": 1,
    "dependency": 1,
    "conversion": 1,
    "review": 1,
    "execution": 1,
    "reconciliation": 1,
    "approval": 1,
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ReadinessError(ValueError):
    """Raised when readiness inputs or report destinations are invalid."""


@dataclass(frozen=True)
class EvidenceRecord:
    category: str
    path: str | None
    sha256: str | None
    status: str
    summary: Mapping[str, Any]
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "path": self.path,
            "sha256": self.sha256,
            "status": self.status,
            "summary": dict(self.summary),
            "error": self.error,
        }


@dataclass(frozen=True)
class ReadinessReport:
    project_id: str
    project_name: str
    repository_root: str
    generated_at: str
    status: str
    approval_status: str
    evidence_manifest_sha256: str
    evidence: tuple[EvidenceRecord, ...]
    blockers: tuple[Mapping[str, str], ...]
    schema_version: int = READINESS_REPORT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "repository_root": self.repository_root,
            "generated_at": self.generated_at,
            "status": self.status,
            "approval_status": self.approval_status,
            "evidence_manifest": {
                "schema_version": EVIDENCE_MANIFEST_SCHEMA_VERSION,
                "sha256": self.evidence_manifest_sha256,
                "items": [item.to_dict() for item in self.evidence],
            },
            "blockers": [dict(item) for item in self.blockers],
            "summary": {
                "evidence_count": len(self.evidence),
                "missing_evidence_count": sum(
                    item.status == "missing" for item in self.evidence
                ),
                "invalid_evidence_count": sum(
                    item.status == "invalid" for item in self.evidence
                ),
                "blocker_count": len(self.blockers),
            },
        }

    def save(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def _record_path(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return str(path)


def _evidence_paths(value: Sequence[str | Path] | str | Path | None) -> list[str | Path]:
    if value is None:
        return []
    if isinstance(value, (str, Path)):
        return [value]
    if not isinstance(value, (list, tuple)):
        raise ReadinessError("Evidence paths must be paths or a sequence of paths.")
    return list(value)


def _read_report(
    category: str,
    raw_path: str | Path | None,
    root: Path,
    project_id: str,
) -> tuple[EvidenceRecord, dict[str, Any] | None]:
    if raw_path is None:
        return EvidenceRecord(category, None, None, "missing", {}), None
    try:
        candidate = Path(raw_path)
        if not candidate.is_absolute():
            candidate = root / candidate
        if candidate.is_symlink():
            return (
                EvidenceRecord(
                    category,
                    str(candidate),
                    None,
                    "invalid",
                    {},
                    "Evidence path cannot be a symbolic link.",
                ),
                None,
            )
        path = candidate.resolve(strict=True)
        recorded_path = _record_path(path, root)
    except (OSError, RuntimeError) as error:
        return (
            EvidenceRecord(category, str(raw_path), None, "missing", {}, str(error)),
            None,
        )
    if path.is_symlink() or not path.is_file():
        return (
            EvidenceRecord(
                category,
                recorded_path,
                None,
                "invalid",
                {},
                "Evidence path must be a regular, non-symlink file.",
            ),
            None,
        )
    try:
        contents = path.read_bytes()
        raw = json.loads(contents)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        return (
            EvidenceRecord(category, recorded_path, None, "invalid", {}, str(error)),
            None,
        )
    digest = hashlib.sha256(contents).hexdigest()
    try:
        if not isinstance(raw, dict):
            raise ReadinessError("Report must be a JSON object.")
        if type(raw.get("schema_version")) is not int:
            raise ReadinessError("Report has no integer schema_version.")
        if raw["schema_version"] != _EXPECTED_SCHEMA_VERSIONS[category]:
            raise ReadinessError(
                f"Unsupported {category} evidence schema version "
                f"{raw['schema_version']!r}."
            )
        if category == "inventory":
            report_root = raw.get("root")
            if not isinstance(report_root, str) or Path(report_root).resolve() != root:
                raise ReadinessError("Inventory report belongs to a different repository root.")
            summary = raw.get("summary")
        else:
            if raw.get("project_id") != project_id:
                raise ReadinessError("Report belongs to a different migration project.")
            report_root = raw.get("repository_root")
            if not isinstance(report_root, str) or Path(report_root).resolve() != root:
                raise ReadinessError("Report belongs to a different repository root.")
            summary = raw.get("summary")
        if category == "approval":
            approval = ApprovalReport.load(path)
            summary = approval.to_dict()["summary"]
        if category == "review":
            review = ReviewReport.load(path)
            summary = review.to_dict()["summary"]
        if not isinstance(summary, dict):
            raise ReadinessError("Report summary must be an object.")
        report_summary = summary
        if category in _SUMMARY_FIELDS:
            for field in _SUMMARY_FIELDS[category]:
                value = report_summary.get(field)
                if type(value) is not int or value < 0:
                    raise ReadinessError(
                        f"Report summary field {field!r} must be a non-negative integer."
                    )
        if category == "inventory" and not isinstance(raw.get("programs"), list):
            raise ReadinessError("Inventory report has no programs array.")
        if category == "inventory" and not isinstance(raw.get("assets"), list):
            raise ReadinessError("Inventory report has no assets array.")
        if category == "inventory" and not isinstance(raw.get("issues"), list):
            raise ReadinessError("Inventory report has no issues array.")
        if category == "dependency" and not isinstance(raw.get("programs"), list):
            raise ReadinessError("Dependency report has no programs array.")
        if category == "dependency" and not isinstance(raw.get("cycles"), list):
            raise ReadinessError("Dependency report has no cycles array.")
        if category == "dependency" and not isinstance(raw.get("unresolved_references"), list):
            raise ReadinessError("Dependency report has no unresolved_references array.")
        if category == "dependency" and not isinstance(raw.get("dependencies"), list):
            raise ReadinessError("Dependency report has no dependencies array.")
        if category == "conversion" and not isinstance(raw.get("outcomes"), list):
            raise ReadinessError("Conversion report has no outcomes array.")
        if category == "review" and not isinstance(raw.get("issues"), list):
            raise ReadinessError("Review report has no issues array.")
        if category == "execution" and not isinstance(raw.get("results"), list):
            raise ReadinessError("Execution report has no results array.")
        if category == "reconciliation" and not isinstance(raw.get("datasets"), list):
            raise ReadinessError("Reconciliation report has no datasets array.")
        if category == "inventory":
            for item in raw["programs"]:
                if (
                    not isinstance(item, dict)
                    or not isinstance(item.get("path"), str)
                    or not isinstance(item.get("sha256"), str)
                    or not _SHA256.fullmatch(item["sha256"])
                ):
                    raise ReadinessError(
                        "Inventory program entries require a path and SHA-256."
                    )
            if len(raw["programs"]) != report_summary["program_count"]:
                raise ReadinessError("Inventory program count does not match its summary.")
            if len(raw["assets"]) != report_summary["asset_count"]:
                raise ReadinessError("Inventory asset count does not match its summary.")
            if len(raw["issues"]) != report_summary["issue_count"]:
                raise ReadinessError("Inventory issue count does not match its summary.")
        elif category == "dependency":
            if len(raw["programs"]) != report_summary["program_count"]:
                raise ReadinessError("Dependency program count does not match its summary.")
            if len(raw["cycles"]) != report_summary["cycle_count"]:
                raise ReadinessError("Dependency cycle count does not match its summary.")
            if len(raw["unresolved_references"]) != report_summary["unresolved_reference_count"]:
                raise ReadinessError("Unresolved-reference count does not match its summary.")
            if len(raw["dependencies"]) != report_summary["dependency_count"]:
                raise ReadinessError("Dependency edge count does not match its summary.")
        elif category == "conversion":
            outcomes = raw["outcomes"]
            if len(outcomes) != report_summary["program_count"]:
                raise ReadinessError("Conversion count does not match its summary.")
            outcome_statuses = [
                item.get("status") for item in outcomes if isinstance(item, dict)
            ]
            if (
                len(outcome_statuses) != len(outcomes)
                or outcome_statuses.count("succeeded") != report_summary["succeeded"]
                or outcome_statuses.count("failed") != report_summary["failed"]
                or any(status not in {"succeeded", "failed"} for status in outcome_statuses)
            ):
                raise ReadinessError("Conversion outcomes do not match their summary.")
        elif category == "execution":
            results = raw["results"]
            if len(results) != report_summary["program_count"]:
                raise ReadinessError("Execution count does not match its summary.")
            result_statuses = [
                item.get("status") for item in results if isinstance(item, dict)
            ]
            if (
                len(result_statuses) != len(results)
                or result_statuses.count("succeeded") != report_summary["succeeded"]
                or result_statuses.count("failed") != report_summary["failed"]
                or result_statuses.count("timed_out") != report_summary["timed_out"]
                or any(
                    status not in {"succeeded", "failed", "timed_out"}
                    for status in result_statuses
                )
            ):
                raise ReadinessError("Execution results do not match their summary.")
        elif category == "reconciliation":
            datasets = raw["datasets"]
            if len(datasets) != report_summary["dataset_count"]:
                raise ReadinessError("Reconciliation count does not match its summary.")
            dataset_statuses = [item.get("status") for item in datasets if isinstance(item, dict)]
            if (
                len(dataset_statuses) != len(datasets)
                or dataset_statuses.count("pass") != report_summary["pass"]
                or dataset_statuses.count("fail") != report_summary["fail"]
                or dataset_statuses.count("inconclusive") != report_summary["inconclusive"]
                or any(
                    status not in {"pass", "fail", "inconclusive"}
                    for status in dataset_statuses
                )
            ):
                raise ReadinessError("Reconciliation results do not match their summary.")
        return (
            EvidenceRecord(category, recorded_path, digest, "present", report_summary),
            raw,
        )
    except (
        GovernanceError,
        ReviewWorkflowError,
        ReadinessError,
        OSError,
        TypeError,
        ValueError,
    ) as error:
        return (
            EvidenceRecord(category, recorded_path, digest, "invalid", {}, str(error)),
            None,
        )


def build_readiness_report(
    project: MigrationProject,
    repository_root: str | Path,
    *,
    evidence_paths: Mapping[
        str, Sequence[str | Path] | str | Path | None
    ],
) -> ReadinessReport:
    """Assess a project using only explicitly linked report files."""
    project.validate()
    try:
        root = Path(repository_root).resolve(strict=True)
    except OSError as error:
        raise ReadinessError(
            f"Cannot access repository root {str(repository_root)!r}: {error}"
        ) from error
    if not root.is_dir():
        raise ReadinessError(f"Repository root is not a directory: {root}.")
    unknown_categories = set(evidence_paths).difference(EVIDENCE_CATEGORIES)
    if unknown_categories:
        raise ReadinessError(
            "Unknown evidence categories: " + ", ".join(sorted(unknown_categories))
        )

    records: list[EvidenceRecord] = []
    loaded: dict[str, list[dict[str, Any]]] = {
        category: [] for category in EVIDENCE_CATEGORIES
    }
    blockers: list[dict[str, str]] = []
    for category in EVIDENCE_CATEGORIES:
        paths = _evidence_paths(evidence_paths.get(category))
        if category not in {"execution", "reconciliation"} and len(paths) > 1:
            raise ReadinessError(
                f"Only one {category} evidence report may be linked."
            )
        if not paths:
            paths = [None]
        for raw_path in paths:
            record, raw = _read_report(category, raw_path, root, project.project_id)
            records.append(record)
            if record.status == "missing":
                blockers.append(
                    {
                        "code": "missing_evidence",
                        "category": category,
                        "message": f"Required {category} evidence is missing.",
                    }
                )
            elif record.status == "invalid":
                blockers.append(
                    {
                        "code": "invalid_evidence",
                        "category": category,
                        "message": record.error or f"{category} evidence is invalid.",
                    }
                )
            elif raw is not None:
                loaded[category].append(raw)

    def add_blocker(code: str, category: str, message: str) -> None:
        blockers.append({"code": code, "category": category, "message": message})

    inventory = loaded["inventory"]
    inventory_hashes: dict[str, str] = {}
    programs_by_id = {program.id: program for program in project.programs}
    if inventory:
        inventory_hashes = {
            item["path"].casefold(): item["sha256"]
            for item in inventory[0]["programs"]
        }
        inventory_programs = {
            item.get("path", "").casefold()
            for item in inventory[0].get("programs", [])
            if isinstance(item, dict) and isinstance(item.get("path"), str)
        }
        project_programs = {program.path.casefold() for program in project.programs}
        if project_programs != inventory_programs:
            add_blocker(
                "inventory_incomplete",
                "inventory",
                "Repository inventory and registered SAS program paths do not match.",
            )
        if inventory[0]["summary"]["issue_count"]:
            add_blocker(
                "inventory_issues",
                "inventory",
                "Repository inventory contains unreadable files or discovery issues.",
            )
    if not project.programs:
        add_blocker(
            "empty_project",
            "inventory",
            "No SAS programs are registered in the migration project.",
        )

    for report in loaded["dependency"]:
        summary = report["summary"]
        if summary["program_count"] != len(project.programs):
            add_blocker(
                "dependency_coverage",
                "dependency",
                "Dependency evidence does not cover every registered SAS program.",
            )
        dependency_program_ids: set[str] = set()
        for item in report["programs"]:
            if not isinstance(item, dict):
                add_blocker(
                    "dependency_program_mismatch",
                    "dependency",
                    "Dependency evidence contains a malformed program entry.",
                )
                continue
            program_id = item.get("program_id")
            program = (
                programs_by_id.get(program_id)
                if isinstance(program_id, str)
                else None
            )
            if isinstance(program_id, str):
                dependency_program_ids.add(program_id)
            if program is None or item.get("path") != program.path:
                add_blocker(
                    "dependency_program_mismatch",
                    "dependency",
                    "Dependency evidence contains an unknown program or path.",
                )
                continue
            if item.get("source_sha256") != inventory_hashes.get(program.path.casefold()):
                add_blocker(
                    "stale_dependency_evidence",
                    "dependency",
                    f"Dependency analysis is stale for {program.path}.",
                )
        if dependency_program_ids != set(programs_by_id):
            add_blocker(
                "dependency_coverage",
                "dependency",
                "Dependency evidence does not identify every registered program exactly once.",
            )
        if summary["unresolved_reference_count"] or summary["cycle_count"] or summary["blocked_program_count"]:
            add_blocker(
                "dependency_blockers",
                "dependency",
                "Dependency analysis has unresolved references, cycles, or blocked programs.",
            )

    for report in loaded["conversion"]:
        outcomes = report["outcomes"]
        represented: set[str] = set()
        for outcome in outcomes:
            if not isinstance(outcome, dict) or not isinstance(outcome.get("program_id"), str):
                add_blocker("invalid_conversion_outcome", "conversion", "Conversion outcome is malformed.")
                continue
            program_id = outcome["program_id"]
            represented.add(program_id)
            program = programs_by_id.get(program_id)
            if (
                program is None
                or outcome.get("source_path") != program.path
                or outcome.get("source_sha256")
                != inventory_hashes.get(program.path.casefold())
            ):
                add_blocker(
                    "stale_conversion_evidence",
                    "conversion",
                    f"Conversion evidence does not match the inventoried source for "
                    f"program {program_id}.",
                )
            if outcome.get("status") != "succeeded":
                add_blocker(
                    "conversion_failed",
                    "conversion",
                    f"Conversion for program {program_id} did not succeed.",
                )
        expected_programs = {program.id for program in project.programs}
        if represented != expected_programs:
            add_blocker(
                "conversion_coverage",
                "conversion",
                "Conversion evidence does not contain exactly the registered programs.",
            )

    review_reports = [
        _review_report_from_dict(raw) for raw in loaded["review"]
    ]
    approval_reports = [
        _approval_report_from_dict(raw) for raw in loaded["approval"]
    ]

    critical_issues: dict[str, Any] = {}
    for review in review_reports:
        for issue in review.issues:
            if issue.kind in {"unsupported_statement", "conversion_error"}:
                critical_issues[issue.id] = issue
    for approval in approval_reports:
        for issue_id in approval.critical_issue_ids:
            matching = next(
                (
                    issue
                    for review in review_reports
                    for issue in review.issues
                    if issue.id == issue_id
                ),
                None,
            )
            if matching is None:
                add_blocker(
                    "critical_finding_missing",
                    "approval",
                    f"Approval policy references missing critical finding {issue_id}.",
                )
            else:
                critical_issues[issue_id] = matching
    for issue_id, issue in sorted(critical_issues.items()):
        if issue.decision not in {"accepted", "corrected"}:
            add_blocker(
                "critical_finding_unresolved",
                "review",
                f"Critical finding {issue_id} remains unresolved ({issue.decision}).",
            )

    execution_successes: set[str] = set()
    for report in loaded["execution"]:
        for result in report["results"]:
            if not isinstance(result, dict):
                add_blocker("invalid_execution_result", "execution", "Execution result is malformed.")
                continue
            program_id = result.get("program_id")
            if program_id not in {program.id for program in project.programs}:
                add_blocker(
                    "execution_unknown_program",
                    "execution",
                    f"Execution report references unregistered program "
                    f"{program_id if isinstance(program_id, str) else '<unknown>'}.",
                )
            program = (
                programs_by_id.get(program_id)
                if isinstance(program_id, str)
                else None
            )
            if (
                program is None
                or result.get("source_path") != program.path
                or result.get("source_sha256")
                != inventory_hashes.get(program.path.casefold())
            ):
                add_blocker(
                    "stale_execution_evidence",
                    "execution",
                    f"Execution evidence does not match the inventoried source for "
                    f"program {program_id}.",
                )
            if result.get("status") == "succeeded":
                if isinstance(program_id, str):
                    execution_successes.add(program_id)
            else:
                add_blocker(
                    "execution_failed",
                    "execution",
                    f"Execution for program {result.get('program_id', '<unknown>')} "
                    f"did not succeed ({result.get('status', 'unknown')}).",
                )
    missing_execution = {program.id for program in project.programs}.difference(
        execution_successes
    )
    if loaded["execution"] and missing_execution:
        add_blocker(
            "execution_coverage",
            "execution",
            "No successful execution evidence for program(s): "
            + ", ".join(sorted(missing_execution)),
        )

    for report in loaded["reconciliation"]:
        summary = report["summary"]
        if summary["fail"] or summary["inconclusive"]:
            add_blocker(
                "reconciliation_not_passed",
                "reconciliation",
                "One or more reconciliation comparisons failed or were inconclusive.",
            )
        if summary["dataset_count"] == 0:
            add_blocker(
                "reconciliation_empty",
                "reconciliation",
                "Reconciliation report contains no dataset comparisons.",
            )

    approval_status = "missing" if not approval_reports else "pending"
    for approval in approval_reports:
        if not review_reports:
            add_blocker(
                "approval_requires_review",
                "approval",
                "Approval cannot be evaluated without its linked review report.",
            )
            continue
        try:
            status, approval_blockers = evaluate_approval(approval, review_reports[0])
        except GovernanceError as error:
            add_blocker("approval_invalid", "approval", str(error))
            approval_status = "blocked"
            continue
        approval_status = status
        for message in approval_blockers:
            add_blocker("approval_incomplete", "approval", message)
        if status != "approved" and not approval_blockers:
            add_blocker(
                "approval_incomplete",
                "approval",
                f"Migration approval status is {status}.",
            )

    ordered_records = sorted(
        records, key=lambda record: (record.category, record.path or "")
    )
    manifest_entries = [record.to_dict() for record in ordered_records]
    manifest_payload = {
        "project_id": project.project_id,
        "repository_root": str(root),
        "items": manifest_entries,
    }
    manifest_digest = hashlib.sha256(
        json.dumps(
            manifest_payload, sort_keys=True, separators=(",", ":"), default=str
        ).encode("utf-8")
    ).hexdigest()
    unique_blockers = {
        (item["code"], item["category"], item["message"]): item for item in blockers
    }
    final_blockers = tuple(
        unique_blockers[key] for key in sorted(unique_blockers)
    )
    return ReadinessReport(
        project_id=project.project_id,
        project_name=project.name,
        repository_root=str(root),
        generated_at=datetime.now(timezone.utc).isoformat(),
        status="ready" if not final_blockers else "blocked",
        approval_status=approval_status,
        evidence_manifest_sha256=manifest_digest,
        evidence=tuple(ordered_records),
        blockers=final_blockers,
    )
