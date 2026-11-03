"""Auditable approval gates for migration projects."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from .project import MigrationProject
from .review_workflow import ReviewReport


APPROVAL_REPORT_SCHEMA_VERSION = 1
APPROVAL_GATES = ("business", "data", "security", "platform")
APPROVAL_DECISIONS = ("approved", "changes_requested", "rejected")


class GovernanceError(ValueError):
    """Raised when approval policy or audit data is invalid."""


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GovernanceError(f"{field} must be a non-empty string.")
    return value


@dataclass(frozen=True)
class ApprovalEvent:
    event_id: str
    gate: str
    decision: str
    reviewer: str
    timestamp: str
    comments: str | None = None
    evidence_references: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "gate": self.gate,
            "decision": self.decision,
            "reviewer": self.reviewer,
            "timestamp": self.timestamp,
            "comments": self.comments,
            "evidence_references": list(self.evidence_references),
        }


@dataclass(frozen=True)
class ApprovalReport:
    project_id: str
    project_name: str
    repository_root: str
    generated_at: str
    required_gates: tuple[str, ...]
    critical_issue_ids: tuple[str, ...]
    events: tuple[ApprovalEvent, ...]
    schema_version: int = APPROVAL_REPORT_SCHEMA_VERSION

    def gate_statuses(self) -> dict[str, str]:
        statuses: dict[str, str] = {}
        for event in self.events:
            statuses[event.gate] = event.decision
        return statuses

    def to_dict(self) -> dict[str, Any]:
        statuses = self.gate_statuses()
        return {
            "schema_version": self.schema_version,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "repository_root": self.repository_root,
            "generated_at": self.generated_at,
            "policy": {
                "required_gates": list(self.required_gates),
                "critical_issue_ids": list(self.critical_issue_ids),
            },
            "gate_statuses": dict(sorted(statuses.items())),
            "events": [event.to_dict() for event in self.events],
            "summary": {
                "required_gate_count": len(self.required_gates),
                "approved_gate_count": sum(
                    statuses.get(gate) == "approved" for gate in self.required_gates
                ),
                "event_count": len(self.events),
            },
        }

    def save(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @classmethod
    def load(cls, path: str | Path) -> ApprovalReport:
        source = Path(path)
        try:
            raw = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise GovernanceError(
                f"Cannot read approval report {str(source)!r}: {error}"
            ) from error
        return _approval_report_from_dict(raw)


def _approval_report_from_dict(raw: Any) -> ApprovalReport:
    if not isinstance(raw, dict):
        raise GovernanceError("Approval report must be a JSON object.")
    expected = {
        "schema_version",
        "project_id",
        "project_name",
        "repository_root",
        "generated_at",
        "policy",
        "gate_statuses",
        "events",
        "summary",
    }
    if set(raw) != expected:
        raise GovernanceError("Approval report has missing or unknown fields.")
    if type(raw["schema_version"]) is not int or raw["schema_version"] != APPROVAL_REPORT_SCHEMA_VERSION:
        raise GovernanceError(
            f"Unsupported approval report schema version: {raw['schema_version']!r}."
        )
    policy = raw["policy"]
    if not isinstance(policy, dict) or set(policy) != {
        "required_gates",
        "critical_issue_ids",
    }:
        raise GovernanceError("Approval policy has missing or unknown fields.")
    required_gates = _normalized_gates(policy["required_gates"])
    critical_issue_ids = _normalized_issue_ids(policy["critical_issue_ids"])
    if not isinstance(raw["events"], list):
        raise GovernanceError("Approval events must be an array.")

    events: list[ApprovalEvent] = []
    seen_events: set[str] = set()
    event_fields = {
        "event_id",
        "gate",
        "decision",
        "reviewer",
        "timestamp",
        "comments",
        "evidence_references",
    }
    for item in raw["events"]:
        if not isinstance(item, dict) or set(item) != event_fields:
            raise GovernanceError("Approval event has missing or unknown fields.")
        event_id = _required_string(item["event_id"], "event.event_id")
        if event_id in seen_events:
            raise GovernanceError("Duplicate approval event ID.")
        seen_events.add(event_id)
        gate = _required_string(item["gate"], "event.gate")
        decision = _required_string(item["decision"], "event.decision")
        if gate not in APPROVAL_GATES:
            raise GovernanceError(f"Unknown approval gate: {gate!r}.")
        if decision not in APPROVAL_DECISIONS:
            raise GovernanceError(f"Invalid approval decision: {decision!r}.")
        timestamp = _required_string(item["timestamp"], "event.timestamp")
        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError as error:
            raise GovernanceError("Approval event timestamp must be ISO-8601.") from error
        comments = item["comments"]
        if comments is not None and not isinstance(comments, str):
            raise GovernanceError("event.comments must be a string or null.")
        references = item["evidence_references"]
        if not isinstance(references, list):
            raise GovernanceError("event.evidence_references must be an array.")
        events.append(
            ApprovalEvent(
                event_id=event_id,
                gate=gate,
                decision=decision,
                reviewer=_required_string(item["reviewer"], "event.reviewer"),
                timestamp=timestamp,
                comments=comments,
                evidence_references=tuple(
                    _required_string(reference, "evidence reference")
                    for reference in references
                ),
            )
        )

    report = ApprovalReport(
        project_id=_required_string(raw["project_id"], "report.project_id"),
        project_name=_required_string(raw["project_name"], "report.project_name"),
        repository_root=_required_string(raw["repository_root"], "report.repository_root"),
        generated_at=_required_string(raw["generated_at"], "report.generated_at"),
        required_gates=required_gates,
        critical_issue_ids=critical_issue_ids,
        events=tuple(events),
    )
    try:
        datetime.fromisoformat(report.generated_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise GovernanceError("generated_at must be an ISO-8601 datetime.") from error
    expected_statuses = report.gate_statuses()
    if raw["gate_statuses"] != dict(sorted(expected_statuses.items())):
        raise GovernanceError("Approval gate statuses do not match the audit events.")
    if raw["summary"] != report.to_dict()["summary"]:
        raise GovernanceError("Approval report summary does not match its events.")
    return report


def _normalized_gates(gates: Sequence[str]) -> tuple[str, ...]:
    if not isinstance(gates, (list, tuple)) or not gates:
        raise GovernanceError("At least one required approval gate must be configured.")
    normalized = tuple(gates)
    if any(not isinstance(gate, str) or gate not in APPROVAL_GATES for gate in normalized):
        raise GovernanceError(
            f"Required gates must be selected from: {', '.join(APPROVAL_GATES)}."
        )
    if len(set(normalized)) != len(normalized):
        raise GovernanceError("Required approval gates must be unique.")
    return tuple(gate for gate in APPROVAL_GATES if gate in normalized)


def _normalized_issue_ids(issue_ids: Sequence[str]) -> tuple[str, ...]:
    if not isinstance(issue_ids, (list, tuple)):
        raise GovernanceError("Critical issue IDs must be an array.")
    normalized = tuple(
        _required_string(issue_id, "critical issue ID") for issue_id in issue_ids
    )
    if len(set(normalized)) != len(normalized):
        raise GovernanceError("Critical issue IDs must be unique.")
    return tuple(sorted(normalized))


def create_approval_report(
    project: MigrationProject,
    repository_root: str | Path,
    review_report: ReviewReport,
    *,
    required_gates: Sequence[str] = APPROVAL_GATES,
    critical_issue_ids: Sequence[str] = (),
) -> ApprovalReport:
    project.validate()
    root = Path(repository_root).resolve(strict=True)
    if not root.is_dir():
        raise GovernanceError(f"Repository root is not a directory: {root}.")
    if review_report.project_id != project.project_id:
        raise GovernanceError("Review report belongs to a different project.")
    gates = _normalized_gates(required_gates)
    critical_ids = _normalized_issue_ids(critical_issue_ids)
    known_issue_ids = {issue.id for issue in review_report.issues}
    unknown_ids = set(critical_ids).difference(known_issue_ids)
    if unknown_ids:
        raise GovernanceError(
            "Critical issue IDs are not present in the review report: "
            + ", ".join(sorted(unknown_ids))
        )
    return ApprovalReport(
        project_id=project.project_id,
        project_name=project.name,
        repository_root=str(root),
        generated_at=datetime.now(timezone.utc).isoformat(),
        required_gates=gates,
        critical_issue_ids=critical_ids,
        events=(),
    )


def record_approval(
    report: ApprovalReport,
    gate: str,
    decision: str,
    *,
    reviewer: str,
    review_report: ReviewReport,
    comments: str | None = None,
    evidence_references: Sequence[str] = (),
    required_gates: Sequence[str] | None = None,
    critical_issue_ids: Sequence[str] | None = None,
) -> ApprovalReport:
    if gate not in APPROVAL_GATES:
        raise GovernanceError(f"Unknown approval gate: {gate!r}.")
    if decision not in APPROVAL_DECISIONS:
        raise GovernanceError(
            f"Decision must be one of: {', '.join(APPROVAL_DECISIONS)}."
        )
    if review_report.project_id != report.project_id:
        raise GovernanceError("Review report belongs to a different project.")
    reviewer = _required_string(reviewer, "reviewer")
    if comments is not None and not isinstance(comments, str):
        raise GovernanceError("comments must be a string.")
    references = tuple(
        _required_string(reference, "evidence reference")
        for reference in evidence_references
    )
    gates = _normalized_gates(
        report.required_gates if required_gates is None else required_gates
    )
    if gates != report.required_gates:
        raise GovernanceError(
            "Required approval gates cannot change within an existing report."
        )
    critical_ids = _normalized_issue_ids(
        report.critical_issue_ids
        if critical_issue_ids is None
        else critical_issue_ids
    )
    if critical_ids != report.critical_issue_ids:
        raise GovernanceError(
            "Critical issue policy cannot change within an existing report."
        )
    known_issue_ids = {issue.id for issue in review_report.issues}
    if set(critical_ids).difference(known_issue_ids):
        raise GovernanceError("Critical issue IDs must exist in the linked review report.")
    event = ApprovalEvent(
        event_id=str(uuid.uuid4()),
        gate=gate,
        decision=decision,
        reviewer=reviewer,
        timestamp=datetime.now(timezone.utc).isoformat(),
        comments=comments,
        evidence_references=references,
    )
    updated = ApprovalReport(
        project_id=report.project_id,
        project_name=report.project_name,
        repository_root=report.repository_root,
        generated_at=datetime.now(timezone.utc).isoformat(),
        required_gates=gates,
        critical_issue_ids=critical_ids,
        events=(*report.events, event),
    )
    _approval_report_from_dict(updated.to_dict())
    return updated


def evaluate_approval(
    report: ApprovalReport, review_report: ReviewReport
) -> tuple[str, tuple[str, ...]]:
    if review_report.project_id != report.project_id:
        raise GovernanceError("Review report belongs to a different project.")
    issues_by_id = {issue.id: issue for issue in review_report.issues}
    critical_ids = set(report.critical_issue_ids)
    critical_ids.update(
        issue.id
        for issue in review_report.issues
        if issue.kind in {"unsupported_statement", "conversion_error"}
    )
    unknown_ids = critical_ids.difference(issues_by_id)
    if unknown_ids:
        raise GovernanceError(
            "Approval policy references missing critical issues: "
            + ", ".join(sorted(unknown_ids))
        )

    blockers: list[str] = []
    gate_statuses = report.gate_statuses()
    for gate in report.required_gates:
        status = gate_statuses.get(gate)
        if status != "approved":
            blockers.append(
                f"Required {gate} approval is {status or 'missing'}."
            )
    for issue_id in sorted(critical_ids):
        issue = issues_by_id[issue_id]
        if issue.decision not in {"accepted", "corrected"}:
            blockers.append(
                f"Critical finding {issue_id} is unresolved ({issue.decision})."
            )
    if not blockers:
        return "approved", ()
    if any(
        gate_statuses.get(gate) in {"rejected", "changes_requested"}
        for gate in report.required_gates
    ) or any(
        issues_by_id[issue_id].decision in {"rejected", "deferred"}
        for issue_id in critical_ids
    ):
        return "blocked", tuple(blockers)
    return "pending", tuple(blockers)
