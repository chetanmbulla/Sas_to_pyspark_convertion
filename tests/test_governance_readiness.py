import json
import tempfile
import unittest
from pathlib import Path

from sas2pyspark import (
    ApprovalReport,
    Conversion,
    GovernanceError,
    MigrationProject,
    build_readiness_report,
    build_review_report,
    create_approval_report,
    evaluate_approval,
    record_approval,
    record_review_decision,
)
from sas2pyspark.cli import main
from sas2pyspark.governance import APPROVAL_GATES


class GovernanceAndReadinessTests(unittest.TestCase):
    def _project(self, root: Path, *, critical: bool = False):
        project = MigrationProject.create("Governed migration")
        program = project.add_program("sas/program.sas")
        if critical:
            project.add_conversion(
                Conversion.create(
                    program.id,
                    "a" * 64,
                    "0.1.0",
                    status="failed",
                    error="Unsupported statement at line 3.",
                )
            )
        review = build_review_report(project, root)
        return project, program, review

    def test_approval_is_pending_until_required_gates_and_critical_findings_resolve(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project, _, review = self._project(root, critical=True)
            critical_issue = review.issues[0]
            approvals = create_approval_report(project, root, review)

            for gate in APPROVAL_GATES:
                approvals = record_approval(
                    approvals,
                    gate,
                    "approved",
                    reviewer=f"{gate}-reviewer",
                    review_report=review,
                    comments=f"{gate} review complete",
                    evidence_references=(f"evidence/{gate}.pdf",),
                )

            status, blockers = evaluate_approval(approvals, review)
            self.assertEqual(status, "pending")
            self.assertTrue(any(critical_issue.id in blocker for blocker in blockers))
            self.assertEqual(approvals.gate_statuses(), {gate: "approved" for gate in APPROVAL_GATES})

            review = record_review_decision(
                review,
                critical_issue.id,
                "accepted",
                reviewer="migration-owner",
                repository_root=root,
                notes="Accepted after validating the generated implementation.",
            )
            status, blockers = evaluate_approval(approvals, review)
            self.assertEqual((status, blockers), ("approved", ()))

            report_path = root / "approvals.json"
            approvals.save(report_path)
            loaded = ApprovalReport.load(report_path)
            self.assertEqual(len(loaded.events), 4)
            self.assertEqual(loaded.events[0].reviewer, "business-reviewer")
            self.assertEqual(
                loaded.events[0].evidence_references, ("evidence/business.pdf",)
            )
            with self.assertRaisesRegex(GovernanceError, "cannot change"):
                record_approval(
                    loaded,
                    "business",
                    "approved",
                    reviewer="replacement-reviewer",
                    review_report=review,
                    required_gates=("business",),
                    critical_issue_ids=("new-critical-issue",),
                )

    def test_approval_cli_records_configured_gate_and_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "project.json"
            project, _, review = self._project(root)
            project.save(manifest)
            review.save(root / "project.review.json")

            status = main(
                [
                    "project",
                    "approval",
                    str(manifest),
                    "--root",
                    str(root),
                    "--gate",
                    "business",
                    "--decision",
                    "approved",
                    "--reviewer",
                    "business-owner",
                    "--required-gate",
                    "business",
                    "--evidence",
                    "ticket:ABC-123",
                ]
            )

            self.assertEqual(status, 0)
            report = ApprovalReport.load(root / "project.approvals.json")
            self.assertEqual(report.required_gates, ("business",))
            self.assertEqual(report.gate_statuses(), {"business": "approved"})

    def test_readiness_lists_missing_evidence_and_saves_a_versioned_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project, _, _ = self._project(root)
            report = build_readiness_report(project, root, evidence_paths={})

            self.assertEqual(report.status, "blocked")
            self.assertEqual(report.approval_status, "missing")
            self.assertEqual(len(report.evidence), 7)
            self.assertEqual(
                {item["code"] for item in report.blockers},
                {"missing_evidence"},
            )
            destination = root / "readiness.json"
            report.save(destination)
            saved = json.loads(destination.read_text(encoding="utf-8"))
            self.assertEqual(saved["schema_version"], 1)
            self.assertEqual(saved["evidence_manifest"]["schema_version"], 1)
            self.assertTrue(saved["evidence_manifest"]["sha256"])

    def test_readiness_cli_returns_nonzero_and_writes_blockers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = MigrationProject.create("Incomplete migration")
            project.add_program("sas/program.sas")
            manifest = root / "project.json"
            project.save(manifest)
            destination = root / "readiness.json"

            status = main(
                [
                    "project",
                    "readiness",
                    str(manifest),
                    "--root",
                    str(root),
                    "--report",
                    str(destination),
                ]
            )

            self.assertEqual(status, 1)
            report = json.loads(destination.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "blocked")
            self.assertEqual(report["summary"]["missing_evidence_count"], 7)

    def test_readiness_becomes_ready_only_with_complete_passing_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project, program, review = self._project(root)
            source_hash = "a" * 64
            review_path = root / "review.json"
            review.save(review_path)
            approval = create_approval_report(
                project, root, review, required_gates=("business",)
            )
            approval = record_approval(
                approval,
                "business",
                "approved",
                reviewer="business-owner",
                review_report=review,
                evidence_references=("ticket:ABC-123",),
            )
            approval_path = root / "approval.json"
            approval.save(approval_path)

            reports = {
                "inventory": {
                    "schema_version": 1,
                    "root": str(root.resolve()),
                    "programs": [
                        {"path": "sas/program.sas", "sha256": source_hash}
                    ],
                    "assets": [],
                    "issues": [],
                    "summary": {
                        "program_count": 1,
                        "asset_count": 0,
                        "issue_count": 0,
                    },
                },
                "dependency": {
                    "schema_version": 1,
                    "project_id": project.project_id,
                    "repository_root": str(root.resolve()),
                    "programs": [
                        {
                            "program_id": program.id,
                            "path": program.path,
                            "source_sha256": source_hash,
                        }
                    ],
                    "dependencies": [],
                    "unresolved_references": [],
                    "cycles": [],
                    "summary": {
                        "program_count": 1,
                        "dataset_count": 0,
                        "dependency_count": 0,
                        "unresolved_reference_count": 0,
                        "cycle_count": 0,
                        "blocked_program_count": 0,
                    },
                },
                "conversion": {
                    "schema_version": 1,
                    "project_id": project.project_id,
                    "repository_root": str(root.resolve()),
                    "outcomes": [
                        {
                            "program_id": program.id,
                            "source_path": program.path,
                            "source_sha256": source_hash,
                            "status": "succeeded",
                        }
                    ],
                    "summary": {"program_count": 1, "succeeded": 1, "failed": 0},
                },
                "execution": {
                    "schema_version": 1,
                    "project_id": project.project_id,
                    "repository_root": str(root.resolve()),
                    "results": [
                        {
                            "program_id": program.id,
                            "source_path": program.path,
                            "source_sha256": source_hash,
                            "status": "succeeded",
                        }
                    ],
                    "summary": {
                        "program_count": 1,
                        "succeeded": 1,
                        "failed": 0,
                        "timed_out": 0,
                    },
                },
                "reconciliation": {
                    "schema_version": 1,
                    "project_id": project.project_id,
                    "repository_root": str(root.resolve()),
                    "datasets": [{"dataset": "work.output", "status": "pass"}],
                    "summary": {
                        "dataset_count": 1,
                        "pass": 1,
                        "fail": 0,
                        "inconclusive": 0,
                    },
                },
            }
            evidence_paths = {"review": str(review_path), "approval": str(approval_path)}
            for category, data in reports.items():
                path = root / f"{category}.json"
                path.write_text(json.dumps(data), encoding="utf-8")
                evidence_paths[category] = str(path)

            report = build_readiness_report(
                project, root, evidence_paths=evidence_paths
            )

            self.assertEqual(report.status, "ready", report.blockers)
            self.assertEqual(report.approval_status, "approved")
            self.assertEqual(len(report.evidence), 7)
            self.assertTrue(all(item.sha256 for item in report.evidence))
            repeated = build_readiness_report(
                project, root, evidence_paths=evidence_paths
            )
            self.assertEqual(
                report.evidence_manifest_sha256,
                repeated.evidence_manifest_sha256,
            )

            reports["reconciliation"]["summary"]["inconclusive"] = 1
            reports["reconciliation"]["summary"]["pass"] = 0
            reports["reconciliation"]["datasets"][0]["status"] = "inconclusive"
            (root / "reconciliation.json").write_text(
                json.dumps(reports["reconciliation"]), encoding="utf-8"
            )
            blocked = build_readiness_report(
                project, root, evidence_paths=evidence_paths
            )
            self.assertEqual(blocked.status, "blocked")
            self.assertTrue(
                any(item["code"] == "reconciliation_not_passed" for item in blocked.blockers)
            )
            reports["execution"]["results"][0]["source_sha256"] = "b" * 64
            (root / "execution.json").write_text(
                json.dumps(reports["execution"]), encoding="utf-8"
            )
            stale = build_readiness_report(
                project, root, evidence_paths=evidence_paths
            )
            self.assertTrue(
                any(item["code"] == "stale_execution_evidence" for item in stale.blockers)
            )


if __name__ == "__main__":
    unittest.main()
