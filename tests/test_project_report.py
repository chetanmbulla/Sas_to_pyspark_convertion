import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from sas2pyspark.project_report import build_project_report
from sas2pyspark.cli import main


class ProjectReportTests(unittest.TestCase):
    def setUp(self):
        self.pipeline = {
            "schema_version": 1,
            "scripts": [
                {
                    "path": "jobs/main.sas",
                    "server": "DI",
                    "status": "reachable",
                }
            ],
            "includes": [],
            "issues": [],
            "external_connections": [],
            "remote_references": [],
        }
        self.catalog = {
            "schema_version": 1,
            "systems": [],
            "issues": [],
            "resources": [
                {
                    "name": "work.output",
                    "kind": "sas_dataset",
                    "direction": "output",
                    "schema": {"status": "partial"},
                    "systems": [],
                    "evidence": [
                        {"source": "jobs/main.sas", "line": 1, "operation": "DATA"}
                    ],
                }
            ],
        }
        self.plan = {
            "schema_version": 1,
            "project_id": "project-1",
            "project_name": "Test",
            "programs": [
                {
                    "program_id": "program-1",
                    "path": "jobs/main.sas",
                    "complexity_score": 2,
                    "complexity": "low",
                }
            ],
            "datasets": [
                {
                    "dataset_id": "dataset-1",
                    "name": "work.output",
                }
            ],
            "dependencies": [
                {
                    "source_id": "program-1",
                    "target_id": "dataset-1",
                    "kind": "produces",
                }
            ],
            "unresolved_references": [],
        }
        self.conversion = {
            "schema_version": 1,
            "project_id": "project-1",
            "project_name": "Test",
            "summary": {"succeeded": 0, "failed": 1},
            "outcomes": [
                {
                    "source_path": "jobs/main.sas",
                    "status": "failed",
                    "warnings": [],
                }
            ],
        }

    def test_conversion_failure_changes_complexity_and_mermaid_links(self):
        report = build_project_report(
            self.pipeline, self.catalog, self.plan, self.conversion
        )

        self.assertEqual(report["complexity_score"], 12)
        self.assertEqual(report["complexity"], "high")
        self.assertIn("script0 --> resource0", report["markdown"])
        self.assertIn("Conversion adjustment", report["markdown"])
        self.assertIn("does not execute SAS", report["markdown"])

    def test_project_report_cli_writes_markdown(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = {}
            for name, value in (
                ("pipeline", self.pipeline),
                ("catalog", self.catalog),
                ("plan", self.plan),
                ("conversion", self.conversion),
            ):
                path = root / f"{name}.json"
                path.write_text(json.dumps(value), encoding="utf-8")
                paths[name] = path
            destination = root / "report.md"
            with contextlib.redirect_stdout(io.StringIO()):
                code = main(
                    [
                        "project",
                        "report",
                        "--pipeline-report",
                        str(paths["pipeline"]),
                        "--catalog-report",
                        str(paths["catalog"]),
                        "--plan-report",
                        str(paths["plan"]),
                        "--conversion-report",
                        str(paths["conversion"]),
                        "--output",
                        str(destination),
                    ]
                )

            self.assertEqual(code, 0)
            self.assertIn("# Test", destination.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
