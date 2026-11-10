import unittest
import contextlib
import io
import json
import tempfile
from pathlib import Path

from sas2pyspark.coverage_report import CoverageReportError, build_coverage_report
from sas2pyspark.cli import main


class CoverageReportTests(unittest.TestCase):
    def test_groups_observed_blockers_without_claiming_general_coverage(self):
        report = build_coverage_report(
            {
                "schema_version": 1,
                "outcomes": [
                    {"status": "succeeded", "source_path": "ok.sas"},
                    {
                        "status": "failed",
                        "source_path": "remote.sas",
                        "error": "RSUBMIT is unsupported",
                    },
                    {
                        "status": "failed",
                        "source_path": "macro.sas",
                        "error": "Undefined macro variable &x",
                    },
                    {"status": "pending", "source_path": "pending.sas"},
                ],
            }
        )
        self.assertEqual(report["summary"]["observed_conversion_rate"], 0.25)
        self.assertEqual(report["summary"]["pending"], 1)
        self.assertFalse(report["general_sas_coverage_claim"])
        self.assertEqual(
            [item["category"] for item in report["blockers"]],
            ["macro_or_generated_code", "remote_execution_boundary"],
        )

    def test_rejects_bad_conversion_report(self):
        with self.assertRaises(CoverageReportError):
            build_coverage_report({"schema_version": 2, "outcomes": []})
        with self.assertRaises(CoverageReportError):
            build_coverage_report(
                {"schema_version": 1, "outcomes": [{"status": "unknown"}]}
            )

    def test_cli_writes_observed_coverage_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "conversion.json"
            source.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "outcomes": [
                            {"status": "succeeded", "source_path": "one.sas"}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            target = root / "coverage.json"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                result = main(
                    [
                        "project",
                        "coverage",
                        "--conversion-report",
                        str(source),
                        "--output",
                        str(target),
                    ]
                )
            self.assertEqual(result, 0)
            report = json.loads(target.read_text(encoding="utf-8"))
            self.assertFalse(report["general_sas_coverage_claim"])
            self.assertIn("Observed conversion rate", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
