import hashlib
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from sas2pyspark.validation import CodeValidationError, validate_generated_code
from sas2pyspark.cli import main


class CodeValidationTests(unittest.TestCase):
    def test_validates_syntax_and_source_freshness_without_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_root = root / "sas"
            code_root = root / "code"
            source_root.mkdir()
            code_root.mkdir()
            source = source_root / "job.sas"
            source.write_text("data x; run;", encoding="utf-8")
            output = code_root / "job.py"
            output.write_text("df_x = df_input\n", encoding="utf-8")
            report = validate_generated_code(
                {
                    "schema_version": 1,
                    "outcomes": [
                        {
                            "status": "succeeded",
                            "source_path": "job.sas",
                            "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                            "output_path": "job.py",
                        },
                        {"status": "failed", "source_path": "other.sas"},
                    ],
                },
                source_root=source_root,
                code_root=code_root,
            )
        self.assertEqual(report["summary"], {
            "validated": 1,
            "passed": 1,
            "failed": 0,
            "conversion_failed": 1,
        })
        self.assertFalse(report["executed_generated_code"])

    def test_reports_syntax_and_stale_source_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_root = root / "sas"
            code_root = root / "code"
            source_root.mkdir()
            code_root.mkdir()
            (source_root / "job.sas").write_text("data x;", encoding="utf-8")
            (code_root / "job.py").write_text("def broken(:\n", encoding="utf-8")
            report = validate_generated_code(
                {
                    "schema_version": 1,
                    "outcomes": [
                        {
                            "status": "succeeded",
                            "source_path": "job.sas",
                            "source_sha256": "stale",
                            "output_path": "job.py",
                        }
                    ],
                },
                source_root=source_root,
                code_root=code_root,
            )
        self.assertEqual(report["summary"]["failed"], 1)
        self.assertEqual(len(report["results"][0]["issues"]), 2)

    def test_rejects_output_path_escape_and_bad_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_root = root / "sas"
            code_root = root / "code"
            source_root.mkdir()
            code_root.mkdir()
            with self.assertRaises(CodeValidationError):
                validate_generated_code(
                    {"schema_version": 2, "outcomes": []},
                    source_root=source_root,
                    code_root=code_root,
                )
            with self.assertRaises(CodeValidationError):
                validate_generated_code(
                    {
                        "schema_version": 1,
                        "outcomes": [
                            {
                                "status": "succeeded",
                                "source_path": "job.sas",
                                "output_path": "../outside.py",
                            }
                        ],
                    },
                    source_root=source_root,
                    code_root=code_root,
                )

    def test_cli_writes_static_validation_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_root = root / "sas"
            code_root = root / "code"
            source_root.mkdir()
            code_root.mkdir()
            source = source_root / "job.sas"
            source.write_text("data x;", encoding="utf-8")
            (code_root / "job.py").write_text("df_x = df_input\n", encoding="utf-8")
            conversion = root / "conversion.json"
            conversion.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "outcomes": [
                            {
                                "status": "succeeded",
                                "source_path": "job.sas",
                                "source_sha256": hashlib.sha256(
                                    source.read_bytes()
                                ).hexdigest(),
                                "output_path": "job.py",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report_path = root / "validation.json"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                result = main(
                    [
                        "project",
                        "validate-code",
                        "--conversion-report",
                        str(conversion),
                        "--source-root",
                        str(source_root),
                        "--code-root",
                        str(code_root),
                        "--report",
                        str(report_path),
                    ]
                )
            self.assertEqual(result, 0)
            self.assertEqual(
                json.loads(report_path.read_text(encoding="utf-8"))["summary"]["passed"],
                1,
            )
            self.assertIn("Statically validated 1 of 1", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
