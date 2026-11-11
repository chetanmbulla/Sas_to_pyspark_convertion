import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from sas2pyspark.cli import main
from sas2pyspark.phase1_analysis import Phase1AnalysisError, run_phase1_analysis


class Phase1AnalysisTests(unittest.TestCase):
    def setUp(self):
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        base = Path(self._temporary.name)
        self.root = base / "repo"
        job = self.root / "server-1" / "V" / "jobs" / "main.sas"
        job.parent.mkdir(parents=True)
        job.write_text(
            "data work.out;\n  length id 8 name $ 10;\n  set work.in;\nrun;\n",
            encoding="utf-8",
        )
        self.config = base / "pipeline.json"
        self.config.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "servers": {"DI": {"drives": {"V": "server-1/V"}}},
                    "entrypoints": [
                        {"server": "DI", "path": "server-1/V/jobs/main.sas"}
                    ],
                }
            ),
            encoding="utf-8",
        )
        self.out = base / "analysis"

    def test_writes_every_artifact_to_one_folder_without_touching_sources(self):
        before = sorted(path.name for path in self.root.rglob("*"))
        index = run_phase1_analysis(self.root, self.config, self.out, sas_version="9.4M4")

        for relative in index["artifacts"].values():
            self.assertTrue((self.out / relative).exists(), relative)
        self.assertEqual(index, json.loads((self.out / "phase1-index.json").read_text("utf-8")))
        self.assertEqual(index["summaries"]["pipeline"]["script_count"], 1)
        self.assertTrue((self.out / "code" / "pyspark").is_dir())
        self.assertTrue((self.out / "validation.json").is_file())
        self.assertTrue((self.out / "enterprise-guide.json").is_file())
        self.assertTrue((self.out / "coverage.json").is_file())
        self.assertTrue((self.out / "phase1-quality.json").is_file())
        self.assertTrue((self.out / "file-classification.json").is_file())
        self.assertIn("validation", index["summaries"])
        self.assertIn("coverage", index["summaries"])
        self.assertIn("quality", index["summaries"])
        self.assertIn("classification", index["summaries"])
        classification = json.loads(
            (self.out / "file-classification.json").read_text("utf-8")
        )
        self.assertEqual(classification["summary"]["file_count"], 1)
        self.assertEqual(len(classification["files"]), 1)
        quality = json.loads((self.out / "phase1-quality.json").read_text("utf-8"))
        self.assertEqual(quality["assessment"], "not_certified")
        self.assertEqual(before, sorted(path.name for path in self.root.rglob("*")))

    def test_refuses_non_empty_output_without_force(self):
        run_phase1_analysis(self.root, self.config, self.out)
        with self.assertRaisesRegex(Phase1AnalysisError, "not empty"):
            run_phase1_analysis(self.root, self.config, self.out)
        run_phase1_analysis(self.root, self.config, self.out, force=True)

    def test_rejects_output_inside_repository(self):
        with self.assertRaisesRegex(Phase1AnalysisError, "separate"):
            run_phase1_analysis(self.root, self.config, self.root / "out")

    def test_cli_analyze_reports_summary_and_index(self):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = main(
                [
                    "project", "analyze",
                    "--root", str(self.root),
                    "--config", str(self.config),
                    "--out", str(self.out),
                ]
            )
        self.assertEqual(code, 0)
        self.assertIn("Phase 1 analysis:", stdout.getvalue())
        self.assertTrue((self.out / "phase1-index.json").exists())


if __name__ == "__main__":
    unittest.main()
