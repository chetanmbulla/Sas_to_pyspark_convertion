import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sas2pyspark import Conversion, ExecutionWorkflowError, MigrationProject, run_project
from sas2pyspark.cli import main


_PYSPARK_STUB = {
    "__init__.py": '__version__ = "test-stub"\n',
    "sql/__init__.py": """\
class DataFrame:
    def __init__(self, path=None):
        self.path = path

    def createOrReplaceTempView(self, name):
        return None


class Reader:
    def option(self, name, value):
        return self

    def csv(self, path):
        return DataFrame(path)

    def parquet(self, path):
        return DataFrame(path)

    def json(self, path):
        return DataFrame(path)


class SparkSession:
    class Builder:
        def master(self, value):
            return self

        def appName(self, value):
            return self

        def config(self, key, value):
            return self

        def getOrCreate(self):
            return SparkSession()

    builder = Builder()

    def __init__(self):
        self.read = Reader()

    def stop(self):
        return None
""",
}


class ExecutionWorkflowTests(unittest.TestCase):
    def _project(self, root, programs):
        project = MigrationProject.create("Execution test")
        for source_path, code in programs.items():
            source = root / source_path
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("data output; run;", encoding="utf-8")
            output_path = Path("pyspark") / Path(source_path).with_suffix(".py")
            output = root / output_path
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(code, encoding="utf-8")
            program = project.add_program(source_path)
            source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
            project.add_conversion(
                Conversion.create(
                    program.id,
                    source_hash,
                    "0.1.0",
                    status="succeeded",
                    output_path=output_path.as_posix(),
                )
            )
        return project

    def _write_pyspark_stub(self, root):
        stub_root = root / "pyspark_stub"
        for relative, content in _PYSPARK_STUB.items():
            path = stub_root / "pyspark" / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        return stub_root

    def test_runs_selected_programs_with_inputs_logs_and_separate_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_path = root / "data" / "sample.csv"
            source_path.parent.mkdir()
            source_path.write_text("id,name\n1,Ada\n", encoding="utf-8")
            project = self._project(
                root,
                {
                    "sas/good.sas": (
                        "print(df_people.path)\n"
                        "with open('result.txt', 'w') as stream:\n"
                        "    stream.write('ok')\n"
                    ),
                    "sas/fails.sas": "raise RuntimeError('execution failed')\n",
                    "sas/slow.sas": "import time\ntime.sleep(10)\n",
                },
            )
            stub_root = self._write_pyspark_stub(root)
            good = next(item for item in project.programs if item.path == "sas/good.sas")

            with patch.dict(os.environ, {"PYTHONPATH": str(stub_root)}):
                report = run_project(
                    project,
                    root,
                    program_selectors=[good.path, "sas/fails.sas", "sas/slow.sas"],
                    input_mappings=["WORK.people=data/sample.csv"],
                    timeout_seconds=2,
                    master="local[1]",
                    spark_conf={"spark.sql.shuffle.partitions": "2"},
                    environment={"MIGRATION_MODE": "test"},
                )

            data = report.to_dict()
            results = {item["source_path"]: item for item in data["results"]}
            report_path = root / report.run_directory / "run.json"
            saved_report = json.loads(report_path.read_text(encoding="utf-8"))
            good_result = results["sas/good.sas"]
            good_stdout = (root / good_result["stdout_path"]).read_text(
                encoding="utf-8"
            )
            failed_stderr = (root / results["sas/fails.sas"]["stderr_path"]).read_text(
                encoding="utf-8"
            )
            snapshot_exists = (
                root / good_result["execution_snapshot_path"]
            ).is_file()

        self.assertEqual(
            [results[path]["status"] for path in ("sas/good.sas", "sas/fails.sas", "sas/slow.sas")],
            ["succeeded", "failed", "timed_out"],
        )
        self.assertEqual(results["sas/fails.sas"]["exit_code"], 1)
        self.assertTrue(results["sas/slow.sas"]["timed_out"])
        self.assertTrue(results["sas/slow.sas"]["error"].startswith("Execution exceeded timeout"))
        self.assertEqual(data["summary"], {
            "program_count": 3,
            "succeeded": 1,
            "failed": 1,
            "timed_out": 1,
        })
        self.assertEqual(data["inputs"][0]["dataset"], "WORK.people")
        self.assertTrue(data["inputs"][0]["sha256"])
        self.assertTrue(good_result["source_sha256"])
        self.assertTrue(good_result["generated_code_sha256"])
        self.assertTrue(snapshot_exists)
        self.assertEqual(data["master"], "local[1]")
        self.assertEqual(data["spark_conf"]["spark.sql.shuffle.partitions"], "2")
        self.assertEqual(data["environment"]["environment_overrides"], ["MIGRATION_MODE"])
        self.assertIn("sample.csv", good_stdout)
        self.assertIn("RuntimeError: execution failed", failed_stderr)
        self.assertTrue(any(path.endswith("result.txt") for path in good_result["output_paths"]))
        self.assertEqual(saved_report["run_id"], report.run_id)
        self.assertTrue(all(result["command"] for result in data["results"]))

    def test_rejects_stale_conversions_and_paths_outside_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = self._project(
                root, {"program.sas": "print('not run')\n"}
            )
            (root / "program.sas").write_text("data changed; run;", encoding="utf-8")

            with self.assertRaisesRegex(ExecutionWorkflowError, "normalized relative"):
                run_project(
                    project,
                    root,
                    program_selectors=["program.sas"],
                    input_mappings=["WORK.people=../outside.csv"],
                )

            report = run_project(
                project, root, program_selectors=["program.sas"]
            )

        self.assertEqual(report.results[0].status, "failed")
        error = report.results[0].error
        assert error is not None
        self.assertIn("No successful conversion matches", error)
        self.assertIsNone(report.results[0].exit_code)

    def test_run_cli_returns_failure_status_and_persists_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory, "repository")
            root.mkdir()
            project = self._project(
                root, {"program.sas": "raise ValueError('bad run')\n"}
            )
            stub_root = self._write_pyspark_stub(root)
            manifest = Path(directory, "migration.project.json")
            project.save(manifest)
            program = project.programs[0]

            with patch.dict(os.environ, {"PYTHONPATH": str(stub_root)}):
                status = main(
                    [
                        "project",
                        "run",
                        str(manifest),
                        "--root",
                        str(root),
                        "--program",
                        program.path,
                        "--timeout",
                        "10",
                    ]
                )

            run_directories = list((root / "runs").iterdir())
            saved = json.loads(
                (run_directories[0] / "run.json").read_text(encoding="utf-8")
            )
            stderr_path = root / saved["results"][0]["stderr_path"]
            stderr_exists = stderr_path.is_file()
            stderr_content = stderr_path.read_text(encoding="utf-8")

        self.assertEqual(status, 1)
        self.assertEqual(saved["summary"]["failed"], 1)
        self.assertTrue(stderr_exists)
        self.assertIn("ValueError: bad run", stderr_content)

    def test_rejects_invalid_timeout_and_unselected_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = self._project(root, {"program.sas": "print('ok')\n"})

            with self.assertRaisesRegex(ExecutionWorkflowError, "positive"):
                run_project(
                    project,
                    root,
                    program_selectors=["program.sas"],
                    timeout_seconds=float("nan"),
                )
            with self.assertRaisesRegex(ExecutionWorkflowError, "at least one"):
                run_project(project, root, program_selectors=[])

    def test_sensitive_spark_settings_are_referenced_not_persisted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = self._project(
                root, {"program.sas": "print('safe')\n"}
            )
            stub_root = self._write_pyspark_stub(root)

            with self.assertRaisesRegex(ExecutionWorkflowError, "must reference"):
                run_project(
                    project,
                    root,
                    program_selectors=["program.sas"],
                    spark_conf={
                        "spark.hadoop.fs.s3a.secret.key": "do-not-store-this"
                    },
                )

            with patch.dict(os.environ, {"PYTHONPATH": str(stub_root)}):
                report = run_project(
                    project,
                    root,
                    program_selectors=["program.sas"],
                    spark_conf={
                        "spark.hadoop.fs.s3a.secret.key": "${S3_SECRET}"
                    },
                    environment={"S3_SECRET": "do-not-store-this"},
                )
            run_root = root / report.run_directory
            config_text = next(run_root.glob("program-*/execution-config.json")).read_text(
                encoding="utf-8"
            )
            report_text = (run_root / "run.json").read_text(encoding="utf-8")

        self.assertEqual(report.results[0].status, "succeeded")
        self.assertIn("${S3_SECRET}", config_text)
        self.assertNotIn("do-not-store-this", config_text)
        self.assertNotIn("do-not-store-this", report_text)


if __name__ == "__main__":
    unittest.main()
