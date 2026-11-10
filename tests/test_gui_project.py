import importlib.util
import json
import shutil
import tempfile
import time
import unittest
import zipfile
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

from sas2pyspark.gui_project import (
    GuiProjectError,
    analyze_zip,
    extract_project_zip,
    regenerate_synthetic_data,
    run_converted_programs,
)


def make_zip(files):
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buffer.getvalue()


@contextmanager
def spark_test_directory():
    directory = tempfile.mkdtemp()
    try:
        yield directory
    finally:
        for attempt in range(20):
            try:
                shutil.rmtree(directory)
                break
            except PermissionError:
                if attempt == 19:
                    raise
                time.sleep(1)


class GuiProjectTests(unittest.TestCase):
    def test_extract_rejects_traversal_and_duplicate_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, files in enumerate(
                [
                    {"../outside.sas": "data a; run;"},
                    {"C:\\escape.sas": "data a; run;"},
                    {"A.sas": "a", "a.sas": "b"},
                    {"CON.sas": "data a; run;"},
                    {"path/aux.txt": "data a; run;"},
                    {"COM\u00b9.sas": "data a; run;"},
                    {"path/file.sas:stream": "data a; run;"},
                    {"path./file.sas": "data a; run;"},
                ]
            ):
                with self.subTest(files=files), self.assertRaises(GuiProjectError):
                    extract_project_zip(make_zip(files), root / str(index))
            self.assertFalse((root / "outside.sas").exists())

    def test_extract_accepts_nonreserved_names(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "project"
            files = {"console.sas": "data a; run;", "com10.sas": "data b; run;"}
            self.assertEqual(extract_project_zip(make_zip(files), destination), list(files))
            self.assertTrue((destination / "console.sas").is_file())

    def test_zip_analysis_and_synthetic_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            real_mkdtemp = tempfile.mkdtemp

            def temporary_workspace(prefix, **kwargs):
                return real_mkdtemp(prefix=prefix, dir=directory, **kwargs)

            with patch("sas2pyspark.gui_project.tempfile.mkdtemp", temporary_workspace):
                result = analyze_zip(
                    make_zip({"main.sas": "data result; set source; run;"}),
                    sas_version="9.4M4",
                )
            output = Path(result["output_root"])
            conversion = json.loads((output / "conversion.json").read_text("utf-8"))
            self.assertEqual(conversion["summary"]["succeeded"], 1)
            self.assertEqual(result["configuration"], "all_scripts_fallback")
            self.assertEqual(result["index"]["summaries"]["inventory"]["program_count"], 1)
            synthetic = regenerate_synthetic_data(output, rows=2, seed=3)
            self.assertEqual(synthetic["rows_per_dataset"], 2)

    def test_explicit_config_preserves_archive_relative_paths(self):
        config = {
            "schema_version": 1,
            "servers": {"DI": {"drives": {"V": "drive"}}},
            "entrypoints": [{"server": "DI", "path": "drive/main.sas"}],
        }
        with tempfile.TemporaryDirectory() as directory:
            real_mkdtemp = tempfile.mkdtemp

            def temporary_workspace(prefix, **kwargs):
                return real_mkdtemp(prefix=prefix, dir=directory, **kwargs)

            with patch("sas2pyspark.gui_project.tempfile.mkdtemp", temporary_workspace):
                result = analyze_zip(
                    make_zip({"drive/main.sas": "data result; set source; run;"}),
                    config=json.dumps(config).encode("utf-8"),
                )
            self.assertEqual(result["configuration"], "uploaded")
            self.assertEqual(result["index"]["summaries"]["pipeline"]["reachable_script_count"], 1)
            self.assertTrue((Path(result["source_root"]) / "drive" / "main.sas").is_file())

    @unittest.skipUnless(
        importlib.util.find_spec("pyspark") is not None and shutil.which("java"),
        "PySpark and Java are required",
    )
    def test_execution_uses_synthetic_input_and_reports_failure(self):
        with spark_test_directory() as directory:
            real_mkdtemp = tempfile.mkdtemp

            def temporary_workspace(prefix, **kwargs):
                return real_mkdtemp(prefix=prefix, dir=directory, **kwargs)

            with patch("sas2pyspark.gui_project.tempfile.mkdtemp", temporary_workspace):
                result = analyze_zip(
                    make_zip(
                        {
                            "ok.sas": "data result; set source; run;",
                            "missing.sas": "data missing_out; set missing_input; run;",
                        }
                    )
                )
            output = Path(result["output_root"])
            fixtures = output / "fixture"
            fixtures.mkdir()
            (fixtures / "source.csv").write_text("id\n1\n", encoding="utf-8")
            manifest = {
                "output_directory": str(fixtures),
                "datasets": [
                    {"dataset": "work.source", "status": "generated", "path": "source.csv"}
                ],
            }
            report = run_converted_programs(
                result, manifest, ["work.source"], timeout_seconds=120
            )
            self.assertEqual(report["summary"]["succeeded"], 1, report)
            self.assertEqual(report["summary"]["failed"], 1, report)
            by_path = {item["source_path"]: item for item in report["results"]}
            self.assertEqual(by_path["input/ok.sas"]["status"], "succeeded")
            failure = by_path["input/missing.sas"]
            self.assertIn("Worker exited", failure["error"])
            log = Path(report["execution_root"]) / failure["stderr_path"]
            self.assertIn("df_missing_input", log.read_text(encoding="utf-8", errors="replace"))


@unittest.skipUnless(
    importlib.util.find_spec("streamlit") is not None,
    "Streamlit is required",
)
class GuiProjectPageTests(unittest.TestCase):
    def test_project_page_renders(self):
        from streamlit.testing.v1 import AppTest

        page = Path(__file__).resolve().parents[1] / "app" / "pages" / "1_Project_Analysis.py"
        app = AppTest.from_file(str(page)).run()
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.title[0].value, "SAS project analysis")
        self.assertTrue(any("ZIP" in getattr(item, "label", "") for item in app.get("file_uploader")))

    def test_completed_results_and_stale_inputs_render(self):
        from streamlit.testing.v1 import AppTest

        with tempfile.TemporaryDirectory() as directory:
            real_mkdtemp = tempfile.mkdtemp

            def temporary_workspace(prefix, **kwargs):
                return real_mkdtemp(prefix=prefix, dir=directory, **kwargs)

            with patch("sas2pyspark.gui_project.tempfile.mkdtemp", temporary_workspace):
                result = analyze_zip(make_zip({"main.sas": "data result; set source; run;"}))
            output = Path(result["output_root"])
            conversion_path = output / "conversion.json"
            conversion = json.loads(conversion_path.read_text(encoding="utf-8"))
            conversion["outcomes"].append(
                {"source_path": "bad.sas", "status": "failed", "error": "Unsupported syntax"}
            )
            conversion["summary"].update(program_count=2, failed=1)
            conversion_path.write_text(json.dumps(conversion), encoding="utf-8")
            manifest = json.loads(
                (output / "synthetic-data" / "synthetic-data-manifest.json").read_text("utf-8")
            )
            (output / "stderr.log").write_text("Example execution failure", encoding="utf-8")
            page = Path(__file__).resolve().parents[1] / "app" / "pages" / "1_Project_Analysis.py"
            app = AppTest.from_file(str(page))
            app.session_state["project_analysis"] = result
            app.session_state["project_archive_hash"] = "previous-upload"
            app.session_state["project_config_hash"] = None
            app.session_state["project_analysis_version"] = "auto"
            app.session_state["project_synthetic"] = manifest
            app.session_state["project_execution"] = {
                "execution_root": str(output),
                "summary": {"program_count": 1, "succeeded": 0, "failed": 1, "timed_out": 0},
                "results": [{
                    "source_path": "main.sas", "status": "failed",
                    "error": "Worker exited", "stderr_path": "stderr.log",
                }],
            }
            app.run(timeout=30)
            self.assertEqual(len(app.exception), 0)
            self.assertTrue(any("previous analysis" in item.value for item in app.warning))
            self.assertTrue(any(item.label == "Converted" and item.value == "1" for item in app.metric))
            self.assertTrue(any(item.label == "Failed" and item.value == "1" for item in app.metric))
            self.assertTrue(any("Example execution failure" in item.value for item in app.code))
            self.assertTrue(next(item for item in app.button if item.label.startswith("Create synthetic")).disabled)
            self.assertTrue(next(item for item in app.button if item.label.startswith("Execute all")).disabled)
            app.session_state["project_archive_hash"] = None
            app.run(timeout=30)
            self.assertFalse(any("previous analysis" in item.value for item in app.warning))
            self.assertFalse(next(item for item in app.button if item.label.startswith("Create synthetic")).disabled)
            app.selectbox[0].select("9.4M4").run(timeout=30)
            self.assertTrue(any("previous analysis" in item.value for item in app.warning))
            self.assertTrue(next(item for item in app.button if item.label.startswith("Create synthetic")).disabled)


if __name__ == "__main__":
    unittest.main()
