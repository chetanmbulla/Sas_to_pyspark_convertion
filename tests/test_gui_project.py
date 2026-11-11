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
    pipeline_flow_graph,
    preview_synthetic_data,
    read_converted_code,
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

    def test_analysis_uses_uploaded_schema_metadata(self):
        schemas = {
            "datasets": [{
                "name": "work.source",
                "columns": [
                    {"name": "id", "type": "numeric", "length": 8},
                    {"name": "label", "type": "character", "length": 4},
                ],
            }]
        }
        with tempfile.TemporaryDirectory() as directory:
            real_mkdtemp = tempfile.mkdtemp

            def temporary_workspace(prefix, **kwargs):
                return real_mkdtemp(prefix=prefix, dir=directory, **kwargs)

            with patch("sas2pyspark.gui_project.tempfile.mkdtemp", temporary_workspace):
                result = analyze_zip(
                    make_zip({"main.sas": "data result; set source; run;"}),
                    schema_asset=("tables.json", json.dumps(schemas).encode()),
                    rows=3,
                )
            output = Path(result["output_root"])
            catalog = json.loads((output / "catalog.json").read_text(encoding="utf-8"))
            resource = next(item for item in catalog["resources"] if item["name"] == "work.source")
            self.assertEqual(resource["schema"]["status"], "complete")
            manifest = json.loads((output / "synthetic-data" / "synthetic-data-manifest.json").read_text(encoding="utf-8"))
            header, rows = preview_synthetic_data(output, manifest, "work.source", limit=2)
            self.assertEqual(header, ["id", "label"])
            self.assertEqual(len(rows), 2)
            self.assertEqual([row[0] for row in rows], ["1", "2"])
            self.assertTrue(all(len(row[1]) <= 4 for row in rows))
            with self.assertRaises(GuiProjectError):
                preview_synthetic_data(output, manifest, "work.source", limit=101)
            record = next(item for item in manifest["datasets"] if item["dataset"] == "work.source")
            fixture = Path(manifest["output_directory"]) / record["path"]
            fixture.write_text("wrong,header\n1,abc\n", encoding="utf-8")
            with self.assertRaisesRegex(GuiProjectError, "does not match"):
                preview_synthetic_data(output, manifest, "work.source")
            record["path"] = "../outside.csv"
            with self.assertRaises(GuiProjectError):
                preview_synthetic_data(output, manifest, "work.source")
            manifest["output_directory"] = directory
            with self.assertRaisesRegex(GuiProjectError, "outside"):
                preview_synthetic_data(output, manifest, "work.source")

    def test_pipeline_graph_limits_and_escapes_labels(self):
        graph, truncated = pipeline_flow_graph({
            "jobs": [{"name": 'job "A"', "entrypoint": "a.sas"}],
            "includes": [
                {"status": "resolved", "source": "a.sas", "target": "b.sas"},
                {"status": "unresolved", "source": "a.sas", "target": "ignored.sas"},
            ],
        }, max_nodes=2)
        self.assertTrue(truncated)
        self.assertIn('job \\"A\\"', graph)
        self.assertIn(" -> ", graph)
        self.assertNotIn("ignored.sas", graph)

    def test_selected_program_and_invalid_edits(self):
        with tempfile.TemporaryDirectory() as directory:
            real_mkdtemp = tempfile.mkdtemp

            def temporary_workspace(prefix, **kwargs):
                return real_mkdtemp(prefix=prefix, dir=directory, **kwargs)

            with patch("sas2pyspark.gui_project.tempfile.mkdtemp", temporary_workspace):
                result = analyze_zip(make_zip({
                    "first.sas": "data a; set source; run;",
                    "second.sas": "data b; set source; run;",
                }))
            output = Path(result["output_root"])
            manifest = json.loads((output / "synthetic-data" / "synthetic-data-manifest.json").read_text(encoding="utf-8"))
            for programs, edits in [
                ([], None), (["unknown.sas"], None),
                (["input/first.sas"], {"input/second.sas": "pass"}),
                (["input/first.sas"], {"input/first.sas": "invalid syntax !!"}),
            ]:
                with self.subTest(programs=programs, edits=edits), self.assertRaises(GuiProjectError):
                    run_converted_programs(result, manifest, [], timeout_seconds=5, programs=programs, edited_code=edits)
            fake = unittest.mock.Mock()
            fake.to_dict.return_value = {"results": []}
            with patch("sas2pyspark.gui_project.run_project", return_value=fake) as runner:
                report = run_converted_programs(
                    result, manifest, [], timeout_seconds=5,
                    programs=["input/first.sas"], edited_code={"input/first.sas": "print('new')"},
                )
            stage = Path(report["execution_root"])
            self.assertEqual((stage / "pyspark" / "input" / "first.py").read_text(encoding="utf-8"), "print('new')")
            self.assertFalse((stage / "pyspark" / "input" / "second.py").exists())
            self.assertEqual(runner.call_args.kwargs["program_selectors"], ["input/first.sas"])

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
                    ),
                    schema_asset=("schema.json", json.dumps({
                        "datasets": [{"name": "work.source", "columns": [
                            {"name": "id", "type": "numeric"}
                        ]}]
                    }).encode()),
                    rows=2,
                )
            output = Path(result["output_root"])
            manifest = json.loads(
                (output / "synthetic-data" / "synthetic-data-manifest.json").read_text(encoding="utf-8")
            )
            report = run_converted_programs(
                result, manifest, ["work.source"], timeout_seconds=120,
                edited_code={
                    "input/ok.sas": read_converted_code(output, "pyspark/input/ok.py")
                    + "\nprint('edited test code ran')\n"
                },
            )
            self.assertEqual(report["summary"]["succeeded"], 1, report)
            self.assertEqual(report["summary"]["failed"], 1, report)
            by_path = {item["source_path"]: item for item in report["results"]}
            self.assertEqual(by_path["input/ok.sas"]["status"], "succeeded")
            self.assertEqual(report["edited_programs"], ["input/ok.sas"])
            stdout = Path(report["execution_root"]) / by_path["input/ok.sas"]["stdout_path"]
            self.assertIn("edited test code ran", stdout.read_text(encoding="utf-8", errors="replace"))
            self.assertNotIn("edited test code ran", read_converted_code(output, "pyspark/input/ok.py"))
            failure = by_path["input/missing.sas"]
            self.assertIn("Worker exited", failure["error"])
            log = Path(report["execution_root"]) / failure["stderr_path"]
            self.assertIn("df_missing_input", log.read_text(encoding="utf-8", errors="replace"))


@unittest.skipUnless(
    importlib.util.find_spec("streamlit") is not None,
    "Streamlit is required",
)
class GuiProjectPageTests(unittest.TestCase):
    def test_phase2_empty_and_result_views(self):
        from streamlit.testing.v1 import AppTest

        page = Path(__file__).resolve().parents[1] / "app" / "pages" / "2_Phase_2_Synthetic_Data.py"
        app = AppTest.from_file(str(page)).run(timeout=30)
        self.assertEqual(len(app.exception), 0)
        self.assertTrue(any("First upload" in item.value for item in app.info))
        with tempfile.TemporaryDirectory() as directory:
            real_mkdtemp = tempfile.mkdtemp

            def temporary_workspace(prefix, **kwargs):
                return real_mkdtemp(prefix=prefix, dir=directory, **kwargs)

            schema = {"datasets": [{"name": "work.source", "columns": [
                {"name": "id", "type": "numeric"},
                {"name": "label", "type": "character", "length": 4, "label": "Description"},
            ]}]}
            with patch("sas2pyspark.gui_project.tempfile.mkdtemp", temporary_workspace):
                result = analyze_zip(
                    make_zip({"main.sas": "data result; set source missing_input; run;"}),
                    schema_asset=("schema.json", json.dumps(schema).encode()), rows=3,
                )
            output = Path(result["output_root"])
            manifest = json.loads((output / "synthetic-data" / "synthetic-data-manifest.json").read_text(encoding="utf-8"))
            app.session_state["project_analysis"] = result
            app.session_state["project_synthetic"] = manifest
            app.run(timeout=30)
            app.selectbox[0].select("work.source").run(timeout=30)
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(list(app.dataframe[1].value["Attribute"]), ["id", "label"])
            self.assertEqual(app.dataframe[1].value["Label"].iloc[1], "Description")
            self.assertEqual(list(app.dataframe[2].value.columns), ["id", "label"])
            self.assertEqual(len(app.dataframe[2].value), 3)
            app.number_input[0].set_value(2)
            app.button[0].click().run(timeout=30)
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(len(app.dataframe[2].value), 2)
            self.assertEqual(app.session_state["project_synthetic"]["rows_per_dataset"], 2)
            app.session_state["project_phase1_stale"] = True
            app.run(timeout=30)
            self.assertTrue(app.button[0].disabled)
            app.selectbox[0].select("work.missing_input").run(timeout=30)
            self.assertEqual(len(app.exception), 0)
            self.assertTrue(any("No fixture was generated" in item.value for item in app.warning))

    def test_project_page_renders(self):
        from streamlit.testing.v1 import AppTest

        page = Path(__file__).resolve().parents[1] / "app" / "pages" / "1_Project_Analysis.py"
        app = AppTest.from_file(str(page)).run()
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.title[0].value, "Phase 1: Pipeline analysis and assets")
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
            self.assertTrue(any(item.label == "Score" for item in app.metric))
            self.assertTrue(any(item.label == "Jobs and includes" for item in app.tabs))
            app.session_state["project_archive_hash"] = None
            app.run(timeout=30)
            self.assertFalse(any("previous analysis" in item.value for item in app.warning))
            app.selectbox[0].select("9.4M4").run(timeout=30)
            self.assertTrue(any("previous analysis" in item.value for item in app.warning))

    def test_phase3_program_selection_edit_and_run(self):
        from streamlit.testing.v1 import AppTest

        page = Path(__file__).resolve().parents[1] / "app" / "pages" / "3_Phase_3_Conversion_Testing.py"
        app = AppTest.from_file(str(page)).run(timeout=30)
        self.assertFalse(app.exception)
        self.assertTrue(any("First upload" in item.value for item in app.info))
        with tempfile.TemporaryDirectory() as directory:
            real_mkdtemp = tempfile.mkdtemp

            def temporary_workspace(prefix, **kwargs):
                return real_mkdtemp(prefix=prefix, dir=directory, **kwargs)

            schemas = {"datasets": [{"name": "work.source", "columns": [{"name": "id", "type": "numeric"}]}]}
            with patch("sas2pyspark.gui_project.tempfile.mkdtemp", temporary_workspace):
                result = analyze_zip(
                    make_zip({"main.sas": "data result; set source; run;"}),
                    schema_asset=("schema.json", json.dumps(schemas).encode()),
                )
            output = Path(result["output_root"])
            manifest = json.loads((output / "synthetic-data" / "synthetic-data-manifest.json").read_text(encoding="utf-8"))
            app.session_state["project_analysis"] = result
            app.session_state["project_synthetic"] = manifest
            app.run(timeout=30)
            self.assertFalse(app.exception)
            self.assertEqual(app.selectbox[0].value, "input/main.sas")
            self.assertIn("work.source", app.multiselect[0].value)
            self.assertTrue(app.button[0].disabled)
            app.checkbox[0].check()
            app.text_area[0].set_value("print('edited')")
            app.run(timeout=30)
            fake = {"execution_root": str(output), "results": [{
                "source_path": "input/main.sas", "status": "succeeded",
                "stdout_path": "stdout.log", "stderr_path": "stderr.log",
            }]}
            (output / "stdout.log").write_text("edited", encoding="utf-8")
            (output / "stderr.log").write_text("", encoding="utf-8")
            with patch("sas2pyspark.gui_project.run_converted_programs", return_value=fake) as run:
                app.button[0].click().run(timeout=30)
            self.assertFalse(app.exception)
            self.assertEqual(run.call_args.kwargs["programs"], ["input/main.sas"])
            self.assertEqual(run.call_args.kwargs["edited_code"], {"input/main.sas": "print('edited')"})
            self.assertEqual(run.call_args.args[2], ["work.source"])
            self.assertTrue(any("Status: succeeded" in item.value for item in app.markdown))


if __name__ == "__main__":
    unittest.main()
