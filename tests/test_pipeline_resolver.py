import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from sas2pyspark import PipelineResolverError, resolve_pipeline
from sas2pyspark.cli import main


class PipelineResolverTests(unittest.TestCase):
    def test_uses_configured_server_initialization_for_macro_based_fileref(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "server" / "V"
            for name in ("autoexec.sas", "main.sas", "code", "code/job.sas"):
                path = base / name
                if path.suffix.casefold() == ".sas":
                    path.parent.mkdir(parents=True, exist_ok=True)
            (base / "autoexec.sas").write_text(
                '%let root=V:; filename files "&root.\\code";',
                encoding="utf-8",
            )
            (base / "main.sas").write_text(
                "%include files(job.sas);", encoding="utf-8"
            )
            (base / "code" / "job.sas").write_text(
                "data work.result; run;", encoding="utf-8"
            )
            config = {
                "schema_version": 1,
                "servers": {
                    "DI": {
                        "drives": {"V": "server/V"},
                        "initialization_scripts": ["server/V/autoexec.sas"],
                    }
                },
                "entrypoints": [{"server": "DI", "path": "server/V/main.sas"}],
            }

            report = resolve_pipeline(root, config).to_dict()

        self.assertEqual(report["summary"]["reachable_script_count"], 3)
        self.assertEqual(report["includes"][0]["target"], "server/V/code/job.sas")

    def test_resolves_entrypoint_includes_libraries_and_remote_server_links(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            di_root = root / "server-DI" / "V"
            bi_root = root / "server-BI" / "U"
            di_job = di_root / "dwh" / "Jams" / "DI.sas"
            di_child = di_root / "dwh" / "jobs" / "first.sas"
            di_nested = di_root / "dwh" / "jobs" / "nested.sas"
            bi_job = bi_root / "dwh" / "Jams" / "BI.sas"
            bi_child = bi_root / "dwh" / "Stock_FIP" / "daily.sas"
            unused = bi_root / "dwh" / "unused.sas"
            for path in (di_job, di_child, di_nested, bi_job, bi_child, unused):
                path.parent.mkdir(parents=True, exist_ok=True)
            di_job.write_text(
                "libname ods 'v:\\dwh\\ods'; "
                "filename proglib 'v:\\dwh\\jobs'; "
                "%include proglib(first.sas);",
                encoding="utf-8",
            )
            di_child.write_text('%include "nested.sas";', encoding="utf-8")
            di_nested.write_text("data work.a; run;", encoding="utf-8")
            bi_job.write_text(
                "x 'cd u:\\dwh'; "
                "filename proglib 'Stock_FIP'; "
                "%include proglib(daily.sas); "
                "libname remote server=node slibref=warehouse; "
                "proc sql; connect to oledb; quit; "
                "signon dwh02; rsubmit dwh02; endrsubmit dwh02;",
                encoding="utf-8",
            )
            bi_child.write_text("proc sort data=work.a; run;", encoding="utf-8")
            unused.write_text("data work.unused; run;", encoding="utf-8")
            config = {
                "schema_version": 1,
                "servers": {
                    "DI": {"drives": {"V": "server-DI/V"}},
                    "BI": {"drives": {"U": "server-BI/U"}},
                },
                "entrypoints": [
                    {"server": "DI", "path": "server-DI/V/dwh/Jams/DI.sas"},
                    {"server": "BI", "path": "server-BI/U/dwh/Jams/BI.sas"},
                ],
                "remote_aliases": {"dwh02": "DI", "node": "DI"},
            }

            report = resolve_pipeline(root, config).to_dict()

        scripts = {item["path"]: item for item in report["scripts"]}
        self.assertEqual(len([s for s in scripts.values() if s["status"] == "reachable"]), 5)
        self.assertEqual(scripts["server-BI/U/dwh/unused.sas"]["status"], "unreached")
        self.assertEqual(
            {item["target"] for item in report["includes"] if item["status"] == "resolved"},
            {
                "server-DI/V/dwh/jobs/first.sas",
                "server-DI/V/dwh/jobs/nested.sas",
                "server-BI/U/dwh/Stock_FIP/daily.sas",
            },
        )
        self.assertEqual(
            report["libraries"][0]["repository_path"], "server-DI/V/dwh/ods"
        )
        self.assertEqual(
            {item["target_server"] for item in report["remote_references"]}, {"DI"}
        )
        self.assertEqual(
            {(item["mode"], item["system"]) for item in report["external_connections"]},
            {("sas_connect_libref", "node"), ("sql_pass_through", "oledb")},
        )
        self.assertEqual(report["summary"]["unreached_script_count"], 1)

    def test_rsubmit_block_resolves_includes_on_mapped_remote_server(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            di_root = root / "server-DI" / "V"
            bi_root = root / "server-BI" / "U"
            di_job = di_root / "job.sas"
            remote_job = di_root / "jobs" / "remote.sas"
            bi_job = bi_root / "job.sas"
            for path in (di_job, remote_job, bi_job):
                path.parent.mkdir(parents=True, exist_ok=True)
            di_job.write_text("data work.di; run;", encoding="utf-8")
            remote_job.write_text("data work.remote; run;", encoding="utf-8")
            bi_job.write_text(
                "signon dwh02; rsubmit; "
                "%include 'v:\\jobs\\remote.sas'; endrsubmit;",
                encoding="utf-8",
            )
            config = {
                "schema_version": 1,
                "servers": {
                    "DI": {"drives": {"V": "server-DI/V"}},
                    "BI": {"drives": {"U": "server-BI/U"}},
                },
                "entrypoints": [{"server": "BI", "path": "server-BI/U/job.sas"}],
                "remote_aliases": {"dwh02": "DI"},
            }

            report = resolve_pipeline(root, config).to_dict()

        self.assertEqual(report["remote_blocks"][0]["target_server"], "DI")
        self.assertEqual(report["remote_blocks"][0]["status"], "resolved")
        self.assertEqual(report["includes"][0]["target"], "server-DI/V/jobs/remote.sas")
        scripts = {item["path"]: item for item in report["scripts"]}
        self.assertEqual(scripts["server-DI/V/jobs/remote.sas"]["status"], "reachable")

    def test_unmapped_rsubmit_does_not_guess_local_include_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "server-BI" / "U"
            main = base / "job.sas"
            local_target = base / "remote.sas"
            for path in (main, local_target):
                path.parent.mkdir(parents=True, exist_ok=True)
            main.write_text(
                "rsubmit unknown_host; %include 'remote.sas'; endrsubmit;",
                encoding="utf-8",
            )
            local_target.write_text("data work.local; run;", encoding="utf-8")
            config = {
                "schema_version": 1,
                "servers": {"BI": {"drives": {"U": "server-BI/U"}}},
                "entrypoints": [{"server": "BI", "path": "server-BI/U/job.sas"}],
            }

            report = resolve_pipeline(root, config).to_dict()

        self.assertEqual(report["remote_blocks"][0]["status"], "unknown_endpoint")
        self.assertEqual(report["includes"][0]["status"], "unresolved")
        self.assertIsNone(report["includes"][0]["target"])
        self.assertEqual(report["scripts"][0]["status"], "reachable")

    def test_rsubmit_without_alias_is_reported_as_unresolved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / "server" / "V" / "job.sas"
            script.parent.mkdir(parents=True)
            script.write_text(
                "rsubmit; data work.remote; run; endrsubmit;",
                encoding="utf-8",
            )
            config = {
                "schema_version": 1,
                "servers": {"DI": {"drives": {"V": "server/V"}}},
                "entrypoints": [{"server": "DI", "path": "server/V/job.sas"}],
            }

            report = resolve_pipeline(root, config).to_dict()

        self.assertEqual(report["remote_blocks"][0]["status"], "unknown_endpoint")
        self.assertIn(
            "remote_alias_missing",
            {issue["kind"] for issue in report["issues"]},
        )

    def test_reports_dynamic_include_and_include_cycle_without_recursing_forever(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "server" / "V"
            (base / "a.sas").parent.mkdir(parents=True)
            (base / "a.sas").write_text(
                'filename files "v:\\\\"; %include "b.sas"; %include &next; '
                "filename tmp temp; %include tmp;",
                encoding="utf-8",
            )
            (base / "b.sas").write_text('%include "a.sas";', encoding="utf-8")
            config = {
                "schema_version": 1,
                "servers": {"DI": {"drives": {"V": "server/V"}}},
                "entrypoints": [{"server": "DI", "path": "server/V/a.sas"}],
            }

            report = resolve_pipeline(root, config).to_dict()

        self.assertTrue(any(item["kind"] == "include_cycle" for item in report["issues"]))
        self.assertTrue(
            any(
                item["status"] == "unresolved" and "&next" in item["reference"]
                for item in report["includes"]
            )
        )
        self.assertTrue(any(item["kind"] == "filename" for item in report["issues"]))

    def test_rejects_unsafe_drive_mappings_and_unknown_entrypoint_server(self):
        config = {
            "schema_version": 1,
            "servers": {"DI": {"drives": {"V": "../outside"}}},
            "entrypoints": [{"server": "UNKNOWN", "path": "main.sas"}],
        }
        with self.assertRaisesRegex(PipelineResolverError, "stay within"):
            resolve_pipeline(".", config)
        config["servers"]["DI"]["drives"]["V"] = "server/V"
        with self.assertRaisesRegex(PipelineResolverError, "unknown server"):
            resolve_pipeline(".", config)

    def test_resolve_pipeline_cli_writes_report_and_returns_incomplete_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "server" / "main.sas"
            source.parent.mkdir()
            source.write_text("%include missing(member.sas);", encoding="utf-8")
            config = Path(directory, "pipeline.json")
            config.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "servers": {"DI": {"drives": {"V": "server"}}},
                        "entrypoints": [{"server": "DI", "path": "server/main.sas"}],
                    }
                ),
                encoding="utf-8-sig",
            )
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                status = main(
                    [
                        "project",
                        "resolve-pipeline",
                        "--root",
                        str(root),
                        "--config",
                        str(config),
                    ]
                )
            report_path = config.with_suffix(".pipeline.json")

            self.assertEqual(status, 1)
            self.assertTrue(report_path.is_file())
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["summary"]["reachable_script_count"], 1)
            self.assertIn("1 unresolved", output.getvalue())


if __name__ == "__main__":
    unittest.main()
