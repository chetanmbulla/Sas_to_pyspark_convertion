import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from sas2pyspark import PipelineResolverError, resolve_pipeline
from sas2pyspark.cli import main


class PipelineResolverTests(unittest.TestCase):
    def test_detects_file_email_ods_and_host_io_with_direction(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self._write_job(
                Path(directory),
                "%let out = v:\\out;\n"
                "proc export data=a outfile=\"&out.\\a.xlsx\" dbms=xlsx replace; run;\n"
                "proc import datafile='v:\\in.csv' out=b dbms=csv; run;\n"
                "filename mail email type='text/html' to='x@y.z';\n"
                "filename f ftp 'data.txt' host='h.example.com' user='u' pass='p';\n"
                "ods html body='v:\\r.html';\n"
                "ods html close;\n"
                "x 'del v:\\old.txt';\n"
                "data _null_; infile 'v:\\raw.txt'; file print; run;\n",
            )
            report = resolve_pipeline(directory, config).to_dict()

        found = {
            (c["mode"], c["direction"]): c["system"]
            for c in report["external_connections"]
        }
        self.assertEqual(found[("file_export", "output")], "v:\\out\\a.xlsx")
        self.assertIn(("file_import", "input"), found)
        self.assertIn(("email", "output"), found)
        self.assertIn(("ftp", "both"), found)
        self.assertEqual(found[("ods_output", "output")], "v:\\r.html")
        self.assertIn(("host_command", None), found)
        self.assertIn(("file_read", "input"), found)
        self.assertFalse(any(m == "file_write" for m, _ in found))
        self.assertFalse(any(i["kind"] == "filename" for i in report["issues"]))
        self.assertNotIn("x@y.z", json.dumps(report))
        self.assertNotIn("'p'", json.dumps(report))
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

    def _write_job(self, root: Path, source: str) -> dict:
        base = root / "server" / "V"
        base.mkdir(parents=True)
        (base / "a.sas").write_text(source, encoding="utf-8")
        (base / "inc.sas").write_text("data x; run;", encoding="utf-8")
        return {
            "schema_version": 1,
            "servers": {"DI": {"drives": {"V": "server/V"}}},
            "entrypoints": [{"server": "DI", "path": "server/V/a.sas"}],
        }

    def test_include_of_file_written_by_same_script_is_runtime_generated(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self._write_job(
                Path(directory),
                "filename tmp '&temp'; data _null_; file tmp; put 'x;'; run; %include tmp;",
            )
            report = resolve_pipeline(directory, config).to_dict()

        statuses = [item["status"] for item in report["includes"]]
        self.assertEqual(statuses, ["runtime_generated"])
        self.assertEqual(report["summary"]["runtime_generated_include_count"], 1)
        self.assertEqual(report["summary"]["unresolved_include_count"], 0)
        self.assertFalse(any(item["kind"] == "include" for item in report["issues"]))

    def test_config_macro_variables_resolve_includes(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self._write_job(Path(directory), "%include '&base.\\inc.sas';")
            report = resolve_pipeline(directory, config).to_dict()
            self.assertEqual(report["summary"]["unresolved_include_count"], 1)
            config["macro_variables"] = {"BASE": "v:"}
            report = resolve_pipeline(directory, config).to_dict()

        self.assertEqual(report["summary"]["resolved_include_count"], 1)
        self.assertEqual(report["summary"]["unresolved_include_count"], 0)

    def test_waived_include_is_recorded_with_reason_and_not_unresolved(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self._write_job(Path(directory), "%include 'V:\\Admin\\missing.sas';")
            config["waived_includes"] = [
                {"reference": "v:/admin/MISSING.sas", "reason": "Not provided by client."}
            ]
            report = resolve_pipeline(directory, config).to_dict()

        self.assertEqual(report["includes"][0]["status"], "waived")
        self.assertEqual(report["includes"][0]["reason"], "Not provided by client.")
        self.assertEqual(report["summary"]["waived_include_count"], 1)
        self.assertEqual(report["summary"]["unresolved_include_count"], 0)

    def test_drive_relative_include_requires_opt_in_and_records_assumption(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self._write_job(Path(directory), "%include 'V:inc.sas';")
            rejected = resolve_pipeline(directory, config).to_dict()
            config["drive_relative_paths"] = "drive_root"
            accepted = resolve_pipeline(directory, config).to_dict()

        self.assertEqual(rejected["summary"]["unresolved_include_count"], 1)
        self.assertEqual(accepted["summary"]["resolved_include_count"], 1)
        self.assertTrue(
            any(item["kind"] == "drive_relative_assumed_root" for item in accepted["issues"])
        )

    def test_inc_abbreviation_and_remote_host_mapping_with_seeding(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            di = root / "di" / "V"
            bi = root / "bi" / "U"
            di.mkdir(parents=True)
            bi.mkdir(parents=True)
            (di / "macro.sas").write_text(
                "%let tcpsec = user.pwd;\n"
                "%let node = bi.example.net 1473;\n"
                "options remote=node; signon node;\n"
                "rsubmit node; %let enduse = u: ; endrsubmit node;\n",
                encoding="utf-8",
            )
            (di / "a.sas").write_text(
                "%inc 'v:\\macro.sas'; rsubmit node; libname l 'u:\\x'; endrsubmit node;",
                encoding="utf-8",
            )
            (bi / "b.sas").write_text(
                "libname d '&enduse.\\data';", encoding="utf-8"
            )
            config = {
                "schema_version": 1,
                "servers": {
                    "DI": {"drives": {"V": "di/V"}},
                    "BI": {"drives": {"U": "bi/U"}},
                },
                "entrypoints": [
                    {"server": "DI", "path": "di/V/a.sas"},
                    {"server": "BI", "path": "bi/U/b.sas"},
                ],
                "remote_hosts": {"BI.example.net": "BI"},
                "seed_remote_macro_variables": True,
                "jams_jobs": [
                    {
                        "name": "JOB_A",
                        "entrypoint": "di/V/a.sas",
                        "depends_on": [],
                        "parameters": {"password": "x", "mode": "full"},
                    }
                ],
            }
            report = resolve_pipeline(directory, config).to_dict()

        self.assertEqual(report["summary"]["resolved_remote_block_count"], 2)
        self.assertTrue(all(b["status"] == "resolved" for b in report["remote_blocks"]))
        self.assertEqual(report["remote_endpoints"][0]["target_server"], "BI")
        self.assertEqual(report["remote_endpoints"][0]["port"], 1473)
        self.assertNotIn("user.pwd", json.dumps(report))
        self.assertEqual(report["summary"]["unresolved_library_count"], 0)
        self.assertTrue(
            any(i["kind"] == "remote_macro_variable_seeded" for i in report["issues"])
        )
        job = next(j for j in report["jobs"] if j["name"] == "JOB_A")
        self.assertEqual(job["jams"]["parameters"]["password"], "<redacted>")

    def test_unmapped_drive_is_summarized_with_reason_code(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self._write_job(Path(directory), "libname q 'W:\\data';")
            report = resolve_pipeline(directory, config).to_dict()

        self.assertEqual(report["libraries"][0]["reason_code"], "drive_not_mapped")
        self.assertEqual(
            report["summary"]["unmapped_drives"],
            [{"server": "DI", "drive": "W", "library_count": 1}],
        )

    def test_jams_job_dependency_must_exist(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self._write_job(Path(directory), "data x; run;")
            config["jams_jobs"] = [
                {"name": "A", "entrypoint": "server/V/a.sas", "depends_on": ["B"]}
            ]
            with self.assertRaises(PipelineResolverError):
                resolve_pipeline(directory, config)

    def test_rejects_invalid_resolution_options(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self._write_job(Path(directory), "data x; run;")
            for key, value in (
                ("drive_relative_paths", "guess"),
                ("waived_includes", [{"reference": "a.sas"}]),
                ("macro_variables", {"1bad": "x"}),
            ):
                with self.subTest(key=key):
                    broken = {**config, key: value}
                    with self.assertRaises(PipelineResolverError):
                        resolve_pipeline(directory, broken)

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
