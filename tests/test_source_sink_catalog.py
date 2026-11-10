import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from sas2pyspark import build_source_sink_catalog, resolve_pipeline
from sas2pyspark.cli import main


class SourceSinkCatalogTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)
        self.program = self.root / "server" / "V" / "job.sas"
        self.program.parent.mkdir(parents=True)
        self.program.write_text(
            "\n".join(
                [
                    "libname ods 'v:\\data\\ods';",
                    "libname remote server=node slibref=warehouse;",
                    "data ods.out;",
                    "length id 8 name $20;",
                    "set ods.source remote.fact;",
                    "name = upcase(name);",
                    "run;",
                    "proc sort data=ods.source; by id; run;",
                    'proc import datafile="v:\\input\\ref.csv" out=work.ref dbms=csv;',
                    "run;",
                    'proc export data=ods.out outfile="v:\\output\\out.csv" dbms=csv;',
                    "run;",
                    "proc sql;",
                    "connect to oledb;",
                    "create table work.sqlout as select id from ods.source;",
                    "quit;",
                    "signon node;",
                    "rsubmit node;",
                    "endrsubmit node;",
                ]
            ),
            encoding="utf-8",
        )
        self.config = {
            "schema_version": 1,
            "servers": {
                "DI": {"drives": {"V": "server/V"}},
                "BI": {"drives": {"U": "server-BI/U"}},
            },
            "entrypoints": [{"server": "DI", "path": "server/V/job.sas"}],
            "remote_aliases": {"node": "BI"},
        }

    def _catalog(self):
        report = resolve_pipeline(self.root, self.config).to_dict()
        return build_source_sink_catalog(self.root, report)

    def test_catalogs_directions_systems_files_and_partial_schemas(self):
        catalog = self._catalog()
        resources = {resource["name"]: resource for resource in catalog["resources"]}

        self.assertEqual(resources["ods.out"]["direction"], "input/output")
        self.assertEqual(resources["ods.source"]["direction"], "input/output")
        self.assertEqual(resources["remote.fact"]["direction"], "input")
        self.assertEqual(resources["work.ref"]["direction"], "output")
        self.assertEqual(resources["v:\\input\\ref.csv"]["direction"], "input")
        self.assertEqual(resources["v:\\output\\out.csv"]["direction"], "output")

        output_schema = resources["ods.out"]["schema"]
        self.assertEqual(output_schema["status"], "partial")
        columns = {column["name"].casefold(): column for column in output_schema["columns"]}
        self.assertEqual(columns["id"]["type"], "numeric")
        self.assertEqual(columns["id"]["length"], 8)
        self.assertEqual(columns["name"]["type"], "character")
        self.assertEqual(columns["name"]["length"], 20)
        self.assertEqual(resources["work.ref"]["schema"]["status"], "unknown")

        remote_systems = {
            system["kind"]: system for system in catalog["systems"]
            if system["kind"] == "sas_connect_libref"
        }
        self.assertEqual(len(remote_systems), 1)
        self.assertEqual(next(iter(remote_systems.values()))["target_server"], "BI")
        submit_systems = [
            system
            for system in catalog["systems"]
            if system["kind"] == "sas_remote_submit"
        ]
        self.assertEqual(len(submit_systems), 1)
        self.assertEqual(submit_systems[0]["target_server"], "BI")
        self.assertIn("rsubmit", submit_systems[0]["operations"])
        self.assertGreater(catalog["summary"]["unknown_schema_count"], 0)
        self.assertIn("unknown_schema", {issue["kind"] for issue in catalog["issues"]})

    def test_catalog_cli_writes_json_report(self):
        pipeline_report_path = self.root / "pipeline.json"
        pipeline_report_path.write_text(
            json.dumps(resolve_pipeline(self.root, self.config).to_dict()),
            encoding="utf-8",
        )
        with contextlib.redirect_stdout(io.StringIO()):
            result = main(
                [
                    "project",
                    "catalog",
                    "--root",
                    str(self.root),
                    "--pipeline-report",
                    str(pipeline_report_path),
                ]
            )

        self.assertEqual(result, 0)
        catalog_path = self.root / "pipeline.catalog.json"
        self.assertTrue(catalog_path.exists())
        self.assertEqual(json.loads(catalog_path.read_text())["schema_version"], 1)

    def test_connection_credentials_are_redacted_from_catalog_evidence(self):
        self.program.write_text(
            "libname private 'odbc:host user=alice password=topsecret';",
            encoding="utf-8",
        )
        report = resolve_pipeline(self.root, self.config).to_dict()
        catalog = build_source_sink_catalog(self.root, report)
        serialized = json.dumps({"pipeline": report, "catalog": catalog})

        self.assertNotIn("alice", serialized)
        self.assertNotIn("topsecret", serialized)
        self.assertIn("<redacted>", serialized)

    def test_remote_submit_dataset_uses_mapped_target_server(self):
        remote_program = self.root / "server-BI" / "U" / "job.sas"
        remote_program.parent.mkdir(parents=True)
        remote_program.write_text(
            "rsubmit dwh02; "
            "libname remote 'v:\\data\\warehouse'; "
            "data remote.output; length id 8; set remote.input; run; "
            "endrsubmit;",
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
        pipeline = resolve_pipeline(self.root, config).to_dict()
        catalog = build_source_sink_catalog(self.root, pipeline)
        output = next(
            resource
            for resource in catalog["resources"]
            if resource["name"] == "remote.output"
        )
        system = next(
            item
            for item in catalog["systems"]
            if item["id"] in output["systems"]
        )

        self.assertEqual(pipeline["remote_blocks"][0]["target_server"], "DI")
        self.assertEqual(system["server"], "DI")
        self.assertEqual(output["evidence"][0]["server"], "DI")


if __name__ == "__main__":
    unittest.main()
