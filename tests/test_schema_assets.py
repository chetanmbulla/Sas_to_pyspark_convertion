import json
import tempfile
import unittest
from pathlib import Path

from sas2pyspark import build_source_sink_catalog, resolve_pipeline
from sas2pyspark.schema_assets import SchemaAssetError, load_schema_assets


class SchemaAssetTests(unittest.TestCase):
    def setUp(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.root = Path(temporary_directory.name)

    def test_loads_csv_json_and_ddl(self):
        (self.root / "a.csv").write_text(
            "LIBNAME,MEMNAME,NAME,TYPE,LENGTH,VARNUM\nODS,Orders,id,1,8,1\nODS,Orders,name,2,20,2\n",
            encoding="utf-8",
        )
        (self.root / "b.json").write_text(
            json.dumps({"datasets": [{"name": "t", "library": "x", "columns": [{"name": "c", "type": "char"}]}]}),
            encoding="utf-8",
        )
        (self.root / "c.sql").write_text("create table lib.fact (id num, label char(10));", encoding="utf-8")
        assets, warnings = load_schema_assets(self.root)
        self.assertEqual(warnings, [])
        self.assertEqual([c["name"] for c in assets["ods.orders"]["columns"]], ["id", "name"])
        self.assertIn("x.t", assets)
        self.assertIn("lib.fact", assets)

    def test_duplicate_dataset_keeps_first_and_warns(self):
        (self.root / "a.sql").write_text("create table lib.t (id num);", encoding="utf-8")
        (self.root / "b.sql").write_text("create table lib.t (other num);", encoding="utf-8")
        assets, warnings = load_schema_assets(self.root)
        self.assertEqual(assets["lib.t"]["columns"][0]["name"], "id")
        self.assertEqual(len(warnings), 1)

    def test_repeated_column_is_an_error(self):
        (self.root / "a.sql").write_text("create table lib.t (id num, id num);", encoding="utf-8")
        with self.assertRaises(SchemaAssetError):
            load_schema_assets(self.root)


class SchemaInferenceCatalogTests(unittest.TestCase):
    def setUp(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.root = Path(temporary_directory.name)
        (self.root / "server" / "V").mkdir(parents=True)
        (self.root / "server" / "V" / "job.sas").write_text(
            "\n".join(
                [
                    "libname ods 'v:\\data\\ods';",
                    "data ods.out;",
                    "length id 8 name $20;",
                    "set ods.source;",
                    "run;",
                    "proc sql;",
                    "create table work.agg as select a.id, 1 as flag from ods.out a;",
                    "quit;",
                ]
            ),
            encoding="utf-8",
        )

    def _schemas(self, assets=None):
        config = {
            "schema_version": 1,
            "servers": {"DI": {"drives": {"V": "server/V"}}},
            "entrypoints": [{"server": "DI", "path": "server/V/job.sas"}],
        }
        report = resolve_pipeline(self.root, config).to_dict()
        catalog = build_source_sink_catalog(self.root, report, schema_assets=assets)
        return {s["dataset"]: s for s in catalog["schemas"]}, catalog

    def test_inference_propagates_columns(self):
        schemas, catalog = self._schemas()
        self.assertEqual(schemas["ods.source"]["status"], "unknown")
        self.assertEqual(schemas["ods.out"]["status"], "partial")
        self.assertEqual(schemas["work.agg"]["status"], "inferred")
        columns = {c["name"]: c for c in schemas["work.agg"]["columns"]}
        self.assertEqual(set(columns), {"id", "flag"})
        self.assertEqual(columns["flag"]["type"], "numeric")
        self.assertGreaterEqual(catalog["summary"]["inferred_schema_count"], 1)

    def test_assets_make_schema_complete_and_unused_are_reported(self):
        assets = {
            "ods.source": {"columns": [{"name": "id", "type": "numeric", "length": 8}], "origin": "s.csv"},
            "ods.nothing": {"columns": [{"name": "x", "type": "numeric"}], "origin": "s.csv"},
        }
        schemas, catalog = self._schemas(assets)
        self.assertEqual(schemas["ods.source"]["status"], "complete")
        self.assertEqual(catalog["summary"]["complete_schema_count"], 1)
        self.assertEqual(catalog["summary"]["unused_schema_assets"], ["ods.nothing"])


if __name__ == "__main__":
    unittest.main()
