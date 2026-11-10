import tempfile
import unittest
from pathlib import Path

from sas2pyspark import build_source_sink_catalog, resolve_pipeline
from sas2pyspark.column_lineage import extract_assignments, trace_column_lineage
from sas2pyspark.schema_inference import expression_refs


class ExpressionTests(unittest.TestCase):
    def test_refs_skip_functions_literals_and_keywords(self):
        refs = expression_refs("upcase(a.name) || 'x.y' || b + 1 and first.id and c in (1,2)")
        self.assertEqual(refs, [("a", "name"), (None, "b"), (None, "c")])
        self.assertEqual(expression_refs("put(d, $fmt.) || put(n, best12.)"), [(None, "d"), (None, "n")])

    def test_assignments_capture_conditions(self):
        found = extract_assignments(
            [("x = a + 1", 3), ("if a > 1 then y = 'big'", 4), ("else y = b", 5), ("retain z 0", 6)]
        )
        self.assertEqual([(i["column"], i["condition"]) for i in found], [("x", None), ("y", "a > 1"), ("y", "else")])


class ColumnLineageTests(unittest.TestCase):
    def setUp(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        (root / "server" / "V").mkdir(parents=True)
        (root / "server" / "V" / "job.sas").write_text(
            "\n".join(
                [
                    "libname ods 'v:\\data\\ods';",
                    "proc sql;",
                    "create table work.agg as select a.id, a.amt as amount, sum(a.amt) as total,",
                    "1 as flag from ods.source a;",
                    "quit;",
                    "data ods.out;",
                    "set work.agg;",
                    "net = amount * 2;",
                    "if net > 10 then tier = 'high';",
                    "run;",
                ]
            ),
            encoding="utf-8",
        )
        config = {
            "schema_version": 1,
            "servers": {"DI": {"drives": {"V": "server/V"}}},
            "entrypoints": [{"server": "DI", "path": "server/V/job.sas"}],
        }
        report = resolve_pipeline(root, config).to_dict()
        self.catalog = build_source_sink_catalog(root, report)

    def _edges(self, target, column):
        return [
            e for e in self.catalog["column_lineage"]
            if e["target"] == target and e["column"].lower() == column
        ]

    def test_sql_transforms(self):
        (rename,) = self._edges("work.agg", "amount")
        self.assertEqual(rename["transform"], "rename")
        self.assertEqual(rename["sources"][0], {"dataset": "ods.source", "column": "amt"})
        (total,) = self._edges("work.agg", "total")
        self.assertEqual(total["transform"], "aggregate")
        (flag,) = self._edges("work.agg", "flag")
        self.assertEqual(flag["transform"], "constant")
        self.assertTrue(flag["resolved"])

    def test_data_step_assignments_and_inheritance(self):
        (net,) = self._edges("ods.out", "net")
        self.assertEqual(net["sources"], [{"dataset": "work.agg", "column": "amount"}])
        (tier,) = self._edges("ods.out", "tier")
        self.assertEqual(tier["transform"], "constant")
        (inherited,) = self._edges("ods.out", "id")
        self.assertEqual(inherited["transform"], "inherited")

    def test_trace_reaches_ultimate_source(self):
        trace = trace_column_lineage(self.catalog["column_lineage"], "ods.out", "net")
        self.assertEqual(trace["ultimate_sources"], [{"dataset": "ods.source", "column": "amt"}])
        self.assertGreater(self.catalog["summary"]["column_lineage_edge_count"], 0)


if __name__ == "__main__":
    unittest.main()
