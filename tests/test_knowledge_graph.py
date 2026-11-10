import contextlib
import io
import json
import tempfile
import unittest
import xml.etree.ElementTree as ElementTree
from pathlib import Path

from sas2pyspark import build_source_sink_catalog, resolve_pipeline
from sas2pyspark.cli import main
from sas2pyspark.knowledge_graph import (
    KnowledgeGraphError,
    build_knowledge_graph,
    find_path,
    resolve_node,
    search_nodes,
    to_graphml,
    to_mermaid,
    traverse,
    ultimate_nodes,
)


class KnowledgeGraphTests(unittest.TestCase):
    def setUp(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.root = Path(temporary_directory.name)
        base = self.root / "server" / "V"
        base.mkdir(parents=True)
        (base / "main.sas").write_text("%include 'v:\\load.sas';\n", encoding="utf-8")
        (base / "load.sas").write_text(
            "\n".join(
                [
                    "libname ods 'v:\\data\\ods';",
                    "proc sql;",
                    "create table work.agg as select a.id, a.amt as amount from ods.source a;",
                    "quit;",
                    "data ods.out;",
                    "set work.agg;",
                    "net = amount * 2;",
                    "run;",
                ]
            ),
            encoding="utf-8",
        )
        config = {
            "schema_version": 1,
            "servers": {"DI": {"drives": {"V": "server/V"}}},
            "entrypoints": [{"server": "DI", "path": "server/V/main.sas"}],
        }
        self.pipeline = resolve_pipeline(self.root, config).to_dict()
        self.catalog = build_source_sink_catalog(self.root, self.pipeline)
        self.graph = build_knowledge_graph(self.pipeline, self.catalog)

    def test_node_and_edge_types(self):
        summary = self.graph["summary"]
        self.assertEqual(summary["nodes_by_type"]["script"], 2)
        for edge_type in ("includes", "reads", "writes", "derived_from", "column_derived_from"):
            self.assertIn(edge_type, summary["edges_by_type"])
        without = build_knowledge_graph(self.pipeline, self.catalog, include_columns=False)
        self.assertNotIn("column", without["summary"]["nodes_by_type"])

    def test_upstream_downstream_and_sources(self):
        up = traverse(self.graph, "dataset:ods.out", "upstream", types=["dataset"])
        self.assertEqual({n["label"] for n in up["nodes"]}, {"work.agg", "ods.source"})
        down = traverse(self.graph, "dataset:ods.source", "downstream", types=["dataset"])
        self.assertEqual({n["label"] for n in down["nodes"]}, {"work.agg", "ods.out"})
        sources = ultimate_nodes(self.graph, "dataset:ods.out", "upstream", ["dataset"])
        self.assertEqual([n["label"] for n in sources], ["ods.source"])
        limited = traverse(self.graph, "dataset:ods.out", "upstream", max_depth=1, types=["dataset"])
        self.assertIn("work.agg", {n["label"] for n in limited["nodes"]})

    def test_column_walk_stays_on_columns(self):
        up = traverse(self.graph, "column:ods.out.net", "upstream")
        labels = [n["label"] for n in up["nodes"]]
        self.assertIn("ods.source.amt", labels)
        self.assertTrue(all(n["type"] == "column" for n in up["nodes"]))

    def test_path_resolve_and_search(self):
        path = find_path(self.graph, "dataset:ods.source", "dataset:ods.out")
        self.assertEqual(path[0], "dataset:ods.source")
        self.assertEqual(path[-1], "dataset:ods.out")
        self.assertIsNone(find_path(self.graph, "dataset:ods.out", "dataset:ods.source"))
        self.assertEqual(resolve_node(self.graph, "ods.out"), "dataset:ods.out")
        self.assertEqual(resolve_node(self.graph, "load.sas"), "script:server/V/load.sas")
        with self.assertRaises(KnowledgeGraphError):
            resolve_node(self.graph, "nothing-here")
        self.assertTrue(search_nodes(self.graph, "ods.", ["dataset"]))

    def test_exports(self):
        root = ElementTree.fromstring(to_graphml(self.graph))
        self.assertTrue(root.tag.endswith("graphml"))
        mermaid = to_mermaid(self.graph, ["dataset:ods.source", "dataset:ods.out", "dataset:work.agg"])
        self.assertTrue(mermaid.startswith("flowchart LR"))
        self.assertIn("derived_from", mermaid)

    def test_cli_graph_and_lineage(self):
        pipeline_path = self.root / "pipeline.json"
        catalog_path = self.root / "catalog.json"
        pipeline_path.write_text(json.dumps(self.pipeline), encoding="utf-8")
        catalog_path.write_text(json.dumps(self.catalog), encoding="utf-8")
        graph_path = self.root / "graph.json"
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(
                main(["project", "graph", "--pipeline-report", str(pipeline_path),
                      "--catalog-report", str(catalog_path), "--output", str(graph_path)]),
                0,
            )
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(
                main(["project", "graph", "--pipeline-report", str(pipeline_path),
                      "--catalog-report", str(catalog_path), "--output", str(graph_path)]),
                2,
            )
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = main(["project", "lineage", "--graph", str(graph_path), "--query", "sources",
                         "--node", "ods.out", "--json"])
        self.assertEqual(code, 0)
        self.assertEqual([n["label"] for n in json.loads(buffer.getvalue())["nodes"]], ["ods.source"])


if __name__ == "__main__":
    unittest.main()
