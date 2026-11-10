import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from sas2pyspark import build_source_sink_catalog, resolve_pipeline
from sas2pyspark.business_context import (
    BusinessContextError,
    apply_responses,
    build_business_context,
    build_context_packets,
    build_prompts,
    load_responses,
    render_markdown,
)
from sas2pyspark.cli import main


class BusinessContextTests(unittest.TestCase):
    def setUp(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.root = Path(temporary_directory.name)
        base = self.root / "server" / "V"
        base.mkdir(parents=True)
        (base / "main.sas").write_text(
            "**********;\n* Daily stock load. Owner jane.doe@example.com;\n**********;\n%include 'v:\\load.sas';\n",
            encoding="utf-8",
        )
        (base / "load.sas").write_text(
            "/* Builds the stock fact */\n"
            "libname ods 'v:\\data\\ods';\n"
            "data ods.out;\nset ods.source;\nrun;\n",
            encoding="utf-8",
        )
        config = {
            "schema_version": 1,
            "servers": {"DI": {"drives": {"V": "server/V"}}},
            "entrypoints": [{"server": "DI", "path": "server/V/main.sas"}],
        }
        self.pipeline = resolve_pipeline(self.root, config).to_dict()
        self.catalog = build_source_sink_catalog(self.root, self.pipeline)

    def _packets(self):
        return {p["id"]: p for p in build_context_packets(self.pipeline, self.catalog)}

    def test_packets_cover_jobs_scripts_and_final_datasets(self):
        packets = self._packets()
        job = next(p for p in packets.values() if p["type"] == "job")
        self.assertEqual(job["script_count"], 2)
        self.assertIn("ods.out", job["final_outputs"]["items"])
        self.assertEqual(job["ultimate_sources"]["items"], ["ods.source"])
        self.assertIn("dataset:ods.out", packets)
        script = packets["script:server/V/load.sas"]
        self.assertEqual(script["header_comment"], "Builds the stock fact")

    def test_email_is_redacted_and_asterisk_header_ignored(self):
        text = json.dumps(self._packets())
        self.assertNotIn("jane.doe", text)
        job = next(p for p in self._packets().values() if p["type"] == "job")
        self.assertIsNone(job["header_comment"])

    def test_templates_are_factual_and_flag_open_questions(self):
        context = build_business_context(self.pipeline, self.catalog)
        record = next(r for r in context["records"] if r["id"] == "dataset:ods.out")
        self.assertEqual(record["source"], "template")
        self.assertIn("final output", record["description"]["purpose"])
        self.assertTrue(record["description"]["open_questions"])
        self.assertIn("# Business context", render_markdown(context))

    def test_prompts_embed_packet_and_never_need_network(self):
        prompts = build_prompts(build_context_packets(self.pipeline, self.catalog))
        self.assertTrue(all("EVIDENCE PACKET" in p["prompt"] and p["packet_hash"] for p in prompts))

    def test_responses_stale_review_and_human_precedence(self):
        packets = build_context_packets(self.pipeline, self.catalog)
        target = "dataset:ods.out"
        current = next(p["packet_hash"] for p in packets if p["id"] == target)
        ai = {"id": target, "packet_hash": current, "description": {"purpose": "AI text"}, "model": "m"}
        merged = apply_responses(packets, [ai])["records"][target]
        self.assertEqual((merged["source"], merged["needs_review"], merged["stale"]), ("ai", True, False))
        stale = apply_responses(packets, [{**ai, "packet_hash": "old"}])["records"][target]
        self.assertTrue(stale["stale"])
        human = {"id": target, "source": "human", "description": {"purpose": "Human text"}}
        both = apply_responses(packets, [human, ai])
        self.assertEqual(both["records"][target]["description"]["purpose"], "Human text")
        self.assertFalse(both["records"][target]["needs_review"])
        self.assertEqual(apply_responses(packets, [{**ai, "id": "dataset:missing"}])["unmatched_response_ids"], ["dataset:missing"])

    def test_invalid_responses_are_rejected(self):
        path = self.root / "bad.json"
        path.write_text(json.dumps([{"id": "x"}]), encoding="utf-8")
        with self.assertRaises(BusinessContextError):
            load_responses(path)

    def test_cli_writes_outputs_and_protects_existing(self):
        pipeline_path = self.root / "p.json"
        catalog_path = self.root / "c.json"
        pipeline_path.write_text(json.dumps(self.pipeline), encoding="utf-8")
        catalog_path.write_text(json.dumps(self.catalog), encoding="utf-8")
        output = self.root / "ctx.json"
        arguments = ["project", "context", "--pipeline-report", str(pipeline_path),
                     "--catalog-report", str(catalog_path), "--output", str(output),
                     "--markdown", str(self.root / "ctx.md"), "--prompts", str(self.root / "pr.json")]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(arguments), 0)
        self.assertTrue((self.root / "ctx.md").exists() and (self.root / "pr.json").exists())
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(main(arguments), 2)


if __name__ == "__main__":
    unittest.main()
