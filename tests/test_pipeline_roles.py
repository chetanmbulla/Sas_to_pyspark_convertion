import unittest

from sas2pyspark.pipeline_roles import classify_resources, classify_scripts, summarize_roles


def _evidence(source, direction):
    return {"source": source, "line": 1, "operation": "x", "direction": direction}


class PipelineRoleTests(unittest.TestCase):
    def setUp(self):
        self.report = {
            "entrypoints": [{"path": "jobs/main.sas", "server": "DI"}],
            "scripts": [
                {"path": "jobs/main.sas", "server": "DI", "status": "reachable"},
                {"path": "lib/load.sas", "server": "DI", "status": "reachable"},
                {"path": "lib/util.sas", "server": "DI", "status": "reachable"},
                {"path": "lib/stage.sas", "server": "DI", "status": "reachable"},
                {"path": "lib/report.sas", "server": "DI", "status": "reachable"},
                {"path": "lib/load_old.sas", "server": "DI", "status": "unreached"},
                {"path": "lib/other.sas", "server": "DI", "status": "unreached"},
            ],
            "includes": [
                {"source": "jobs/main.sas", "target": "lib/load.sas", "status": "resolved"},
            ],
        }
        self.resources = [
            {"name": "ods.raw", "kind": "sas_dataset", "evidence": [_evidence("lib/load.sas", "input")]},
            {
                "name": "ods.clean",
                "kind": "sas_dataset",
                "evidence": [_evidence("lib/load.sas", "output"), _evidence("lib/report.sas", "input")],
            },
            {"name": "ods.final", "kind": "sas_dataset", "evidence": [_evidence("lib/load.sas", "output")]},
            {"name": "work.tmp", "kind": "sas_dataset", "evidence": [_evidence("lib/stage.sas", "output")]},
            {"name": "out.csv", "kind": "file", "evidence": [_evidence("lib/report.sas", "output")]},
        ]

    def test_script_roles_and_activity(self):
        scripts = {s["path"]: s for s in classify_scripts(self.report, self.resources)}
        self.assertEqual(scripts["jobs/main.sas"]["role"], "entrypoint")
        self.assertEqual(scripts["lib/load.sas"]["role"], "included")
        self.assertEqual(scripts["lib/load.sas"]["activity"], "producer")
        self.assertEqual(scripts["lib/stage.sas"]["activity"], "work_only")
        self.assertEqual(scripts["lib/report.sas"]["activity"], "producer")
        self.assertEqual(scripts["lib/util.sas"]["activity"], "no_io")
        self.assertEqual(scripts["lib/load_old.sas"]["activity"], "unreached")
        self.assertTrue(scripts["lib/load_old.sas"]["suspect_name"])
        self.assertFalse(scripts["lib/other.sas"]["suspect_name"])

    def test_resource_roles_and_ultimate_sources(self):
        edges = {"ods.clean": {"ods.raw"}, "ods.final": {"ods.clean"}}
        roles = {r["name"]: r for r in classify_resources(self.resources, edges)}
        self.assertEqual(roles["ods.raw"]["role"], "ultimate_source")
        self.assertEqual(roles["ods.clean"]["role"], "intermediate")
        self.assertEqual(roles["ods.final"]["role"], "final_output")
        self.assertEqual(roles["ods.final"]["ultimate_sources"], ["ods.raw"])
        self.assertTrue(roles["work.tmp"]["temporary"])
        self.assertEqual(roles["out.csv"]["role"], "final_output")

    def test_summary(self):
        scripts = classify_scripts(self.report, self.resources)
        resources = classify_resources(self.resources, {})
        summary = summarize_roles(scripts, resources)
        self.assertEqual(summary["unused_script_count"], 2)
        self.assertEqual(summary["suspect_unused_script_count"], 1)
        self.assertEqual(summary["temporary_dataset_count"], 1)


if __name__ == "__main__":
    unittest.main()
