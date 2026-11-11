import tempfile
import unittest
from pathlib import Path

from sas2pyspark.discovery import discover_repository
from sas2pyspark.file_classification import build_file_classification


class FileClassificationTests(unittest.TestCase):
    def test_classifies_files_with_static_evidence_and_unknown_runtime(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "main.sas").write_text(
                "/* stock inventory refresh */\n"
                "%include 'macros.sas';\n"
                "%normalize();\n"
                "data work.stock_summary;\n"
                "  merge ods.stock ods.material;\n"
                "  by sku;\n"
                "run;\n",
                encoding="utf-8",
            )
            (root / "macros.sas").write_text(
                "%macro normalize();\n"
                "  %let source=ods.stock;\n"
                "%mend;\n",
                encoding="utf-8",
            )
            (root / "orphan.sas").write_text(
                "data _null_;\n  put 'hello';\nrun;\n", encoding="utf-8"
            )
            inventory = discover_repository(root)
            pipeline = {
                "entrypoints": [{"path": "main.sas", "server": "DI"}],
                "scripts": [
                    {"path": "main.sas", "server": "DI", "status": "reachable"},
                    {"path": "macros.sas", "server": "DI", "status": "reachable"},
                    {"path": "orphan.sas", "server": "BI", "status": "unreached"},
                ],
                "includes": [
                    {
                        "source": "main.sas",
                        "line": 2,
                        "reference": "macros.sas",
                        "target": "macros.sas",
                        "status": "resolved",
                    }
                ],
                "jobs": [
                    {
                        "name": "nightly-stock",
                        "entrypoint": "main.sas",
                        "schedule": "daily",
                        "depends_on": [],
                    }
                ],
            }
            catalog = {
                "resources": [
                    {
                        "name": "ods.stock",
                        "kind": "sas_dataset",
                        "evidence": [
                            {
                                "source": "main.sas",
                                "line": 6,
                                "direction": "input",
                            }
                        ],
                    },
                    {
                        "name": "work.stock_summary",
                        "kind": "sas_dataset",
                        "evidence": [
                            {
                                "source": "main.sas",
                                "line": 4,
                                "direction": "output",
                            }
                        ],
                    },
                ],
                "systems": [
                    {
                        "id": "system:ods",
                        "sources": ["main.sas"],
                    }
                ],
            }

            report = build_file_classification(root, inventory.programs, pipeline, catalog)
            files = {item["file_name"]: item for item in report["files"]}

            main = files["main.sas"]
            self.assertEqual(main["primary_technical_category"], "Data Integration / Merging")
            self.assertEqual(main["primary_business_domain"], "Inventory / Stock Management")
            self.assertEqual(main["primary_program_type"], "Main / Entry-Point Program")
            self.assertEqual(main["execution_mode"], "scheduled")
            self.assertEqual(main["execution_frequency"], ["daily"])
            self.assertEqual(main["execution_status"], "unknown")
            self.assertEqual(main["usage_classification"], "referenced_but_not_observed")
            self.assertEqual(main["review_status"], "review_required")
            self.assertEqual(main["input_datasets"], ["ods.stock"])
            self.assertEqual(main["output_datasets"], ["work.stock_summary"])
            self.assertEqual(main["dataset_evidence"][0]["line"], 6)
            self.assertEqual(main["external_systems"], ["system:ods"])
            self.assertEqual(main["include_dependencies"][0]["relationship"], "direct")

            macro = files["macros.sas"]
            self.assertEqual(macro["primary_program_type"], "Reusable Macro Library")
            self.assertEqual(macro["macro_definitions"], ["normalize"])
            self.assertEqual(macro["macro_callers"], ["main.sas"])

            orphan = files["orphan.sas"]
            self.assertEqual(orphan["primary_business_domain"], "Unknown / Unclassified")
            self.assertEqual(orphan["primary_program_type"], "Unknown / Unclassified")
            self.assertEqual(orphan["review_status"], "review_required")
            self.assertEqual(orphan["usage_classification"], "orphan_candidate")
            self.assertIsNone(orphan["last_execution_date"])
            self.assertEqual(report["summary"]["file_count"], 3)

    def test_fallback_entrypoints_are_not_authoritative(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "main.sas").write_text("options mprint;\n", encoding="utf-8")
            inventory = discover_repository(root)
            report = build_file_classification(
                root,
                inventory.programs,
                {
                    "entrypoints": [{"path": "main.sas", "server": "LOCAL"}],
                    "scripts": [{"path": "main.sas", "server": "LOCAL", "status": "reachable"}],
                },
                {"resources": [], "systems": []},
                configuration_source="all_scripts_fallback",
            )
            item = report["files"][0]
            self.assertEqual(item["primary_program_type"], "Unknown / Unclassified")
            self.assertEqual(item["entrypoint_status"], "fallback_assigned_not_authoritative")


if __name__ == "__main__":
    unittest.main()
