import unittest

from sas2pyspark.phase1_quality import build_phase1_quality_report


class Phase1QualityTests(unittest.TestCase):
    def test_fallback_analysis_reports_authority_and_schema_blockers(self):
        report = build_phase1_quality_report(
            configuration="all_scripts_fallback",
            pipeline_config={
                "servers": {"LOCAL": {}},
                "entrypoints": [{"path": "a.sas"}, {"path": "b.sas"}],
            },
            pipeline_report={
                "jobs": [{"jams": None}],
                "includes": [
                    {"status": "resolved"},
                    {"status": "unresolved"},
                    {"status": "runtime_generated"},
                    {"status": "waived"},
                ],
                "libraries": [
                    {"status": "mapped"},
                    {"status": "unresolved"},
                ],
                "summary": {
                    "unresolved_library_count": 1,
                    "unmapped_drives": [{"drive": "W"}],
                },
            },
            catalog_report={
                "resources": [
                    {"kind": "sas_dataset", "schema": {"status": "complete", "columns": [
                        {"name": "id", "type": "numeric"}
                    ]}},
                    {"kind": "sas_dataset", "schema": {"status": "unknown", "columns": []}},
                    {"kind": "sas_dataset", "schema": {"status": "partial", "columns": [
                        {"name": "value", "type": "unknown"}
                    ]}},
                ],
                "systems": [
                    {"status": "mapped"},
                    {"status": "unknown_endpoint"},
                ],
                "summary": {
                    "dynamic_reference_count": 2,
                    "unresolved_column_lineage_count": 3,
                },
            },
            phase1_index={"summaries": {"business_context": {"by_source": {"template": 4}}}},
        )
        self.assertEqual(report["assessment"], "not_certified")
        self.assertTrue(report["poc_assumptions"])
        self.assertIn("NA", report["missing_value_convention"])
        self.assertEqual(report["summary"]["dataset_count"], 3)
        self.assertEqual(report["summary"]["schema_status_counts"], {
            "complete": 1, "partial": 1, "unknown": 1,
        })
        self.assertEqual(report["summary"]["unknown_or_unsupported_column_count"], 1)
        gates = {item["id"]: item for item in report["gates"]}
        self.assertEqual(gates["pipeline_configuration"]["status"], "blocked")
        self.assertEqual(gates["jams_metadata"]["status"], "blocked")
        self.assertEqual(gates["include_resolution"]["status"], "blocked")
        self.assertEqual(gates["authoritative_schemas"]["status"], "blocked")
        self.assertEqual(gates["external_systems"]["status"], "blocked")
        self.assertEqual(gates["business_context"]["status"], "review_required")
        self.assertEqual(
            gates["jams_metadata"]["unavailable_fields"][0],
            {"field": "jams_schedules", "value": "NA"},
        )
        self.assertEqual(
            gates["authoritative_schemas"]["unavailable_fields"][0]["value"],
            "NA",
        )
        self.assertIn("poc_assumption", gates["include_resolution"])

    def test_uploaded_config_does_not_claim_independent_verification(self):
        report = build_phase1_quality_report(
            configuration="uploaded",
            pipeline_config={
                "servers": {"DI": {}},
                "entrypoints": [{"path": "job.sas"}],
                "jams_jobs": [],
            },
            pipeline_report={
                "jobs": [],
                "includes": [],
                "libraries": [],
                "summary": {"unresolved_library_count": 0, "unmapped_drives": []},
            },
            catalog_report={
                "resources": [],
                "systems": [],
                "summary": {
                    "dynamic_reference_count": 0,
                    "unresolved_column_lineage_count": 0,
                },
            },
            phase1_index={"summaries": {}},
        )
        gates = {item["id"]: item for item in report["gates"]}
        self.assertEqual(gates["pipeline_configuration"]["status"], "review_required")
        self.assertIn("not independently verified", gates["pipeline_configuration"]["evidence"])
        self.assertEqual(gates["jams_metadata"]["status"], "blocked")
        self.assertEqual(gates["authoritative_schemas"]["status"], "review_required")
        self.assertEqual(gates["authoritative_schemas"]["unavailable_fields"], [])


if __name__ == "__main__":
    unittest.main()
