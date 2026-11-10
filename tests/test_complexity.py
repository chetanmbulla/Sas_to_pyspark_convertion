import json
import tempfile
import unittest
from pathlib import Path

from sas2pyspark import complexity as cx


def _factors(**overrides):
    values = dict(
        plan_score=0,
        conversion_adjustment=0,
        external_connections=0,
        remote_blocks=0,
        uncertain_schemas=0,
        derived_columns=0,
        unresolved_findings=0,
        weights=cx.load_weights(None)["script"],
    )
    values.update(overrides)
    return cx.script_factors(**values)


class ComplexityModelTests(unittest.TestCase):
    def test_factors_are_capped_and_attributed(self):
        factors = _factors(remote_blocks=10, plan_score=5)
        self.assertEqual(factors["remote_blocks"]["points"], 8)
        self.assertEqual(factors["remote_blocks"]["raw"], 10)
        self.assertEqual(factors["static_plan"]["points"], 5)

    def test_bands(self):
        self.assertEqual(cx.band(10), "low")
        self.assertEqual(cx.band(30), "medium")
        self.assertEqual(cx.band(60), "high")
        self.assertEqual(cx.band(90), "very_high")

    def test_weights_override_and_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "w.json"
            path.write_text(json.dumps({"script": {"remote_blocks": {"weight": 1}}}))
            self.assertEqual(cx.load_weights(path)["script"]["remote_blocks"]["weight"], 1.0)
            path.write_text(json.dumps({"script": {"bogus": {"weight": 1}}}))
            with self.assertRaises(cx.ComplexityError):
                cx.load_weights(path)
            path.write_text(json.dumps({"script": {"remote_blocks": {"weight": -1}}}))
            with self.assertRaises(cx.ComplexityError):
                cx.load_weights(path)

    def test_project_score_is_size_weighted_with_confidence(self):
        weights = cx.load_weights(None)["project"]
        catalog = {
            "resources": [
                {"kind": "sas_dataset", "schema": {"status": "unknown"}},
                {"kind": "sas_dataset", "schema": {"status": "inferred"}},
            ],
            "column_lineage": [{"resolved": False}],
            "systems": [{}] * 10,
        }
        result = cx.project_factors(
            script_scores=[(10, 100), (50, 1)],
            pipeline_report={"summary": {"unresolved_library_count": 4}, "issues": []},
            catalog_report=catalog,
            attempted=1,
            weights=weights,
        )
        self.assertAlmostEqual(result["size_weighted_script_score"], 10.4, places=1)
        self.assertEqual(result["factors"]["unmapped_libraries"]["points"], 2)
        self.assertEqual(result["factors"]["unknown_schema_ratio"]["points"], 5)
        self.assertLess(result["confidence"], 1.0)
        self.assertGreater(result["score"], result["size_weighted_script_score"])

    def test_evidence_counts_signals(self):
        evidence = cx.script_evidence(
            {
                "external_connections": [{"source": "A/x.sas"}],
                "remote_references": [{"source": "a/x.sas"}],
                "issues": [{"source": "a\\x.sas"}],
            },
            {
                "resources": [
                    {
                        "kind": "sas_dataset",
                        "schema": {"status": "unknown"},
                        "evidence": [{"source": "a/x.sas"}, {"source": "a/x.sas"}],
                    }
                ],
                "column_lineage": [
                    {"source": "a/x.sas", "transform": "expression"},
                    {"source": "a/x.sas", "transform": "copy"},
                ],
            },
        )["a/x.sas"]
        self.assertEqual(
            evidence,
            {"connections": 1, "remote": 1, "uncertain": 1, "derived": 1, "findings": 1},
        )

    def test_calibration_fits_hours(self):
        programs = [{"path": f"s{i}.sas", "complexity_score": i * 10} for i in range(1, 5)]
        actuals = {f"s{i}.sas": i * 2.0 for i in range(1, 5)}
        result = cx.calibrate(programs, actuals)
        self.assertEqual(result["spearman"], 1.0)
        self.assertEqual(result["hours_per_point"], 0.2)
        self.assertEqual(cx.calibrate(programs[:2], actuals)["matched"], 2)

    def test_read_actuals_validates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "a.csv"
            path.write_text("path,hours\nA\\b.sas,3.5\n")
            self.assertEqual(cx.read_actuals(path), {"a/b.sas": 3.5})
            path.write_text("path,hours\nx.sas,abc\n")
            with self.assertRaises(cx.ComplexityError):
                cx.read_actuals(path)


if __name__ == "__main__":
    unittest.main()
