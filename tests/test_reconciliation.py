import csv
import importlib
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sas2pyspark import MigrationProject
from sas2pyspark.cli import main
from sas2pyspark.reconciliation import _reconcile_one


@unittest.skipUnless(
    importlib.util.find_spec("pyspark") is not None,
    "install the reconcile extra",
)
class ReconciliationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("sas2pyspark-reconciliation-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _write_csv(self, root, relative, header, rows):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(header)
            writer.writerows(rows)
        return path

    def _compare(
        self,
        root,
        reference_rows,
        spark_rows,
        *,
        keys=(),
        atol: float = 0.0,
        rtol: float = 0.0,
        header=("id", "amount", "label"),
    ):
        self._write_csv(root, "reference.csv", header, reference_rows)
        self._write_csv(root, "spark.csv", header, spark_rows)
        return _reconcile_one(
            self.spark,
            root,
            "work.results",
            "reference.csv",
            "spark.csv",
            tuple(keys),
            atol=atol,
            rtol=rtol,
            max_examples=10,
        )

    def test_keyed_comparison_ignores_row_order_and_applies_numeric_tolerance(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self._compare(
                Path(directory),
                [(1, 10.0, "A"), (2, 20.0, "B")],
                [(2, 20.0, "B"), (1, 10.05, "A")],
                keys=("id",),
                atol=0.1,
            )

        self.assertEqual(result.status, "pass")
        self.assertTrue(result.schema_match)
        self.assertEqual(result.reference_row_count, 2)
        self.assertEqual(result.spark_row_count, 2)
        self.assertEqual(result.differing_rows, 0)
        explanation = result.explanation
        assert explanation is not None
        self.assertIn("order", explanation)
        self.assertEqual(result.key_columns, ("id",))

    def test_keyed_comparison_reports_differences_and_missing_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self._compare(
                Path(directory),
                [(1, 10.0, "A"), (2, 20.0, "B")],
                [(1, 11.0, "A"), (3, 30.0, "C")],
                keys=("id",),
            )

        self.assertEqual(result.status, "fail")
        self.assertEqual(result.missing_from_reference, 1)
        self.assertEqual(result.missing_from_spark, 1)
        self.assertEqual(result.differing_rows, 1)
        self.assertTrue(
            any("amount" in item["differences"] for item in result.examples)
        )

    def test_unkeyed_comparison_is_an_unordered_multiset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = self._compare(
                root,
                [(1, 10.0, "A"), (1, 10.0, "A"), (2, 20.0, "B")],
                [(2, 20.0, "B"), (1, 10.0, "A"), (1, 10.0, "A")],
            )
            mismatch = self._compare(
                root,
                [(1, 10.0, "A"), (2, 20.0, "B")],
                [(1, 10.0, "A"), (3, 30.0, "C")],
            )

        self.assertEqual(result.status, "pass", result.error or result.explanation)
        self.assertEqual(
            result.explanation,
            "Row order is ignored; rows are compared as an exact multiset.",
        )
        self.assertEqual(mismatch.status, "fail")
        self.assertEqual(mismatch.differing_rows, 2)
        self.assertTrue(mismatch.examples)

    def test_tolerances_without_keys_are_inconclusive_and_duplicate_keys_are_inconclusive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            no_keys = self._compare(
                root,
                [(1, 10.0, "A")],
                [(1, 10.01, "A")],
                atol=0.1,
            )
            duplicates = self._compare(
                root,
                [(1, 10.0, "A"), (1, 20.0, "B")],
                [(1, 10.0, "A"), (1, 20.0, "B")],
                keys=("id",),
            )

        self.assertEqual(no_keys.status, "inconclusive")
        explanation = no_keys.explanation
        assert explanation is not None
        self.assertIn("require configured keys", explanation)
        self.assertEqual(duplicates.status, "inconclusive", duplicates.error)
        self.assertEqual(duplicates.reference_duplicate_key_groups, 1)
        duplicate_explanation = duplicates.explanation
        assert duplicate_explanation is not None
        self.assertIn("not unique", duplicate_explanation)

    def test_native_xport_reference_matches_spark_csv(self):
        try:
            pd = importlib.import_module("pandas")
            pyreadstat = importlib.import_module("pyreadstat")
        except ImportError:
            self.skipTest("install the reconcile extra for native SAS support")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reference = root / "reference.xpt"
            frame = pd.DataFrame(
                {"ID": [1, 2], "AMOUNT": [10.0, 20.0], "LABEL": ["A", "B"]}
            )
            pyreadstat.write_xport(frame, str(reference))
            self._write_csv(
                root,
                "spark.csv",
                ("id", "amount", "label"),
                [(2, 20.0, "B"), (1, 10.0, "A")],
            )
            result = _reconcile_one(
                self.spark,
                root,
                "work.results",
                "reference.xpt",
                "spark.csv",
                ("id",),
                atol=0,
                rtol=0,
                max_examples=10,
            )

        self.assertEqual(result.status, "pass", result.error or result.explanation)
        self.assertTrue(result.reference_sha256)
        self.assertEqual(result.reference_row_count, 2)

    def test_reconcile_cli_saves_comparison_settings_and_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory, "repository")
            root.mkdir()
            self._write_csv(
                root, "sas-reference.csv", ("id", "amount"), [(1, 10.0)]
            )
            self._write_csv(root, "spark.csv", ("id", "amount"), [(1, 10.02)])
            manifest = Path(directory, "migration.project.json")
            project = MigrationProject.create("Reconciliation CLI")
            project.save(manifest)
            report_path = Path(directory, "reconciliation.json")

            with patch.dict(
                os.environ,
                {
                    "HADOOP_USER_CLASSPATH_FIRST": "",
                    "HADOOP_CONF_DIR": "",
                },
            ):
                status = main(
                    [
                        "project",
                        "reconcile",
                        str(manifest),
                        "--root",
                        str(root),
                        "--reference",
                        "WORK.RESULTS=sas-reference.csv",
                        "--spark-output",
                        "work.results=spark.csv",
                        "--keys",
                        "work.results=id",
                        "--atol",
                        "0.05",
                        "--master",
                        "local[1]",
                        "--report",
                        str(report_path),
                    ]
                )
            self.assertEqual(self.spark.range(1).count(), 1)
            report = json.loads(report_path.read_text(encoding="utf-8"))

        self.assertEqual(status, 0)
        self.assertEqual(report["summary"]["pass"], 1)
        self.assertEqual(report["settings"]["absolute_tolerance"], 0.05)
        self.assertEqual(
            report["settings"]["ordering"],
            "per_dataset_keyed_or_unordered_multiset",
        )
        self.assertEqual(report["datasets"][0]["dataset"], "work.results")


if __name__ == "__main__":
    unittest.main()
