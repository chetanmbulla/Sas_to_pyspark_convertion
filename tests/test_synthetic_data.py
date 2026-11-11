import contextlib
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path

from sas2pyspark.cli import main
from sas2pyspark.synthetic_data import SyntheticDataError, generate_synthetic_data


class SyntheticDataTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)
        self.catalog = {
            "schema_version": 1,
            "generated_at": "2026-01-01T00:00:00+00:00",
            "resources": [
                {
                    "name": "ods.fact",
                    "kind": "sas_dataset",
                    "schema": {
                        "status": "partial",
                        "columns": [
                            {"name": "id", "type": "numeric", "length": 8},
                            {"name": "label", "type": "character", "length": 12},
                        ],
                    },
                },
                {
                    "name": "ods.unknown",
                    "kind": "sas_dataset",
                    "schema": {"status": "unknown", "columns": []},
                },
            ],
        }

    def test_generates_repeatable_rows_and_marks_partial_schema(self):
        first_directory = self.root / "first"
        second_directory = self.root / "second"
        first = generate_synthetic_data(
            self.catalog, first_directory, rows=4, seed=7
        )
        second = generate_synthetic_data(
            self.catalog, second_directory, rows=4, seed=7
        )

        self.assertEqual(first["summary"]["generated_count"], 1)
        self.assertEqual(first["summary"]["partial_schema_count"], 1)
        self.assertEqual(first["summary"]["skipped_count"], 1)
        generated = first["datasets"][0]
        self.assertEqual(generated["status"], "generated_partial_schema")
        self.assertEqual(
            (first_directory / generated["path"]).read_bytes(),
            (second_directory / second["datasets"][0]["path"]).read_bytes(),
        )
        with (first_directory / generated["path"]).open(
            encoding="utf-8", newline=""
        ) as stream:
            rows = list(csv.reader(stream))
        self.assertEqual(rows[0], ["id", "label"])
        self.assertEqual(len(rows), 5)
        self.assertEqual([row[0] for row in rows[1:]], ["1", "2", "3", "4"])

    def test_existing_files_are_preserved_without_force(self):
        output = self.root / "output"
        generate_synthetic_data(self.catalog, output, rows=1)
        manifest_path = output / "synthetic-data-manifest.json"
        manifest_before = manifest_path.read_bytes()

        with self.assertRaises(FileExistsError):
            generate_synthetic_data(self.catalog, output, rows=1)

        self.assertEqual(manifest_path.read_bytes(), manifest_before)

    def test_assumes_unknown_types_are_character_and_records_assumptions(self):
        catalog = {
            "schema_version": 1,
            "resources": [
                {
                    "name": "ods.bad",
                    "kind": "sas_dataset",
                    "schema": {
                        "status": "complete",
                        "columns": [
                            {"name": "id", "type": "numeric"},
                            {"name": "value", "type": "unknown"},
                            {"name": "missing_type"},
                        ],
                    },
                }
            ],
        }
        report = generate_synthetic_data(catalog, self.root / "unknown", rows=2)

        self.assertEqual(report["summary"]["generated_count"], 1)
        self.assertEqual(report["summary"]["assumed_type_dataset_count"], 1)
        record = report["datasets"][0]
        self.assertEqual(record["status"], "generated_partial_schema")
        self.assertEqual(record["assumed_character_columns"], ["value", "missing_type"])
        self.assertEqual([col["type"] for col in record["columns"]], [
            "numeric", "character", "character",
        ])
        with (self.root / "unknown" / record["path"]).open(
            encoding="utf-8", newline=""
        ) as stream:
            data = list(csv.reader(stream))
        self.assertEqual(data[0], ["id", "value", "missing_type"])
        self.assertEqual(len(data), 3)
        self.assertTrue(data[1][1].startswith("SYN_"))
        self.assertEqual(catalog["resources"][0]["schema"]["columns"][1]["type"], "unknown")

    def test_unsupported_types_and_missing_columns_remain_skipped(self):
        catalog = {
            "schema_version": 1,
            "resources": [
                {"name": "ods.bad", "kind": "sas_dataset", "schema": {
                    "status": "partial", "columns": [{"name": "value", "type": "binary"}]
                }},
                {"name": "ods.empty", "kind": "sas_dataset", "schema": {
                    "status": "unknown", "columns": []
                }},
            ],
        }
        report = generate_synthetic_data(catalog, self.root / "unsupported")
        self.assertEqual(report["summary"]["skipped_count"], 2)
        records = {record["dataset"]: record for record in report["datasets"]}
        self.assertIn("unsupported types", records["ods.bad"]["reason"])
        self.assertIn("no column definitions were discovered", records["ods.empty"]["reason"])
        self.assertIn("SAS files/ZIP", records["ods.empty"]["reason"])

    def test_cli_writes_manifest(self):
        catalog_path = self.root / "catalog.json"
        catalog_path.write_text(json.dumps(self.catalog), encoding="utf-8")
        output = self.root / "cli-output"
        with contextlib.redirect_stdout(io.StringIO()):
            code = main(
                [
                    "project",
                    "synthesize",
                    "--catalog",
                    str(catalog_path),
                    "--output-dir",
                    str(output),
                    "--rows",
                    "2",
                    "--seed",
                    "99",
                ]
            )

        self.assertEqual(code, 0)
        self.assertTrue((output / "synthetic-data-manifest.json").exists())

    def test_rejects_invalid_row_count(self):
        with self.assertRaises(SyntheticDataError):
            generate_synthetic_data(self.catalog, self.root / "invalid", rows=0)


if __name__ == "__main__":
    unittest.main()
