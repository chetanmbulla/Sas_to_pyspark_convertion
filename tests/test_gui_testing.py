import importlib.util
import shutil
import textwrap
import unittest

from sas2pyspark.gui_testing import performance_notes, run_pyspark_test


HAS_SPARK_RUNTIME = (
    importlib.util.find_spec("pyspark") is not None
    and (shutil.which("java") is not None)
)


class GuiTestingTests(unittest.TestCase):
    def test_performance_notes_find_driver_actions_and_single_partition(self):
        notes = performance_notes(
            textwrap.dedent(
                """
            # df.collect() in a comment should not count.
            rows = frame.collect()
            local = frame.toPandas()
            output = frame.repartition(1)
            """
            )
        )

        self.assertEqual(len(notes), 3)
        self.assertTrue(any("collect()" in note for note in notes))
        self.assertTrue(any("toPandas()" in note for note in notes))
        self.assertTrue(any("repartition(1)" in note for note in notes))

    def test_spark_test_rejects_invalid_python_before_execution(self):
        with self.assertRaises(SyntaxError):
            run_pyspark_test("", "if:", [])

    def test_spark_test_rejects_ambiguous_uploaded_dataset_names(self):
        with self.assertRaisesRegex(ValueError, "Multiple uploaded files"):
            run_pyspark_test(
                "",
                "print('test')",
                [("people.csv", b"id\n1\n"), ("people.json", b'{"id": 1}\n')],
            )

    @unittest.skipUnless(HAS_SPARK_RUNTIME, "PySpark and Java are required")
    def test_spark_test_executes_code_and_returns_csv_preview(self):
        result = run_pyspark_test(
            "data output; set people; run;",
            """
from pyspark.sql import functions as F
df_adults = df_people.filter(F.col("age") >= 18)
df_adults.write.mode("overwrite").option("header", True).csv("adults.csv")
""",
            [("people.csv", b"id,age\n1,17\n2,30\n")],
            timeout_seconds=120,
            master="local[1]",
            shuffle_partitions=1,
        )

        self.assertEqual(result.status, "succeeded", result.stderr)
        self.assertTrue(result.code_sha256)
        self.assertEqual(len(result.output_previews), 1)
        self.assertIn("2,30", result.output_previews[0][1])


if __name__ == "__main__":
    unittest.main()
