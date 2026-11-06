import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

from sas2pyspark import SasToPySparkConverter


HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None
FIXTURES = Path(__file__).parent / "fixtures" / "parity"


def _java_major_version():
    java_home = os.environ.get("JAVA_HOME")
    java = Path(java_home, "bin", "java.exe") if java_home else None
    executable = str(java) if java and java.is_file() else shutil.which("java")
    if executable is None:
        return None
    try:
        result = subprocess.run(
            [executable, "-version"],
            capture_output=True,
            check=False,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    output = result.stdout + result.stderr
    match = re.search(r'version "(?:1\.)?(\d+)', output)
    return int(match.group(1)) if match else None


@unittest.skipUnless(HAS_PYSPARK, "install the 'test' extra to run semantic fixtures")
class SemanticFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import pyspark

        java_version = _java_major_version()
        spark_major = int(pyspark.__version__.split(".", maxsplit=1)[0])
        required_java = 17 if spark_major >= 4 else 8
        if java_version is None:
            raise unittest.SkipTest("a Java runtime is required for Spark integration tests")
        if java_version < required_java:
            raise unittest.SkipTest(
                f"PySpark {pyspark.__version__} requires Java {required_java}+; "
                f"found Java {java_version}"
            )

        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("sas2pyspark-semantic-fixtures")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_curated_and_sas_reference_fixtures(self):
        fixtures = sorted(FIXTURES.glob("*.json"))
        self.assertTrue(fixtures, "at least one semantic fixture is required")
        for path in fixtures:
            with self.subTest(fixture=path.name):
                fixture = json.loads(path.read_text(encoding="utf-8"))
                self.assertIn(fixture["reference_origin"], {"curated", "sas_runtime"})
                namespace = {"spark": self.spark}
                for name, dataset in fixture["inputs"].items():
                    namespace[f"df_{name}"] = self.spark.createDataFrame(
                        dataset["rows"], dataset["schema"]
                    )

                result = SasToPySparkConverter().convert(fixture["source"])
                exec(compile(result.code, f"<{fixture['name']}>", "exec"), namespace)

                for name, expected in fixture["outputs"].items():
                    actual = namespace[f"df_{name}"]
                    self.assertEqual(actual.columns, expected["columns"])
                    self.assertEqual(
                        [field.dataType.simpleString() for field in actual.schema.fields],
                        expected["types"],
                    )
                    rows = [
                        list(row)
                        for row in actual.collect()
                    ]
                    self.assertEqual(
                        sorted(rows, key=repr),
                        sorted(expected["rows"], key=repr),
                    )
