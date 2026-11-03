"""Worker process that initializes Spark and runs one converted program."""

from __future__ import annotations

import argparse
import json
import os
import re
import runpy
from pathlib import Path
from typing import Any


_ENVIRONMENT_REFERENCE = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def _spark_setting_value(value: str) -> str:
    match = _ENVIRONMENT_REFERENCE.fullmatch(value)
    if match is None:
        return value
    variable = match.group(1)
    try:
        return os.environ[variable]
    except KeyError as error:
        raise ValueError(
            f"Spark setting references unset environment variable {variable!r}."
        ) from error


def _read_input(spark: Any, path: Path) -> Any:
    suffix = path.suffix.casefold()
    if suffix == ".csv":
        return (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .option("mode", "FAILFAST")
            .csv(str(path))
        )
    if suffix == ".parquet":
        return spark.read.parquet(str(path))
    if suffix in {".json", ".jsonl"}:
        return spark.read.json(str(path))
    raise ValueError(
        f"Unsupported input format for {str(path)!r}; use CSV, Parquet, or JSON."
    )


def run_program(script: Path, config_path: Path) -> None:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(config["master"])
        .appName(config["app_name"])
        .config("spark.ui.enabled", "false")
    )
    for key, value in config["spark_conf"].items():
        builder = builder.config(key, _spark_setting_value(value))
    spark = builder.getOrCreate()
    namespace: dict[str, Any] = {"spark": spark, "inputs": {}}
    try:
        for item in config["inputs"]:
            dataset = item["dataset"]
            path = Path(item["path"])
            frame = _read_input(spark, path)
            table_name = dataset.rsplit(".", maxsplit=1)[-1].casefold()
            frame.createOrReplaceTempView(table_name)
            namespace[f"df_{table_name}"] = frame
            namespace["inputs"][dataset] = frame
        runpy.run_path(str(script), init_globals=namespace, run_name="__main__")
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--script", required=True)
    parser.add_argument("--config", required=True)
    arguments = parser.parse_args(argv)
    run_program(Path(arguments.script), Path(arguments.config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
