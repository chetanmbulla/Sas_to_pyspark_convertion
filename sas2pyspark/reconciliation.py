"""Semantic and data reconciliation for SAS reference and Spark outputs."""

from __future__ import annotations

import hashlib
import importlib
import json
import math
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .execution_workflow import _normalized_relative, _resolve_inside
from .project import MigrationProject


RECONCILIATION_SCHEMA_VERSION = 1
_DATASET_NAME = re.compile(r"^(?:[A-Za-z_]\w*\.)?[A-Za-z_]\w*$")


class ReconciliationError(ValueError):
    """Raised when reconciliation options or paths are invalid."""


@dataclass(frozen=True)
class DatasetReconciliation:
    dataset: str
    status: str
    reference_path: str
    spark_output_path: str
    reference_sha256: str | None
    spark_output_sha256: str | None
    schema_match: bool | None
    reference_schema: tuple[Mapping[str, str], ...]
    spark_schema: tuple[Mapping[str, str], ...]
    reference_row_count: int | None
    spark_row_count: int | None
    reference_null_counts: Mapping[str, int]
    spark_null_counts: Mapping[str, int]
    key_columns: tuple[str, ...]
    reference_duplicate_key_groups: int | None
    spark_duplicate_key_groups: int | None
    missing_from_reference: int | None
    missing_from_spark: int | None
    differing_rows: int | None
    differing_row_count_delta: int | None
    examples: tuple[Mapping[str, Any], ...] = ()
    explanation: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "status": self.status,
            "reference_path": self.reference_path,
            "spark_output_path": self.spark_output_path,
            "reference_sha256": self.reference_sha256,
            "spark_output_sha256": self.spark_output_sha256,
            "schema_match": self.schema_match,
            "reference_schema": [dict(field) for field in self.reference_schema],
            "spark_schema": [dict(field) for field in self.spark_schema],
            "reference_row_count": self.reference_row_count,
            "spark_row_count": self.spark_row_count,
            "reference_null_counts": dict(self.reference_null_counts),
            "spark_null_counts": dict(self.spark_null_counts),
            "key_columns": list(self.key_columns),
            "reference_duplicate_key_groups": self.reference_duplicate_key_groups,
            "spark_duplicate_key_groups": self.spark_duplicate_key_groups,
            "missing_from_reference": self.missing_from_reference,
            "missing_from_spark": self.missing_from_spark,
            "differing_rows": self.differing_rows,
            "differing_row_count_delta": self.differing_row_count_delta,
            "examples": [dict(example) for example in self.examples],
            "explanation": self.explanation,
            "error": self.error,
        }


@dataclass(frozen=True)
class ReconciliationReport:
    reconciliation_id: str
    project_id: str
    project_name: str
    repository_root: str
    generated_at: str
    relative_tolerance: float
    absolute_tolerance: float
    ordering: str
    key_columns: Mapping[str, tuple[str, ...]]
    max_examples: int
    spark_master: str
    spark_conf: Mapping[str, str]
    datasets: tuple[DatasetReconciliation, ...]
    execution_run_id: str | None = None
    schema_version: int = RECONCILIATION_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        counts = {
            status: sum(item.status == status for item in self.datasets)
            for status in ("pass", "fail", "inconclusive")
        }
        return {
            "schema_version": self.schema_version,
            "reconciliation_id": self.reconciliation_id,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "repository_root": self.repository_root,
            "generated_at": self.generated_at,
            "execution_run_id": self.execution_run_id,
            "settings": {
                "relative_tolerance": self.relative_tolerance,
                "absolute_tolerance": self.absolute_tolerance,
                "ordering": self.ordering,
                "key_columns": {
                    key: list(value)
                    for key, value in sorted(self.key_columns.items())
                },
                "max_examples": self.max_examples,
                "spark_master": self.spark_master,
                "spark_conf": dict(sorted(self.spark_conf.items())),
            },
            "datasets": [item.to_dict() for item in self.datasets],
            "summary": {
                "dataset_count": len(self.datasets),
                "pass": counts["pass"],
                "fail": counts["fail"],
                "inconclusive": counts["inconclusive"],
            },
        }

    def save(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )


def _assignments(values: Sequence[str], field: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        dataset, separator, path = value.partition("=")
        if not separator or not dataset or not path:
            raise ReconciliationError(f"{field} must use DATASET=PATH syntax.")
        if not _DATASET_NAME.fullmatch(dataset):
            raise ReconciliationError(f"Invalid dataset name in {field}: {dataset!r}.")
        canonical = dataset.casefold()
        if canonical in result:
            raise ReconciliationError(
                f"{field} contains duplicate dataset {dataset!r}."
            )
        result[canonical] = path
    return result


def _key_assignments(values: Sequence[str]) -> dict[str, tuple[str, ...]]:
    raw = _assignments(values, "--keys")
    result: dict[str, tuple[str, ...]] = {}
    for dataset, columns in raw.items():
        keys = tuple(item.strip().casefold() for item in columns.split(","))
        if not keys or any(not item for item in keys) or len(set(keys)) != len(keys):
            raise ReconciliationError(
                f"Keys for dataset {dataset!r} must be unique comma-separated column names."
            )
        result[dataset] = keys
    return result


def _sha256_path(path: Path) -> str:
    if path.is_symlink():
        raise ReconciliationError(f"Dataset path cannot be a symbolic link: {path}.")
    digest = hashlib.sha256()
    files = [path] if path.is_file() else sorted(
        item for item in path.rglob("*") if item.is_file()
    )
    if not files:
        raise ReconciliationError(f"Dataset path contains no files: {path}.")
    for item in files:
        if item.is_symlink():
            raise ReconciliationError(
                f"Dataset contains a symbolic link and cannot be hashed safely: {item}."
            )
        if item.name.startswith("_") or item.name.startswith("."):
            continue
        digest.update(item.relative_to(path).as_posix().encode("utf-8") if path.is_dir() else b"")
        with item.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def _type_family(data_type: Any) -> str:
    name = type(data_type).__name__.casefold()
    if name in {
        "byteType".casefold(),
        "shortType".casefold(),
        "integerType".casefold(),
        "longType".casefold(),
        "floatType".casefold(),
        "doubleType".casefold(),
        "decimalType".casefold(),
    }:
        return "numeric"
    if name == "stringtype":
        return "string"
    if name == "booleantype":
        return "boolean"
    if name == "datetype":
        return "date"
    if name in {"timestamptype", "timestampntztype"}:
        return "timestamp"
    if name == "binarytype":
        return "binary"
    return name


def _read_dataset(spark: Any, path: Path) -> Any:
    suffix = path.suffix.casefold()
    if suffix in {".sas7bdat", ".xpt"}:
        try:
            pyreadstat = importlib.import_module("pyreadstat")
        except ImportError as error:
            raise ReconciliationError(
                "Native SAS input requires pyreadstat. Install with "
                "`py -m pip install -e \".[reconcile]\"`."
            ) from error
        if suffix == ".sas7bdat":
            pandas_frame, _ = pyreadstat.read_sas7bdat(str(path))
        else:
            pandas_frame, _ = pyreadstat.read_xport(str(path))
        return spark.createDataFrame(pandas_frame)
    if suffix == ".parquet" or (path.is_dir() and suffix == ".parquet"):
        return spark.read.parquet(str(path))
    if suffix in {".json", ".jsonl"}:
        return spark.read.json(str(path))
    if suffix == ".csv" or path.is_dir():
        return (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .option("mode", "FAILFAST")
            .csv(str(path))
        )
    raise ReconciliationError(
        f"Unsupported dataset format for {str(path)!r}; use CSV, Parquet, JSON, "
        "SAS7BDAT, or XPORT."
    )


def _canonicalize(dataframe: Any) -> tuple[Any, dict[str, Any], tuple[dict[str, str], ...]]:
    fields = dataframe.schema.fields
    canonical: dict[str, Any] = {}
    schema: list[dict[str, str]] = []
    for field in fields:
        name = field.name.casefold()
        if name in canonical:
            raise ReconciliationError(
                f"Column names are ambiguous after case normalization: {field.name!r}."
            )
        canonical[name] = field.dataType
        schema.append(
            {
                "name": field.name,
                "canonical_name": name,
                "type": field.dataType.simpleString(),
                "type_family": _type_family(field.dataType),
                "nullable": str(field.nullable).lower(),
            }
        )
    frame = dataframe.select(
        *[dataframe[field.name].alias(field.name.casefold()) for field in fields]
    )
    return frame, canonical, tuple(schema)


def _column(alias: str, name: str, functions: Any) -> Any:
    escaped = name.replace("`", "``")
    return functions.col(f"{alias}.`{escaped}`")


def _null_counts(frame: Any, columns: Sequence[str], functions: Any) -> dict[str, int]:
    if not columns:
        return {}
    values = frame.agg(
        *[
            functions.sum(functions.when(frame[name].isNull(), 1).otherwise(0)).alias(name)
            for name in columns
        ]
    ).first()
    return {name: int(values[name] or 0) for name in columns}


def _schema_match(
    reference_types: Mapping[str, Any],
    spark_types: Mapping[str, Any],
) -> bool:
    return (
        set(reference_types) == set(spark_types)
        and all(
            _type_family(reference_types[name]) == _type_family(spark_types[name])
            for name in reference_types
        )
    )


def _value_equal(
    left: Any, right: Any, family: str, functions: Any, atol: float, rtol: float
) -> Any:
    left_column = left
    right_column = right
    null_equal = left_column.isNull() & right_column.isNull()
    one_null = left_column.isNull() | right_column.isNull()
    if family == "numeric":
        if atol == 0 and rtol == 0:
            return left_column.eqNullSafe(right_column)
        left_double = left_column.cast("double")
        right_double = right_column.cast("double")
        both_nan = functions.isnan(left_double) & functions.isnan(right_double)
        within_tolerance = functions.abs(left_double - right_double) <= (
            functions.lit(atol)
            + functions.lit(rtol) * functions.abs(left_double)
        )
        return (
            null_equal
            | (
                ~one_null
                & (both_nan | (~functions.isnan(left_double) & ~functions.isnan(right_double) & within_tolerance))
            )
        )
    return left_column.eqNullSafe(right_column)


def _empty_result(
    dataset: str,
    reference_path: str,
    spark_output_path: str,
    keys: tuple[str, ...],
    *,
    error: str,
) -> DatasetReconciliation:
    return DatasetReconciliation(
        dataset=dataset,
        status="inconclusive",
        reference_path=reference_path,
        spark_output_path=spark_output_path,
        reference_sha256=None,
        spark_output_sha256=None,
        schema_match=None,
        reference_schema=(),
        spark_schema=(),
        reference_row_count=None,
        spark_row_count=None,
        reference_null_counts={},
        spark_null_counts={},
        key_columns=keys,
        reference_duplicate_key_groups=None,
        spark_duplicate_key_groups=None,
        missing_from_reference=None,
        missing_from_spark=None,
        differing_rows=None,
        differing_row_count_delta=None,
        error=error,
    )


def _reconcile_one(
    spark: Any,
    root: Path,
    dataset: str,
    reference_relative: str,
    spark_relative: str,
    keys: tuple[str, ...],
    *,
    atol: float,
    rtol: float,
    max_examples: int,
) -> DatasetReconciliation:
    try:
        reference_path = _resolve_inside(root, reference_relative, "reference path")
        spark_path = _resolve_inside(root, spark_relative, "Spark output path")
        if not reference_path.exists():
            raise ReconciliationError(f"Reference output does not exist: {reference_relative!r}.")
        if not spark_path.exists():
            raise ReconciliationError(f"Spark output does not exist: {spark_relative!r}.")
        reference_hash = _sha256_path(reference_path)
        spark_hash = _sha256_path(spark_path)
        reference_raw = _read_dataset(spark, reference_path)
        spark_raw = _read_dataset(spark, spark_path)
        reference, reference_types, reference_schema = _canonicalize(reference_raw)
        actual, spark_types, spark_schema = _canonicalize(spark_raw)
        schema_matches = _schema_match(reference_types, spark_types)
        reference_count = reference.count()
        actual_count = actual.count()
        functions = __import__("pyspark.sql.functions", fromlist=["functions"])
        reference_nulls = _null_counts(
            reference, sorted(reference_types), functions
        )
        actual_nulls = _null_counts(
            actual, sorted(spark_types), functions
        )
        common_columns = sorted(set(reference_types) & set(spark_types))
        keys = tuple(key.casefold() for key in keys)
        unknown_keys = set(keys).difference(common_columns)
        if unknown_keys:
            raise ReconciliationError(
                f"Configured keys are absent from one or both schemas: "
                f"{', '.join(sorted(unknown_keys))}."
            )
        if keys and any(
            _type_family(reference_types[key]) != _type_family(spark_types[key])
            for key in keys
        ):
            raise ReconciliationError("Configured key columns have incompatible types.")

        schema_differences = not schema_matches
        row_count_difference = reference_count != actual_count
        examples: list[Mapping[str, Any]] = []
        duplicate_reference: int | None = None
        duplicate_actual: int | None = None
        missing_reference: int | None = None
        missing_actual: int | None = None
        differing_rows: int | None = None
        explanation: str | None = None
        marker = "__sas2pyspark_presence__"
        while marker in reference_types or marker in spark_types:
            marker += "_"

        if keys:
            duplicate_reference = (
                reference.groupBy(*keys)
                .agg(functions.count(functions.lit(1)).alias("__key_count__"))
                .filter(functions.col("__key_count__") > 1)
                .count()
            )
            duplicate_actual = (
                actual.groupBy(*keys)
                .agg(functions.count(functions.lit(1)).alias("__key_count__"))
                .filter(functions.col("__key_count__") > 1)
                .count()
            )
            if duplicate_reference or duplicate_actual:
                explanation = (
                    "Configured keys are not unique; one-to-one value comparison "
                    "is ambiguous."
                )
                ref_keys = (
                    reference.select(*keys)
                    .distinct()
                    .withColumn(marker, functions.lit(True))
                    .alias("r")
                )
                actual_keys = (
                    actual.select(*keys)
                    .distinct()
                    .withColumn(marker, functions.lit(True))
                    .alias("s")
                )
                join_condition = None
                for key in keys:
                    condition = _column("r", key, functions).eqNullSafe(
                        _column("s", key, functions)
                    )
                    join_condition = condition if join_condition is None else join_condition & condition
                if join_condition is not None:
                    key_join = ref_keys.alias("r").join(
                        actual_keys.alias("s"), join_condition, "full_outer"
                    )
                    missing_reference = key_join.filter(
                        _column("r", marker, functions).isNull()
                    ).count()
                    missing_actual = key_join.filter(
                        _column("s", marker, functions).isNull()
                    ).count()
            else:
                left = reference.withColumn(marker, functions.lit(True)).alias("r")
                right = actual.withColumn(marker, functions.lit(True)).alias("s")
                join_condition = None
                for key in keys:
                    condition = _column("r", key, functions).eqNullSafe(
                        _column("s", key, functions)
                    )
                    join_condition = condition if join_condition is None else join_condition & condition
                if join_condition is None:
                    raise ReconciliationError("Keyed comparison requires at least one key.")
                joined = left.join(right, join_condition, "full_outer")
                ref_present = _column("r", marker, functions).isNotNull()
                actual_present = _column("s", marker, functions).isNotNull()
                missing_actual = joined.filter(~ref_present).count()
                missing_reference = joined.filter(~actual_present).count()
                comparisons = {
                    column: _value_equal(
                        _column("r", column, functions),
                        _column("s", column, functions),
                        _type_family(reference_types[column]),
                        functions,
                        atol,
                        rtol,
                    )
                    for column in common_columns
                    if column not in keys
                    and _type_family(reference_types[column])
                    == _type_family(spark_types[column])
                }
                differences = [
                    (~comparison) & ref_present & actual_present
                    for comparison in comparisons.values()
                ]
                if differences:
                    any_difference = differences[0]
                    for difference in differences[1:]:
                        any_difference = any_difference | difference
                    differing_rows = joined.filter(any_difference).count()
                else:
                    differing_rows = 0
                needs_example = (
                    (~ref_present)
                    | (~actual_present)
                    | (any_difference if differences else functions.lit(False))
                )
                selected_columns = [
                    functions.coalesce(
                        _column("r", key, functions),
                        _column("s", key, functions),
                    ).alias(f"key__{key}")
                    for key in keys
                ]
                selected_columns.extend(
                    [
                        ref_present.alias("__in_reference"),
                        actual_present.alias("__in_spark"),
                    ]
                )
                for column in comparisons:
                    selected_columns.extend(
                        [
                            _column("r", column, functions).alias(f"reference__{column}"),
                            _column("s", column, functions).alias(f"spark__{column}"),
                            (~comparisons[column]).alias(f"different__{column}"),
                        ]
                    )
                for row in joined.filter(needs_example).select(*selected_columns).limit(max_examples).collect():
                    values = row.asDict(recursive=True)
                    examples.append(
                        {
                            "keys": {
                                key: values.get(f"key__{key}") for key in keys
                            },
                            "in_reference": values["__in_reference"],
                            "in_spark": values["__in_spark"],
                            "differences": {
                                column: {
                                    "reference": values.get(f"reference__{column}"),
                                    "spark": values.get(f"spark__{column}"),
                                }
                                for column in comparisons
                                if values.get(f"different__{column}")
                            },
                        }
                    )
        else:
            if not common_columns:
                explanation = "No common columns are available for unordered row comparison."
            elif atol > 0 or rtol > 0:
                explanation = (
                    "Numeric tolerances require configured keys to align rows; "
                    "unordered tolerant matching is ambiguous."
                )
            else:
                left_counts = reference.groupBy(*common_columns).agg(
                    functions.count(functions.lit(1)).alias("__row_count__")
                ).alias("r")
                right_counts = actual.groupBy(*common_columns).agg(
                    functions.count(functions.lit(1)).alias("__row_count__")
                ).alias("s")
                join_condition = None
                for column in common_columns:
                    condition = _column("r", column, functions).eqNullSafe(
                        _column("s", column, functions)
                    )
                    join_condition = condition if join_condition is None else join_condition & condition
                if join_condition is None:
                    raise ReconciliationError(
                        "Unordered comparison requires at least one common column."
                    )
                grouped = left_counts.join(right_counts, join_condition, "full_outer")
                reference_multiplicity = functions.coalesce(
                    functions.col("r.__row_count__"), functions.lit(0)
                )
                spark_multiplicity = functions.coalesce(
                    functions.col("s.__row_count__"), functions.lit(0)
                )
                changed = (
                    reference_multiplicity != spark_multiplicity
                )
                differing_rows = grouped.filter(changed).count()
                missing_reference = grouped.filter(
                    functions.col("r.__row_count__").isNull()
                ).count()
                missing_actual = grouped.filter(
                    functions.col("s.__row_count__").isNull()
                ).count()
                examples_frame = grouped.filter(changed).select(
                    *[
                        _column("r", column, functions).alias(column)
                        for column in common_columns
                    ],
                    functions.col("r.__row_count__").alias(
                        "reference_multiplicity"
                    ),
                    functions.col("s.__row_count__").alias("spark_multiplicity"),
                )
                for row in examples_frame.limit(max_examples).collect():
                    values = row.asDict(recursive=True)
                    examples.append(
                        {
                            "row": {
                                column: values.get(column) for column in common_columns
                            },
                            "reference_multiplicity": values.get(
                                "reference_multiplicity"
                            ),
                            "spark_multiplicity": values.get("spark_multiplicity"),
                        }
                    )
                explanation = "Row order is ignored; rows are compared as an exact multiset."

        if schema_differences:
            status = "fail"
            explanation = "Schema columns or normalized types differ."
        elif row_count_difference:
            status = "fail"
            explanation = "Reference and Spark row counts differ."
        elif keys and (duplicate_reference or duplicate_actual):
            status = "inconclusive"
            explanation = (
                "Configured keys are not unique; one-to-one value comparison "
                "is ambiguous."
            )
        elif not keys and (atol > 0 or rtol > 0):
            status = "inconclusive"
            explanation = (
                "Numeric tolerances require configured keys to align rows; "
                "unordered tolerant matching is ambiguous."
            )
        elif missing_reference or missing_actual or differing_rows:
            status = "fail"
        elif reference_nulls != actual_nulls:
            status = "fail"
            explanation = "Per-column null counts differ."
        else:
            status = "pass"
            if keys:
                explanation = "Rows are matched by configured keys; input row order is ignored."

        return DatasetReconciliation(
            dataset=dataset,
            status=status,
            reference_path=reference_relative,
            spark_output_path=spark_relative,
            reference_sha256=reference_hash,
            spark_output_sha256=spark_hash,
            schema_match=schema_matches,
            reference_schema=reference_schema,
            spark_schema=spark_schema,
            reference_row_count=reference_count,
            spark_row_count=actual_count,
            reference_null_counts=reference_nulls,
            spark_null_counts=actual_nulls,
            key_columns=keys,
            reference_duplicate_key_groups=duplicate_reference,
            spark_duplicate_key_groups=duplicate_actual,
            missing_from_reference=missing_reference,
            missing_from_spark=missing_actual,
            differing_rows=differing_rows,
            differing_row_count_delta=reference_count - actual_count,
            examples=tuple(examples),
            explanation=explanation,
        )
    except Exception as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        return _empty_result(
            dataset,
            reference_relative,
            spark_relative,
            keys,
            error=f"{type(error).__name__}: {error}",
        )


def reconcile_project(
    project: MigrationProject,
    repository_root: str | Path,
    *,
    reference_mappings: Sequence[str],
    spark_output_mappings: Sequence[str],
    key_mappings: Sequence[str] = (),
    relative_tolerance: float = 0.0,
    absolute_tolerance: float = 0.0,
    max_examples: int = 20,
    master: str = "local[*]",
    spark_conf: Mapping[str, str] | None = None,
    execution_report: str | None = None,
) -> ReconciliationReport:
    """Compare explicit reference and Spark output mappings."""
    project.validate()
    if (
        isinstance(relative_tolerance, bool)
        or not isinstance(relative_tolerance, (int, float))
        or isinstance(absolute_tolerance, bool)
        or not isinstance(absolute_tolerance, (int, float))
        or not math.isfinite(relative_tolerance)
        or not math.isfinite(absolute_tolerance)
        or relative_tolerance < 0
        or absolute_tolerance < 0
    ):
        raise ReconciliationError("Numeric tolerances must be finite and non-negative.")
    if type(max_examples) is not int or max_examples < 0:
        raise ReconciliationError("Maximum examples must be a non-negative integer.")
    if not isinstance(master, str) or not master.strip():
        raise ReconciliationError("Spark master must be a non-empty string.")
    root = Path(repository_root).resolve(strict=True)
    if not root.is_dir():
        raise ReconciliationError(f"Repository root is not a directory: {root}.")

    reference = _assignments(reference_mappings, "--reference")
    spark_outputs = _assignments(spark_output_mappings, "--spark-output")
    if not reference:
        raise ReconciliationError("Provide at least one --reference mapping.")
    if set(reference) != set(spark_outputs):
        raise ReconciliationError(
            "--reference and --spark-output must name exactly the same datasets."
        )
    keys = _key_assignments(key_mappings)
    if set(keys).difference(reference):
        raise ReconciliationError("--keys includes a dataset without output mappings.")
    spark_settings = dict(spark_conf or {})
    if any(
        not isinstance(key, str)
        or not key.startswith("spark.")
        or not isinstance(value, str)
        for key, value in spark_settings.items()
    ):
        raise ReconciliationError("Spark configuration keys must start with 'spark.'.")
    if "spark.master" in spark_settings:
        raise ReconciliationError("Use --master to configure the Spark master.")

    for dataset_paths in (reference, spark_outputs):
        for raw_path in dataset_paths.values():
            relative = _normalized_relative(raw_path, "dataset path")
            _resolve_inside(root, relative, "dataset path")

    execution_run_id = None
    if execution_report is not None:
        report_relative = _normalized_relative(execution_report, "execution report")
        report_path = _resolve_inside(root, report_relative, "execution report")
        try:
            raw_execution = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ReconciliationError(
                f"Cannot read execution report {execution_report!r}: {error}"
            ) from error
        if not isinstance(raw_execution, dict):
            raise ReconciliationError("Execution report must be a JSON object.")
        if raw_execution.get("project_id") != project.project_id:
            raise ReconciliationError("Execution report belongs to a different project.")
        execution_run_id = raw_execution.get("run_id")
        if not isinstance(execution_run_id, str):
            raise ReconciliationError("Execution report has no run ID.")

    try:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise ReconciliationError(
            "Reconciliation requires PySpark. Install with "
            "`py -m pip install -e \".[reconcile]\"`."
        ) from error

    try:
        existing_spark = SparkSession.getActiveSession()
        if existing_spark is None:
            existing_spark = SparkSession.getDefaultSession()
        owns_spark_session = existing_spark is None
        builder = (
            SparkSession.builder.master(master)
            .appName(f"sas2pyspark-reconcile-{uuid.uuid4().hex[:8]}")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        )
        for key, value in spark_settings.items():
            builder = builder.config(key, value)
        spark = existing_spark or builder.getOrCreate()
    except Exception as error:
        raise ReconciliationError(f"Cannot initialize Spark for reconciliation: {error}") from error
    results: list[DatasetReconciliation] = []
    try:
        for dataset in sorted(reference):
            results.append(
                _reconcile_one(
                    spark,
                    root,
                    dataset,
                    reference[dataset],
                    spark_outputs[dataset],
                    keys.get(dataset, ()),
                    atol=absolute_tolerance,
                    rtol=relative_tolerance,
                    max_examples=max_examples,
                )
            )
    finally:
        if owns_spark_session:
            spark.stop()

    return ReconciliationReport(
        reconciliation_id=str(uuid.uuid4()),
        project_id=project.project_id,
        project_name=project.name,
        repository_root=str(root),
        generated_at=datetime.now(timezone.utc).isoformat(),
        relative_tolerance=relative_tolerance,
        absolute_tolerance=absolute_tolerance,
        ordering="per_dataset_keyed_or_unordered_multiset",
        key_columns=keys,
        max_examples=max_examples,
        spark_master=master,
        spark_conf=spark_settings,
        datasets=tuple(results),
        execution_run_id=execution_run_id,
    )
