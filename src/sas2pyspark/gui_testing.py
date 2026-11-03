"""Helpers for testing PySpark code from the Streamlit interface."""

from __future__ import annotations

import ast
import hashlib
import re
import shutil
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Generator, Sequence

from .execution_workflow import run_project
from .project import Conversion, MigrationProject


_MAX_INPUT_BYTES = 100 * 1024 * 1024
_SUPPORTED_INPUTS = {".csv", ".parquet", ".json", ".jsonl"}


@contextmanager
def _temporary_directory() -> Generator[Path, None, None]:
    root = Path(tempfile.mkdtemp(prefix="sas2pyspark-gui-"))
    try:
        yield root
    finally:
        for attempt in range(10):
            try:
                shutil.rmtree(root)
                break
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.25)


@dataclass(frozen=True)
class PySparkTestReport:
    status: str
    duration_seconds: float
    error: str | None
    stdout: str
    stderr: str
    output_previews: tuple[tuple[str, str], ...]
    code_sha256: str


def performance_notes(code: str) -> tuple[str, ...]:
    """Return heuristic notices for common Spark performance risks."""
    tree = ast.parse(code)
    notes: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Attribute):
            if node.func.attr == "collect":
                notes.append(
                    "collect() brings all matching rows to the driver; use it only "
                    "when the result is known to be small."
                )
            elif node.func.attr == "toPandas":
                notes.append(
                    "toPandas() brings the full result to driver memory; use it only "
                    "for data known to fit comfortably in memory."
                )
            elif (
                node.func.attr == "repartition"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == 1
            ):
                notes.append(
                    "repartition(1) forces a single output partition and can "
                    "bottleneck large datasets."
                )
        elif isinstance(node.func, ast.Name) and node.func.id in {
            "udf",
            "pandas_udf",
        }:
            notes.append(
                "Python UDFs can be slower than built-in Spark SQL functions; "
                "prefer built-ins where possible."
            )
    return tuple(dict.fromkeys(notes))


def _input_dataset_name(filename: str) -> tuple[str, str]:
    input_path = Path(filename)
    extension = input_path.suffix.casefold()
    if extension not in _SUPPORTED_INPUTS:
        raise ValueError(
            f"Unsupported input format {extension or '(no extension)'!r}; "
            "use CSV, Parquet, JSON, or JSONL."
        )
    dataset = re.sub(r"\W+", "_", input_path.stem, flags=re.ASCII).strip("_")
    if not dataset:
        raise ValueError(f"Cannot derive a dataset name from {filename!r}.")
    if dataset[0].isdigit():
        dataset = f"dataset_{dataset}"
    return dataset.casefold(), extension


def _output_previews(
    root: Path, output_paths: Sequence[str]
) -> tuple[tuple[str, str], ...]:
    previews: list[tuple[str, str]] = []
    for relative in output_paths:
        path = (root / relative).resolve(strict=False)
        try:
            path.relative_to(root)
        except ValueError:
            continue
        if path.suffix.casefold() != ".csv" or not path.is_file():
            continue
        with path.open("rb") as output_file:
            preview = output_file.read(64 * 1024).decode("utf-8-sig", errors="replace")
        previews.append((relative, "\n".join(preview.splitlines()[:12])))
    return tuple(previews)


def run_pyspark_test(
    source: str,
    code: str,
    datasets: Sequence[tuple[str, bytes]],
    *,
    timeout_seconds: int = 120,
    master: str = "local[1]",
    shuffle_partitions: int = 4,
) -> PySparkTestReport:
    """Run converted code against uploaded datasets in a temporary project."""
    compile(code, "<converted-pyspark>", "exec")
    code_sha256 = hashlib.sha256(code.encode("utf-8")).hexdigest()

    named_datasets: list[tuple[str, str, bytes]] = []
    seen_names: set[str] = set()
    total_bytes = 0
    for filename, contents in datasets:
        dataset, extension = _input_dataset_name(filename)
        if dataset in seen_names:
            raise ValueError(
                f"Multiple uploaded files map to dataset {dataset!r}; "
                "rename the files so their base names are unique."
            )
        seen_names.add(dataset)
        total_bytes += len(contents)
        if total_bytes > _MAX_INPUT_BYTES:
            raise ValueError("Uploaded datasets exceed the 100 MB total limit.")
        named_datasets.append((dataset, extension, contents))

    with _temporary_directory() as root:
        source_path = root / "program.sas"
        source_path.write_text(source, encoding="utf-8")
        code_path = root / "pyspark" / "converted.py"
        code_path.parent.mkdir()
        code_path.write_text(code, encoding="utf-8")

        input_directory = root / "inputs"
        input_directory.mkdir()
        input_mappings: list[str] = []
        for dataset, extension, contents in named_datasets:
            relative_path = Path("inputs") / f"{dataset}{extension}"
            (root / relative_path).write_bytes(contents)
            input_mappings.append(f"{dataset}={relative_path.as_posix()}")

        project = MigrationProject.create("Streamlit PySpark test")
        program = project.add_program("program.sas")
        source_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
        project.add_conversion(
            Conversion.create(
                program.id,
                source_hash,
                "streamlit-gui",
                status="succeeded",
                output_path="pyspark/converted.py",
            )
        )
        report = run_project(
            project,
            root,
            program_selectors=[program.path],
            input_mappings=input_mappings,
            timeout_seconds=timeout_seconds,
            master=master,
            spark_conf={"spark.sql.shuffle.partitions": str(shuffle_partitions)},
        )
        result = report.results[0]
        stdout = (root / result.stdout_path).read_text(
            encoding="utf-8", errors="replace"
        )
        stderr = (root / result.stderr_path).read_text(
            encoding="utf-8", errors="replace"
        )
        return PySparkTestReport(
            status=result.status,
            duration_seconds=result.duration_seconds,
            error=result.error,
            stdout=stdout,
            stderr=stderr,
            output_previews=_output_previews(root, result.output_paths),
            code_sha256=code_sha256,
        )
