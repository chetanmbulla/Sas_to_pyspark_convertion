"""Deterministic synthetic CSV generation from source/sink catalog schemas."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


class SyntheticDataError(ValueError):
    """Raised when synthetic-data options or catalog metadata are invalid."""


_SAFE_NAME = re.compile(r"[^A-Za-z0-9_-]+")
_SUPPORTED_TYPES = {"numeric", "character"}
_MAX_ROWS = 1_000_000


def _fixture_type(column: Mapping[str, Any]) -> str | None:
    value = column.get("type")
    if value is None or value == "" or (
        isinstance(value, str) and value.casefold() == "unknown"
    ):
        return "character"
    if isinstance(value, str) and value.casefold() in _SUPPORTED_TYPES:
        return value.casefold()
    return None


def load_catalog_report(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SyntheticDataError(
            f"Cannot read catalog report {str(path)!r}: {error}"
        ) from error
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise SyntheticDataError("Catalog report has an unsupported schema version.")
    return value


def _stable_file_stem(dataset_name: str) -> str:
    slug = _SAFE_NAME.sub("_", dataset_name).strip("_").lower()[:48] or "dataset"
    digest = hashlib.sha256(dataset_name.casefold().encode("utf-8")).hexdigest()[:10]
    return f"{slug}_{digest}"


def _write_text(path: Path, text: str, *, force: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        raise FileExistsError(
            f"Synthetic output already exists: {path}. Use --force to replace it."
        )
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary.write(text)
            temporary_path = Path(temporary.name)
        if force:
            os.replace(temporary_path, path)
        else:
            os.rename(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _synthetic_value(
    column: Mapping[str, Any],
    row_index: int,
    randomizer: random.Random,
) -> str:
    name = str(column["name"])
    data_type = str(column["type"]).casefold()
    if data_type == "numeric":
        if re.search(r"(^id$|_id$|^id_|key$)", name, re.I):
            return str(row_index + 1)
        return f"{randomizer.randint(1, 999_999)}.{randomizer.randint(0, 99):02d}"
    length = column.get("length")
    if length is not None and (not isinstance(length, int) or length < 1):
        raise SyntheticDataError(
            f"Character column {name!r} has an invalid SAS length {length!r}."
        )
    width = min(length or 32, 32)
    if width == 1:
        return chr(ord("A") + row_index % 26)
    token = f"SYN_{randomizer.randint(0, 999_999_999):09d}"
    return token[:width]


def generate_synthetic_data(
    catalog: Mapping[str, Any],
    output_directory: str | Path,
    *,
    rows: int = 100,
    seed: int = 0,
    force: bool = False,
) -> dict[str, Any]:
    """Generate CSVs, assuming character for columns with unknown types."""
    if catalog.get("schema_version") != 1:
        raise SyntheticDataError("Catalog has an unsupported schema version.")
    if not isinstance(rows, int) or isinstance(rows, bool) or not 1 <= rows <= _MAX_ROWS:
        raise SyntheticDataError(f"rows must be an integer from 1 to {_MAX_ROWS}.")
    if not isinstance(seed, int) or isinstance(seed, bool):
        raise SyntheticDataError("seed must be an integer.")
    try:
        destination = Path(output_directory).resolve(strict=False)
        destination.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise SyntheticDataError(
            f"Cannot create synthetic output directory {str(output_directory)!r}: {error}"
        ) from error
    if not destination.is_dir():
        raise SyntheticDataError(
            f"Synthetic output path is not a directory: {str(destination)!r}."
        )

    manifest_path = destination / "synthetic-data-manifest.json"
    if manifest_path.exists() and not force:
        raise FileExistsError(
            f"Synthetic output already exists: {manifest_path}. Use --force to replace it."
        )
    for resource in catalog.get("resources", []):
        if resource.get("kind") != "sas_dataset":
            continue
        columns = (resource.get("schema") or {}).get("columns") or []
        if not columns or any(
            not isinstance(column, Mapping)
            or not column.get("name")
            or _fixture_type(column) is None
            for column in columns
        ):
            continue
        output_path = destination / f"{_stable_file_stem(str(resource.get('name') or ''))}.csv"
        if output_path.exists() and not force:
            raise FileExistsError(
                f"Synthetic output already exists: {output_path}. Use --force to replace it."
            )

    records: list[dict[str, Any]] = []
    for resource in sorted(
        (
            item
            for item in catalog.get("resources", [])
            if item.get("kind") == "sas_dataset"
        ),
        key=lambda item: str(item.get("name", "")).casefold(),
    ):
        dataset_name = str(resource.get("name") or "")
        schema = resource.get("schema") or {}
        columns = schema.get("columns") or []
        if not dataset_name:
            records.append(
                {
                    "dataset": dataset_name,
                    "status": "skipped",
                    "reason": "Catalog dataset has no name.",
                }
            )
            continue
        if not columns:
            records.append(
                {
                    "dataset": dataset_name,
                    "status": "skipped",
                    "reason": (
                        f"Schema status is {schema.get('status', 'unknown')}; no column "
                        "definitions were discovered in the SAS files/ZIP or supplied "
                        "schema metadata."
                    ),
                }
            )
            continue
        invalid_columns: list[str] = []
        for column in columns:
            if not isinstance(column, Mapping):
                invalid_columns.append("<invalid column record>")
            elif (
                not column.get("name")
                or _fixture_type(column) is None
            ):
                invalid_columns.append(str(column.get("name") or "<unnamed>"))
        if invalid_columns:
            records.append(
                {
                    "dataset": dataset_name,
                    "status": "skipped",
                    "reason": "One or more columns are unnamed, invalid, or have unsupported types.",
                    "columns": invalid_columns,
                }
            )
            continue

        fixture_columns = []
        assumed_columns = []
        for column in columns:
            fixture_column = dict(column)
            fixture_column["type"] = _fixture_type(column)
            if column.get("type") is None or str(column.get("type")).casefold() in {"", "unknown"}:
                assumed_columns.append(str(column["name"]))
            fixture_columns.append(fixture_column)

        output_path = destination / f"{_stable_file_stem(dataset_name)}.csv"
        if output_path.exists() and not force:
            raise FileExistsError(
                f"Synthetic output already exists: {output_path}. Use --force to replace it."
            )
        dataset_seed = int.from_bytes(
            hashlib.sha256(f"{seed}:{dataset_name.casefold()}".encode("utf-8")).digest()[:8],
            "big",
        )
        randomizer = random.Random(dataset_seed)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="",
                dir=destination,
                prefix=f".{output_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                writer = csv.writer(temporary, lineterminator="\n")
                writer.writerow([column["name"] for column in fixture_columns])
                for row_index in range(rows):
                    writer.writerow(
                        [
                            _synthetic_value(column, row_index, randomizer)
                            for column in fixture_columns
                        ]
                    )
                temporary_path = Path(temporary.name)
            if force:
                os.replace(temporary_path, output_path)
            else:
                os.rename(temporary_path, output_path)
            temporary_path = None
        except OSError as error:
            raise SyntheticDataError(
                f"Cannot write synthetic data for {dataset_name!r}: {error}"
            ) from error
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        records.append(
            {
                "dataset": dataset_name,
                "status": (
                    "generated"
                    if schema.get("status") == "complete" and not assumed_columns
                    else "generated_partial_schema"
                ),
                "schema_status": schema.get("status", "unknown"),
                "rows": rows,
                "seed": seed,
                "assumed_character_columns": assumed_columns,
                "columns": [
                    {
                        "name": column["name"],
                        "type": str(column["type"]).casefold(),
                        "length": column.get("length"),
                        "format": column.get("format"),
                    }
                    for column in fixture_columns
                ],
                "path": output_path.name,
            }
        )

    generated = [
        item
        for item in records
        if item["status"] in {"generated", "generated_partial_schema"}
    ]
    report = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "catalog_generated_at": catalog.get("generated_at"),
        "output_directory": str(destination),
        "rows_per_dataset": rows,
        "seed": seed,
        "datasets": records,
        "summary": {
            "dataset_count": len(records),
            "generated_count": len(generated),
            "partial_schema_count": sum(
                item["status"] == "generated_partial_schema" for item in generated
            ),
            "skipped_count": sum(item["status"] == "skipped" for item in records),
            "assumed_type_dataset_count": sum(
                bool(item.get("assumed_character_columns")) for item in generated
            ),
        },
        "limitations": [
            "Generated rows are deterministic synthetic fixtures, not a statistical model of production data.",
            "Unknown column types are assumed character for fixtures, not inferred SAS types.",
            "Datasets without discovered columns and columns with unsupported types are skipped.",
            "Partial schemas produce partial fixtures; downstream programs may require uncatalogued columns.",
            "Missing-value rates, keys, constraints, and cross-table relationships are not inferred.",
        ],
    }
    _write_text(
        manifest_path,
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        force=force,
    )
    return report
