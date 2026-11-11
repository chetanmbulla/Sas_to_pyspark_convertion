"""Project-level helpers for the local Streamlit interface."""

from __future__ import annotations

import csv
import json
import ntpath
import shutil
import stat
import tempfile
import zipfile
from io import BytesIO
from itertools import islice
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Mapping, Sequence

from .execution_workflow import run_project
from .phase1_analysis import run_phase1_analysis
from .project import MigrationProject
from .synthetic_data import generate_synthetic_data, load_catalog_report

MAX_ZIP_BYTES = 100 * 1024 * 1024
MAX_EXTRACTED_BYTES = 512 * 1024 * 1024
MAX_ZIP_FILES = 10_000


class GuiProjectError(ValueError):
    """Raised when a GUI project archive or its inputs are invalid."""


def _is_reserved_windows_name(part: str) -> bool:
    is_reserved = getattr(ntpath, "isreserved", None)
    if is_reserved is not None:
        return is_reserved(part)
    return PureWindowsPath(part).is_reserved()


def _inside(root: Path, relative: str) -> Path:
    normalized = relative.replace("\\", "/")
    posix = PurePosixPath(normalized)
    windows = PureWindowsPath(relative)
    if (
        not normalized
        or posix.is_absolute()
        or windows.is_absolute()
        or windows.drive
        or any(part in {"", ".", ".."} for part in normalized.split("/"))
        or any(
            _is_reserved_windows_name(part)
            or part.endswith((" ", "."))
            or any(character in '<>:"|?*' or ord(character) < 32 for character in part)
            for part in normalized.split("/")
        )
    ):
        raise GuiProjectError(f"Invalid relative path: {relative!r}.")
    target = (root / Path(*posix.parts)).resolve(strict=False)
    if not target.is_relative_to(root.resolve()):
        raise GuiProjectError(f"Path escapes the project workspace: {relative!r}.")
    return target


def extract_project_zip(data: bytes, destination: Path) -> list[str]:
    """Extract regular files only, with bounded size and safe relative paths."""
    if len(data) > MAX_ZIP_BYTES:
        raise GuiProjectError("ZIP exceeds the 100 MiB upload limit.")
    destination.mkdir(parents=True, exist_ok=False)
    files: list[str] = []
    seen: set[str] = set()
    try:
        with zipfile.ZipFile(BytesIO(data)) as archive:
            members = archive.infolist()
            if len(members) > MAX_ZIP_FILES:
                raise GuiProjectError("ZIP exceeds the 10,000 member limit.")
            if sum(item.file_size for item in members) > MAX_EXTRACTED_BYTES:
                raise GuiProjectError("ZIP exceeds the 512 MiB extracted size limit.")
            for item in members:
                relative = item.filename.replace("\\", "/").rstrip("/")
                if not relative:
                    raise GuiProjectError("ZIP contains an empty member name.")
                target = _inside(destination, relative)
                mode = item.external_attr >> 16
                if stat.S_ISLNK(mode) or (mode and stat.S_IFMT(mode) not in (0, stat.S_IFREG, stat.S_IFDIR)):
                    raise GuiProjectError(f"ZIP contains a non-regular member: {relative!r}.")
                if item.flag_bits & 1:
                    raise GuiProjectError(f"Encrypted ZIP member is unsupported: {relative!r}.")
                key = relative.casefold()
                if key in seen:
                    raise GuiProjectError(f"ZIP contains duplicate paths: {relative!r}.")
                seen.add(key)
                if item.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(item) as source, target.open("xb") as output:
                    shutil.copyfileobj(source, output, length=1024 * 1024)
                files.append(relative)
    except (zipfile.BadZipFile, RuntimeError, OSError) as error:
        raise GuiProjectError(f"Cannot extract project ZIP: {error}") from error
    return files


def analyze_zip(
    archive: bytes,
    *,
    config: bytes | None = None,
    schema_asset: tuple[str, bytes] | None = None,
    sas_version: str = "auto",
    rows: int = 100,
    seed: int = 0,
) -> dict[str, Any]:
    """Create a persistent local workspace and run the existing Phase 1 workflow."""
    workspace = Path(tempfile.mkdtemp(prefix="sas2pyspark-project-")).resolve()
    repository = workspace / "repository"
    repository.mkdir()
    extracted = repository / "input"
    files = extract_project_zip(archive, extracted)
    sas_files = sorted((name for name in files if name.casefold().endswith(".sas")), key=str.casefold)
    if not sas_files:
        raise GuiProjectError(f"No SAS files found in ZIP. Workspace: {workspace}")
    config_path = workspace / "pipeline-config.json"
    if config is None:
        source_root = repository
        document = {
            "schema_version": 1,
            "servers": {"LOCAL": {"drives": {"X": "input"}}},
            "entrypoints": [
                {"server": "LOCAL", "path": f"input/{name}"} for name in sas_files
            ],
        }
        config_path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    else:
        source_root = extracted
        config_path.write_bytes(config)
    schema_path = None
    if schema_asset is not None:
        filename, content = schema_asset
        suffix = Path(filename).suffix.lower()
        if suffix not in {".csv", ".json", ".sql", ".ddl"}:
            raise GuiProjectError("Schema asset must be a CSV, JSON, SQL or DDL file.")
        if len(content) > 50_000_000:
            raise GuiProjectError("Schema asset exceeds the 50 MB limit.")
        schema_path = workspace / f"schema-asset{suffix}"
        schema_path.write_bytes(content)
    output = workspace / "phase1"
    index = run_phase1_analysis(
        source_root, config_path, output, sas_version=sas_version, rows=rows, seed=seed,
        schemas=schema_path,
    )
    return {
        "workspace": str(workspace),
        "source_root": str(source_root),
        "output_root": str(output),
        "configuration": "all_scripts_fallback" if config is None else "uploaded",
        "index": index,
    }


def regenerate_synthetic_data(
    output_root: Path, *, rows: int, seed: int
) -> dict[str, Any]:
    """Generate a separate version so previous execution inputs remain intact."""
    target = Path(tempfile.mkdtemp(prefix="synthetic-", dir=output_root))
    return generate_synthetic_data(
        load_catalog_report(output_root / "catalog.json"), target, rows=rows, seed=seed
    )


def preview_synthetic_data(
    output_root: Path, manifest: dict[str, Any], dataset: str, *, limit: int = 20
) -> tuple[list[str], list[list[str]]]:
    """Read a bounded preview of a generated fixture inside this project."""
    if not 1 <= limit <= 100:
        raise GuiProjectError("Preview row limit must be between 1 and 100.")
    records = [
        item for item in manifest["datasets"]
        if item["dataset"] == dataset
        and item["status"] in {"generated", "generated_partial_schema"}
    ]
    if len(records) != 1:
        raise GuiProjectError(f"Generated synthetic dataset is not uniquely available: {dataset!r}.")
    root = Path(output_root).resolve(strict=True)
    fixture_root = Path(manifest["output_directory"]).resolve(strict=True)
    if not fixture_root.is_relative_to(root):
        raise GuiProjectError("Synthetic input directory is outside this project output.")
    path = _inside(fixture_root, records[0]["path"])
    try:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream, strict=True)
            header = next(reader, None)
            if not header:
                raise GuiProjectError(f"Synthetic dataset {dataset!r} has no CSV header.")
            expected = [column["name"] for column in records[0]["columns"]]
            if header != expected:
                raise GuiProjectError(f"Synthetic dataset {dataset!r} does not match its manifest columns.")
            rows = list(islice(reader, limit))
            if any(len(row) != len(header) for row in rows):
                raise GuiProjectError(f"Synthetic dataset {dataset!r} has a malformed CSV row.")
    except (OSError, UnicodeError, csv.Error) as error:
        raise GuiProjectError(f"Cannot preview synthetic dataset {dataset!r}: {error}") from error
    return header, rows


def read_converted_code(output_root: Path, relative_path: str) -> str:
    """Read a generated program from the Phase 1 code directory."""
    return _inside(output_root / "code", relative_path).read_text(encoding="utf-8")


def pipeline_flow_graph(pipeline: Mapping[str, Any], *, max_nodes: int = 80) -> tuple[str, bool]:
    """Create a bounded DOT graph of job entrypoints and resolved includes."""
    if max_nodes < 1:
        raise GuiProjectError("Graph node limit must be positive.")
    nodes: dict[tuple[str, str], str] = {}
    edges: set[tuple[str, str]] = set()
    truncated = False

    def node(kind: str, label: str) -> str | None:
        nonlocal truncated
        key = (kind, label)
        if key not in nodes:
            if len(nodes) >= max_nodes:
                truncated = True
                return None
            nodes[key] = f"n{len(nodes)}"
        return nodes[key]

    for job in pipeline.get("jobs", []):
        if job.get("entrypoint"):
            source = node("Job", str(job.get("name") or job["entrypoint"]))
            target = node("Script", str(job["entrypoint"]))
            if source and target:
                edges.add((source, target))
    for include in pipeline.get("includes", []):
        if include.get("status") == "resolved" and include.get("source") and include.get("target"):
            source = node("Script", str(include["source"]))
            target = node("Script", str(include["target"]))
            if source and target:
                edges.add((source, target))
    lines = ["digraph pipeline {", "  rankdir=LR;"]
    for (kind, label), identifier in nodes.items():
        lines.append(f"  {identifier} [label={json.dumps(kind + ': ' + label)}, shape=box];")
    lines.extend(f"  {source} -> {target};" for source, target in sorted(edges))
    lines.append("}")
    return "\n".join(lines), truncated


def run_converted_programs(
    project_state: dict[str, Any],
    synthetic_manifest: dict[str, Any],
    datasets: Sequence[str],
    *,
    timeout_seconds: int,
    master: str = "local[1]",
    programs: Sequence[str] | None = None,
    edited_code: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Stage successful conversions and selected synthetic inputs, then run them."""
    source_root = Path(project_state["source_root"]).resolve(strict=True)
    output_root = Path(project_state["output_root"]).resolve(strict=True)
    project = MigrationProject.load(output_root / "project.json")
    conversion_report = json.loads((output_root / "conversion.json").read_text(encoding="utf-8"))
    successful = [item for item in conversion_report["outcomes"] if item["status"] == "succeeded"]
    if not successful:
        raise GuiProjectError("No successfully converted programs are available to execute.")
    if programs is not None:
        if not programs or len(programs) != len(set(programs)):
            raise GuiProjectError("Select one or more distinct converted programs.")
        available = {item["source_path"] for item in successful}
        missing = set(programs) - available
        if missing:
            raise GuiProjectError(f"Programs are not successfully converted: {', '.join(sorted(missing))}.")
        selected_set = set(programs)
        successful = [item for item in successful if item["source_path"] in selected_set]
    edits = dict(edited_code or {})
    if set(edits) - {item["source_path"] for item in successful}:
        raise GuiProjectError("Edited code must belong to a selected converted program.")
    for path, code in edits.items():
        if not isinstance(code, str) or not code.strip():
            raise GuiProjectError(f"Edited code is empty for {path!r}.")
        try:
            compile(code, path, "exec")
        except SyntaxError as error:
            raise GuiProjectError(f"Edited code has a syntax error for {path!r}: {error}") from error
    selected_records = {
        item["dataset"]: item
        for item in synthetic_manifest["datasets"]
        if item["status"] in {"generated", "generated_partial_schema"}
    }
    if len(datasets) != len(set(datasets)):
        raise GuiProjectError("Synthetic input datasets must not be selected more than once.")
    for name in datasets:
        if name not in selected_records:
            raise GuiProjectError(f"Synthetic dataset is not available: {name!r}.")
    fixture_root = Path(synthetic_manifest["output_directory"]).resolve(strict=True)
    if not fixture_root.is_relative_to(output_root):
        raise GuiProjectError("Synthetic input directory is outside this project output.")

    stage = Path(tempfile.mkdtemp(prefix="execution-", dir=output_root)).resolve()
    for item in successful:
        for origin, relative in (
            (source_root, item["source_path"]),
            (output_root / "code", item["output_path"]),
        ):
            source = _inside(origin, relative)
            destination = _inside(stage, relative)
            if destination.exists():
                raise GuiProjectError(f"Execution staging path collision: {relative!r}.")
            destination.parent.mkdir(parents=True, exist_ok=True)
            if origin == output_root / "code" and item["source_path"] in edits:
                destination.write_text(edits[item["source_path"]], encoding="utf-8")
            else:
                shutil.copyfile(source, destination)
    mappings = []
    for name in datasets:
        record = selected_records[name]
        source = _inside(fixture_root, record["path"])
        relative = f"synthetic-inputs/{record['path']}"
        destination = _inside(stage, relative)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        mappings.append(f"{name}={relative}")
    result = run_project(
        project,
        stage,
        program_selectors=[item["source_path"] for item in successful],
        input_mappings=mappings,
        timeout_seconds=timeout_seconds,
        master=master,
    )
    report = result.to_dict()
    report["execution_root"] = str(stage)
    report["edited_programs"] = sorted(edits)
    return report
