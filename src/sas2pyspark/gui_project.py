"""Project-level helpers for the local Streamlit interface."""

from __future__ import annotations

import json
import ntpath
import shutil
import stat
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Sequence

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
    output = workspace / "phase1"
    index = run_phase1_analysis(
        source_root, config_path, output, sas_version=sas_version, rows=rows, seed=seed
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


def run_converted_programs(
    project_state: dict[str, Any],
    synthetic_manifest: dict[str, Any],
    datasets: Sequence[str],
    *,
    timeout_seconds: int,
    master: str = "local[1]",
) -> dict[str, Any]:
    """Stage successful conversions and selected synthetic inputs, then run them."""
    source_root = Path(project_state["source_root"]).resolve(strict=True)
    output_root = Path(project_state["output_root"]).resolve(strict=True)
    project = MigrationProject.load(output_root / "project.json")
    conversion_report = json.loads((output_root / "conversion.json").read_text(encoding="utf-8"))
    successful = [item for item in conversion_report["outcomes"] if item["status"] == "succeeded"]
    if not successful:
        raise GuiProjectError("No successfully converted programs are available to execute.")
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
    return report
