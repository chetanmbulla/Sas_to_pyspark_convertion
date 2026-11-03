"""Run selected converted programs in isolated local worker processes."""

from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import re
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Mapping, Sequence

from .project import Conversion, MigrationProject, Program


EXECUTION_REPORT_SCHEMA_VERSION = 1
_DATASET = re.compile(r"^(?:[A-Za-z_]\w*\.)?([A-Za-z_]\w*)$")
_ENVIRONMENT_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ENVIRONMENT_REFERENCE = re.compile(r"^\$\{[A-Za-z_][A-Za-z0-9_]*\}$")
_SENSITIVE_SETTING = re.compile(
    r"password|secret|token|credential|access[._-]?key|private[._-]?key",
    re.IGNORECASE,
)


class ExecutionWorkflowError(ValueError):
    """Raised when execution options or project paths are invalid."""


@dataclass(frozen=True)
class ExecutionInput:
    dataset: str
    path: str
    sha256: str

    def to_dict(self) -> dict[str, str]:
        return {"dataset": self.dataset, "path": self.path, "sha256": self.sha256}


@dataclass(frozen=True)
class ProgramExecutionResult:
    program_id: str
    source_path: str
    source_sha256: str | None
    conversion_id: str | None
    generated_code_path: str | None
    execution_snapshot_path: str | None
    generated_code_sha256: str | None
    status: str
    command: tuple[str, ...]
    started_at: str
    completed_at: str
    duration_seconds: float
    exit_code: int | None
    timed_out: bool
    stdout_path: str
    stderr_path: str
    output_paths: tuple[str, ...] = ()
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "program_id": self.program_id,
            "source_path": self.source_path,
            "source_sha256": self.source_sha256,
            "conversion_id": self.conversion_id,
            "generated_code_path": self.generated_code_path,
            "execution_snapshot_path": self.execution_snapshot_path,
            "generated_code_sha256": self.generated_code_sha256,
            "status": self.status,
            "command": list(self.command),
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "exit_code": self.exit_code,
            "timed_out": self.timed_out,
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
            "output_paths": list(self.output_paths),
            "error": self.error,
        }


@dataclass(frozen=True)
class ExecutionReport:
    run_id: str
    project_id: str
    project_name: str
    repository_root: str
    run_directory: str
    started_at: str
    completed_at: str
    timeout_seconds: float
    master: str
    spark_conf: Mapping[str, str]
    inputs: tuple[ExecutionInput, ...]
    environment: Mapping[str, Any]
    results: tuple[ProgramExecutionResult, ...]
    schema_version: int = EXECUTION_REPORT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        succeeded = sum(result.status == "succeeded" for result in self.results)
        failed = sum(result.status == "failed" for result in self.results)
        timed_out = sum(result.status == "timed_out" for result in self.results)
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "repository_root": self.repository_root,
            "run_directory": self.run_directory,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "timeout_seconds": self.timeout_seconds,
            "master": self.master,
            "spark_conf": dict(sorted(self.spark_conf.items())),
            "inputs": [item.to_dict() for item in self.inputs],
            "environment": dict(self.environment),
            "results": [result.to_dict() for result in self.results],
            "summary": {
                "program_count": len(self.results),
                "succeeded": succeeded,
                "failed": failed,
                "timed_out": timed_out,
            },
        }

    def save(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def _normalized_relative(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ExecutionWorkflowError(f"{field} must be a non-empty relative path.")
    normalized = value.replace("\\", "/")
    posix = PurePosixPath(normalized)
    windows = PureWindowsPath(value)
    if (
        posix.is_absolute()
        or windows.is_absolute()
        or windows.drive
        or any(part in {"", ".", ".."} for part in normalized.split("/"))
    ):
        raise ExecutionWorkflowError(f"{field} must be a normalized relative path.")
    return normalized


def _resolve_inside(root: Path, relative: str, field: str) -> Path:
    candidate = (root / Path(relative)).resolve(strict=False)
    try:
        candidate.relative_to(root)
    except ValueError:
        raise ExecutionWorkflowError(
            f"{field} resolves outside the repository root: {relative!r}."
        ) from None
    return candidate


def _parse_assignments(
    values: Sequence[str], field: str
) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key or not item:
            raise ExecutionWorkflowError(f"{field} must use KEY=VALUE syntax.")
        if key in result:
            raise ExecutionWorkflowError(f"{field} contains duplicate key {key!r}.")
        result[key] = item
    return result


def _input_records(
    root: Path, mappings: Sequence[str]
) -> tuple[ExecutionInput, ...]:
    assignments = _parse_assignments(mappings, "--input")
    inputs: list[ExecutionInput] = []
    aliases: set[str] = set()
    for dataset, raw_path in sorted(assignments.items()):
        match = _DATASET.fullmatch(dataset)
        if not match:
            raise ExecutionWorkflowError(
                f"Input dataset must be a SAS dataset name: {dataset!r}."
            )
        table_name = match.group(1).casefold()
        if table_name in aliases:
            raise ExecutionWorkflowError(
                f"Input datasets have a duplicate table name: {table_name!r}."
            )
        aliases.add(table_name)
        relative = _normalized_relative(raw_path, "--input path")
        path = _resolve_inside(root, relative, "--input path")
        if not path.is_file():
            raise ExecutionWorkflowError(f"Input file does not exist: {relative!r}.")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        inputs.append(ExecutionInput(dataset, relative, digest))
    return tuple(inputs)


def _select_programs(
    project: MigrationProject, selectors: Sequence[str]
) -> list[Program]:
    if not selectors:
        raise ExecutionWorkflowError("Select at least one program with --program.")
    by_id = {program.id: program for program in project.programs}
    by_path = {program.path.casefold(): program for program in project.programs}
    selected = []
    seen: set[str] = set()
    for selector in selectors:
        normalized = selector.replace("\\", "/")
        program = by_id.get(selector) or by_path.get(normalized.casefold())
        if program is None:
            raise ExecutionWorkflowError(
                f"Selected program is not registered: {selector!r}."
            )
        if program.id in seen:
            raise ExecutionWorkflowError(
                f"Program selected more than once: {program.path!r}."
            )
        seen.add(program.id)
        selected.append(program)
    return selected


def _conversion_for_program(
    project: MigrationProject, program_id: str, source_hash: str | None
) -> Conversion | None:
    matching = [
        item
        for item in project.conversions
        if item.program_id == program_id
        and item.status == "succeeded"
        and item.source_sha256 == source_hash
        and item.output_path is not None
    ]
    if len(matching) > 1:
        raise ExecutionWorkflowError(
            "Multiple successful conversions match this source; remove the "
            "ambiguity before running the program."
        )
    return matching[0] if matching else None


def _input_environment(
    package_root: Path, overrides: Mapping[str, str]
) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(overrides)
    search_path = [str(package_root)]
    if environment.get("PYTHONPATH"):
        search_path.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(search_path)
    environment.setdefault("PYSPARK_PYTHON", sys.executable)
    environment.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return environment


def _terminate_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        try:
            process.send_signal(signal.CTRL_BREAK_EVENT)
            process.wait(timeout=3)
            return
        except (OSError, subprocess.TimeoutExpired):
            process.kill()
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait()


def _output_files(output_directory: Path, root: Path) -> tuple[str, ...]:
    paths: list[str] = []
    for item in sorted(output_directory.rglob("*")):
        if item.is_file():
            try:
                paths.append(item.resolve(strict=False).relative_to(root).as_posix())
            except ValueError:
                paths.append(str(item.resolve(strict=False)))
    return tuple(paths)


def _python_environment(worker_environment: Mapping[str, str]) -> dict[str, Any]:
    try:
        pyspark_version = version("pyspark")
    except PackageNotFoundError:
        pyspark_version = None
    return {
        "python_executable": sys.executable,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "pyspark_version": pyspark_version,
        "runtime_environment": {
            key: worker_environment[key]
            for key in (
                "JAVA_HOME",
                "SPARK_HOME",
                "PYSPARK_PYTHON",
                "PYSPARK_DRIVER_PYTHON",
                "SPARK_LOCAL_IP",
                "HADOOP_CONF_DIR",
            )
            if key in worker_environment
        },
        "environment_overrides": [],
    }


def run_project(
    project: MigrationProject,
    repository_root: str | Path,
    *,
    program_selectors: Sequence[str],
    input_mappings: Sequence[str] = (),
    timeout_seconds: float = 3600,
    master: str = "local[*]",
    spark_conf: Mapping[str, str] | None = None,
    environment: Mapping[str, str] | None = None,
    run_directory: str = "runs",
) -> ExecutionReport:
    """Run selected, current successful conversions in separate worker processes."""
    project.validate()
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(timeout_seconds)
        or timeout_seconds <= 0
    ):
        raise ExecutionWorkflowError("Timeout must be a positive number of seconds.")
    if not isinstance(master, str) or not master.strip():
        raise ExecutionWorkflowError("Spark master must be a non-empty string.")
    root = Path(repository_root).resolve(strict=True)
    if not root.is_dir():
        raise ExecutionWorkflowError(f"Repository root is not a directory: {root}.")
    run_relative = _normalized_relative(run_directory, "run directory")
    run_base = _resolve_inside(root, run_relative, "run directory")
    inputs = _input_records(root, input_mappings)
    selected = _select_programs(project, program_selectors)
    spark_settings = dict(spark_conf or {})
    if any(
        not isinstance(key, str)
        or not key.startswith("spark.")
        or not isinstance(value, str)
        for key, value in spark_settings.items()
    ):
        raise ExecutionWorkflowError("Spark configuration keys must start with 'spark.'.")
    if "spark.master" in spark_settings:
        raise ExecutionWorkflowError("Use --master to configure the Spark master.")
    for key, value in spark_settings.items():
        if _SENSITIVE_SETTING.search(key) and not _ENVIRONMENT_REFERENCE.fullmatch(value):
            raise ExecutionWorkflowError(
                f"Sensitive Spark setting {key!r} must reference an environment "
                "variable using ${NAME}."
            )
    process_environment = dict(environment or {})
    for key, value in process_environment.items():
        if not _ENVIRONMENT_KEY.fullmatch(key) or not isinstance(value, str):
            raise ExecutionWorkflowError("Environment overrides require valid KEY=VALUE pairs.")
    try:
        run_base.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ExecutionWorkflowError(f"Cannot create run directory: {error}") from error

    run_id = str(uuid.uuid4())
    run_root = run_base / run_id
    run_root.mkdir(parents=False, exist_ok=False)
    package_root = Path(__file__).resolve().parent.parent
    worker_environment = _input_environment(package_root, process_environment)
    started = datetime.now(timezone.utc).isoformat()
    results: list[ProgramExecutionResult] = []

    for index, program in enumerate(selected):
        program_directory = run_root / f"program-{index + 1:03d}-{hashlib.sha256(program.id.encode()).hexdigest()[:10]}"
        outputs_directory = program_directory / "outputs"
        outputs_directory.mkdir(parents=True)
        stdout_path = program_directory / "stdout.log"
        stderr_path = program_directory / "stderr.log"
        stdout_path.touch()
        stderr_path.touch()
        stdout_relative = stdout_path.relative_to(root).as_posix()
        stderr_relative = stderr_path.relative_to(root).as_posix()
        program_started = datetime.now(timezone.utc).isoformat()
        program_clock = time.monotonic()
        exit_code: int | None = None
        timed_out = False
        error_message: str | None = None
        command: tuple[str, ...] = ()
        conversion: Conversion | None = None
        generated_relative: str | None = None
        snapshot_relative: str | None = None
        generated_hash: str | None = None
        source_hash: str | None = None

        try:
            source_path = _resolve_inside(root, program.path, "SAS source")
            source_hash = (
                hashlib.sha256(source_path.read_bytes()).hexdigest()
                if source_path.is_file()
                else None
            )
            conversion = _conversion_for_program(project, program.id, source_hash)
            if conversion is None:
                raise ExecutionWorkflowError(
                    "No successful conversion matches the current SAS source."
                )
            generated_relative = conversion.output_path
            generated_path = _resolve_inside(
                root, generated_relative or "", "converted program"
            )
            if not generated_path.is_file():
                raise ExecutionWorkflowError(
                    f"Converted program does not exist: {generated_relative!r}."
                )
            generated_code = generated_path.read_bytes()
            generated_hash = hashlib.sha256(generated_code).hexdigest()
            snapshot_path = program_directory / "converted.py"
            snapshot_path.write_bytes(generated_code)
            snapshot_relative = snapshot_path.relative_to(root).as_posix()

            config_path = program_directory / "execution-config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "app_name": f"sas2pyspark-{run_id[:8]}-{program.id[-8:]}",
                        "master": master,
                        "spark_conf": spark_settings,
                        "inputs": [
                            {
                                "dataset": item.dataset,
                                "path": str(_resolve_inside(root, item.path, "input")),
                            }
                            for item in inputs
                        ],
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            command = (
                sys.executable,
                "-m",
                "sas2pyspark.execution_worker",
                "--script",
                str(snapshot_path),
                "--config",
                str(config_path),
            )
            creation_flags = (
                subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
            )
            with stdout_path.open("wb") as stdout_file, stderr_path.open("wb") as stderr_file:
                process = subprocess.Popen(
                    command,
                    cwd=outputs_directory,
                    env=worker_environment,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    start_new_session=os.name != "nt",
                    creationflags=creation_flags,
                )
                try:
                    exit_code = process.wait(timeout=timeout_seconds)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    _terminate_process(process)
                    exit_code = process.returncode
            if timed_out:
                error_message = f"Execution exceeded timeout of {timeout_seconds} seconds."
            elif exit_code != 0:
                error_message = f"Worker exited with status {exit_code}."
        except (OSError, ExecutionWorkflowError) as error:
            error_message = str(error)

        try:
            output_paths = _output_files(outputs_directory, root)
        except OSError as error:
            output_paths = ()
            error_message = (
                f"{error_message}; cannot inventory outputs: {error}"
                if error_message
                else f"Cannot inventory outputs: {error}"
            )
        status = (
            "timed_out"
            if timed_out
            else "succeeded"
            if exit_code == 0 and error_message is None
            else "failed"
        )
        results.append(
            ProgramExecutionResult(
                program_id=program.id,
                source_path=program.path,
                source_sha256=source_hash,
                conversion_id=conversion.id if conversion else None,
                generated_code_path=generated_relative,
                execution_snapshot_path=snapshot_relative,
                generated_code_sha256=generated_hash,
                status=status,
                command=command,
                started_at=program_started,
                completed_at=datetime.now(timezone.utc).isoformat(),
                duration_seconds=round(time.monotonic() - program_clock, 3),
                exit_code=exit_code,
                timed_out=timed_out,
                stdout_path=stdout_relative,
                stderr_path=stderr_relative,
                output_paths=output_paths,
                error=error_message,
            )
        )

    report = ExecutionReport(
        run_id=run_id,
        project_id=project.project_id,
        project_name=project.name,
        repository_root=str(root),
        run_directory=run_root.relative_to(root).as_posix(),
        started_at=started,
        completed_at=datetime.now(timezone.utc).isoformat(),
        timeout_seconds=timeout_seconds,
        master=master,
        spark_conf=spark_settings,
        inputs=inputs,
        environment={
            **_python_environment(worker_environment),
            "environment_overrides": sorted(process_environment),
        },
        results=tuple(results),
    )
    report.save(run_root / "run.json")
    return report
