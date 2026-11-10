"""Static validation of batch-generated Python artifacts."""

from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


class CodeValidationError(ValueError):
    """Raised when a conversion report cannot be validated."""


def validate_generated_code(
    conversion_report: Mapping[str, Any],
    *,
    source_root: str | Path,
    code_root: str | Path,
) -> dict[str, Any]:
    """Syntax-check successful outputs and verify source hashes without executing code."""
    if conversion_report.get("schema_version") != 1:
        raise CodeValidationError("Conversion report has an unsupported schema version.")
    outcomes = conversion_report.get("outcomes")
    if not isinstance(outcomes, list):
        raise CodeValidationError("Conversion report outcomes must be a list.")
    try:
        source_base = Path(source_root).resolve(strict=True)
        code_base = Path(code_root).resolve(strict=True)
    except OSError as error:
        raise CodeValidationError(f"Cannot access validation roots: {error}") from error
    if not source_base.is_dir() or not code_base.is_dir():
        raise CodeValidationError("Source and generated-code roots must be directories.")

    results: list[dict[str, Any]] = []
    seen_outputs: set[str] = set()
    for outcome in outcomes:
        if not isinstance(outcome, dict):
            raise CodeValidationError("Each conversion outcome must be an object.")
        status = outcome.get("status")
        if status not in {"succeeded", "failed", "pending"}:
            raise CodeValidationError(f"Unknown conversion status {status!r}.")
        if status != "succeeded":
            continue
        output = outcome.get("output_path")
        source = outcome.get("source_path")
        if not isinstance(output, str) or not output.strip():
            raise CodeValidationError("Successful conversion is missing output_path.")
        if not isinstance(source, str) or not source.strip():
            raise CodeValidationError("Successful conversion is missing source_path.")
        output_path = (code_base / Path(output.replace("\\", "/"))).resolve(strict=False)
        source_path = (source_base / Path(source.replace("\\", "/"))).resolve(strict=False)
        try:
            output_path.relative_to(code_base)
            source_path.relative_to(source_base)
        except ValueError as error:
            raise CodeValidationError(
                "A conversion source or output path escapes its declared root."
            ) from error
        key = output_path.as_posix().casefold()
        if key in seen_outputs:
            raise CodeValidationError(f"Duplicate generated output path {output!r}.")
        seen_outputs.add(key)

        result: dict[str, Any] = {
            "source_path": source.replace("\\", "/"),
            "output_path": output.replace("\\", "/"),
            "status": "passed",
            "issues": [],
        }
        try:
            code = output_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            result["status"] = "failed"
            result["issues"].append(f"Cannot read generated output: {error}")
        else:
            try:
                ast.parse(code, filename=output)
                compile(code, output, "exec")
            except SyntaxError as error:
                result["status"] = "failed"
                result["issues"].append(
                    f"Generated Python syntax error at line {error.lineno}: {error.msg}."
                )
        try:
            source_bytes = source_path.read_bytes()
        except OSError as error:
            result["status"] = "failed"
            result["issues"].append(f"Cannot read source for freshness check: {error}")
        else:
            expected_hash = outcome.get("source_sha256")
            actual_hash = hashlib.sha256(source_bytes).hexdigest()
            if expected_hash != actual_hash:
                result["status"] = "failed"
                result["issues"].append("Source changed since conversion.")
        results.append(result)

    passed = sum(item["status"] == "passed" for item in results)
    failed = sum(item["status"] == "failed" for item in results)
    conversion_failed = sum(item.get("status") == "failed" for item in outcomes)
    return {
        "schema_version": 1,
        "validation": "static_python_syntax_and_source_freshness",
        "executed_generated_code": False,
        "summary": {
            "validated": len(results),
            "passed": passed,
            "failed": failed,
            "conversion_failed": conversion_failed,
        },
        "results": results,
    }


def load_conversion_report(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise CodeValidationError(
            f"Cannot read conversion report {str(path)!r}: {error}"
        ) from error
    if not isinstance(value, dict):
        raise CodeValidationError("Conversion report must be a JSON object.")
    return value
