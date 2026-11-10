"""Transparent, factor-based migration complexity model with optional calibration.

The score is a prioritisation heuristic, not an effort estimate. Every point is
attributed to a named factor so reviewers can challenge it, and weights can be
overridden or calibrated against actual migration hours.
"""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping

MODEL_VERSION = "2"

# Per-script factor weights (points per unit) and caps (maximum points).
DEFAULT_WEIGHTS: dict[str, dict[str, float]] = {
    "static_plan": {"weight": 1.0, "cap": 40},
    "conversion": {"weight": 1.0, "cap": 10},
    "external_connections": {"weight": 1.0, "cap": 6},
    "remote_blocks": {"weight": 2.0, "cap": 8},
    "schema_uncertainty": {"weight": 0.5, "cap": 6},
    "column_derivations": {"weight": 0.05, "cap": 6},
    "unresolved_findings": {"weight": 1.0, "cap": 6},
}
# Project-level factors: points per unit and cap, added to the size-aware script score.
DEFAULT_PROJECT_WEIGHTS: dict[str, dict[str, float]] = {
    "unmapped_libraries": {"weight": 0.5, "cap": 10},
    "unknown_schema_ratio": {"weight": 10.0, "cap": 10},
    "unresolved_lineage": {"weight": 0.2, "cap": 5},
    "system_count": {"weight": 0.02, "cap": 5},
}
_DERIVED_TRANSFORMS = {"expression", "aggregate", "conditional", "derived", "function"}
BANDS = ((20, "low"), (40, "medium"), (70, "high"))


class ComplexityError(ValueError):
    """Raised for invalid weights or calibration data."""


def band(score: float) -> str:
    for limit, name in BANDS:
        if score <= limit:
            return name
    return "very_high"


def load_weights(path: str | Path | None) -> dict[str, dict[str, dict[str, float]]]:
    """Return merged script and project weights, applying an optional JSON override."""
    weights = {
        "script": {key: dict(value) for key, value in DEFAULT_WEIGHTS.items()},
        "project": {key: dict(value) for key, value in DEFAULT_PROJECT_WEIGHTS.items()},
    }
    if path is None:
        return weights
    try:
        override = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ComplexityError(f"Cannot read weights {str(path)!r}: {error}") from error
    if not isinstance(override, dict):
        raise ComplexityError("Weights must be a JSON object.")
    for scope in ("script", "project"):
        section = override.get(scope, {})
        if not isinstance(section, dict):
            raise ComplexityError(f"Weights {scope!r} must be an object.")
        for name, values in section.items():
            if name not in weights[scope] or not isinstance(values, dict):
                raise ComplexityError(f"Unknown or invalid {scope} weight {name!r}.")
            for field, number in values.items():
                if field not in {"weight", "cap"} or isinstance(number, bool) or not isinstance(number, (int, float)) or number < 0:
                    raise ComplexityError(f"Invalid {scope} weight {name}.{field}.")
                weights[scope][name][field] = float(number)
    return weights


def _points(raw: float, spec: Mapping[str, float]) -> float:
    return round(min(raw * spec["weight"], spec["cap"]), 2)


def _key(value: Any) -> str:
    return str(value or "").replace("\\", "/").casefold()


def script_factors(
    *,
    plan_score: float,
    conversion_adjustment: float,
    external_connections: int,
    remote_blocks: int,
    uncertain_schemas: int,
    derived_columns: int,
    unresolved_findings: int,
    weights: Mapping[str, Mapping[str, float]],
) -> dict[str, dict[str, float]]:
    """Return factor name -> {raw, points} for one script."""
    raws = {
        "static_plan": plan_score,
        "conversion": conversion_adjustment,
        "external_connections": external_connections,
        "remote_blocks": remote_blocks,
        "schema_uncertainty": uncertain_schemas,
        "column_derivations": derived_columns,
        "unresolved_findings": unresolved_findings,
    }
    return {
        name: {"raw": raw, "points": _points(raw, weights[name])}
        for name, raw in raws.items()
    }


def script_evidence(
    pipeline_report: Mapping[str, Any], catalog_report: Mapping[str, Any]
) -> dict[str, dict[str, int]]:
    """Count per-script signals used by the extra factors, keyed by normalised path."""
    evidence: dict[str, dict[str, int]] = {}

    def bump(path: Any, name: str, amount: int = 1) -> None:
        entry = evidence.setdefault(
            _key(path),
            {"connections": 0, "remote": 0, "uncertain": 0, "derived": 0, "findings": 0},
        )
        entry[name] += amount

    for connection in pipeline_report.get("external_connections", []):
        bump(connection.get("source"), "connections")
    for remote in pipeline_report.get("remote_references", []):
        bump(remote.get("source"), "remote")
    for issue in list(pipeline_report.get("issues", [])) + list(
        catalog_report.get("issues", [])
    ):
        bump(issue.get("source"), "findings")
    for resource in catalog_report.get("resources", []):
        status = (resource.get("schema") or {}).get("status", "unknown")
        if resource.get("kind") == "sas_dataset" and status in {"unknown", "partial"}:
            for source in {_key(e.get("source")) for e in resource.get("evidence", [])}:
                bump(source, "uncertain")
    for edge in catalog_report.get("column_lineage", []):
        if str(edge.get("transform", "")).casefold() in _DERIVED_TRANSFORMS:
            bump(edge.get("source"), "derived")
    return evidence


def project_factors(
    *,
    script_scores: list[tuple[float, int]],
    pipeline_report: Mapping[str, Any],
    catalog_report: Mapping[str, Any],
    attempted: int,
    weights: Mapping[str, Mapping[str, float]],
) -> dict[str, Any]:
    """Aggregate script scores (size-aware) and add project-level factors."""
    total_weight = sum(size for _, size in script_scores)
    base = (
        sum(score * size for score, size in script_scores) / total_weight
        if total_weight
        else 0.0
    )
    summary = pipeline_report.get("summary", {})
    schema_counts: dict[str, int] = {}
    datasets = [r for r in catalog_report.get("resources", []) if r.get("kind") == "sas_dataset"]
    for resource in datasets:
        status = (resource.get("schema") or {}).get("status", "unknown")
        schema_counts[status] = schema_counts.get(status, 0) + 1
    unknown_ratio = schema_counts.get("unknown", 0) / len(datasets) if datasets else 0.0
    lineage = catalog_report.get("column_lineage", [])
    unresolved_edges = sum(1 for edge in lineage if edge.get("resolved") is False)
    raws = {
        "unmapped_libraries": float(
            summary.get("unresolved_library_count", 0)
            or sum(1 for i in pipeline_report.get("issues", []) if i.get("kind") == "library_path")
        ),
        "unknown_schema_ratio": unknown_ratio,
        "unresolved_lineage": float(unresolved_edges),
        "system_count": float(len(catalog_report.get("systems", []))),
    }
    factors = {
        name: {"raw": round(raw, 4), "points": _points(raw, weights[name])}
        for name, raw in raws.items()
    }
    extra = sum(item["points"] for item in factors.values())
    score = min(100.0, base + extra)
    gaps = {
        "unknown_schema_ratio": unknown_ratio,
        "unresolved_findings": min(1.0, len(pipeline_report.get("issues", [])) / 20),
        "not_converted": 1.0 - (attempted / len(script_scores)) if script_scores else 1.0,
    }
    confidence = round(max(0.0, 1.0 - sum(gaps.values()) / len(gaps)), 2)
    return {
        "score": round(score),
        "band": band(score),
        "size_weighted_script_score": round(base, 2),
        "factors": factors,
        "confidence": confidence,
        "confidence_gaps": {name: round(value, 2) for name, value in gaps.items()},
        "schema_status_counts": schema_counts,
    }


def _rank(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    position = 0
    while position < len(order):
        end = position
        while end + 1 < len(order) and values[order[end + 1]] == values[order[position]]:
            end += 1
        for index in order[position : end + 1]:
            ranks[index] = (position + end) / 2 + 1
        position = end + 1
    return ranks


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    mean_x, mean_y = sum(xs) / len(xs), sum(ys) / len(ys)
    sx = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    sy = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if sx == 0 or sy == 0:
        return None
    return sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / (sx * sy)


def read_actuals(path: str | Path) -> dict[str, float]:
    """Read a CSV with `path` and `hours` columns into {normalised path: hours}."""
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except (OSError, UnicodeError, csv.Error) as error:
        raise ComplexityError(f"Cannot read actuals {str(path)!r}: {error}") from error
    actuals: dict[str, float] = {}
    for number, row in enumerate(rows, start=2):
        try:
            hours = float(row["hours"])
            script = row["path"]
        except (KeyError, TypeError, ValueError) as error:
            raise ComplexityError(f"Actuals row {number} needs 'path' and numeric 'hours'.") from error
        if hours < 0 or not script:
            raise ComplexityError(f"Actuals row {number} is invalid.")
        actuals[_key(script)] = hours
    return actuals


def calibrate(
    programs: Iterable[Mapping[str, Any]], actuals: Mapping[str, float]
) -> dict[str, Any]:
    """Compare script scores with actual hours (rank correlation and a linear fit)."""
    pairs = [
        (float(p["complexity_score"]), actuals[_key(p["path"])])
        for p in programs
        if _key(p["path"]) in actuals
    ]
    result: dict[str, Any] = {"matched": len(pairs), "provided": len(actuals)}
    if len(pairs) < 3:
        result["note"] = "At least 3 matched scripts are needed for calibration."
        return result
    scores = [s for s, _ in pairs]
    hours = [h for _, h in pairs]
    result["spearman"] = _round(_pearson(_rank(scores), _rank(hours)))
    mean_s, mean_h = sum(scores) / len(scores), sum(hours) / len(hours)
    variance = sum((s - mean_s) ** 2 for s in scores)
    if variance:
        slope = sum((s - mean_s) * (h - mean_h) for s, h in pairs) / variance
        result["hours_per_point"] = round(slope, 4)
        result["intercept_hours"] = round(mean_h - slope * mean_s, 4)
        result["pearson"] = _round(_pearson(scores, hours))
    return result


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 4)
