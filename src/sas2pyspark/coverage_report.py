"""Observed corpus conversion gaps; not a claim of general SAS coverage."""

from __future__ import annotations

from typing import Any, Mapping


class CoverageReportError(ValueError):
    """Raised when a conversion report cannot be summarized."""


def _category(error: str) -> str:
    text = error.casefold()
    if any(token in text for token in ("rsubmit", "sas/connect", "remote session")):
        return "remote_execution_boundary"
    if any(token in text for token in ("%include", "cannot read", "drive ", "not mapped")):
        return "dependency_or_path"
    if any(
        token in text
        for token in ("macro variable", "macro function", "%sysfunc", "call execute")
    ):
        return "macro_or_generated_code"
    if any(token in text for token in ("order by", "observation order", "duplicate by")):
        return "order_or_state_semantics"
    if any(
        token in text
        for token in (
            "unsupported",
            "cannot be flattened",
            "requires a",
            "cannot be used",
        )
    ):
        return "unsupported_converter_construct"
    return "other"


def build_coverage_report(conversion_report: Mapping[str, Any]) -> dict[str, Any]:
    """Summarize conversion outcomes and failure categories for one analyzed corpus."""
    if conversion_report.get("schema_version") != 1:
        raise CoverageReportError("Conversion report has an unsupported schema version.")
    outcomes = conversion_report.get("outcomes")
    if not isinstance(outcomes, list):
        raise CoverageReportError("Conversion report outcomes must be a list.")
    grouped: dict[str, list[str]] = {}
    succeeded = 0
    failed = 0
    for outcome in outcomes:
        if not isinstance(outcome, dict):
            raise CoverageReportError("Each conversion outcome must be an object.")
        status = outcome.get("status")
        if status == "succeeded":
            succeeded += 1
        elif status == "failed":
            failed += 1
            category = _category(str(outcome.get("error") or ""))
            grouped.setdefault(category, []).append(
                str(outcome.get("source_path") or "<unknown>")
            )
        elif status != "pending":
            raise CoverageReportError(f"Unknown conversion status {status!r}.")
    program_count = len(outcomes)
    categories = [
        {
            "category": name,
            "count": len(paths),
            "source_paths": sorted(set(paths), key=str.casefold),
        }
        for name, paths in sorted(
            grouped.items(), key=lambda item: (-len(item[1]), item[0])
        )
    ]
    return {
        "schema_version": 1,
        "scope": "observed conversion outcomes for the supplied corpus only",
        "general_sas_coverage_claim": False,
        "summary": {
            "program_count": program_count,
            "succeeded": succeeded,
            "failed": failed,
            "pending": program_count - succeeded - failed,
            "observed_conversion_rate": (
                round(succeeded / program_count, 4) if program_count else 0.0
            ),
            "blocker_category_count": len(categories),
        },
        "blockers": categories,
    }
