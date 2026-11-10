"""One-command Phase 1 analysis that writes every artifact into a single folder."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .conversion_workflow import convert_project
from .dependency import build_migration_plan
from .discovery import (
    DEFAULT_ASSET_EXTENSIONS,
    DEFAULT_EXCLUDED_DIRECTORIES,
    discover_repository,
)
from .pipeline_resolver import load_pipeline_config, resolve_pipeline
from .project import Dependency, MigrationProject
from .project_report import build_project_report_from_files
from .knowledge_graph import build_knowledge_graph
from .business_context import build_business_context, build_prompts, render_markdown
from .source_sink_catalog import build_source_sink_catalog, load_pipeline_report
from .schema_assets import load_schema_assets
from .synthetic_data import generate_synthetic_data, load_catalog_report
from .validation import validate_generated_code
from .enterprise_guide import build_enterprise_guide_inventory
from .coverage_report import build_coverage_report

_SIMPLE_LET = re.compile(
    r"^[ \t]*%let[ \t]+([A-Za-z_]\w*)[ \t]*=[ \t]*([^;&%\r\n]*);",
    re.IGNORECASE | re.MULTILINE,
)

INDEX_NAME = "phase1-index.json"
INDEX_SCHEMA_VERSION = 1


class Phase1AnalysisError(ValueError):
    """Raised when the Phase 1 analysis cannot run."""


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_phase1_analysis(
    root: str | Path,
    config_path: str | Path,
    output_dir: str | Path,
    *,
    project_name: str = "phase1-analysis",
    sas_version: str = "auto",
    rows: int = 100,
    seed: int = 0,
    force: bool = False,
    schemas: str | Path | None = None,
) -> dict[str, Any]:
    """Run discover, resolve, catalog, plan, convert, report and synthesize.

    Returns the index written to ``<output_dir>/phase1-index.json``. Generated
    PySpark code goes under ``<output_dir>/code`` so the SAS tree is never modified.
    """
    try:
        repository_root = Path(root).resolve(strict=True)
        config_file = Path(config_path).resolve(strict=True)
    except OSError as error:
        raise Phase1AnalysisError(f"Cannot access input: {error}") from error
    if not repository_root.is_dir():
        raise Phase1AnalysisError(f"Repository root is not a directory: {repository_root}")
    out = Path(output_dir).resolve(strict=False)
    if out == repository_root or repository_root in out.parents or out in repository_root.parents:
        raise Phase1AnalysisError(
            "Output directory must be separate from the repository root (not inside or around it)."
        )
    if out.exists() and (not out.is_dir() or any(out.iterdir())) and not force:
        raise Phase1AnalysisError(
            f"Output directory is not empty: {out}. Use --force to reuse it."
        )
    out.mkdir(parents=True, exist_ok=True)

    files = {
        "project": out / "project.json",
        "inventory": out / "inventory.json",
        "pipeline_report": out / "pipeline-report.json",
        "catalog": out / "catalog.json",
        "knowledge_graph": out / "knowledge-graph.json",
        "business_context": out / "business-context.json",
        "business_context_markdown": out / "business-context.md",
        "ai_prompts": out / "ai-prompts.json",
        "plan": out / "plan.json",
        "conversion": out / "conversion.json",
        "report": out / "project-report.md",
        "complexity": out / "complexity.json",
        "validation": out / "validation.json",
        "enterprise_guide": out / "enterprise-guide.json",
        "coverage": out / "coverage.json",
        "synthetic_data": out / "synthetic-data",
        "generated_code": out / "code",
    }

    project = MigrationProject.create(project_name)
    inventory = discover_repository(
        repository_root,
        excluded_directories=tuple(DEFAULT_EXCLUDED_DIRECTORIES),
        asset_extensions=tuple(DEFAULT_ASSET_EXTENSIONS),
        excluded_paths=(),
    )
    for item in inventory.programs:
        project.add_program(item.path)
    inventory.save(files["inventory"])
    enterprise_guide = build_enterprise_guide_inventory(repository_root)
    _write_json(files["enterprise_guide"], enterprise_guide)

    pipeline = resolve_pipeline(repository_root, load_pipeline_config(config_file))
    pipeline.save(files["pipeline_report"])

    assets, asset_warnings = (
        load_schema_assets(schemas) if schemas else ({}, [])
    )
    catalog = build_source_sink_catalog(
        repository_root, load_pipeline_report(files["pipeline_report"]), assets
    )
    catalog["schema_asset_warnings"] = asset_warnings
    _write_json(files["catalog"], catalog)
    graph = build_knowledge_graph(pipeline.to_dict(), catalog)
    _write_json(files["knowledge_graph"], graph)
    context = build_business_context(pipeline.to_dict(), catalog, root=repository_root)
    _write_json(files["business_context"], context)
    files["business_context_markdown"].write_text(
        render_markdown(context), encoding="utf-8", newline="\n"
    )
    files["ai_prompts"].write_text(
        json.dumps(build_prompts(context["packets"]), indent=2) + "\n", encoding="utf-8"
    )

    plan = build_migration_plan(project, repository_root)
    plan.save(files["plan"])
    dataset_ids = {dataset.id for dataset in project.datasets}
    for flow in plan.datasets:
        if flow.dataset_id not in dataset_ids:
            project.add_dataset(flow.name)
            dataset_ids.add(flow.dataset_id)
    for edge in plan.dependencies:
        project.dependencies.append(
            Dependency.create(edge.source_id, edge.target_id, f"analysis:{edge.kind}")
        )

    server_by_path = {
        str(item.get("path", "")).casefold(): item.get("server")
        for item in pipeline.to_dict().get("scripts", [])
    }
    config_data = json.loads(config_file.read_text(encoding="utf-8-sig"))

    def include_drives(source_path: str) -> dict[str, Path]:
        server = server_by_path.get(source_path.replace("\\", "/").casefold())
        drives = config_data.get("servers", {}).get(server, {}).get("drives", {})
        return {letter: repository_root / target for letter, target in drives.items()}

    def macro_variables(source_path: str) -> dict[str, str]:
        server = server_by_path.get(source_path.replace("\\", "/").casefold())
        values: dict[str, str] = {}
        settings = config_data.get("servers", {}).get(server, {})
        for script in settings.get("initialization_scripts", []):
            try:
                text = (repository_root / script).read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            for match in _SIMPLE_LET.finditer(text):
                values[match.group(1)] = match.group(2).strip()
        values.update(
            {
                name: value
                for name, value in config_data.get("macro_variables", {}).items()
                if isinstance(value, str)
            }
        )
        return values

    conversion = convert_project(
        project,
        repository_root,
        output_directory="pyspark",
        force=force,
        sas_version=sas_version,
        output_base=files["generated_code"],
        include_drives=include_drives,
        macro_variables=macro_variables,
    )
    conversion.save(files["conversion"])
    validation = validate_generated_code(
        conversion.to_dict(),
        source_root=repository_root,
        code_root=files["generated_code"],
    )
    _write_json(files["validation"], validation)
    coverage = build_coverage_report(conversion.to_dict())
    _write_json(files["coverage"], coverage)
    project.validate()
    project.save(files["project"])

    report = build_project_report_from_files(
        pipeline_report_path=files["pipeline_report"],
        catalog_report_path=files["catalog"],
        plan_report_path=files["plan"],
        conversion_report_path=files["conversion"],
    )
    files["report"].write_text(report["markdown"], encoding="utf-8", newline="\n")
    files["complexity"].write_text(
        json.dumps(
            {
                "schema_version": 1,
                "project": report["complexity_model"],
                "scripts": [
                    {
                        "path": item["path"],
                        "complexity_score": item["complexity_score"],
                        "complexity": item["complexity"],
                        "factors": item["complexity_factors"],
                    }
                    for item in report["programs"]
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )

    synthetic = generate_synthetic_data(
        load_catalog_report(files["catalog"]),
        files["synthetic_data"],
        rows=rows,
        seed=seed,
        force=force,
    )

    summaries = {
        "inventory": {
            "program_count": len(inventory.programs),
            "asset_count": len(inventory.assets),
            "issue_count": len(inventory.issues),
        },
        "pipeline": pipeline.to_dict()["summary"],
        "catalog": catalog["summary"],
        "knowledge_graph": graph["summary"],
        "business_context": context["summary"],
        "plan": plan.to_dict()["summary"],
        "conversion": conversion.to_dict()["summary"],
        "validation": validation["summary"],
        "enterprise_guide": enterprise_guide["summary"],
        "coverage": coverage["summary"],
        "report": {
            "complexity_score": report["complexity_score"],
            "complexity": report["complexity"],
            "complexity_confidence": report["complexity_model"]["confidence"],
            "summary": report["summary"],
        },
        "synthetic_data": synthetic["summary"],
    }
    index = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repository_root": str(repository_root),
        "pipeline_config": str(config_file),
        "sas_version": sas_version,
        "artifacts": {
            name: path.relative_to(out).as_posix() for name, path in files.items()
        },
        "summaries": summaries,
        "limitations": [
            "Reachability, libraries, systems and schemas are static analysis results; "
            "unresolved and unknown items are listed in the pipeline report and catalog.",
            "Schemas come only from explicit LENGTH/ATTRIB/FORMAT declarations and are partial.",
            "Complexity is a prioritization heuristic, not an effort estimate.",
            "Remote (RSUBMIT) code is tracked but not converted or executed.",
        ],
    }
    _write_json(out / INDEX_NAME, index)
    return index
