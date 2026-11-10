"""Command-line interface for sas2pyspark."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from .converter import ConversionError, SasToPySparkConverter
from .discovery import (
    DEFAULT_ASSET_EXTENSIONS,
    DEFAULT_EXCLUDED_DIRECTORIES,
    DiscoveryError,
    discover_repository,
)
from .dependency import DependencyAnalysisError, build_migration_plan
from .conversion_workflow import BatchConversionError, convert_project
from .execution_workflow import ExecutionWorkflowError, run_project
from .pipeline_resolver import (
    PipelineResolverError,
    load_pipeline_config,
    resolve_pipeline,
)
from .source_sink_catalog import (
    SourceSinkCatalogError,
    build_source_sink_catalog,
    load_pipeline_report,
)
from .project_report import ProjectReportError, build_project_report_from_files
from .phase1_analysis import Phase1AnalysisError, run_phase1_analysis
from .schema_assets import SchemaAssetError, load_schema_assets
from .synthetic_data import (
    SyntheticDataError,
    generate_synthetic_data,
    load_catalog_report,
)
from .reconciliation import ReconciliationError, reconcile_project
from .project import Dependency, MigrationProject, ProjectError
from .governance import (
    APPROVAL_DECISIONS,
    APPROVAL_GATES,
    ApprovalReport,
    GovernanceError,
    create_approval_report,
    evaluate_approval,
    record_approval,
)
from .readiness import ReadinessError, build_readiness_report
from .review_workflow import (
    REVIEW_DECISIONS,
    ReviewReport,
    ReviewWorkflowError,
    build_review_report,
    record_review_decision,
)


def _project_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="sas2pyspark project",
        description="Manage a migration project and its analysis reports.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    init_parser = commands.add_parser("init", help="Create an empty project manifest")
    init_parser.add_argument("path", help="Destination JSON manifest path")
    init_parser.add_argument("--name", required=True, help="Human-readable project name")
    validate_parser = commands.add_parser(
        "validate", help="Validate an existing project manifest"
    )
    validate_parser.add_argument("path", help="Project JSON manifest path")
    discover_parser = commands.add_parser(
        "discover", help="Inventory SAS programs and companion repository assets"
    )
    discover_parser.add_argument("path", help="Project JSON manifest path to update")
    discover_parser.add_argument(
        "--root",
        required=True,
        help="Repository directory to scan; reported paths are relative to this root",
    )
    discover_parser.add_argument(
        "--report",
        help="Inventory JSON destination; defaults to <project-name>.inventory.json",
    )
    discover_parser.add_argument(
        "--exclude-dir",
        action="append",
        default=[],
        metavar="PATTERN",
        help=(
            "Additional directory name or relative-path glob to skip; "
            "may be repeated"
        ),
    )
    discover_parser.add_argument(
        "--asset-extension",
        action="append",
        default=None,
        metavar="EXT",
        help=(
            "Asset extension to inventory (for example .csv); repeat to specify "
            "several, replacing the default list"
        ),
    )
    pipeline_parser = commands.add_parser(
        "resolve-pipeline",
        help="Resolve include reachability from configured SAS server entrypoints",
    )
    pipeline_parser.add_argument(
        "--root",
        required=True,
        help="Repository directory containing the server folder trees",
    )
    pipeline_parser.add_argument(
        "--config",
        required=True,
        help="JSON configuration with server drive mappings and job entrypoints",
    )
    pipeline_parser.add_argument(
        "--report",
        help="Pipeline report destination; defaults beside the config file",
    )
    catalog_parser = commands.add_parser(
        "catalog",
        help="Catalog dataset sources, sinks, external systems, and known schema details",
    )
    catalog_parser.add_argument(
        "--root",
        required=True,
        help="Repository directory containing the pipeline SAS sources",
    )
    catalog_parser.add_argument(
        "--pipeline-report",
        required=True,
        help="JSON report produced by project resolve-pipeline",
    )
    catalog_parser.add_argument(
        "--report",
        help="Catalog JSON destination; defaults beside the pipeline report",
    )
    catalog_parser.add_argument(
        "--schemas",
        help="Schema assets: a file or folder of PROC CONTENTS CSV, JSON or SQL DDL",
    )
    report_parser = commands.add_parser(
        "report",
        help="Generate project Markdown documentation and a Mermaid pipeline graph",
    )
    report_parser.add_argument("--pipeline-report", required=True)
    report_parser.add_argument("--catalog-report", required=True)
    report_parser.add_argument("--plan-report", required=True)
    report_parser.add_argument("--conversion-report", required=True)
    report_parser.add_argument(
        "--output",
        required=True,
        help="Markdown report destination",
    )
    report_parser.add_argument(
        "--force",
        action="store_true",
        help="Replace an existing Markdown report",
    )
    synthetic_parser = commands.add_parser(
        "synthesize",
        help="Generate deterministic synthetic CSV fixtures from catalog schemas",
    )
    synthetic_parser.add_argument("--catalog", required=True)
    synthetic_parser.add_argument("--output-dir", required=True)
    synthetic_parser.add_argument("--rows", type=int, default=100)
    synthetic_parser.add_argument("--seed", type=int, default=0)
    synthetic_parser.add_argument("--force", action="store_true")
    analyze_parser = commands.add_parser(
        "analyze",
        help="Run the full Phase 1 analysis and write all artifacts to one folder",
    )
    analyze_parser.add_argument("--root", required=True, help="Repository directory with the SAS server trees")
    analyze_parser.add_argument("--config", required=True, help="Pipeline JSON configuration")
    analyze_parser.add_argument("--out", required=True, help="Output folder (outside --root)")
    analyze_parser.add_argument("--name", default="phase1-analysis", help="Project name")
    analyze_parser.add_argument(
        "--sas-version", choices=("auto", "9.4M4", "9.4M8", "Viya"), default="auto"
    )
    analyze_parser.add_argument("--rows", type=int, default=100)
    analyze_parser.add_argument(
        "--schemas",
        help="Schema assets: a file or folder of PROC CONTENTS CSV, JSON or SQL DDL",
    )
    analyze_parser.add_argument("--seed", type=int, default=0)
    analyze_parser.add_argument("--force", action="store_true", help="Reuse a non-empty output folder")
    plan_parser = commands.add_parser(
        "plan", help="Analyze dependencies and generate an ordered migration plan"
    )
    plan_parser.add_argument("path", help="Project JSON manifest path")
    plan_parser.add_argument(
        "--root",
        required=True,
        help="Repository directory used as the base for project-relative SAS paths",
    )
    plan_parser.add_argument(
        "--report",
        help="Plan JSON destination; defaults to <project-name>.plan.json",
    )
    convert_parser = commands.add_parser(
        "convert", help="Batch-convert registered SAS programs independently"
    )
    convert_parser.add_argument("path", help="Project JSON manifest path to update")
    convert_parser.add_argument(
        "--root",
        required=True,
        help="Repository directory used as the base for project-relative SAS paths",
    )
    convert_parser.add_argument(
        "--output-dir",
        default="pyspark",
        help="Generated-code directory relative to --root (default: pyspark)",
    )
    convert_parser.add_argument(
        "--report",
        help="Conversion report destination; defaults to <project-name>.conversion.json",
    )
    convert_parser.add_argument(
        "--sas-version",
        choices=("auto", "9.4M4", "9.4M8", "Viya"),
        default="auto",
        help=(
            "Source SAS release profile; auto reads the source marker or defaults "
            "to Viya (default: auto)"
        ),
    )
    convert_parser.add_argument(
        "--force",
        action="store_true",
        help="Replace existing generated files in --output-dir",
    )
    run_parser = commands.add_parser(
        "run", help="Execute selected converted programs in local worker processes"
    )
    run_parser.add_argument("path", help="Project JSON manifest path")
    run_parser.add_argument(
        "--root",
        required=True,
        help="Repository directory used as the base for project-relative paths",
    )
    run_parser.add_argument(
        "--program",
        action="append",
        required=True,
        help="Registered source path or program ID to execute; repeat to select several",
    )
    run_parser.add_argument(
        "--input",
        action="append",
        default=[],
        metavar="DATASET=PATH",
        help="Bind an input SAS dataset to a CSV, Parquet, or JSON file under --root",
    )
    run_parser.add_argument(
        "--master",
        default="local[*]",
        help="Spark master URL (default: local[*])",
    )
    run_parser.add_argument(
        "--spark-conf",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Spark configuration entry; repeat as needed",
    )
    run_parser.add_argument(
        "--timeout",
        type=float,
        default=3600,
        help="Per-program timeout in seconds (default: 3600)",
    )
    run_parser.add_argument(
        "--run-dir",
        default="runs",
        help="Repository-relative directory for logs, run reports, and outputs",
    )
    run_parser.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Environment override for worker processes; values are not copied to the report",
    )
    reconcile_parser = commands.add_parser(
        "reconcile", help="Compare SAS reference datasets with Spark outputs"
    )
    reconcile_parser.add_argument("path", help="Project JSON manifest path")
    reconcile_parser.add_argument(
        "--root",
        required=True,
        help="Repository directory used as the base for project-relative paths",
    )
    reconcile_parser.add_argument(
        "--reference",
        action="append",
        required=True,
        metavar="DATASET=PATH",
        help="SAS reference dataset file or directory; repeat per dataset",
    )
    reconcile_parser.add_argument(
        "--spark-output",
        action="append",
        required=True,
        metavar="DATASET=PATH",
        help="Spark output file or directory, paired by dataset name; repeat per dataset",
    )
    reconcile_parser.add_argument(
        "--keys",
        action="append",
        default=[],
        metavar="DATASET=COLUMN[,COLUMN...]",
        help="Unique key columns for row matching; omit to compare unordered multisets",
    )
    reconcile_parser.add_argument(
        "--rtol",
        type=float,
        default=0.0,
        help="Relative numeric tolerance (default: 0)",
    )
    reconcile_parser.add_argument(
        "--atol",
        type=float,
        default=0.0,
        help="Absolute numeric tolerance (default: 0)",
    )
    reconcile_parser.add_argument(
        "--max-examples",
        type=int,
        default=20,
        help="Maximum differing-row examples to include per dataset (default: 20)",
    )
    reconcile_parser.add_argument(
        "--master",
        default="local[*]",
        help="Spark master URL for reconciliation (default: local[*])",
    )
    reconcile_parser.add_argument(
        "--spark-conf",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Spark configuration entry; repeat as needed",
    )
    reconcile_parser.add_argument(
        "--execution-report",
        help="Optional repository-relative execution run report to link",
    )
    reconcile_parser.add_argument(
        "--report",
        help="Reconciliation JSON report destination (default: unique file under reconciliation/)",
    )
    review_parser = commands.add_parser(
        "review", help="Generate or refresh a report of conversion issues"
    )
    review_parser.add_argument("path", help="Project JSON manifest path")
    review_parser.add_argument(
        "--root",
        required=True,
        help="Repository directory used as the base for project-relative paths",
    )
    review_parser.add_argument(
        "--report",
        help="Review report destination; defaults to <project-name>.review.json",
    )
    decision_parser = commands.add_parser(
        "review-decision", help="Record a human decision for a reported issue"
    )
    decision_parser.add_argument("path", help="Project JSON manifest path")
    decision_parser.add_argument(
        "--root",
        required=True,
        help="Repository directory used as the base for project-relative paths",
    )
    decision_parser.add_argument(
        "--report",
        help="Review report to update; defaults to <project-name>.review.json",
    )
    decision_parser.add_argument("--issue", required=True, help="Stable review issue ID")
    decision_parser.add_argument(
        "--decision",
        required=True,
        choices=sorted(REVIEW_DECISIONS),
        help="Human review decision",
    )
    decision_parser.add_argument(
        "--reviewer", required=True, help="Reviewer name or identifier"
    )
    decision_parser.add_argument("--notes", help="Decision rationale or reviewer notes")
    decision_parser.add_argument(
        "--corrected-code",
        help="Python file containing the correction (required for corrected decisions)",
    )
    approval_parser = commands.add_parser(
        "approval", help="Record an auditable decision for a governance gate"
    )
    approval_parser.add_argument("path", help="Project JSON manifest path")
    approval_parser.add_argument("--root", required=True, help="Repository directory")
    approval_parser.add_argument(
        "--report",
        help="Approval report; defaults to <project-name>.approvals.json",
    )
    approval_parser.add_argument(
        "--review-report",
        help="Linked review report; defaults to <project-name>.review.json",
    )
    approval_parser.add_argument(
        "--gate", required=True, choices=APPROVAL_GATES, help="Reviewer gate"
    )
    approval_parser.add_argument(
        "--decision",
        required=True,
        choices=APPROVAL_DECISIONS,
        help="Gate decision",
    )
    approval_parser.add_argument("--reviewer", required=True, help="Reviewer identifier")
    approval_parser.add_argument("--comments", help="Decision rationale")
    approval_parser.add_argument(
        "--evidence",
        action="append",
        default=[],
        help="Evidence reference (path, URL, or ticket); repeat as needed",
    )
    approval_parser.add_argument(
        "--required-gate",
        action="append",
        choices=APPROVAL_GATES,
        default=None,
        help="Override the required gate set; repeat (default: all four gates)",
    )
    approval_parser.add_argument(
        "--critical-issue",
        action="append",
        default=None,
        help="Mark a review issue ID as critical; repeat as needed",
    )
    readiness_parser = commands.add_parser(
        "readiness", help="Build a versioned evidence manifest and readiness report"
    )
    readiness_parser.add_argument("path", help="Project JSON manifest path")
    readiness_parser.add_argument("--root", required=True, help="Repository directory")
    readiness_parser.add_argument("--inventory", help="Repository inventory report")
    readiness_parser.add_argument("--dependency", help="Dependency analysis report")
    readiness_parser.add_argument("--conversion", help="Batch conversion report")
    readiness_parser.add_argument("--review-report", help="Human review report")
    readiness_parser.add_argument(
        "--execution-report",
        action="append",
        default=[],
        help="Execution run report to include; repeat for additional runs",
    )
    readiness_parser.add_argument(
        "--reconciliation-report",
        action="append",
        default=[],
        help="Reconciliation report to include; repeat for additional comparisons",
    )
    readiness_parser.add_argument("--approval-report", help="Governance approval report")
    readiness_parser.add_argument(
        "--report",
        help="Readiness report destination; defaults to a unique file under readiness/",
    )
    args = parser.parse_args(argv)

    try:
        if args.command == "init":
            destination = Path(args.path)
            if destination.exists():
                raise ProjectError(
                    f"Refusing to overwrite existing project file {str(destination)!r}."
                )
            MigrationProject.create(args.name).save(destination)
            print(f"Created migration project: {destination}")
        elif args.command == "validate":
            project = MigrationProject.load(args.path)
            print(
                f"Valid migration project {project.name!r} "
                f"(schema v{project.schema_version}): {args.path}"
            )
        elif args.command == "discover":
            manifest_path = Path(args.path).resolve(strict=True)
            report_path = (
                Path(args.report).resolve(strict=False)
                if args.report
                else manifest_path.with_suffix(".inventory.json")
            )
            if report_path == manifest_path:
                raise ProjectError(
                    "Inventory report path cannot replace the project manifest."
                )
            project = MigrationProject.load(manifest_path)
            exclusions = tuple(DEFAULT_EXCLUDED_DIRECTORIES) + tuple(args.exclude_dir)
            extensions = (
                tuple(args.asset_extension)
                if args.asset_extension is not None
                else tuple(DEFAULT_ASSET_EXTENSIONS)
            )
            report = discover_repository(
                args.root,
                excluded_directories=exclusions,
                asset_extensions=extensions,
                excluded_paths=(manifest_path, report_path),
            )
            known_paths = {program.path.casefold() for program in project.programs}
            added_programs = 0
            for item in report.programs:
                if item.path.casefold() not in known_paths:
                    project.add_program(item.path)
                    known_paths.add(item.path.casefold())
                    added_programs += 1
            project.save(manifest_path)
            report.save(report_path)
            print(
                f"Discovered {len(report.programs)} SAS program(s), "
                f"{len(report.assets)} asset(s), and {len(report.issues)} issue(s); "
                f"registered {added_programs} new program(s)."
            )
            print(f"Inventory report: {report_path}")
            if report.issues:
                return 1
        elif args.command == "resolve-pipeline":
            repository_root = Path(args.root).resolve(strict=True)
            config_path = Path(args.config).resolve(strict=True)
            report_path = (
                Path(args.report).resolve(strict=False)
                if args.report
                else config_path.with_suffix(".pipeline.json")
            )
            if report_path == config_path or report_path.suffix.casefold() == ".sas":
                raise PipelineResolverError(
                    "Pipeline report path conflicts with the config or a SAS source file."
                )
            config = load_pipeline_config(config_path)
            report = resolve_pipeline(repository_root, config)
            report.save(report_path)
            summary = report.to_dict()["summary"]
            print(
                f"Resolved {summary['reachable_script_count']} of "
                f"{summary['script_count']} SAS script(s); "
                f"{summary['resolved_include_count']} include(s) resolved, "
                f"{summary['unresolved_include_count']} unresolved; "
                f"{summary['external_connection_count']} external connection(s) recorded."
            )
            print(f"Pipeline report: {report_path}")
            if summary["unresolved_include_count"] or summary["issue_count"]:
                return 1
        elif args.command == "catalog":
            repository_root = Path(args.root).resolve(strict=True)
            pipeline_report_path = Path(args.pipeline_report).resolve(strict=True)
            report_path = (
                Path(args.report).resolve(strict=False)
                if args.report
                else pipeline_report_path.with_suffix(".catalog.json")
            )
            if (
                report_path == pipeline_report_path
                or report_path.suffix.casefold() == ".sas"
            ):
                raise SourceSinkCatalogError(
                    "Catalog report path conflicts with the pipeline report or a SAS source file."
                )
            pipeline_report = load_pipeline_report(pipeline_report_path)
            assets: dict[str, Any] | None = None
            if args.schemas:
                assets, asset_warnings = load_schema_assets(args.schemas)
                for warning in asset_warnings:
                    print(
                        f"warning: {warning['dataset']} defined in {warning['kept']} and "
                        f"{warning['ignored']}; kept {warning['kept']}.",
                        file=sys.stderr,
                    )
            report = build_source_sink_catalog(repository_root, pipeline_report, assets)
            report_path.write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            summary = report["summary"]
            print(
                f"Catalogued {summary['dataset_count']} dataset(s), "
                f"{summary['file_resource_count']} file resource(s), and "
                f"{summary['system_count']} system(s); "
                f"{summary['unknown_schema_count']} schema(s) unknown."
            )
            print(f"Source/sink catalog: {report_path}")
        elif args.command == "report":
            report_path = Path(args.output).resolve(strict=False)
            if report_path.suffix.casefold() != ".md":
                raise ProjectReportError("Project report output must use a .md extension.")
            if report_path.exists() and not args.force:
                raise ProjectReportError(
                    f"Project report already exists: {report_path}. Use --force to replace it."
                )
            report = build_project_report_from_files(
                pipeline_report_path=args.pipeline_report,
                catalog_report_path=args.catalog_report,
                plan_report_path=args.plan_report,
                conversion_report_path=args.conversion_report,
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            temporary_path: Path | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    newline="\n",
                    dir=report_path.parent,
                    prefix=f".{report_path.name}.",
                    suffix=".tmp",
                    delete=False,
                ) as temporary:
                    temporary.write(report["markdown"])
                    temporary_path = Path(temporary.name)
                if args.force:
                    temporary_path.replace(report_path)
                else:
                    temporary_path.rename(report_path)
            finally:
                if temporary_path is not None and temporary_path.exists():
                    temporary_path.unlink()
            print(
                f"Project report: {report_path} "
                f"(complexity {report['complexity_score']}/100, "
                f"{report['summary']['unresolved_finding_count']} finding(s))."
            )
        elif args.command == "synthesize":
            catalog = load_catalog_report(args.catalog)
            report = generate_synthetic_data(
                catalog,
                args.output_dir,
                rows=args.rows,
                seed=args.seed,
                force=args.force,
            )
            print(
                f"Generated {report['summary']['generated_count']} dataset fixture(s); "
                f"{report['summary']['skipped_count']} skipped, "
                f"{report['summary']['partial_schema_count']} from partial schemas."
            )
            print(f"Synthetic data manifest: {Path(args.output_dir).resolve() / 'synthetic-data-manifest.json'}")
        elif args.command == "analyze":
            index = run_phase1_analysis(
                args.root,
                args.config,
                args.out,
                project_name=args.name,
                sas_version=args.sas_version,
                rows=args.rows,
                seed=args.seed,
                force=args.force,
                schemas=args.schemas,
            )
            pipeline = index["summaries"]["pipeline"]
            catalog = index["summaries"]["catalog"]
            conversion = index["summaries"]["conversion"]
            print(
                f"Phase 1 analysis: {pipeline['reachable_script_count']} of "
                f"{pipeline['script_count']} script(s) reachable; "
                f"{pipeline['unresolved_include_count']} unresolved include(s); "
                f"{catalog['dataset_count']} dataset(s), {catalog['system_count']} system(s), "
                f"{catalog['unknown_schema_count']} unknown schema(s); "
                f"{conversion['succeeded']} of {conversion['program_count']} converted; "
                f"complexity {index['summaries']['report']['complexity_score']}/100."
            )
            print(f"Phase 1 index: {Path(args.out).resolve() / 'phase1-index.json'}")
        elif args.command == "plan":
            manifest_path = Path(args.path).resolve(strict=True)
            report_path = (
                Path(args.report).resolve(strict=False)
                if args.report
                else manifest_path.with_suffix(".plan.json")
            )
            if report_path == manifest_path:
                raise ProjectError("Plan report path cannot replace the project manifest.")
            project = MigrationProject.load(manifest_path)
            plan = build_migration_plan(project, args.root)
            plan.save(report_path)
            project.dependencies = [
                dependency
                for dependency in project.dependencies
                if not dependency.kind.startswith("analysis:")
            ]
            dataset_ids = {dataset.id for dataset in project.datasets}
            for flow in plan.datasets:
                if flow.dataset_id not in dataset_ids:
                    project.add_dataset(flow.name)
                    dataset_ids.add(flow.dataset_id)
            for edge in plan.dependencies:
                project.dependencies.append(
                    Dependency.create(
                        edge.source_id,
                        edge.target_id,
                        f"analysis:{edge.kind}",
                    )
                )
            project.validate()
            project.save(manifest_path)
            summary = plan.to_dict()["summary"]
            print(
                f"Planned {summary['program_count']} program(s) with "
                f"{summary['dependency_count']} dependency edge(s), "
                f"{summary['unresolved_reference_count']} unresolved reference(s), "
                f"and {summary['cycle_count']} cycle(s)."
            )
            print(f"Migration plan: {report_path}")
            if summary["unresolved_reference_count"] or summary["cycle_count"]:
                return 1
        elif args.command == "review":
            manifest_path = Path(args.path).resolve(strict=True)
            report_path = (
                Path(args.report).resolve(strict=False)
                if args.report
                else manifest_path.with_suffix(".review.json")
            )
            if report_path == manifest_path:
                raise ReviewWorkflowError(
                    "Review report path cannot replace the project manifest."
                )
            project = MigrationProject.load(manifest_path)
            repository_root = Path(args.root).resolve(strict=True)
            for program in project.programs:
                source_path = (repository_root / Path(program.path)).resolve(strict=False)
                if report_path == source_path:
                    raise ReviewWorkflowError(
                        "Review report path conflicts with a SAS source file."
                    )
            for conversion in project.conversions:
                if conversion.output_path:
                    output_path = (
                        repository_root / Path(conversion.output_path)
                    ).resolve(strict=False)
                    if report_path == output_path:
                        raise ReviewWorkflowError(
                            "Review report path conflicts with a generated output."
                        )
            existing_report = (
                ReviewReport.load(report_path) if report_path.exists() else None
            )
            report = build_review_report(
                project,
                repository_root,
                existing_report=existing_report,
            )
            report.save(report_path)
            summary = report.to_dict()["summary"]
            print(
                f"Review report contains {summary['issue_count']} issue(s): "
                f"{summary['pending']} pending, {summary['accepted']} accepted, "
                f"{summary['corrected']} corrected, {summary['deferred']} deferred, "
                f"{summary['rejected']} rejected."
            )
            print(f"Review report: {report_path}")
        elif args.command == "review-decision":
            manifest_path = Path(args.path).resolve(strict=True)
            report_path = (
                Path(args.report).resolve(strict=False)
                if args.report
                else manifest_path.with_suffix(".review.json")
            )
            if report_path == manifest_path:
                raise ReviewWorkflowError(
                    "Review report path cannot replace the project manifest."
                )
            project = MigrationProject.load(manifest_path)
            repository_root = Path(args.root).resolve(strict=True)
            for program in project.programs:
                source_path = (repository_root / Path(program.path)).resolve(strict=False)
                if report_path == source_path:
                    raise ReviewWorkflowError(
                        "Review report path conflicts with a SAS source file."
                    )
            for conversion in project.conversions:
                if conversion.output_path:
                    output_path = (
                        repository_root / Path(conversion.output_path)
                    ).resolve(strict=False)
                    if report_path == output_path:
                        raise ReviewWorkflowError(
                            "Review report path conflicts with a generated output."
                        )
            report = ReviewReport.load(report_path)
            if report.project_id != project.project_id:
                raise ReviewWorkflowError("Review report belongs to a different project.")
            report = record_review_decision(
                report,
                args.issue,
                args.decision,
                reviewer=args.reviewer,
                repository_root=repository_root,
                notes=args.notes,
                corrected_code=args.corrected_code,
            )
            report.save(report_path)
            updated_issue = next(item for item in report.issues if item.id == args.issue)
            event = updated_issue.history[-1]
            print(
                f"Recorded {event.decision} decision for {updated_issue.id} "
                f"by {event.reviewer}."
            )
            if event.corrected_output_path:
                print(f"Corrected output: {event.corrected_output_path}")
            print(f"Review report: {report_path}")
        elif args.command == "approval":
            manifest_path = Path(args.path).resolve(strict=True)
            repository_root = Path(args.root).resolve(strict=True)
            project = MigrationProject.load(manifest_path)
            report_path = (
                Path(args.report).resolve(strict=False)
                if args.report
                else manifest_path.with_suffix(".approvals.json")
            )
            review_path = (
                Path(args.review_report).resolve(strict=True)
                if args.review_report
                else manifest_path.with_suffix(".review.json").resolve(strict=True)
            )
            if report_path == manifest_path or report_path == review_path:
                raise GovernanceError(
                    "Approval report path cannot replace the project or review report."
                )
            protected_paths = {manifest_path, review_path}
            protected_paths.update(
                (repository_root / Path(program.path)).resolve(strict=False)
                for program in project.programs
            )
            protected_paths.update(
                (repository_root / Path(conversion.output_path)).resolve(strict=False)
                for conversion in project.conversions
                if conversion.output_path
            )
            if report_path in protected_paths:
                raise GovernanceError(
                    "Approval report path conflicts with a manifest, review report, "
                    "source, or generated-code file."
                )
            review_report = ReviewReport.load(review_path)
            existing = (
                ApprovalReport.load(report_path) if report_path.exists() else None
            )
            if existing is not None and existing.project_id != project.project_id:
                raise GovernanceError("Approval report belongs to a different project.")
            if existing is not None and existing.repository_root != str(repository_root):
                raise GovernanceError("Approval report belongs to a different repository root.")
            approval_report = existing or create_approval_report(
                project,
                repository_root,
                review_report,
                required_gates=args.required_gate or APPROVAL_GATES,
                critical_issue_ids=args.critical_issue or (),
            )
            approval_report = record_approval(
                approval_report,
                args.gate,
                args.decision,
                reviewer=args.reviewer,
                review_report=review_report,
                comments=args.comments,
                evidence_references=args.evidence,
                required_gates=args.required_gate,
                critical_issue_ids=args.critical_issue,
            )
            approval_report.save(report_path)
            status, blockers = evaluate_approval(approval_report, review_report)
            print(
                f"Recorded {args.gate} {args.decision} decision by {args.reviewer}; "
                f"migration approval status: {status}."
            )
            for blocker in blockers:
                print(f"  Blocker: {blocker}")
            print(f"Approval report: {report_path}")
        elif args.command == "readiness":
            manifest_path = Path(args.path).resolve(strict=True)
            repository_root = Path(args.root).resolve(strict=True)
            project = MigrationProject.load(manifest_path)
            evidence_paths = {
                "inventory": args.inventory
                or str(manifest_path.with_suffix(".inventory.json")),
                "dependency": args.dependency
                or str(manifest_path.with_suffix(".plan.json")),
                "conversion": args.conversion
                or str(manifest_path.with_suffix(".conversion.json")),
                "review": args.review_report
                or str(manifest_path.with_suffix(".review.json")),
                "execution": args.execution_report,
                "reconciliation": args.reconciliation_report,
                "approval": args.approval_report
                or str(manifest_path.with_suffix(".approvals.json")),
            }
            report = build_readiness_report(
                project, repository_root, evidence_paths=evidence_paths
            )
            report_path = (
                Path(args.report).resolve(strict=False)
                if args.report
                else repository_root
                / "readiness"
                / f"{report.evidence_manifest_sha256[:16]}.json"
            )
            protected_paths = {manifest_path}
            protected_paths.update(
                (repository_root / Path(program.path)).resolve(strict=False)
                for program in project.programs
            )
            protected_paths.update(
                (repository_root / Path(conversion.output_path)).resolve(strict=False)
                for conversion in project.conversions
                if conversion.output_path
            )
            for paths in evidence_paths.values():
                if isinstance(paths, str):
                    paths = [paths]
                for evidence_path in paths:
                    candidate = Path(evidence_path)
                    if not candidate.is_absolute():
                        candidate = repository_root / candidate
                    protected_paths.add(candidate.resolve(strict=False))
            if report_path in protected_paths:
                raise ReadinessError(
                    "Readiness report path conflicts with the manifest, evidence, "
                    "source, or generated-code file."
                )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report.save(report_path)
            print(
                f"Production readiness: {report.status}; "
                f"approval: {report.approval_status}; "
                f"{len(report.blockers)} blocker(s)."
            )
            for blocker in report.blockers:
                print(f"  [{blocker['category']}] {blocker['message']}")
            print(f"Readiness report: {report_path}")
            if report.status != "ready":
                return 1
        elif args.command == "run":
            manifest_path = Path(args.path).resolve(strict=True)
            project = MigrationProject.load(manifest_path)
            spark_conf = _parse_cli_assignments(args.spark_conf, "--spark-conf")
            environment = _parse_cli_assignments(args.env, "--env")
            report = run_project(
                project,
                args.root,
                program_selectors=args.program,
                input_mappings=args.input,
                timeout_seconds=args.timeout,
                master=args.master,
                spark_conf=spark_conf,
                environment=environment,
                run_directory=args.run_dir,
            )
            summary = report.to_dict()["summary"]
            print(
                f"Run {report.run_id}: {summary['succeeded']} succeeded, "
                f"{summary['failed']} failed, {summary['timed_out']} timed out."
            )
            print(f"Execution report: {report.run_directory}/run.json")
            for result in report.results:
                if result.status != "succeeded":
                    print(
                        f"  {result.source_path}: {result.status}; "
                        f"stderr: {result.stderr_path}"
                    )
            if summary["failed"] or summary["timed_out"]:
                return 1
        elif args.command == "reconcile":
            manifest_path = Path(args.path).resolve(strict=True)
            repository_root = Path(args.root).resolve(strict=True)
            project = MigrationProject.load(manifest_path)
            spark_conf = _parse_cli_assignments(args.spark_conf, "--spark-conf")
            report = reconcile_project(
                project,
                repository_root,
                reference_mappings=args.reference,
                spark_output_mappings=args.spark_output,
                key_mappings=args.keys,
                relative_tolerance=args.rtol,
                absolute_tolerance=args.atol,
                max_examples=args.max_examples,
                master=args.master,
                spark_conf=spark_conf,
                execution_report=args.execution_report,
            )
            report_path = (
                Path(args.report).resolve(strict=False)
                if args.report
                else repository_root
                / "reconciliation"
                / f"{report.reconciliation_id}.json"
            )
            protected_paths = {manifest_path}
            protected_paths.update(
                (repository_root / Path(program.path)).resolve(strict=False)
                for program in project.programs
            )
            for conversion in project.conversions:
                if conversion.output_path:
                    protected_paths.add(
                        (repository_root / Path(conversion.output_path)).resolve(
                            strict=False
                        )
                    )
            if report_path in protected_paths:
                raise ReconciliationError(
                    "Reconciliation report path conflicts with a manifest, source, "
                    "or generated-code file."
                )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report.save(report_path)
            summary = report.to_dict()["summary"]
            print(
                f"Reconciliation: {summary['pass']} passed, {summary['fail']} failed, "
                f"{summary['inconclusive']} inconclusive."
            )
            print(f"Reconciliation report: {report_path}")
            if summary["fail"] or summary["inconclusive"]:
                return 1
        else:
            manifest_path = Path(args.path).resolve(strict=True)
            repository_root = Path(args.root).resolve(strict=True)
            report_path = (
                Path(args.report).resolve(strict=False)
                if args.report
                else manifest_path.with_suffix(".conversion.json")
            )
            if report_path == manifest_path:
                raise ProjectError(
                    "Conversion report path cannot replace the project manifest."
                )
            project = MigrationProject.load(manifest_path)
            output_dir = Path(args.output_dir)
            if output_dir.is_absolute():
                raise BatchConversionError(
                    "--output-dir must be relative to --root."
                )
            for program in project.programs:
                source_path = (repository_root / Path(program.path)).resolve(strict=False)
                generated_path = (
                    repository_root / output_dir / Path(program.path)
                ).with_suffix(".py").resolve(strict=False)
                if report_path in {source_path, generated_path}:
                    raise ProjectError(
                        "Conversion report path conflicts with a source or generated file."
                    )
            report = convert_project(
                project,
                repository_root,
                output_directory=args.output_dir,
                force=args.force,
                sas_version=args.sas_version,
            )
            project.save(manifest_path)
            report.save(report_path)
            summary = report.to_dict()["summary"]
            print(
                f"Converted {summary['succeeded']} of {summary['program_count']} "
                f"program(s); {summary['failed']} failed."
            )
            print(f"Conversion report: {report_path}")
            if summary["failed"]:
                return 1
    except (
        OSError,
        ProjectError,
        DiscoveryError,
        DependencyAnalysisError,
        BatchConversionError,
        ExecutionWorkflowError,
        ReconciliationError,
        PipelineResolverError,
        SourceSinkCatalogError,
        ProjectReportError,
        SyntheticDataError,
        Phase1AnalysisError,
        SchemaAssetError,
        ReadinessError,
    ) as error:
        print(f"sas2pyspark: error: {error}", file=sys.stderr)
        return 2
    return 0


def _parse_cli_assignments(values: list[str], option: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key or not item:
            raise ExecutionWorkflowError(f"{option} must use KEY=VALUE syntax.")
        if key in result:
            raise ExecutionWorkflowError(f"{option} contains duplicate key {key!r}.")
        result[key] = item
    return result


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "project":
        return _project_main(arguments[1:])

    parser = argparse.ArgumentParser(
        prog="sas2pyspark",
        description="Convert a supported SAS subset into generated PySpark code.",
    )
    parser.add_argument("input", help="SAS source file, or '-' to read standard input")
    parser.add_argument("-o", "--output", help="Output Python file; defaults to standard output")
    parser.add_argument(
        "--sas-version",
        choices=("auto", "9.4M4", "9.4M8", "Viya"),
        default="auto",
        help=(
            "Source SAS release profile; auto reads the source marker or defaults "
            "to Viya (default: auto)"
        ),
    )
    args = parser.parse_args(arguments)

    try:
        source = sys.stdin.read() if args.input == "-" else Path(args.input).read_text(encoding="utf-8")
        result = SasToPySparkConverter().convert(
            source,
            sas_version=args.sas_version,
            include_dir=None if args.input == "-" else Path(args.input).resolve().parent,
        )
    except (OSError, UnicodeError, ConversionError) as error:
        print(f"sas2pyspark: error: {error}", file=sys.stderr)
        return 2

    if args.output:
        try:
            Path(args.output).write_text(result.code, encoding="utf-8")
        except OSError as error:
            print(f"sas2pyspark: error: {error}", file=sys.stderr)
            return 2
    else:
        sys.stdout.write(result.code)
    print(
        f"sas2pyspark: SAS source profile: "
        f"{result.source_version} ({result.version_source})",
        file=sys.stderr,
    )
    for warning in result.warnings:
        print(f"sas2pyspark: warning: {warning}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
