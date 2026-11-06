import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from sas2pyspark import (
    Conversion,
    BatchConversionError,
    DiscoveryError,
    MigrationProject,
    ProjectError,
    Review,
    ReviewReport,
    ReviewWorkflowError,
    build_migration_plan,
    build_review_report,
    convert_project,
    discover_repository,
    record_review_decision,
)
from sas2pyspark.cli import main


class MigrationProjectTests(unittest.TestCase):
    def test_project_round_trips_entities_with_stable_ids(self):
        project = MigrationProject.create("Claims migration")
        program = project.add_program("sas/claims.sas")
        dataset = project.add_dataset("warehouse.claims")
        dependency = project.add_dependency(program.id, dataset.id, "produces")
        project.add_conversion(
            Conversion.create(
                program.id,
                "a" * 64,
                "0.1.0",
                status="succeeded",
                output_path="pyspark/claims.py",
                warnings=("Review row ordering assumptions.",),
            )
        )
        project.set_review(
            Review.create(
                program.id,
                status="approved",
                reviewer="data-platform",
                updated_at="2026-11-02T10:00:00+00:00",
                notes="Validated with the business owner.",
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "migration.project.json")
            project.save(path)
            loaded = MigrationProject.load(path)

        self.assertEqual(loaded.to_dict(), project.to_dict())
        self.assertEqual(loaded.programs[0].id, program.id)
        self.assertEqual(loaded.datasets[0].id, dataset.id)
        self.assertEqual(loaded.dependencies[0].id, dependency.id)
        self.assertEqual(loaded.reviews[0].status, "approved")

    def test_program_and_dataset_ids_are_deterministic(self):
        first = MigrationProject.create("First")
        second = MigrationProject.create("Second")
        self.assertEqual(
            first.add_program("SAS\\Claims.sas").id,
            second.add_program("sas/claims.sas").id,
        )
        self.assertEqual(
            first.add_dataset("WORK.Claims").id,
            second.add_dataset("work.claims").id,
        )

    def test_project_rejects_invalid_paths_and_unresolved_references(self):
        project = MigrationProject.create("Claims migration")
        with self.assertRaisesRegex(ProjectError, "project-relative"):
            project.add_program("../outside.sas")

        program = project.add_program("claims.sas")
        conversion = Conversion.create(program.id, "b" * 64, "0.1.0")
        project.add_conversion(conversion)
        project.validate()

        document = project.to_dict()
        document["conversions"][0]["program_id"] = "missing-program"
        with self.assertRaisesRegex(ProjectError, "unknown program"):
            MigrationProject.from_dict(document)

    def test_project_rejects_unsupported_versions_and_unknown_fields(self):
        project = MigrationProject.create("Claims migration")
        document = project.to_dict()
        document["schema_version"] = 2
        with self.assertRaisesRegex(ProjectError, "Unsupported project schema version"):
            MigrationProject.from_dict(document)

        document = project.to_dict()
        document["unexpected"] = True
        with self.assertRaisesRegex(ProjectError, "unknown unexpected"):
            MigrationProject.from_dict(document)

    def test_project_cli_initializes_validates_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "migration.project.json")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(
                    main(["project", "init", str(path), "--name", "Claims migration"]),
                    0,
                )
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["schema_version"], 1)

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(main(["project", "validate", str(path)]), 0)
            self.assertIn("Valid migration project", output.getvalue())

            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                self.assertEqual(
                    main(["project", "init", str(path), "--name", "Overwrite"]),
                    2,
                )
            self.assertIn("Refusing to overwrite", errors.getvalue())

    def test_existing_conversion_cli_still_accepts_a_sas_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "input.sas")
            path.write_text("data output; set source; run;", encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(main([str(path)]), 0)
            self.assertIn("df_output = df_source", output.getvalue())

    def test_existing_conversion_cli_accepts_a_sas_version_override(self):
        with tempfile.TemporaryDirectory() as directory:
                path = Path(directory, "input.sas")
                path.write_text(
                    "/* sas2pyspark: sas-version=9.4M8 */ "
                    "data output; set source; run;",
                    encoding="utf-8",
                )
                output = io.StringIO()
                errors = io.StringIO()
                with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
                    self.assertEqual(main([str(path), "--sas-version", "9.4M8"]), 0)
                self.assertIn("df_output = df_source", output.getvalue())
                self.assertIn("SAS source profile: 9.4M8 (override)", errors.getvalue())

    def test_discovery_inventories_sas_assets_and_skips_configured_directories(self):
        source = b"""/* proc ignored; */
* proc also_ignored; ;
%let source=work.input;
data output; set &source; run;
proc sort data=output; by id; run;
proc sql; select ';' as marker from output; quit;
%* proc macro_comment; ;
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "sas").mkdir()
            (root / ".git").mkdir()
            (root / "generated").mkdir()
            sas_path = root / "sas" / "main.sas"
            sas_path.write_bytes(source)
            asset_path = root / "sas" / "input.csv"
            asset_path.write_bytes(b"id,value\n1,A\n")
            (root / "sas" / "input.log").write_text("not an asset", encoding="utf-8")
            (root / ".git" / "hidden.sas").write_text("data hidden;", encoding="utf-8")
            (root / "generated" / "ignored.sas").write_text(
                "proc ignored;", encoding="utf-8"
            )

            report = discover_repository(
                root,
                excluded_directories=(".git", "generated/**"),
            )

        self.assertEqual([item.path for item in report.programs], ["sas/main.sas"])
        self.assertEqual([item.path for item in report.assets], ["sas/input.csv"])
        program = report.programs[0]
        self.assertEqual(program.encoding, "utf-8")
        self.assertEqual(program.sha256, __import__("hashlib").sha256(source).hexdigest())
        self.assertEqual(program.data_step_count, 1)
        self.assertEqual(program.procedures, {"SORT": 1, "SQL": 1})
        self.assertEqual(program.macro_statement_count, 1)
        self.assertEqual(len(report.issues), 0)

    def test_discovery_detects_legacy_encoding_and_reports_decode_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "legacy.sas").write_bytes("data café; run;".encode("cp1252"))
            (root / "invalid.sas").write_bytes(b"\x81")

            report = discover_repository(root)

        indexed = {program.path: program for program in report.programs}
        self.assertEqual(indexed["legacy.sas"].encoding, "cp1252")
        self.assertIsNone(indexed["legacy.sas"].decode_error)
        self.assertIsNone(indexed["invalid.sas"].encoding)
        self.assertIsNotNone(indexed["invalid.sas"].decode_error)
        self.assertEqual(len(report.issues), 1)

    def test_discovery_rejects_bad_roots_and_patterns(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            file_path = root / "file.sas"
            file_path.write_text("data x;", encoding="utf-8")
            with self.assertRaisesRegex(DiscoveryError, "not a directory"):
                discover_repository(file_path)
            with self.assertRaisesRegex(DiscoveryError, "Invalid excluded"):
                discover_repository(root, excluded_directories=("../private",))
            with self.assertRaisesRegex(DiscoveryError, "Invalid asset extension"):
                discover_repository(root, asset_extensions=("csv; rm",))

    def test_discovery_cli_updates_project_and_writes_repeatable_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory, "repository")
            root.mkdir()
            (root / "program.sas").write_text(
                "data result; set source; run;", encoding="utf-8"
            )
            (root / "source.csv").write_text("id\n1\n", encoding="utf-8")
            manifest = Path(directory, "migration.project.json")
            report_path = Path(directory, "inventory.json")
            MigrationProject.create("Discovery test").save(manifest)

            arguments = [
                "project",
                "discover",
                str(manifest),
                "--root",
                str(root),
                "--report",
                str(report_path),
            ]
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(main(arguments), 0)
            first_report = json.loads(report_path.read_text(encoding="utf-8"))

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(main(arguments), 0)
            second_report = json.loads(report_path.read_text(encoding="utf-8"))
            project = MigrationProject.load(manifest)

        self.assertEqual(len(project.programs), 1)
        self.assertEqual(first_report["programs"], second_report["programs"])
        self.assertEqual(first_report["assets"], second_report["assets"])
        self.assertIn("registered 0 new program(s)", output.getvalue())

    def test_dependency_plan_orders_data_macro_and_include_prerequisites(self):
        sources = {
            "01_source.sas": "data work.raw; set input_csv; run;",
            "02_transform.sas": (
                "data work.clean; set work.raw; run; "
                "proc sort data=work.clean out=work.sorted; run;"
            ),
            "03_report.sas": (
                '%include "macros/build.sas"; '
                "proc sql; create table work.report as "
                "select * from work.sorted join work.lookup; quit; "
                "%build_report;"
            ),
            "macros/build.sas": "%macro build_report; %mend;",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = MigrationProject.create("Dependency plan")
            for path, source in sources.items():
                source_path = root / path
                source_path.parent.mkdir(parents=True, exist_ok=True)
                source_path.write_text(source, encoding="utf-8")
                project.add_program(path)

            plan = build_migration_plan(project, root)
            document = plan.to_dict()

        program_order = [program["path"] for program in document["programs"]]
        self.assertEqual(
            program_order,
            [
                "01_source.sas",
                "02_transform.sas",
                "macros/build.sas",
                "03_report.sas",
            ],
        )
        self.assertTrue(all(item["status"] == "planned" for item in document["programs"]))
        self.assertTrue(any(edge["kind"] == "dataset_dependency" for edge in document["dependencies"]))
        self.assertTrue(any(edge["kind"] == "includes" for edge in document["dependencies"]))
        self.assertTrue(any(edge["kind"] == "calls_macro" for edge in document["dependencies"]))
        flows = {item["name"]: item for item in document["datasets"]}
        self.assertEqual(flows["work.raw"]["classification"], "internal")
        self.assertEqual(flows["work.lookup"]["classification"], "external_input")
        self.assertTrue(
            any(
                item["reference"] == "work.lookup"
                and "external input" in item["reason"]
                for item in document["unresolved_references"]
            )
        )

    def test_dependency_plan_reports_cycles_and_blocks_dependents(self):
        sources = {
            "a.sas": "data work.a; set work.b; run;",
            "b.sas": "data work.b; set work.a; run;",
            "downstream.sas": "data work.final; set work.a; run;",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = MigrationProject.create("Cycle plan")
            for path, source in sources.items():
                (root / path).write_text(source, encoding="utf-8")
                project.add_program(path)

            plan = build_migration_plan(project, root).to_dict()

        self.assertEqual(len(plan["cycles"]), 1)
        statuses = {item["path"]: item["status"] for item in plan["programs"]}
        self.assertEqual(statuses["a.sas"], "blocked_cycle")
        self.assertEqual(statuses["b.sas"], "blocked_cycle")
        self.assertEqual(statuses["downstream.sas"], "blocked_dependency")
        self.assertIsNone(
            next(item for item in plan["programs"] if item["path"] == "downstream.sas")[
                "sequence"
            ]
        )

    def test_dependency_analysis_flags_ambiguous_producers_and_ignores_sql_strings(self):
        sources = {
            "first.sas": "data work.shared; set first_input; run;",
            "second.sas": "data work.shared; set second_input; run;",
            "consumer.sas": (
                "proc sql; create table work.result as "
                "select 'from work.fake' as note from work.shared; quit;"
            ),
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = MigrationProject.create("Ambiguous plan")
            for path, source in sources.items():
                (root / path).write_text(source, encoding="utf-8")
                project.add_program(path)

            plan = build_migration_plan(project, root).to_dict()

        flow = next(item for item in plan["datasets"] if item["name"] == "work.shared")
        self.assertEqual(flow["classification"], "ambiguous_producer")
        self.assertEqual(len(flow["produced_by"]), 2)
        self.assertNotIn("work.fake", {item["name"] for item in plan["datasets"]})
        self.assertTrue(
            any("multiple producers" in item["reason"] for item in plan["unresolved_references"])
        )

    def test_dependency_analysis_marks_macro_names_and_unknown_procs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "dynamic.sas").write_text(
                "data &target; set &source; run; proc mystery data=work.input; run;",
                encoding="utf-8",
            )
            project = MigrationProject.create("Dynamic plan")
            project.add_program("dynamic.sas")

            plan = build_migration_plan(project, root).to_dict()

        program = plan["programs"][0]
        self.assertEqual(program["unsupported_procedures"], ["MYSTERY"])
        self.assertIn("macro_variable_reference", program["macro_features"])
        self.assertTrue(
            any(item["kind"] == "dataset_input" for item in plan["unresolved_references"])
        )
        self.assertTrue(
            any(item["kind"] == "dataset_output" for item in plan["unresolved_references"])
        )

    def test_dependency_analysis_marks_dataset_ranges_unresolved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "range.sas").write_text(
                "data result; set source1-source3; run;", encoding="utf-8"
            )
            project = MigrationProject.create("Range plan")
            project.add_program("range.sas")

            plan = build_migration_plan(project, root).to_dict()

        self.assertNotIn("work.source1", {item["name"] for item in plan["datasets"]})
        self.assertTrue(
            any(
                item["reference"] == "source1-source3"
                and "range cannot be resolved" in item["reason"]
                for item in plan["unresolved_references"]
            )
        )

    def test_dependency_analysis_rejects_non_directory_root(self):
        project = MigrationProject.create("Invalid root")
        project.add_program("safe.sas")
        with tempfile.TemporaryDirectory() as directory:
            file_path = Path(directory, "not-a-directory")
            file_path.write_text("not a directory", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "not a directory"):
                build_migration_plan(project, file_path)

    def test_project_plan_cli_writes_report_and_returns_incomplete_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory, "repository")
            root.mkdir()
            (root / "program.sas").write_text(
                "data result; set external_source; run;", encoding="utf-8"
            )
            manifest = Path(directory, "migration.project.json")
            report_path = Path(directory, "plan.json")
            project = MigrationProject.create("Plan CLI")
            project.add_program("program.sas")
            project.save(manifest)

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                arguments = [
                    "project",
                    "plan",
                    str(manifest),
                    "--root",
                    str(root),
                    "--report",
                    str(report_path),
                ]
                status = main(arguments)
                self.assertEqual(main(arguments), 1)
            plan = json.loads(report_path.read_text(encoding="utf-8"))
            project = MigrationProject.load(manifest)

        self.assertEqual(status, 1)
        self.assertEqual(plan["summary"]["unresolved_reference_count"], 1)
        self.assertIn("Migration plan:", output.getvalue())
        self.assertEqual(len(project.datasets), 2)
        self.assertTrue(project.dependencies)
        self.assertTrue(
            all(item.kind.startswith("analysis:") for item in project.dependencies)
        )

    def test_batch_conversion_keeps_going_and_records_per_program_outcomes(self):
        source_texts = {
            "good.sas": "data good; set source; run;",
            "warn.sas": (
                "data running; order by row_id; retain total 0; set amounts; "
                "total = total + amount; run;"
            ),
            "unsupported.sas": "proc iml; run;",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = MigrationProject.create("Batch conversion")
            for path, source in source_texts.items():
                (root / path).write_text(source, encoding="utf-8")
                project.add_program(path)

            report = convert_project(project, root)
            report_dict = report.to_dict()
            outcomes = {item["source_path"]: item for item in report_dict["outcomes"]}

            self.assertTrue((root / "pyspark" / "good.py").is_file())
            self.assertTrue((root / "pyspark" / "warn.py").is_file())
            self.assertFalse((root / "pyspark" / "unsupported.py").exists())
            project.validate()

        self.assertEqual(report_dict["summary"], {
            "program_count": 3,
            "succeeded": 2,
            "failed": 1,
        })
        self.assertEqual(outcomes["good.sas"]["status"], "succeeded")
        self.assertEqual(outcomes["good.sas"]["output_path"], "pyspark/good.py")
        self.assertTrue(outcomes["good.sas"]["source_sha256"])
        self.assertTrue(outcomes["warn.sas"]["warnings"])
        self.assertIn("Unsupported top-level statement", outcomes["unsupported.sas"]["error"])
        self.assertTrue(all(item["manifest_recorded"] for item in outcomes.values()))
        statuses = {item.program_id: item.status for item in project.conversions}
        self.assertEqual(len(statuses), 3)

    def test_batch_conversion_records_missing_source_and_invalid_encoding(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "invalid.sas").write_bytes(b"\x81")
            project = MigrationProject.create("Bad sources")
            project.add_program("missing.sas")
            project.add_program("invalid.sas")

            report = convert_project(project, root)
            outcomes = {item.source_path: item for item in report.outcomes}
            project.validate()

        self.assertEqual(outcomes["missing.sas"].status, "failed")
        self.assertIsNone(outcomes["missing.sas"].source_sha256)
        self.assertFalse(outcomes["missing.sas"].manifest_recorded)
        self.assertEqual(outcomes["invalid.sas"].status, "failed")
        self.assertTrue(outcomes["invalid.sas"].manifest_recorded)
        self.assertEqual(len(project.conversions), 1)

    def test_batch_conversion_requires_force_to_replace_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "program.sas"
            source.write_text("data output; set source; run;", encoding="utf-8")
            project = MigrationProject.create("Overwrite guard")
            project.add_program("program.sas")

            first = convert_project(project, root)
            second = convert_project(project, root)
            self.assertEqual(first.outcomes[0].status, "succeeded")
            self.assertEqual(second.outcomes[0].status, "failed")
            self.assertIn("Use --force", second.outcomes[0].error)
            self.assertEqual(len(project.conversions), 1)

            forced = convert_project(project, root, force=True)
            self.assertEqual(forced.outcomes[0].status, "succeeded")
            self.assertEqual(len(project.conversions), 1)

    def test_batch_conversion_rejects_output_directory_escape(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = MigrationProject.create("Unsafe output")
            with self.assertRaisesRegex(BatchConversionError, "normalized"):
                convert_project(project, root, output_directory="../outside")

    def test_project_convert_cli_writes_report_and_returns_failure_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory, "repository")
            root.mkdir()
            (root / "good.sas").write_text(
                "/* sas2pyspark: sas-version=9.4M8 */ "
                "data result; set source; run;",
                encoding="utf-8",
            )
            (root / "bad.sas").write_text(
                "proc iml; run;", encoding="utf-8"
            )
            manifest = Path(directory, "migration.project.json")
            report_path = Path(directory, "conversion.json")
            project = MigrationProject.create("Convert CLI")
            project.add_program("good.sas")
            project.add_program("bad.sas")
            project.save(manifest)

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                status = main(
                    [
                        "project",
                        "convert",
                        str(manifest),
                        "--root",
                        str(root),
                        "--output-dir",
                        "generated",
                        "--report",
                        str(report_path),
                        "--sas-version",
                        "9.4M8",
                    ]
                )
            report = json.loads(report_path.read_text(encoding="utf-8"))
            saved_project = MigrationProject.load(manifest)

        self.assertEqual(status, 1)
        self.assertEqual(report["summary"]["succeeded"], 1)
        self.assertEqual(report["summary"]["failed"], 1)
        self.assertIn("Conversion report:", output.getvalue())
        self.assertEqual(len(saved_project.conversions), 2)

    def test_review_report_lists_warnings_and_unsupported_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = MigrationProject.create("Review")
            program = project.add_program("sas/program.sas")
            project.add_conversion(
                Conversion.create(
                    program.id,
                    "a" * 64,
                    "0.1.0",
                    status="succeeded",
                    output_path="pyspark/program.py",
                    warnings=("Review row ordering assumptions.",),
                )
            )
            project.add_conversion(
                Conversion.create(
                    program.id,
                    "b" * 64,
                    "0.1.0",
                    status="failed",
                    output_path="pyspark/program.py",
                    error="Unsupported top-level statement: 'PROC PRINT'.",
                )
            )

            report = build_review_report(project, root)
            report_path = root / "migration.review.json"
            report.save(report_path)
            loaded = ReviewReport.load(report_path)

        self.assertEqual(
            {issue.kind for issue in loaded.issues},
            {"warning", "unsupported_statement"},
        )
        self.assertEqual(
            {issue.decision for issue in loaded.issues},
            {"pending"},
        )
        self.assertEqual(loaded.to_dict()["summary"]["issue_count"], 2)
        self.assertTrue(all(issue.source_path == "sas/program.sas" for issue in loaded.issues))

    def test_review_decisions_append_history_and_preserve_generated_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            generated = root / "pyspark" / "program.py"
            generated.parent.mkdir()
            generated.write_text("# original generated output\n", encoding="utf-8")
            corrected_source = root / "manual-fix.py"
            corrected_source.write_text("result = 42\n", encoding="utf-8")
            project = MigrationProject.create("Review decisions")
            program = project.add_program("program.sas")
            project.add_conversion(
                Conversion.create(
                    program.id,
                    "c" * 64,
                    "0.1.0",
                    status="succeeded",
                    output_path="pyspark/program.py",
                    warnings=("Review output.",),
                )
            )
            report = build_review_report(project, root)
            issue_id = report.issues[0].id

            report = record_review_decision(
                report,
                issue_id,
                "deferred",
                reviewer="analyst-a",
                repository_root=root,
                notes="Waiting for business confirmation.",
            )
            report = record_review_decision(
                report,
                issue_id,
                "corrected",
                reviewer="analyst-b",
                repository_root=root,
                notes="Updated the reviewed implementation.",
                corrected_code=corrected_source,
            )
            report_path = root / "migration.review.json"
            report.save(report_path)
            loaded = ReviewReport.load(report_path)
            issue = loaded.issues[0]
            corrected_output_path = issue.history[-1].corrected_output_path
            assert corrected_output_path is not None
            correction = root / Path(corrected_output_path)
            correction_text = correction.read_text(encoding="utf-8")
            original_text = generated.read_text(encoding="utf-8")
            refreshed = build_review_report(
                project, root, existing_report=loaded
            )

        self.assertEqual(issue.decision, "corrected")
        self.assertEqual([event.decision for event in issue.history], ["deferred", "corrected"])
        self.assertEqual([event.reviewer for event in issue.history], ["analyst-a", "analyst-b"])
        self.assertEqual(refreshed.issues[0].history, issue.history)
        self.assertEqual(correction_text, "result = 42\n")
        self.assertEqual(original_text, "# original generated output\n")
        self.assertNotEqual(correction, generated)

    def test_review_rejects_bad_corrected_code_and_invalid_decision_arguments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            invalid_code = root / "invalid.py"
            invalid_code.write_text("if:\n", encoding="utf-8")
            project = MigrationProject.create("Review validation")
            program = project.add_program("program.sas")
            project.add_conversion(
                Conversion.create(
                    program.id,
                    "d" * 64,
                    "0.1.0",
                    status="succeeded",
                    output_path="pyspark/program.py",
                    warnings=("Review output.",),
                )
            )
            report = build_review_report(project, root)
            issue_id = report.issues[0].id

            with self.assertRaisesRegex(ReviewWorkflowError, "valid Python"):
                record_review_decision(
                    report,
                    issue_id,
                    "corrected",
                    reviewer="reviewer",
                    repository_root=root,
                    corrected_code=invalid_code,
                )
            with self.assertRaisesRegex(ReviewWorkflowError, "only for the corrected"):
                record_review_decision(
                    report,
                    issue_id,
                    "accepted",
                    reviewer="reviewer",
                    repository_root=root,
                    corrected_code=invalid_code,
                )

        self.assertEqual(report.issues[0].decision, "pending")
        self.assertFalse((root / "reviewed").exists())

    def test_project_review_cli_writes_report_and_records_human_decision(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory, "repository")
            root.mkdir()
            (root / "program.sas").write_text("data x; run;", encoding="utf-8")
            generated = root / "pyspark" / "program.py"
            generated.parent.mkdir()
            generated.write_text("# generated\n", encoding="utf-8")
            corrected_code = Path(directory, "corrected.py")
            corrected_code.write_text("result = 1\n", encoding="utf-8")
            manifest = Path(directory, "migration.project.json")
            report_path = Path(directory, "migration.review.json")
            project = MigrationProject.create("Review CLI")
            program = project.add_program("program.sas")
            project.add_conversion(
                Conversion.create(
                    program.id,
                    "e" * 64,
                    "0.1.0",
                    status="succeeded",
                    output_path="pyspark/program.py",
                    warnings=("Review output.",),
                )
            )
            project.save(manifest)

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(
                    main(
                        [
                            "project",
                            "review",
                            str(manifest),
                            "--root",
                            str(root),
                            "--report",
                            str(report_path),
                        ]
                    ),
                    0,
                )
            issue_id = json.loads(report_path.read_text(encoding="utf-8"))["issues"][0]["id"]
            with contextlib.redirect_stdout(output):
                self.assertEqual(
                    main(
                        [
                            "project",
                            "review-decision",
                            str(manifest),
                            "--root",
                            str(root),
                            "--report",
                            str(report_path),
                            "--issue",
                            issue_id,
                            "--decision",
                            "corrected",
                            "--reviewer",
                            "human-reviewer",
                            "--corrected-code",
                            str(corrected_code),
                        ]
                    ),
                    0,
                )
            saved_report = ReviewReport.load(report_path)
            generated_text = generated.read_text(encoding="utf-8")

        self.assertEqual(saved_report.issues[0].decision, "corrected")
        self.assertEqual(saved_report.issues[0].history[0].reviewer, "human-reviewer")
        self.assertEqual(generated_text, "# generated\n")
        self.assertIn("Review report:", output.getvalue())


if __name__ == "__main__":
    unittest.main()
