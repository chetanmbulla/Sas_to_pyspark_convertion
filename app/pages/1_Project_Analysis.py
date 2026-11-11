"""Upload a project and inspect Phase 1 analysis."""

import hashlib
import json
from pathlib import Path

import streamlit as st

from sas2pyspark.discovery import discover_repository
from sas2pyspark.file_classification import build_file_classification
from sas2pyspark.gui_project import analyze_zip, pipeline_flow_graph

st.set_page_config(page_title="Phase 1 pipeline analysis", layout="wide", initial_sidebar_state="expanded")
st.title("Phase 1: Pipeline analysis and assets")
st.caption("Upload a SAS project ZIP and inspect pipeline, schemas, documentation and migration complexity.")

archive = st.file_uploader("SAS project ZIP", type=["zip"], key="project_zip")
config = st.file_uploader(
    "Pipeline configuration JSON (optional)",
    type=["json"],
    key="project_config",
    help="Paths must be relative to the ZIP root. Include server drives and job entrypoints.",
)
schema_asset = st.file_uploader(
    "Table schema metadata (optional)",
    type=["csv", "json", "sql", "ddl"],
    key="project_schema_asset",
    help="Upload PROC CONTENTS CSV, schema JSON, or CREATE TABLE DDL; do not upload production data.",
)
if config is None:
    st.info(
        "Without a pipeline configuration, every SAS file is treated as an entrypoint "
        "on one placeholder server. Reachability and drive mappings are not verified; "
        "upload a configuration for real job and server analysis."
    )
version = st.selectbox("SAS version", ("auto", "9.4M4", "9.4M8", "Viya"), key="project_version")
rows = st.number_input("Synthetic rows per dataset", min_value=1, max_value=1000000, value=100, key="project_rows")
seed = st.number_input("Synthetic data seed", value=0, step=1, key="project_seed")

if st.button("Run Phase 1 analysis", type="primary", disabled=archive is None) and archive is not None:
    st.session_state.pop("project_analysis", None)
    st.session_state.pop("project_synthetic", None)
    st.session_state.pop("project_execution", None)
    st.session_state.pop("project_execution_inputs", None)
    st.session_state.pop("project_trusted", None)
    st.session_state.pop("phase2_dataset", None)
    st.session_state.pop("project_phase1_stale", None)
    st.session_state.pop("phase3_program", None)
    st.session_state.pop("phase3_editor", None)
    st.session_state.pop("phase3_editor_program", None)
    st.session_state.pop("phase3_inputs", None)
    st.session_state.pop("phase3_inputs_program", None)
    st.session_state.pop("phase1_dataset", None)
    try:
        with st.spinner("Analyzing the SAS project and generating synthetic data..."):
            result = analyze_zip(
                archive.getvalue(),
                config=config.getvalue() if config else None,
                schema_asset=(schema_asset.name, schema_asset.getvalue()) if schema_asset else None,
                sas_version=version,
                rows=int(rows),
                seed=int(seed),
            )
        manifest_path = Path(result["output_root"]) / "synthetic-data" / "synthetic-data-manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        st.session_state.project_analysis = result
        st.session_state.project_archive_hash = hashlib.sha256(archive.getvalue()).hexdigest()
        st.session_state.project_config_hash = (
            hashlib.sha256(config.getvalue()).hexdigest() if config else None
        )
        st.session_state.project_schema_hash = (
            (schema_asset.name.rsplit(".", 1)[-1].lower(), hashlib.sha256(schema_asset.getvalue()).hexdigest())
            if schema_asset else None
        )
        st.session_state.project_analysis_version = version
        st.session_state.project_synthetic = manifest
    except (OSError, ValueError) as error:
        st.error(f"Phase 1 analysis failed: {error}")

project = st.session_state.get("project_analysis")
if project:
    output = Path(project["output_root"])
    index = project["index"]
    current_archive_hash = hashlib.sha256(archive.getvalue()).hexdigest() if archive else None
    current_config_hash = hashlib.sha256(config.getvalue()).hexdigest() if config else None
    current_schema_hash = (
        (schema_asset.name.rsplit(".", 1)[-1].lower(), hashlib.sha256(schema_asset.getvalue()).hexdigest())
        if schema_asset else None
    )
    stale = (
        current_archive_hash != st.session_state.get("project_archive_hash")
        or current_config_hash != st.session_state.get("project_config_hash")
        or current_schema_hash != st.session_state.get("project_schema_hash")
        or version != st.session_state.get("project_analysis_version")
    )
    st.session_state.project_phase1_stale = stale
    if stale:
        st.warning("The ZIP, configuration, schema metadata, or SAS version has changed; results below are from the previous analysis.")
    st.info(
        "Phase 1 analysis artifacts were generated. Review the evidence gates below; "
        "static analysis is not an accuracy certification."
    )
    st.caption(f"Local output folder: {output}")
    if project["configuration"] == "all_scripts_fallback":
        st.warning("All-scripts fallback is active: job reachability is not authoritative.")

    st.subheader("Phase 1 results")
    pipeline = index["summaries"]["pipeline"]
    catalog = index["summaries"]["catalog"]
    cols = st.columns(4)
    cols[0].metric("SAS programs", index["summaries"]["inventory"]["program_count"])
    cols[1].metric("Reachable scripts", pipeline["reachable_script_count"])
    cols[2].metric("Catalog datasets", catalog["dataset_count"])
    cols[3].metric("Unknown schemas", catalog["unknown_schema_count"])
    st.caption(
        f"Unresolved includes: {pipeline['unresolved_include_count']} · "
        f"Waived includes: {pipeline['waived_include_count']} · "
        f"Unmapped libraries: {pipeline['unresolved_library_count']}"
    )
    st.download_button(
        "Download Phase 1 index",
        data=(output / "phase1-index.json").read_bytes(),
        file_name="phase1-index.json",
        mime="application/json",
    )
    st.download_button(
        "Download project report",
        data=(output / "project-report.md").read_bytes(),
        file_name="project-report.md",
        mime="text/markdown",
    )
    try:
        pipeline_report = json.loads((output / "pipeline-report.json").read_text(encoding="utf-8"))
        catalog_report = json.loads((output / "catalog.json").read_text(encoding="utf-8"))
        classification_path = output / "file-classification.json"
        if classification_path.is_file():
            classification = json.loads(
                classification_path.read_text(encoding="utf-8")
            )
        else:
            st.warning(
                "This Phase 1 output predates file classification or is missing its "
                "classification artifact. Rebuilding it from the saved source inventory."
            )
            repository_root = Path(index["repository_root"])
            inventory = discover_repository(repository_root)
            classification = build_file_classification(
                repository_root,
                inventory.programs,
                pipeline_report,
                catalog_report,
                configuration_source=project["configuration"],
            )
            classification_path.write_text(
                json.dumps(classification, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            index.setdefault("artifacts", {})["classification"] = (
                "file-classification.json"
            )
            index.setdefault("summaries", {})["classification"] = classification[
                "summary"
            ]
            (output / "phase1-index.json").write_text(
                json.dumps(index, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        complexity = json.loads((output / "complexity.json").read_text(encoding="utf-8"))
        quality = json.loads((output / "phase1-quality.json").read_text(encoding="utf-8"))
        report_text = (output / "project-report.md").read_text(encoding="utf-8")
        business_text = (output / "business-context.md").read_text(encoding="utf-8")
    except (OSError, ValueError) as error:
        st.error(f"Cannot load Phase 1 results: {error}")
        st.stop()
    st.subheader("Phase 1 evidence quality gates")
    st.warning(quality["assessment_note"])
    st.caption(
        "PoC mode: source-derived assumptions are for exploration only. Missing evidence is "
        "shown as NA; conversion errors remain failures with their reported reasons."
    )
    quality_cols = st.columns(3)
    quality_cols[0].metric("Blocker-severity gates", quality["summary"]["blocker_gate_count"])
    quality_cols[1].metric("Human review gates", quality["summary"]["review_required_count"])
    quality_cols[2].metric("Datasets without complete schemas", quality["summary"]["incomplete_schema_dataset_count"])
    st.dataframe(
        [
            {
                "Gate": item["title"],
                "Status": item["status"],
                "Severity": item["severity"],
                "PoC assumption": item["poc_assumption"],
                "Unavailable (NA)": ", ".join(
                    field["field"] for field in item["unavailable_fields"]
                ) or "None",
                "Evidence": item["evidence"],
                "Required action": item["required_action"],
            }
            for item in quality["gates"]
        ],
        use_container_width=True,
        hide_index=True,
    )
    st.download_button(
        "Download Phase 1 evidence gaps",
        data=json.dumps(quality, indent=2).encode("utf-8"),
        file_name="phase1-evidence-gaps.json",
        mime="application/json",
    )
    st.subheader("Migration complexity")
    score = complexity["project"]
    cols = st.columns(3)
    cols[0].metric("Score", f"{score['score']}/100")
    cols[1].metric("Complexity", score["band"])
    cols[2].metric("Confidence", f"{score['confidence']:.0%}")
    st.caption("A prioritization heuristic, not a delivery estimate or proof of equivalence.")
    st.dataframe(
        [{"Factor": name, "Raw": value["raw"], "Points": value["points"]}
         for name, value in score["factors"].items()],
        use_container_width=True, hide_index=True,
    )
    tabs = st.tabs([
        "File classification",
        "Jobs and includes",
        "Libraries and systems",
        "Datasets and schemas",
        "Documentation",
    ])
    with tabs[0]:
        st.caption(
            "Deterministic static classifications; runtime history and human validation "
            "were not supplied."
        )
        class_summary = classification["summary"]
        class_cols = st.columns(3)
        class_cols[0].metric("Files classified", class_summary["file_count"])
        class_cols[1].metric("Review required", class_summary["review_required_count"])
        class_cols[2].metric(
            "Unresolved includes",
            class_summary["unresolved_include_count"],
        )
        summary_dimensions = {
            "Technical category": "technical_category",
            "Business domain": "business_domain",
            "Program type": "program_type",
            "Execution mode": "execution_mode",
            "Execution status": "execution_status",
            "Server": "server",
            "Review status": "review_status",
        }
        selected_dimension = st.selectbox(
            "Classification summary",
            list(summary_dimensions),
            key="phase1_classification_dimension",
        )
        dimension_data = class_summary[summary_dimensions[selected_dimension]]
        st.dataframe(
            [
                {
                    "Classification": name,
                    "Files": values["count"],
                    "Share": f'{values["percentage"]:.1f}%',
                }
                for name, values in dimension_data.items()
            ],
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Download file classification inventory",
            data=json.dumps(classification, indent=2).encode("utf-8"),
            file_name="file-classification.json",
            mime="application/json",
        )
        st.dataframe(
            [
                {
                    "File": item["file_name"],
                    "Full path": item["full_path"],
                    "Server": item["server"] or "Unknown",
                    "Technical category": item["primary_technical_category"],
                    "Technical confidence": item["technical_confidence"],
                    "Business domain": item["primary_business_domain"],
                    "Business confidence": item["business_domain_confidence"],
                    "Program type": item["primary_program_type"],
                    "Program confidence": item["program_type_confidence"],
                    "Execution mode": item["execution_mode"],
                    "Execution confidence": item["execution_confidence"],
                    "Usage": item["usage_classification"],
                    "Review": item["review_status"],
                }
                for item in classification["files"]
            ],
            use_container_width=True,
            hide_index=True,
        )
        if classification["files"]:
            selected_file = st.selectbox(
                "Inspect classification evidence",
                [item["full_path"] for item in classification["files"]],
                key="phase1_classification_file",
            )
            record = next(
                item for item in classification["files"]
                if item["full_path"] == selected_file
            )
            st.subheader(record["file_name"])
            st.code(record["full_path"], language=None)
            st.caption(
                f"SHA-256: {record['file_hash']} · "
                f"Entry point: {record['entrypoint_status']} · "
                f"Review status: {record['review_status']}"
            )
            classification_cols = st.columns(4)
            for column, title, value, confidence in (
                (
                    classification_cols[0],
                    "Technical",
                    record["primary_technical_category"],
                    record["technical_confidence"],
                ),
                (
                    classification_cols[1],
                    "Business domain",
                    record["primary_business_domain"],
                    record["business_domain_confidence"],
                ),
                (
                    classification_cols[2],
                    "Program type",
                    record["primary_program_type"],
                    record["program_type_confidence"],
                ),
                (
                    classification_cols[3],
                    "Execution",
                    record["execution_mode"],
                    record["execution_confidence"],
                ),
            ):
                column.metric(title, value, f"Confidence: {confidence}")
            st.markdown(
                "**Secondary technical categories:** "
                + (", ".join(record["secondary_technical_categories"]) or "None identified")
            )
            st.markdown(
                "**Secondary business domains:** "
                + (", ".join(record["secondary_business_domains"]) or "None identified")
            )
            st.markdown(
                "**Additional program roles:** "
                + (", ".join(record["secondary_program_roles"]) or "None identified")
            )
            st.markdown("**Business purpose**")
            st.write(record["business_purpose_summary"])
            st.markdown(
                "**Execution:** "
                f"{record['execution_mode']} · frequency: "
                f"{', '.join(record['execution_frequency']) or 'Unknown'} · "
                f"server: {record['execution_server'] or 'Unknown'} · "
                f"status: {record['execution_status']}"
            )
            st.markdown("**Data and dependency details**")
            st.json(
                {
                    "input_datasets": record["input_datasets"],
                    "output_datasets": record["output_datasets"],
                    "include_dependencies": record["include_dependencies"],
                    "macro_dependencies": record["macro_dependencies"],
                    "macro_definitions": record["macro_definitions"],
                    "macro_invocations": record["macro_invocations"],
                    "external_systems": record["external_systems"],
                    "JAMS relationships": record["jams_relationship"],
                }
            )
            st.markdown("**Classification evidence**")
            st.dataframe(
                record["classification_evidence"],
                use_container_width=True,
                hide_index=True,
            )
            unresolved_questions = [
                *record["unresolved_technical_questions"],
                *record["unresolved_business_questions"],
                *record["program_type_unresolved_questions"],
                *record["execution_unresolved_questions"],
            ]
            if unresolved_questions:
                st.warning("Manual review questions")
                st.markdown("\n".join(f"- {question}" for question in unresolved_questions))
            st.json(
                {
                    "record": record,
                    "limitations": classification["limitations"],
                },
                expanded=False,
            )
    with tabs[1]:
        st.markdown("**Job and include flow**")
        graph, truncated = pipeline_flow_graph(pipeline_report)
        st.graphviz_chart(graph, use_container_width=True)
        if truncated:
            st.warning("The visual flow is limited to 80 nodes; use the full knowledge graph download for all nodes.")
        st.caption("Resolved static includes only; runtime-generated, waived and unresolved includes are not shown as edges.")
        for label, key in (("Jobs / entrypoints", "jobs"), ("Scripts", "scripts"), ("Recursive includes", "includes")):
            st.markdown(f"**{label}**")
            st.dataframe(pipeline_report.get(key, []), use_container_width=True, hide_index=True)
    with tabs[2]:
        st.markdown("**SAS libraries and physical paths**")
        st.dataframe(pipeline_report.get("libraries", []), use_container_width=True, hide_index=True)
        st.markdown("**External connections (input and output)**")
        st.dataframe(pipeline_report.get("external_connections", []), use_container_width=True, hide_index=True)
        st.markdown("**Identified systems**")
        st.dataframe(catalog_report.get("systems", []), use_container_width=True, hide_index=True)
    with tabs[3]:
        resources = [item for item in catalog_report.get("resources", []) if item.get("kind") == "sas_dataset"]
        st.dataframe([
            {"Dataset": item["name"], "Direction": item.get("direction"),
             "Schema status": (item.get("schema") or {}).get("status"),
             "Attributes": len((item.get("schema") or {}).get("columns") or [])}
            for item in resources
        ], use_container_width=True, hide_index=True)
        names = [item["name"] for item in resources]
        if names:
            selected_dataset = st.selectbox("Inspect schema", names, key="phase1_dataset")
            resource = next((item for item in resources if item["name"] == selected_dataset), None)
            if resource:
                attributes = (resource.get("schema") or {}).get("columns") or []
                if attributes:
                    st.dataframe(attributes, use_container_width=True, hide_index=True)
                else:
                    st.info("No column definitions are available for this dataset.")
    with tabs[4]:
        st.caption("The report contains Mermaid process-flow source. Business context is an offline template, not AI-verified.")
        with st.expander("Migration report and process flow"):
            st.markdown(report_text)
        with st.expander("Business context"):
            st.markdown(business_text)
        for label, name, mime in (
            ("Download business context", "business-context.md", "text/markdown"),
            ("Download knowledge graph", "knowledge-graph.json", "application/json"),
            ("Download catalog", "catalog.json", "application/json"),
            ("Download AI prompts", "ai-prompts.json", "application/json"),
        ):
            st.download_button(label, data=(output / name).read_bytes(), file_name=name, mime=mime)

    conversion = json.loads((output / "conversion.json").read_text(encoding="utf-8"))
    st.subheader("Conversion to PySpark")
    cols = st.columns(3)
    cols[0].metric("Programs", conversion["summary"]["program_count"])
    cols[1].metric("Converted", conversion["summary"]["succeeded"])
    cols[2].metric("Failed", conversion["summary"]["failed"])
    failures = [
        {"SAS program": item["source_path"], "Failure reason": item.get("error") or "Not specified"}
        for item in conversion["outcomes"]
        if item["status"] == "failed"
    ]
    if failures:
        st.dataframe(failures, use_container_width=True, hide_index=True)
    else:
        st.success("No conversion failures were reported.")
    st.download_button(
        "Download conversion report",
        data=(output / "conversion.json").read_bytes(),
        file_name="conversion.json",
        mime="application/json",
    )

    st.info("Continue to Phase 2 Synthetic Data and then Phase 3 Conversion Testing in the sidebar.")
