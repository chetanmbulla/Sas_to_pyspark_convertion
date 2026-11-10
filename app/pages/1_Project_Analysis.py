"""Analyze an uploaded SAS project and run converted programs locally."""

import hashlib
import importlib.util
import json
from pathlib import Path

import streamlit as st

from sas2pyspark.gui_project import (
    analyze_zip,
    regenerate_synthetic_data,
    run_converted_programs,
)

st.set_page_config(page_title="SAS project analysis", layout="wide")
st.title("SAS project analysis")
st.caption("Upload a SAS project ZIP, inspect Phase 1, create fixtures and run converted programs.")
st.warning(
    "Execution runs generated Python under the server account and is not sandboxed. "
    "Use this local app only with trusted source and data."
)

archive = st.file_uploader("SAS project ZIP", type=["zip"], key="project_zip")
config = st.file_uploader(
    "Pipeline configuration JSON (optional)",
    type=["json"],
    key="project_config",
    help="Paths must be relative to the ZIP root. Include server drives and job entrypoints.",
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
    try:
        with st.spinner("Analyzing the SAS project and generating synthetic data..."):
            result = analyze_zip(
                archive.getvalue(),
                config=config.getvalue() if config else None,
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
    stale = (
        current_archive_hash != st.session_state.get("project_archive_hash")
        or current_config_hash != st.session_state.get("project_config_hash")
        or version != st.session_state.get("project_analysis_version")
    )
    if stale:
        st.warning("The ZIP, configuration, or SAS version has changed; results below are from the previous analysis.")
    st.success("Phase 1 analysis completed.")
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

    st.subheader("Synthetic data")
    st.caption(
        "Phase 1 creates fixtures from known catalog columns. Unknown or unsupported "
        "schemas are skipped; partial fixtures may not contain all required columns."
    )
    if st.button("Create synthetic data with current settings", disabled=stale):
        try:
            manifest = regenerate_synthetic_data(output, rows=int(rows), seed=int(seed))
            st.session_state.project_synthetic = manifest
            st.session_state.pop("project_execution", None)
            st.session_state.pop("project_execution_inputs", None)
        except (OSError, ValueError) as error:
            st.error(f"Synthetic data generation failed: {error}")
    manifest = st.session_state.get("project_synthetic")
    if manifest:
        cols = st.columns(3)
        cols[0].metric("Generated", manifest["summary"]["generated_count"])
        cols[1].metric("Partial schemas", manifest["summary"]["partial_schema_count"])
        cols[2].metric("Skipped", manifest["summary"]["skipped_count"])
        st.caption(f"Fixture folder: {manifest['output_directory']}")
        with st.expander("Synthetic dataset details"):
            st.dataframe(
                [
                    {"Dataset": item["dataset"], "Status": item["status"], "Reason": item.get("reason", "")}
                    for item in manifest["datasets"]
                ],
                use_container_width=True,
                hide_index=True,
            )
        st.download_button(
            "Download synthetic data manifest",
            data=json.dumps(manifest, indent=2).encode("utf-8"),
            file_name="synthetic-data-manifest.json",
            mime="application/json",
        )

        st.subheader("Execute converted PySpark programs")
        st.caption(
            "Each successfully converted program runs independently using the selected "
            "fixtures. Program dependencies are not chained. Missing or partial inputs "
            "can cause execution failures; success does not prove equivalence to SAS."
        )
        available = [
            item["dataset"] for item in manifest["datasets"]
            if item["status"] in {"generated", "generated_partial_schema"}
        ]
        selected = st.multiselect("Synthetic input datasets", available, key="project_execution_inputs")
        timeout = st.number_input("Timeout per program (seconds)", min_value=1, max_value=3600, value=120, key="project_timeout")
        trusted = st.checkbox("I trust this project and understand execution is not sandboxed", key="project_trusted")
        has_pyspark = importlib.util.find_spec("pyspark") is not None
        if not has_pyspark:
            st.info("Install the GUI extra to enable PySpark execution.")
        if st.button(
            "Execute all converted programs",
            disabled=stale or not trusted or not has_pyspark or conversion["summary"]["succeeded"] == 0,
        ):
            st.session_state.pop("project_execution", None)
            try:
                with st.spinner("Running converted programs in local Spark workers..."):
                    st.session_state.project_execution = run_converted_programs(
                        project, manifest, selected, timeout_seconds=int(timeout)
                    )
            except (OSError, ValueError, KeyError) as error:
                st.error(f"Could not run converted programs: {error}")

        execution = st.session_state.get("project_execution")
        if execution:
            summary = execution["summary"]
            cols = st.columns(4)
            cols[0].metric("Attempted", summary["program_count"])
            cols[1].metric("Succeeded", summary["succeeded"])
            cols[2].metric("Failed", summary["failed"])
            cols[3].metric("Timed out", summary["timed_out"])
            st.caption(
                f"Only converted programs were attempted; {conversion['summary']['failed']} "
                "conversion failures were not executed."
            )
            st.dataframe(
                [
                    {
                        "SAS program": item["source_path"],
                        "Status": item["status"],
                        "Reason": item.get("error") or "",
                        "Stderr log": str(Path(execution["execution_root"]) / item["stderr_path"]),
                    }
                    for item in execution["results"]
                ],
                use_container_width=True,
                hide_index=True,
            )
            for item in execution["results"]:
                if item["status"] != "succeeded":
                    with st.expander(f"Execution log: {item['source_path']}"):
                        log_path = Path(execution["execution_root"]) / item["stderr_path"]
                        try:
                            with log_path.open("rb") as log:
                                log.seek(0, 2)
                                log.seek(max(0, log.tell() - 32768))
                                text = log.read().decode("utf-8", errors="replace")
                            st.code(text or item.get("error") or "No stderr output.")
                        except OSError as error:
                            st.error(f"Cannot read execution log: {error}")
            st.download_button(
                "Download execution report",
                data=json.dumps(execution, indent=2).encode("utf-8"),
                file_name="execution-report.json",
                mime="application/json",
            )
