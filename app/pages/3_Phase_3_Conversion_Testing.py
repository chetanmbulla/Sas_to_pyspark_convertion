"""Review and test individual converted programs with synthetic inputs."""

import importlib.util
import json
from pathlib import Path

import streamlit as st

from sas2pyspark.gui_project import read_converted_code, run_converted_programs
from sas2pyspark.synthetic_data import load_catalog_report

st.set_page_config(page_title="Phase 3 conversion testing", layout="wide", initial_sidebar_state="expanded")
st.title("Phase 3: Conversion and testing")
st.caption("Select a converted program, review or edit its PySpark, and test it with synthetic inputs.")
st.warning(
    "Generated or edited Python runs with the server account's permissions and is not sandboxed. "
    "Only execute trusted code."
)
project = st.session_state.get("project_analysis")
manifest = st.session_state.get("project_synthetic")
if not project:
    st.info("First upload a ZIP and run Phase 1 on the Project Analysis page.")
    st.stop()
if not manifest:
    st.info("Generate synthetic data on the Phase 2 page before testing.")
    st.stop()

output = Path(project["output_root"])
stale = st.session_state.get("project_phase1_stale", False)
if stale:
    st.warning("Phase 1 inputs changed. Rerun Phase 1 before testing.")
try:
    conversion = json.loads((output / "conversion.json").read_text(encoding="utf-8"))
    catalog = load_catalog_report(output / "catalog.json")
except (OSError, ValueError) as error:
    st.error(f"Cannot load project results: {error}")
    st.stop()

summary = conversion["summary"]
cols = st.columns(3)
cols[0].metric("Programs", summary["program_count"])
cols[1].metric("Converted", summary["succeeded"])
cols[2].metric("Conversion failed", summary["failed"])
with st.expander("Conversion outcomes and issues"):
    st.dataframe(
        [{"SAS program": item["source_path"], "Status": item["status"],
          "Issue": item.get("error") or "", "Warnings": "\n".join(item.get("warnings") or [])}
         for item in conversion["outcomes"]],
        use_container_width=True, hide_index=True,
    )
st.download_button(
    "Download conversion report", data=json.dumps(conversion, indent=2).encode("utf-8"),
    file_name="conversion.json", mime="application/json",
)

successful = {item["source_path"]: item for item in conversion["outcomes"] if item["status"] == "succeeded"}
if not successful:
    st.info("No programs converted successfully. Review the conversion issues above.")
    st.stop()
program = st.selectbox("Select converted SAS program", list(successful), key="phase3_program")
if program is None:
    st.stop()
assert program is not None
try:
    original = read_converted_code(output, successful[program]["output_path"])
except (OSError, UnicodeError, ValueError) as error:
    st.error(f"Cannot read converted code: {error}")
    st.stop()
if st.session_state.get("phase3_editor_program") != program:
    st.session_state.phase3_editor = original
    st.session_state.phase3_editor_program = program
code = st.text_area("Review and edit PySpark", key="phase3_editor", height=350)
st.caption("Edits are staged for this test only; the original conversion remains unchanged. Download edits before switching programs to retain them.")
st.download_button("Download edited PySpark", code, file_name=Path(successful[program]["output_path"]).name, mime="text/x-python")

available = [
    item["dataset"] for item in manifest["datasets"]
    if item["status"] in {"generated", "generated_partial_schema"}
]
inputs = []
for resource in catalog.get("resources", []):
    for evidence in resource.get("evidence", []):
        if (str(evidence.get("source", "")).casefold() == program.casefold()
                and evidence.get("direction") in {"input", "input/output", "both"}):
            inputs.append(resource["name"])
            break
missing = sorted(set(inputs) - set(available), key=str.casefold)
st.caption("Catalog-detected inputs: " + (", ".join(sorted(set(inputs), key=str.casefold)) or "none identified"))
if missing:
    st.warning("No generated fixture for these detected inputs: " + ", ".join(missing))
st.caption("Choose fixtures explicitly; static analysis may miss dynamic inputs. Table aliases must be unique.")
if st.session_state.get("phase3_inputs_program") != program:
    st.session_state.phase3_inputs = [name for name in available if name in inputs]
    st.session_state.phase3_inputs_program = program
selected = st.multiselect("Synthetic input datasets", available, key="phase3_inputs")
assumed_inputs = [
    item for item in manifest["datasets"]
    if item["dataset"] in selected and item.get("assumed_character_columns")
]
if assumed_inputs:
    st.warning(
        "Some selected fixtures use strings for unknown SAS column types: "
        + "; ".join(
            f"{item['dataset']} ({', '.join(item['assumed_character_columns'])})"
            for item in assumed_inputs
        )
        + ". Test behavior may differ from SAS."
    )
timeout = st.number_input("Timeout per program (seconds)", min_value=1, max_value=3600, value=120, key="phase3_timeout")
trusted = st.checkbox("I trust this code and understand execution is not sandboxed", key="phase3_trusted")
has_pyspark = importlib.util.find_spec("pyspark") is not None
if not has_pyspark:
    st.info("Install the GUI extra to enable PySpark execution.")
st.warning(
    "This tests one program independently; upstream outputs are not chained. "
    "Execution success does not establish end-to-end pipeline success or SAS equivalence."
)
run_one = st.button("Run selected program", type="primary", disabled=stale or not trusted or not has_pyspark)
st.caption("For an all-program run, the selected inputs are shared and only the selected program uses editor changes.")
run_all = st.button("Run all converted programs independently", disabled=stale or not trusted or not has_pyspark)
if run_one or run_all:
    st.session_state.pop("project_execution", None)
    try:
        with st.spinner("Running the selected program in a local Spark worker..."):
            result = run_converted_programs(
                project, manifest, selected, timeout_seconds=int(timeout),
                programs=[program] if run_one else list(successful),
                edited_code={program: code} if code != original else None,
            )
        st.session_state.project_execution = result
    except (OSError, ValueError, KeyError) as error:
        st.error(f"Could not run converted program: {error}")

execution = st.session_state.get("project_execution")
if execution:
    st.subheader("Most recent execution")
    st.caption("Results below describe the last run, not necessarily the current editor contents or input selection.")
    if "summary" in execution:
        counts = execution["summary"]
        cols = st.columns(4)
        cols[0].metric("Attempted", counts["program_count"])
        cols[1].metric("Succeeded", counts["succeeded"])
        cols[2].metric("Failed", counts["failed"])
        cols[3].metric("Timed out", counts["timed_out"])
    for result in execution["results"]:
        st.write(f"Program: {result['source_path']} | Status: {result['status']}")
        if result.get("error"):
            st.error(result["error"])
        for label, field in (("Standard output", "stdout_path"), ("Standard error", "stderr_path")):
            with st.expander(f"{label}: {result['source_path']}"):
                try:
                    path = Path(execution["execution_root"]) / result[field]
                    with path.open("rb") as stream:
                        stream.seek(0, 2)
                        stream.seek(max(0, stream.tell() - 32768))
                        text = stream.read().decode("utf-8", errors="replace")
                    st.code(text or "No output.")
                except OSError as error:
                    st.error(f"Cannot read execution log: {error}")
    st.download_button(
        "Download execution report", data=json.dumps(execution, indent=2).encode("utf-8"),
        file_name="execution-report.json", mime="application/json",
    )
