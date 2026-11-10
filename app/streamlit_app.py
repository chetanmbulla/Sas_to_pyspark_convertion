"""Streamlit interface for converting and testing SAS-to-PySpark code."""

import hashlib
import importlib.util

import streamlit as st

from sas2pyspark import ConversionError, convert_sas
from sas2pyspark.discovery import _decode_source
from sas2pyspark.gui_testing import performance_notes, run_pyspark_test


st.set_page_config(page_title="SAS to PySpark", layout="wide")

st.title("SAS to PySpark")
st.write(
    "Convert SAS source, review or edit the generated PySpark, and test it "
    "against sample data."
)
st.info("For ZIP projects and Phase 1 reports, open **Project Analysis** in the sidebar.")
st.warning(
    "Testing executes Python code under the account running this app. "
    "This is not a security sandbox; only run code you trust, and keep the "
    "app local to your machine."
)

uploaded_sas = st.file_uploader("Upload a SAS file", type=["sas"])

if "sas_source" not in st.session_state:
    st.session_state.sas_source = ""
if "pyspark_code" not in st.session_state:
    st.session_state.pyspark_code = ""
if "conversion_warnings" not in st.session_state:
    st.session_state.conversion_warnings = ()
if "conversion_profile" not in st.session_state:
    st.session_state.conversion_profile = ""

load_col, _ = st.columns([1, 4])
with load_col:
    if st.button("Load uploaded SAS file", disabled=uploaded_sas is None):
        source_text, _, decode_error = _decode_source(uploaded_sas.getvalue())
        if decode_error:
            st.error(decode_error)
        elif source_text is None:
            st.error("Could not decode the uploaded SAS file.")
        else:
            st.session_state.sas_source = source_text
            st.session_state.pyspark_code = ""
            st.session_state.conversion_warnings = ()
            st.session_state.conversion_profile = ""
            st.session_state.pop("pyspark_test_result", None)

source = st.text_area(
    "SAS source code",
    key="sas_source",
    height=300,
    placeholder="Paste your SAS program here...",
)

sas_version = st.selectbox(
    "SAS source version",
    ("auto", "9.4M4", "9.4M8", "Viya"),
    help=(
        "Auto reads a sas2pyspark version marker; if absent, the Viya profile is "
        "selected and a warning is shown. Release selection does not certify "
        "complete SAS-version-specific syntax or runtime equivalence."
    ),
)

if st.button("Convert to PySpark", type="primary"):
    if not source.strip():
        st.session_state.pyspark_code = ""
        st.session_state.conversion_warnings = ()
        st.session_state.conversion_profile = ""
        st.session_state.pop("pyspark_test_result", None)
        st.warning("Enter SAS source code before converting.")
    else:
        try:
            result = convert_sas(source, sas_version=sas_version)
            st.session_state.pyspark_code = result.code
            st.session_state.conversion_warnings = result.warnings
            st.session_state.conversion_profile = (
                f"{result.source_version} ({result.version_source})"
            )
            st.session_state.pop("pyspark_test_result", None)
        except ConversionError as error:
            st.session_state.pyspark_code = ""
            st.session_state.conversion_warnings = ()
            st.session_state.conversion_profile = ""
            st.session_state.pop("pyspark_test_result", None)
            st.error(f"Conversion failed: {error}")

if st.session_state.conversion_profile:
    st.info(f"SAS source profile: {st.session_state.conversion_profile}")

if st.session_state.conversion_warnings:
    with st.expander("Conversion warnings", expanded=True):
        for warning in st.session_state.conversion_warnings:
            st.warning(warning)

if st.session_state.pyspark_code:
    st.subheader("Review and edit PySpark code")
    code = st.text_area(
        "PySpark code to validate and run",
        key="pyspark_code",
        height=420,
    )
    st.download_button(
        "Download PySpark code",
        data=code,
        file_name="converted.py",
        mime="text/x-python",
    )

    syntax_col, _ = st.columns([1, 4])
    with syntax_col:
        if st.button("Check Python syntax"):
            try:
                compile(code, "<converted-pyspark>", "exec")
            except SyntaxError as error:
                st.error(
                    f"Syntax error on line {error.lineno}: "
                    f"{error.msg or 'invalid syntax'}"
                )
            else:
                st.success("Python syntax is valid.")

    try:
        code_performance_notes = performance_notes(code)
    except SyntaxError:
        code_performance_notes = ()
    if code_performance_notes:
        with st.expander("Potential performance risks", expanded=True):
            st.caption(
                "These are code-pattern heuristics, not a proof of performance "
                "or correctness."
            )
            for note in code_performance_notes:
                st.warning(note)

    st.subheader("Run a Spark test")
    st.caption(
        "Upload CSV, Parquet, JSON, or JSONL files. Each file's base name becomes "
        "its input dataset name (for example, people.csv provides df_people). "
        "Uploaded data is removed when the test finishes."
    )
    uploaded_datasets = st.file_uploader(
        "Test datasets",
        type=["csv", "parquet", "json", "jsonl"],
        accept_multiple_files=True,
        key="test_datasets",
    )
    with st.expander("Spark test settings"):
        master = st.selectbox("Spark master", ["local[1]", "local[*]"])
        shuffle_partitions = st.number_input(
            "Shuffle partitions", min_value=1, max_value=64, value=4, step=1
        )
        timeout_seconds = st.slider(
            "Execution timeout (seconds)", min_value=30, max_value=600, value=120
        )

    has_pyspark = importlib.util.find_spec("pyspark") is not None
    if not has_pyspark:
        st.info(
            "PySpark is not installed in this environment. Install the GUI extra "
            "to enable Spark execution; Python syntax checks remain available."
        )
    if st.button("Run PySpark test", disabled=not has_pyspark):
        try:
            compile(code, "<converted-pyspark>", "exec")
            test_result = run_pyspark_test(
                source,
                code,
                [(item.name, item.getvalue()) for item in uploaded_datasets],
                timeout_seconds=timeout_seconds,
                master=master,
                shuffle_partitions=int(shuffle_partitions),
            )
            st.session_state.pyspark_test_result = {
                "status": test_result.status,
                "duration_seconds": test_result.duration_seconds,
                "error": test_result.error,
                "stdout": test_result.stdout,
                "stderr": test_result.stderr,
                "output_previews": test_result.output_previews,
                "code_sha256": test_result.code_sha256,
            }
        except SyntaxError as error:
            st.session_state.pop("pyspark_test_result", None)
            st.error(
                f"Cannot run code with a syntax error on line {error.lineno}: "
                f"{error.msg or 'invalid syntax'}"
            )
        except (OSError, ValueError) as error:
            st.session_state.pop("pyspark_test_result", None)
            st.error(f"Could not start the PySpark test: {error}")

    saved_test_result = st.session_state.get("pyspark_test_result")
    if saved_test_result:
        current_hash = hashlib.sha256(code.encode("utf-8")).hexdigest()
        if saved_test_result["code_sha256"] != current_hash:
            st.info("The code has changed since the last test. Run it again.")
        else:
            st.subheader("Test results")
            if saved_test_result["status"] == "succeeded":
                st.success("PySpark code ran successfully on the supplied test data.")
            elif saved_test_result["status"] == "timed_out":
                st.error("The PySpark test exceeded its execution timeout.")
            else:
                st.error("PySpark execution failed.")
            st.metric(
                "Execution time",
                f"{saved_test_result['duration_seconds']:.3f} seconds",
            )
            st.caption(
                "Execution time includes Spark startup and only reflects this test "
                "data; it does not establish production-scale performance."
            )
            if saved_test_result["error"]:
                st.error(saved_test_result["error"])
            for output_path, preview in saved_test_result["output_previews"]:
                st.markdown(f"CSV output: `{output_path}`")
                st.code(preview, language="text")
            if saved_test_result["stdout"]:
                with st.expander("Spark standard output"):
                    st.code(saved_test_result["stdout"], language="text")
            if saved_test_result["stderr"]:
                with st.expander("Spark logs and errors", expanded=True):
                    st.code(saved_test_result["stderr"], language="text")
