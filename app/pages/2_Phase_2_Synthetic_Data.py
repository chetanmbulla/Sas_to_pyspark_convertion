"""Explore Phase 1 dataset schemas and synthetic fixtures."""

import json
from pathlib import Path

import pandas as pd
import streamlit as st

from sas2pyspark.gui_project import preview_synthetic_data, regenerate_synthetic_data
from sas2pyspark.synthetic_data import load_catalog_report

st.set_page_config(page_title="Phase 2 synthetic data", layout="wide", initial_sidebar_state="expanded")
st.title("Phase 2: Synthetic test data")
st.caption("Explore Phase 1 dataset attributes and preview synthetic test data, not production rows.")

project = st.session_state.get("project_analysis")
if not project:
    st.info("First upload a SAS project ZIP and run Phase 1 on the Project Analysis page.")
    st.stop()

output = Path(project["output_root"])
stale = st.session_state.get("project_phase1_stale", False)
if stale:
    st.warning("Phase 1 inputs changed. These are previous results; rerun Phase 1 before regenerating fixtures.")

try:
    catalog = load_catalog_report(output / "catalog.json")
except (OSError, ValueError) as error:
    st.error(f"Cannot load Phase 1 catalog: {error}")
    st.stop()

resources = sorted(
    (resource for resource in catalog.get("resources", []) if resource.get("kind") == "sas_dataset"),
    key=lambda resource: resource["name"].casefold(),
)
manifest = st.session_state.get("project_synthetic")
if manifest is None:
    manifest_path = output / "synthetic-data" / "synthetic-data-manifest.json"
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            st.error(f"Cannot load synthetic data manifest: {error}")
            st.stop()
        st.session_state.project_synthetic = manifest
st.caption(f"Phase 1 workspace: {output}")
st.warning(
    "Fixtures reflect only the available metadata. Inferred or partial schemas may omit columns; "
    "unknown column types are assumed to be strings for generation. Types, formats, keys, "
    "relationships and business distributions are not guaranteed."
)
unused = catalog.get("summary", {}).get("unused_schema_assets", [])
if unused:
    st.warning(f"Schema metadata did not match {len(unused)} catalog dataset(s): {', '.join(unused)}")
if catalog.get("schema_asset_warnings"):
    st.warning("Some schema metadata was ignored. Review schema_asset_warnings in the catalog report.")
rows = st.number_input("Rows per dataset", min_value=1, max_value=1000000, value=100, key="phase2_rows")
seed = st.number_input("Seed", value=0, step=1, key="phase2_seed")
if st.button("Generate synthetic test data", disabled=stale):
    try:
        with st.spinner("Generating synthetic fixtures..."):
            manifest = regenerate_synthetic_data(output, rows=int(rows), seed=int(seed))
        st.session_state.project_synthetic = manifest
        st.session_state.pop("project_execution", None)
        st.session_state.pop("project_execution_inputs", None)
        st.session_state.pop("phase3_inputs", None)
        st.session_state.pop("phase3_inputs_program", None)
        if manifest["summary"]["generated_count"]:
            st.success(
                f"Generated {manifest['summary']['generated_count']} dataset fixture(s); "
                f"{manifest['summary']['skipped_count']} skipped."
            )
        else:
            st.warning(
                "No dataset fixtures were generated. Review the reasons below and "
                "supply schema metadata on the Project Analysis page."
            )
    except (OSError, ValueError) as error:
        st.error(f"Synthetic data generation failed: {error}")

records = {item["dataset"]: item for item in manifest["datasets"]} if manifest else {}
if manifest:
    columns = st.columns(3)
    columns[0].metric("Generated", manifest["summary"]["generated_count"])
    columns[1].metric("Partial fixtures", manifest["summary"]["partial_schema_count"])
    columns[2].metric("Skipped", manifest["summary"]["skipped_count"])
    if manifest["summary"]["skipped_count"]:
        st.warning(
            f"{manifest['summary']['skipped_count']} dataset(s) were skipped because "
            "their metadata is insufficient or unsupported. Inspect the reason for "
            "each object below. Upload matching schema metadata on Project Analysis "
            "and rerun Phase 1 to generate additional fixtures."
        )
    if not manifest["summary"]["generated_count"]:
        st.info("No sample fixtures are available for this Phase 1 catalog.")
    st.caption(f"Fixture folder: {manifest['output_directory']}")
    st.download_button(
        "Download synthetic data manifest", data=json.dumps(manifest, indent=2).encode("utf-8"),
        file_name="synthetic-data-manifest.json", mime="application/json",
    )

st.subheader("Objects and attributes")
if not resources:
    st.info("No SAS datasets were found in the Phase 1 catalog.")
    st.stop()
st.dataframe(
    [
        {
            "Object": resource["name"],
            "Schema status": (resource.get("schema") or {}).get("status", "unknown"),
            "Attributes": len((resource.get("schema") or {}).get("columns") or []),
            "Fixture status": records.get(resource["name"], {}).get("status", "not generated"),
            "Reason": records.get(resource["name"], {}).get("reason", ""),
            "Assumed string columns": ", ".join(
                records.get(resource["name"], {}).get("assumed_character_columns", [])
            ),
        }
        for resource in resources
    ],
    use_container_width=True,
    hide_index=True,
)
by_name = {resource["name"]: resource for resource in resources}
dataset_names = list(by_name)
generated_names = [
    name for name in dataset_names
    if records.get(name, {}).get("status") in {"generated", "generated_partial_schema"}
]
default_dataset = generated_names[0] if generated_names else dataset_names[0]
dataset = st.selectbox(
    "Select a dataset",
    dataset_names,
    index=dataset_names.index(default_dataset),
    key="phase2_dataset",
    help="The first available generated fixture is selected by default; skipped datasets remain selectable.",
)
if dataset is None:
    st.error("Select a dataset to inspect its attributes and sample data.")
    st.stop()
assert dataset is not None
schema = by_name[dataset].get("schema") or {}
attributes = schema.get("columns") or []
if attributes:
    st.dataframe(
        [
            {
                "Attribute": column.get("name", ""),
                "Type": column.get("type", "unknown"),
                "Length": column.get("length"),
                "Format": column.get("format"),
                "Informat": column.get("informat"),
                "Label": column.get("label"),
                "Origin": column.get("origin"),
            }
            for column in attributes
        ],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info(
        "No column definitions were discovered for this object in the SAS files/ZIP "
        "or supplied schema metadata."
    )

st.subheader("Sample synthetic data")
record = records.get(dataset)
if not record:
    st.info("Generate synthetic data to preview this dataset.")
elif record["status"] not in {"generated", "generated_partial_schema"}:
    st.warning(f"No fixture was generated: {record.get('reason', 'Unsupported dataset status.')}")
else:
    assert manifest is not None
    if record["status"] == "generated_partial_schema":
        st.warning("This fixture uses an inferred or partial schema and may be missing attributes.")
    assumed = record.get("assumed_character_columns", [])
    if assumed:
        st.warning(
            "Unknown column types were assumed to be strings for this fixture: "
            + ", ".join(assumed)
            + ". These are not verified SAS types."
        )
    try:
        header, sample = preview_synthetic_data(output, manifest, dataset)
        st.caption(f"Showing up to 20 of {record['rows']} generated rows. CSV values are displayed as text.")
        st.dataframe(pd.DataFrame(sample, columns=header), use_container_width=True, hide_index=True)
    except (OSError, ValueError, KeyError) as error:
        st.error(f"Cannot display synthetic data: {error}")
