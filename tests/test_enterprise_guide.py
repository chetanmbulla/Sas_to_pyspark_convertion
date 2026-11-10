import tempfile
import unittest
import zipfile
import contextlib
import io
from pathlib import Path

from sas2pyspark.cli import main
from sas2pyspark.enterprise_guide import build_enterprise_guide_inventory


class EnterpriseGuideInventoryTests(unittest.TestCase):
    def test_inventories_package_members_and_stored_process_markers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "flow.egp"
            with zipfile.ZipFile(package, "w") as archive:
                archive.writestr("Project.xml", "<Project><Name>Private project</Name></Project>")
                archive.writestr("Code/task.sas", "%stpbegin; data x; run; %stpend;")
            (root / "plain.sas").write_text(
                "/* %stpbegin */\ndata x; set y; run; _webout;\n",
                encoding="utf-8",
            )
            report = build_enterprise_guide_inventory(root)

        self.assertEqual(report["summary"]["package_count"], 1)
        self.assertEqual(report["packages"][0]["sas_members"], ["Code/task.sas"])
        self.assertEqual(report["packages"][0]["xml_root_tags"], {"Project.xml": "Project"})
        self.assertNotIn("Private project", str(report))
        processes = {item["path"]: item for item in report["stored_processes"]}
        self.assertEqual(
            processes["flow.egp!/Code/task.sas"]["classification"],
            "stored_process_marker",
        )
        self.assertEqual(
            processes["plain.sas"]["classification"],
            "web_output_candidate",
        )

    def test_invalid_archive_is_reported_without_extraction(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.egp").write_bytes(b"not a zip")
            report = build_enterprise_guide_inventory(root)
        self.assertEqual(report["summary"]["package_count"], 1)
        self.assertEqual(report["summary"]["issue_count"], 1)
        self.assertTrue(report["packages"][0]["issues"])

    def test_skips_xml_with_entity_declarations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with zipfile.ZipFile(root / "flow.egp", "w") as archive:
                archive.writestr(
                    "metadata.xml",
                    '<!DOCTYPE x [<!ENTITY secret "hidden">]><Project>&secret;</Project>',
                )
            report = build_enterprise_guide_inventory(root)
        self.assertIn("DTD/entity", report["packages"][0]["issues"][0])
        self.assertNotIn("hidden", str(report))

    def test_cli_writes_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"
            root.mkdir()
            report_path = Path(directory) / "eg.json"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                result = main(
                    [
                        "project",
                        "enterprise-guide-inventory",
                        "--root",
                        str(root),
                        "--report",
                        str(report_path),
                    ]
                )
            self.assertEqual(result, 0)
            self.assertTrue(report_path.is_file())
            self.assertIn("Inventoried 0 EG package(s)", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
