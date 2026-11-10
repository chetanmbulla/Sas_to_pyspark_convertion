import tempfile
import unittest
from pathlib import Path

from sas2pyspark.includes import IncludeError, inline_includes


class IncludeHardeningTests(unittest.TestCase):
    def test_crlf_line_endings_are_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a.sas").write_text("data x; set y; run;\r\n", encoding="utf-8", newline="")
            result = inline_includes('%let r = .;\r\n%include "&r\\a.sas"; \r\nrun;\r\n', root)
            self.assertIn("data x; set y; run;", result)

    def test_drive_paths_resolve_through_mapping(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "V" / "lib").mkdir(parents=True)
            (root / "V" / "lib" / "a.sas").write_text("data x; run;\n", encoding="utf-8")
            source = "%let root = v:;\n%include \"&root\\lib\\a.sas\";\n"
            result = inline_includes(source, root, root, {"V": root / "V"})
            self.assertIn("data x; run;", result)
            with self.assertRaises(IncludeError) as raised:
                inline_includes(source, root, root, {"W": root})
            self.assertIn("not mapped", raised.exception.message)
            with self.assertRaises(IncludeError):
                inline_includes(source, root, root)

    def test_fileref_member_include(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "lib").mkdir()
            (root / "lib" / "m.sas").write_text("data x; run;\n", encoding="utf-8")
            source = "%let r = lib;\nfilename proglib \"&r\";\n%include proglib(m.sas);\n"
            self.assertIn("data x; run;", inline_includes(source, root, root))
            with self.assertRaises(IncludeError):
                inline_includes("%include nofile(m.sas);\n", root, root)

    def test_macro_variables_seed_include_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a.sas").write_text("data x; run;\n", encoding="utf-8")
            self.assertIn(
                "data x; run;",
                inline_includes('%include "&ROOT.a.sas";\n', root, root, None, {"root": ""}),
            )

    def test_drive_mapping_cannot_escape_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            (root / "V").mkdir(parents=True)
            with self.assertRaises(IncludeError) as raised:
                inline_includes("%include 'v:\\..\\..\\x.sas';", root, root, {"v": root / "V"})
            self.assertIn("escapes", raised.exception.message)


if __name__ == "__main__":
    unittest.main()
