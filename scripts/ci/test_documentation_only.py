"""Contract cases for the CI fast-path decision."""

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import unittest


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
SPEC = importlib.util.spec_from_file_location("documentation_only", HERE / "documentation_only.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def files(*entries):
    return [json.dumps(entry) for entry in entries]


class DocumentationOnlyTests(unittest.TestCase):
    def test_markdown_and_deletion(self):
        self.assertTrue(MODULE.documentation_only(files(
            {"filename": "README.md", "status": "modified"},
            {"filename": "docs/old.md", "status": "removed"},
        )))

    def test_rename_checks_both_paths(self):
        self.assertTrue(MODULE.documentation_only(files(
            {"filename": "docs/new.md", "previous_filename": "docs/old.md", "status": "renamed"},
        )))
        self.assertFalse(MODULE.documentation_only(files(
            {"filename": "docs/new.md", "previous_filename": "src/old.rs", "status": "renamed"},
        )))
        self.assertFalse(MODULE.documentation_only(files(
            {"filename": "src/new.rs", "previous_filename": "docs/old.md", "status": "renamed"},
        )))

    def test_code_workflow_config_and_mixed_changes(self):
        for path in ("src/lib.rs", "Cargo.lock", "Cargo.toml", ".github/workflows/ci.yml",
                     "scripts/verify-slice-1", "docs/config.json", "docs/guide.md.py"):
            with self.subTest(path=path):
                self.assertFalse(MODULE.documentation_only(files(
                    {"filename": "README.md", "status": "modified"},
                    {"filename": path, "status": "modified"},
                )))

    def test_bad_or_incomplete_lists_fail_closed(self):
        for lines in ([], ["{"], files({"filename": "README.md"}),
                      files({"filename": "README.md", "status": "renamed"})):
            with self.subTest(lines=lines):
                with self.assertRaises(ValueError):
                    MODULE.documentation_only(lines)
        self.assertFalse(MODULE.documentation_only(files(*(
            {"filename": "docs/guide.md", "status": "modified"}
            for _ in range(MODULE.MAX_FILES)
        ))))

    def test_no_rust_source_embeds_markdown(self):
        tracked = subprocess.check_output(["git", "ls-files", "*.rs"], cwd=ROOT, text=True)
        embedded = []
        for path in tracked.splitlines():
            source = (ROOT / path).read_text()
            if re.search(r'include_str!\s*\(\s*"[^"]*\.md"', source) or re.search(
                r'doc\s*=\s*include_str!', source
            ):
                embedded.append(path)
        self.assertEqual(embedded, [], "Rust now embeds Markdown; update the CI fast path")


if __name__ == "__main__":
    unittest.main()
