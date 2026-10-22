import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("coverage_summary", Path(__file__).with_name("coverage-summary.py"))
coverage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(coverage)


class CoverageTests(unittest.TestCase):
    def test_complete_reports_pass_and_missing_report_fails(self):
        previous = Path.cwd()
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {}, clear=True):
            try:
                os.chdir(folder)
                for framework in ("net8.0", "net10.0"):
                    report = Path("coverage") / framework / "run" / "coverage.cobertura.xml"
                    report.parent.mkdir(parents=True)
                    report.write_text('<coverage lines-covered="1" lines-valid="2" line-rate="0.5" branch-rate="0.5"/>')
                coverage.main()
                report.unlink()
                with self.assertRaises(ValueError):
                    coverage.main()
            finally:
                os.chdir(previous)

    def test_empty_report_fails(self):
        previous = Path.cwd()
        with tempfile.TemporaryDirectory() as folder:
            try:
                os.chdir(folder)
                report = Path("coverage/net8.0/coverage.cobertura.xml")
                report.parent.mkdir(parents=True)
                report.write_text('<coverage lines-covered="0" lines-valid="0" line-rate="0" branch-rate="0"/>')
                with self.assertRaises(ValueError):
                    coverage.main()
            finally:
                os.chdir(previous)


if __name__ == "__main__":
    unittest.main()
