import importlib.util
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("security_audit", Path(__file__).with_name("security-audit.py"))
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class AuditTests(unittest.TestCase):
    def test_coverage_manifest_rejects_tampering_and_new_binaries(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            collector = root / 'microsoft.codecoverage/18.11.0'
            collector.mkdir(parents=True)
            binary = collector / 'Dependency.dll'
            binary.write_bytes(b'official package bytes')
            manifest = {'package': 'Microsoft.CodeCoverage', 'version': '18.11.0',
                        'packages': ['Dependency@1.2.3'],
                        'files': {'Dependency.dll': hashlib.sha256(binary.read_bytes()).hexdigest()}}
            (root / 'build').mkdir()
            (root / 'build/codecoverage-components.json').write_text(json.dumps(manifest))
            with patch.object(audit, 'ROOT', root):
                self.assertEqual({'Dependency@1.2.3'}, audit.verified_coverage_components(collector))
                binary.write_bytes(b'tampered')
                with self.assertRaises(ValueError):
                    audit.verified_coverage_components(collector)
                binary.write_bytes(b'official package bytes')
                (collector / 'Unexpected.dll').write_bytes(b'new component')
                with self.assertRaises(ValueError):
                    audit.verified_coverage_components(collector)

    def test_complete_empty_result_passes(self):
        self.assertEqual([], audit.validate_report({"version": 1, "projects": [{"path": "a.csproj"}]}, {"a.csproj"}))

    def test_missing_project_fails(self):
        with self.assertRaises(ValueError):
            audit.validate_report({"version": 1, "projects": []}, {"a.csproj"})

    def test_duplicate_project_fails(self):
        with self.assertRaises(ValueError):
            audit.validate_report({"version": 1, "projects": [{"path": "a.csproj"}] * 2}, {"a.csproj"})

    def test_invalid_shape_fails(self):
        for report in ({}, [], {"version": 1, "projects": "bad"}):
            with self.assertRaises(ValueError):
                audit.validate_report(report, {"a.csproj"})

    def test_source_problem_fails(self):
        with self.assertRaises(ValueError):
            audit.validate_report({"version": 1, "projects": [], "problems": [{"text": "NU1900"}]}, set())

    def test_vulnerability_is_retained(self):
        for severity in ("High", "Moderate", "Critical"):
            report = {"version": 1, "projects": [{"path": "a.csproj", "frameworks": [{
                "framework": "net10.0", "transitivePackages": [{"id": "P", "vulnerabilities": [
                    {"severity": severity, "advisoryurl": "https://example.test/advisory"}]}]}]}]}
            result = audit.validate_report(report, {"a.csproj"})
            self.assertEqual(severity.lower(), result[0][1])

    def test_unknown_severity_fails(self):
        with self.assertRaises(ValueError):
            audit.github_findings(["P@1.0"], lambda _: [{"severity": "unknown", "ghsa_id": "a", "html_url": "x"}])

    def test_github_error_is_not_empty_success(self):
        with self.assertRaises(ValueError):
            audit.github_findings(["P@1.0"], lambda _: {"message": "rate limited"})

    def test_github_pagination_not_silently_truncated(self):
        with self.assertRaises(ValueError):
            audit.github_findings(["P@1.0"], lambda _: [{}] * 100)

    def test_embedded_components_are_included(self):
        packages = audit.embedded_packages({"libraries": {
            "Tool/1.0": {"type": "project"},
            "System.Security.Cryptography.Pkcs/10.0.12": {"type": "package"}}})
        self.assertEqual({"System.Security.Cryptography.Pkcs@10.0.12"}, packages)

    def test_command_failure_fails_closed(self):
        with patch.object(audit.subprocess, "run") as run:
            run.return_value.returncode = 1
            run.return_value.stderr = "network error"
            run.return_value.stdout = "{}"
            with self.assertRaises(ValueError):
                audit.command_json(["gh", "api", "advisories"])

    def test_every_severity_exits_failure(self):
        for severity, expected in (("high", 1), ("critical", 1), ("medium", 1), ("low", 1)):
            projects = {f"p{i}.csproj" for i in range(6)}
            report = {"version": 1, "projects": [{"path": p} for p in projects]}
            with patch.object(audit, "project_paths", return_value=projects), \
                    patch.object(audit, "command_json", return_value=report), \
                    patch.object(audit, "resolved_packages", return_value=(["P@1"], [])), \
                    patch.object(audit, "github_findings", return_value={("GHSA-test", severity, "https://example.test")}), \
                    patch("sys.argv", ["security-audit.py"]), patch.dict(audit.os.environ, {}, clear=True):
                self.assertEqual(expected, audit.main())


if __name__ == "__main__":
    unittest.main()
