import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("security_audit", Path(__file__).with_name("security-audit.py"))
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class AuditTests(unittest.TestCase):
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

    def test_only_registered_embedded_findings_can_be_deferred(self):
        known = {
            ("GHSA-846f-f764-76mx", "high", "https://github.com/advisories/GHSA-846f-f764-76mx"),
            ("GHSA-2fmv-rv2c-59x7", "medium", "https://github.com/advisories/GHSA-2fmv-rv2c-59x7"),
        }
        pkcs = "System.Security.Cryptography.Pkcs@10.0.12"
        scan = lambda packages: known if pkcs in packages else set()
        findings, deferred = audit.scan_package_findings(
            ["coverlet.collector@10.1.0"], [pkcs], {"coverlet.collector/10.1.0"}, True, scan)
        self.assertEqual(known, findings)
        self.assertEqual(known, deferred)

    def test_strict_coverage_does_not_defer(self):
        known = {("GHSA-846f-f764-76mx", "high", "https://github.com/advisories/GHSA-846f-f764-76mx")}
        pkcs = "System.Security.Cryptography.Pkcs@10.0.12"
        findings, deferred = audit.scan_package_findings(
            [], [pkcs], {"coverlet.collector/10.1.0"}, False,
            lambda packages: known if pkcs in packages else set())
        self.assertEqual(known, findings)
        self.assertFalse(deferred)

    def test_changed_tool_or_component_version_is_not_deferred(self):
        known = {("GHSA-846f-f764-76mx", "high", "https://github.com/advisories/GHSA-846f-f764-76mx")}
        for collector, component in (("10.1.1", "10.0.12"), ("10.1.0", "10.0.11")):
            pkcs = "System.Security.Cryptography.Pkcs@" + component
            _, deferred = audit.scan_package_findings(
                [], [pkcs], {"coverlet.collector/" + collector}, True,
                lambda packages: known if pkcs in packages else set())
            self.assertFalse(deferred)

    def test_graph_dependency_with_same_advisory_is_not_deferred(self):
        known = {("GHSA-846f-f764-76mx", "high", "https://github.com/advisories/GHSA-846f-f764-76mx")}
        pkcs = "System.Security.Cryptography.Pkcs@10.0.12"
        _, deferred = audit.scan_package_findings(
            [pkcs], [pkcs], {"coverlet.collector/10.1.0"}, True,
            lambda packages: known if pkcs in packages else set())
        self.assertFalse(deferred)

    def test_new_or_escalated_advisory_remains_blocking(self):
        pkcs = "System.Security.Cryptography.Pkcs@10.0.12"
        for finding in (("GHSA-new", "high", "https://github.com/advisories/GHSA-new"),
                        ("GHSA-846f-f764-76mx", "critical", "https://github.com/advisories/GHSA-846f-f764-76mx")):
            findings, deferred = audit.scan_package_findings(
                [], [pkcs], {"coverlet.collector/10.1.0"}, True,
                lambda packages: {finding} if pkcs in packages else set())
            self.assertIn(finding, findings)
            self.assertFalse(deferred)

    def test_other_embedded_component_is_not_deferred(self):
        known = {("GHSA-846f-f764-76mx", "high", "https://github.com/advisories/GHSA-846f-f764-76mx")}
        _, deferred = audit.scan_package_findings(
            [], ["Other.Package@1.0"], {"coverlet.collector/10.1.0"}, True,
            lambda packages: known if packages else set())
        self.assertFalse(deferred)

    def test_embedded_audit_failure_cannot_be_deferred(self):
        def failed(_):
            raise ValueError("advisory service unavailable")
        with self.assertRaises(ValueError):
            audit.scan_package_findings([], ["System.Security.Cryptography.Pkcs@10.0.12"],
                                        {"coverlet.collector/10.1.0"}, True, failed)

    def test_high_severity_exits_failure(self):
        for severity, expected in (("high", 1), ("critical", 1), ("medium", 0)):
            projects = {f"p{i}.csproj" for i in range(6)}
            report = {"version": 1, "projects": [{"path": p} for p in projects]}
            with patch.object(audit, "project_paths", return_value=projects), \
                    patch.object(audit, "command_json", return_value=report), \
                    patch.object(audit, "resolved_packages", return_value=(["P@1"], [], set())), \
                    patch.object(audit, "github_findings", return_value={("GHSA-test", severity, "https://example.test")}), \
                    patch("sys.argv", ["security-audit.py"]), patch.dict(audit.os.environ, {}, clear=True):
                self.assertEqual(expected, audit.main())


if __name__ == "__main__":
    unittest.main()
