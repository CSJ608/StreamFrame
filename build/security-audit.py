"""核对完整 NuGet 解析图及开发工具内嵌组件，不接受不完整的扫描结果。"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
SEVERITIES = {"low", "moderate", "medium", "high", "critical"}
COVERLET_ISSUE = "https://github.com/CSJ608/StreamFrame/issues/107"
COVERLET_COMPONENT = "System.Security.Cryptography.Pkcs@10.0.12"
COVERLET_DEFERRED = {
    ("GHSA-846f-f764-76mx", "high", "https://github.com/advisories/GHSA-846f-f764-76mx"),
    ("GHSA-2fmv-rv2c-59x7", "medium", "https://github.com/advisories/GHSA-2fmv-rv2c-59x7"),
}


def command_json(command):
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, encoding="utf-8-sig")
    if result.returncode or result.stderr.strip():
        raise ValueError(f"审计命令失败: {' '.join(command)}\n{result.stderr}\n{result.stdout}")
    return json.loads(result.stdout)


def project_paths():
    solution = ET.parse(ROOT / "StreamFrame.slnx")
    return {node.attrib["Path"].replace("\\", "/") for node in solution.iter("Project")}


def validate_report(report, expected):
    if not isinstance(report, dict) or report.get("version") != 1:
        raise ValueError("NuGet 审计结果格式或版本错误")
    projects = report.get("projects")
    if not isinstance(projects, list) or report.get("problems"):
        raise ValueError("NuGet 审计项目结果缺失或含错误")
    seen = set()
    findings = []
    for project in projects:
        if not isinstance(project, dict) or project.get("problems"):
            raise ValueError("NuGet 项目审计失败")
        path = str(project.get("path", "")).replace("\\", "/")
        matches = [p for p in expected if path == p or path.endswith("/" + p)]
        if len(matches) != 1 or matches[0] in seen:
            raise ValueError(f"未知或重复的审计项目: {path}")
        seen.add(matches[0])
        frameworks = project.get("frameworks", [])
        if not isinstance(frameworks, list):
            raise ValueError("项目框架列表格式错误")
        for framework in frameworks:
            if not isinstance(framework, dict) or framework.get("problems"):
                raise ValueError("目标框架审计失败")
            for kind in ("topLevelPackages", "transitivePackages"):
                packages = framework.get(kind, [])
                if not isinstance(packages, list):
                    raise ValueError("包列表格式错误")
                for package in packages:
                    if not isinstance(package, dict):
                        raise ValueError("包审计结果格式错误")
                    vulnerabilities = package.get("vulnerabilities")
                    if not isinstance(vulnerabilities, list) or not vulnerabilities:
                        raise ValueError("漏洞过滤结果中的包缺少漏洞详情")
                    for vulnerability in vulnerabilities:
                        severity = str(vulnerability.get("severity", "")).lower()
                        if severity not in SEVERITIES or not vulnerability.get("advisoryurl"):
                            raise ValueError("漏洞严重性或公告地址缺失")
                        findings.append((package["id"], severity, vulnerability["advisoryurl"]))
    if seen != expected:
        raise ValueError(f"未审计全部项目: {sorted(expected - seen)}")
    return findings


def embedded_packages(deps):
    libraries = deps.get("libraries")
    if not isinstance(libraries, dict):
        raise ValueError("工具内嵌依赖清单缺少 libraries")
    return {name.rsplit("/", 1)[0] + "@" + name.rsplit("/", 1)[1]
            for name, metadata in libraries.items() if metadata.get("type") == "package"}


def resolved_packages(projects, coverage):
    packages = set()
    embedded = set()
    collectors = set()
    collector_versions = set()
    for project in sorted(projects):
        assets_path = ROOT / Path(project).parent / "obj/project.assets.json"
        assets = json.loads(assets_path.read_text(encoding="utf-8-sig"))
        packages.update(embedded_packages(assets))
        for name in assets["libraries"]:
            if name.lower().startswith("coverlet.collector/"):
                collector_versions.add(name.lower())
                matches = [Path(folder) / name.lower() for folder in assets.get("packageFolders", {})
                           if (Path(folder) / name.lower()).is_dir()]
                if not matches:
                    raise ValueError(f"覆盖率工具缓存缺失: {name}")
                collectors.update(matches)
    if collector_versions and not coverage:
        raise ValueError("普通恢复图意外包含覆盖率工具，请使用默认属性重新 restore")
    if coverage:
        if not collectors:
            raise ValueError("覆盖率恢复图缺少 collector，不能跳过内嵌依赖检查")
        for collector in collectors:
            files = list(collector.glob("build/*/coverlet.collector.deps.json"))
            if not files:
                raise ValueError(f"覆盖率工具内嵌清单缺失: {collector}")
            for path in files:
                embedded.update(embedded_packages(json.loads(path.read_text(encoding="utf-8-sig"))))
    return sorted(packages), sorted(embedded), collector_versions


def github_findings(packages, fetch=command_json):
    findings = set()
    for start in range(0, len(packages), 15):
        command = ["gh", "api", "--method", "GET", "advisories", "-f", "ecosystem=nuget",
                   "-f", "type=reviewed", "-f", "per_page=100"]
        for package in packages[start:start + 15]:
            command += ["-f", "affects[]=" + package]
        response = fetch(command)
        if not isinstance(response, list) or len(response) >= 100:
            raise ValueError("GitHub 审计响应错误或分页结果不完整")
        for advisory in response:
            if not isinstance(advisory, dict):
                raise ValueError("GitHub 公告格式错误")
            severity = str(advisory.get("severity", "")).lower()
            if severity not in SEVERITIES or not advisory.get("html_url") or not advisory.get("ghsa_id"):
                raise ValueError("GitHub 公告字段不完整")
            findings.add((advisory["ghsa_id"], severity, advisory["html_url"]))
    return findings


def scan_package_findings(packages, embedded, collector_versions, defer_coverlet, scan=None):
    scan = scan or github_findings
    graph_findings = scan(packages)
    embedded = set(embedded)
    can_defer = (defer_coverlet and collector_versions == {"coverlet.collector/10.1.0"}
                 and COVERLET_COMPONENT in embedded)
    if not can_defer:
        return graph_findings | scan(sorted(embedded)), set()

    # 同一公告若来自项目依赖或另一内嵌组件，不属于本 Issue 的延期范围。
    component_findings = scan([COVERLET_COMPONENT])
    other_findings = scan(sorted(embedded - {COVERLET_COMPONENT}))
    deferred = (component_findings & COVERLET_DEFERRED) - graph_findings - other_findings
    return graph_findings | component_findings | other_findings, deferred


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage", action="store_true")
    parser.add_argument("--defer-coverlet-107", action="store_true",
                        help="仅延期 Issue #107 登记的精确覆盖率组件告警，仍输出全部结果")
    args = parser.parse_args()
    if args.defer_coverlet_107 and not args.coverage:
        parser.error("--defer-coverlet-107 仅适用于 --coverage")
    try:
        projects = project_paths()
        if len(projects) != 6:
            raise ValueError("项目清单已变化，须审核审计覆盖范围")
        report = command_json(["dotnet", "package", "list", "--project", "StreamFrame.slnx",
                               "--vulnerable", "--include-transitive", "--no-restore",
                               "--format", "json", "--output-version", "1"])
        nuget_findings = set(validate_report(report, projects))
        packages, embedded, collector_versions = resolved_packages(projects, args.coverage)
        github_results, deferred = scan_package_findings(
            packages, embedded, collector_versions, args.defer_coverlet_107)
        graph_urls = {url for _, _, url in nuget_findings}
        deferred = {finding for finding in deferred if finding[2] not in graph_urls}
        findings = nuget_findings | github_results
        lines = [f"已核对 {len(projects)} 个项目、{len(set(packages) | set(embedded))} 个包版本。"]
        lines += [f"- {severity}: {name} {url}" for name, severity, url in sorted(findings)]
        failed = any(severity in {"high", "critical"} for _, severity, _ in findings - deferred)
        if deferred:
            lines.append(f"已知覆盖率组件风险按 Issue #107 延期处理，未修复：{COVERLET_ISSUE}")
            if os.environ.get("GITHUB_ACTIONS") == "true":
                print(f"::warning title=Coverlet 已知漏洞延期::仅延期已登记组件告警；未修复，跟踪 {COVERLET_ISSUE}")
        if args.coverage and failed:
            lines.append("覆盖率工具存在未获延期的告警；本次审计失败，不自动豁免。")
        text = "\n".join(lines)
        print(text)
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as file:
                file.write("\n## 依赖安全审计\n\n" + text + "\n")
        return 1 if failed else 0
    except (ValueError, OSError, KeyError, TypeError) as error:
        print(f"安全审计未完成: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
