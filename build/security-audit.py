"""核对完整 NuGet 解析图及开发工具内嵌组件，不接受不完整的扫描结果。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
SEVERITIES = {"low", "moderate", "medium", "high", "critical"}



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


def verified_coverage_components(collector):
    # Microsoft.CodeCoverage 没有 deps.json；按核验过的官方包二进制清单校验，升级必须重新盘点。
    manifest = json.loads((ROOT / "build/codecoverage-components.json").read_text(encoding="utf-8-sig"))
    if collector.name != manifest["version"] or collector.parent.name != manifest["package"].lower():
        raise ValueError("覆盖率工具版本变化，必须重新核验内嵌组件清单")
    files = {p.relative_to(collector).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in collector.rglob("*") if p.is_file() and p.suffix in {".dll", ".so", ".dylib", ".exe"}}
    if not files or files != manifest["files"]:
        raise ValueError("覆盖率工具二进制与已核验清单不一致")
    if not manifest["packages"]:
        raise ValueError("覆盖率工具内嵌包清单为空")
    return set(manifest["packages"])


def resolved_packages(projects, coverage):
    packages = set()
    embedded = set()
    collectors = set()
    for project in sorted(projects):
        assets_path = ROOT / Path(project).parent / "obj/project.assets.json"
        assets = json.loads(assets_path.read_text(encoding="utf-8-sig"))
        packages.update(embedded_packages(assets))
        for name in assets["libraries"]:
            if name.lower().startswith("coverlet."):
                raise ValueError("恢复图包含已移除的 Coverlet 工具，请审核依赖")
            if name.lower().startswith("microsoft.codecoverage/"):
                matches = [Path(folder) / name.lower() for folder in assets.get("packageFolders", {})
                           if (Path(folder) / name.lower()).is_dir()]
                if not matches:
                    raise ValueError(f"覆盖率工具缓存缺失: {name}")
                collectors.update(matches)
    if coverage and not collectors:
        raise ValueError("覆盖率恢复图缺少 collector，不能跳过内嵌依赖检查")
    # 此工具也由 Test.Sdk 间接引入，普通测试图同样检查内嵌依赖。
    for collector in collectors:
        embedded.update(verified_coverage_components(collector))
    return sorted(packages), sorted(embedded)


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage", action="store_true")
    args = parser.parse_args()
    try:
        projects = project_paths()
        if len(projects) != 6:
            raise ValueError("项目清单已变化，须审核审计覆盖范围")
        report = command_json(["dotnet", "package", "list", "--project", "StreamFrame.slnx",
                               "--vulnerable", "--include-transitive", "--no-restore",
                               "--format", "json", "--output-version", "1"])
        nuget_findings = set(validate_report(report, projects))
        packages, embedded = resolved_packages(projects, args.coverage)
        findings = nuget_findings | github_findings(sorted(set(packages) | set(embedded)))
        lines = [f"已核对 {len(projects)} 个项目、{len(set(packages) | set(embedded))} 个包版本。"]
        lines += [f"- {severity}: {name} {url}" for name, severity, url in sorted(findings)]
        failed = bool(findings)
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
