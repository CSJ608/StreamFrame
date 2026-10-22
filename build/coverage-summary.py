import os
from pathlib import Path
import sys
import xml.etree.ElementTree as ET


def main():
    lines = ["## 测试覆盖率", "", "| 目标 | 行覆盖率 | 分支覆盖率 |", "|---|---:|---:|"]
    for framework in ("net8.0", "net10.0"):
        reports = list((Path("coverage") / framework).rglob("coverage.cobertura.xml"))
        if len(reports) != 1:
            raise ValueError(f"{framework} 必须恰有一份覆盖率报告，实际 {len(reports)} 份")
        root = ET.parse(reports[0]).getroot()
        covered = int(root.attrib["lines-covered"])
        valid = int(root.attrib["lines-valid"])
        line_rate = float(root.attrib["line-rate"])
        branch_rate = float(root.attrib["branch-rate"])
        if not 0 < covered <= valid or not 0 < line_rate <= 1 or not 0 <= branch_rate <= 1:
            raise ValueError(f"{framework} 报告为空或覆盖率字段无效")
        lines.append(f"| {framework} | {line_rate:.1%} ({covered}/{valid}) | {branch_rate:.1%} |")
    summary = "\n".join(lines) + "\n"
    print(summary)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as file:
            file.write("\n" + summary)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, ET.ParseError) as error:
        print(f"覆盖率验证失败: {error}", file=sys.stderr)
        sys.exit(1)
