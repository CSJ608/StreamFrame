# 贡献指南

欢迎 issue、讨论与 PR。提 PR 前请先开 issue 或在 [Discussions](https://github.com/CSJ608/StreamFrame/discussions) 里聊一下方向。

## 工作流

所有改动走标准流程（详见 [AGENTS.md](AGENTS.md)，它是本仓库对贡献者与编码代理的统一约定）：

1. 从最新 main 建分支（`fix-xxx` / `feat-xxx` / `docs-xxx`）
2. Conventional Commits 前缀 + 中文摘要，按主题拆分提交
3. 同步更新 `CHANGELOG.md` 的 `Unreleased` 段
4. 推分支 → 开 PR → CI 绿（Ubuntu net8/net10 + Windows net48 全量测试）→ rebase 合并

> main 有分支保护：必须走 PR、必需检查通过、禁 force push、强制线性历史。

## 本地验证

```bash
dotnet build StreamFrame.slnx -c Release
dotnet test test/StreamFrame.Tests/StreamFrame.Tests.csproj -c Release --no-build -f net8.0   # 或 -f net10.0 / -f net48
```

库项目开启了 `TreatWarningsAsErrors` 与 XML 文档强制——新公共 API 必须带文档注释，新告警即构建失败。

发版流程见 [docs/PUBLISHING.md](docs/PUBLISHING.md)。
