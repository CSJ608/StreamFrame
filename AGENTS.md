# AGENTS.md

给编码代理与贡献者的仓库工作约定。

## 标准工作流（所有改动一律遵循）

1. 从最新 main 建分支（命名如 `fix-xxx` / `ci-xxx` / `docs-xxx`）
2. 按主题拆分提交：Conventional Commits 前缀（`feat` / `fix` / `ci` / `docs` / `build`）+ 中文摘要
3. 同步更新 `CHANGELOG.md` 的 `Unreleased` 段（按 新增 / 修复 / 变更 分类）
4. 推送分支 → 开 PR → 等 `ci.yml` 绿 → rebase 合并并删除分支；**不直接推 main**

## 提交前必须通过

```bash
dotnet build StreamFrame.slnx -c Release
dotnet test StreamFrame.slnx -c Release --no-build
```

## 发版

见 [docs/PUBLISHING.md](docs/PUBLISHING.md)：打 `v*` tag 触发 `release.yml` 单条流水线
（版本校验 → 构建 → 测试 → Release → 推 nuget.org）；流水线强制校验 tag 与两个 csproj
的 `<Version>` 一致，发版前先同步版本号。
