# AGENTS.md

给编码代理与贡献者的仓库工作约定。

## 标准工作流（所有改动一律遵循）

1. 从最新 main 建分支（命名如 `fix-xxx` / `ci-xxx` / `docs-xxx`）
2. 按主题拆分提交：Conventional Commits 前缀（`feat` / `fix` / `ci` / `docs` / `build`）+ 中文摘要
3. 同步更新 `CHANGELOG.md` 的 `Unreleased` 段（按 新增 / 修复 / 变更 分类）
   > Dependabot 的依赖升级 PR 不懂本约定：合并后由合并者顺手把版本变化补进 Unreleased（依赖类记入「变更」）
4. 推送分支 → 开 PR → 等 `ci.yml` 绿 → rebase 合并并删除分支；**不直接推 main**

> main 已启用分支保护（GitHub 设置）：必须走 PR、`build-test` 必须通过且分支最新、禁 force push/删除、强制线性历史——直推 main 会被 GitHub 拒绝，规则在 Settings → Branches 可调。注意：CI 是 OS×TFM 矩阵，必需检查的完整名带矩阵后缀（如 `build-test (windows-latest, net48)`）；调整矩阵（增删 OS/框架）后需同步更新分支保护的必需检查名，否则 PR 会被判缺检查而无法合并。

## 提交前必须通过

```bash
dotnet build StreamFrame.slnx -c Release
dotnet test StreamFrame.slnx -c Release --no-build
```

## 发版

见 [docs/PUBLISHING.md](docs/PUBLISHING.md)：打 `v*` tag 触发 `release.yml` 单条流水线
（版本校验 → 构建 → 测试 → Release → 推 nuget.org）；流水线强制校验 tag 与两个 csproj
的 `<Version>` 一致，发版前先同步版本号。
