# 发布指南

StreamFrame 的发布流程：打 `v*` 标签 → GitHub Actions **单条流水线**（`release.yml`）自动校验版本、构建、测试、创建 GitHub Release、推 nuget.org。测试未通过不会发包。

## 一次性的前置配置

### 1. nuget.org 注册 Trusted Publisher（浏览器，一次）
登录 nuget.org → Account → API Keys → **Trusted Publishers** 标签 → Register Publisher：
- **GitHub Account**: `CSJ608`
- **GitHub Repository**: `StreamFrame`
- **Workflow**: `release.yml`
- **Environment**: 留空
- **Subject Identifier**: `repo:CSJ608/StreamFrame:ref:refs/tags/v*`

> 该 subject 仅信任以 `v` 开头的 tag 推送，防止任意分支推送覆盖包。若为个人账号，nuget.org 会要求验证仓库所有权。
>
> **注意：策略绑定 workflow 文件名。** OIDC 令牌携带 workflow 路径，改名/合并 workflow 后必须同步编辑 nuget.org 上的 Trusted Publisher 策略（v2.0.0 发布时曾因策略仍指向旧的 `publish-nuget.yml` 而在 `NuGet Login` 步骤 401）。流水线其余步骤不受影响，修正策略后 Re-run 失败的运行即可完成推送（`--skip-duplicate` 幂等）。

### 2. GitHub 仓库变量 `NUGET_USER`（浏览器，一次）
仓库 → Settings → Secrets and variables → Actions → **Variables** → New repository variable：
- **Name**: `NUGET_USER`
- **Value**: 你的 nuget.org 用户名

## 日常发布流程（打 tag 即发布）

```bash
# 1. 从最新 main 建主题分支，提交、推送分支并开 PR
# PR 通过必需检查后 rebase 合并；禁止直接推 main。
# 发布前另以 PR 同步两个 csproj 版本并归档 CHANGELOG，确认长测与安全门禁。

# 2. 获得发版授权后，在已合并且校验过的提交打标签（示例版本）
git switch main
git pull --ff-only origin main
git tag -a v2.7.1 -m "描述"
git push origin v2.7.1

# 3. 查看流水线结果（发布与推送在同一条运行里）
gh run list --workflow release.yml
```

> 忘了同步 csproj 版本？打包 job 的版本校验会失败并提示哪个文件不一致，不会发出错误版本号的包。

## 发布流水线（release.yml）各阶段做什么

| 阶段 | 说明 |
|---|---|
| 版本校验 | tag 与两个 csproj 的 `<Version>` 必须一致，否则直接失败 |
| 构建 + 测试 | 只读 job 做锁定恢复、安全审计和五个 OS/TFM 组合测试；测试失败不打包发布 |
| 打包 | 独立只读 job 仅构建两个生产项目，生成包、变更说明、提交标识与 SHA256 摘要；上传本次运行的唯一产物 |
| GitHub Release | 发布 job 下载同一次运行的包并校验提交/摘要，附 `*.nupkg` / `*.snupkg` / 摘要；不执行 restore/build/test |
| 推 nuget.org | 仅发布 job 有 `contents: write` / `id-token: write`；固定 SHA 的 `NuGet/login` 用 OIDC 换短时 API key，推送 `--skip-duplicate` 幂等 |

`workflow_dispatch` 手动触发执行测试、审计和打包演练，不创建 Release、不发包（OIDC subject 只信任 tag ref）。产物名包含运行尝试号；需要重试时选择 **Re-run all jobs** 重新生成同一次尝试的产物，不仅重跑下载产物的发布 job。

SDK 基线、依赖锁、覆盖率工具内嵌组件的未解决风险及部署边界见 [安全维护说明](SECURITY.md)。现有 NuGet Trusted Publisher 的 workflow/environment 配置不变；增加发布 environment 前必须单独同步其身份策略。

## 版本号约定

- csproj 中 `<Version>` 与 git tag 需一致（如 `1.0.0` ↔ `v1.0.0`），下次发布手动同步；流水线第一步强制校验。
- 两个库（`StreamFrame`、`StreamFrame.Protocols.Xml`）当前都发布为同一版本。

## CHANGELOG 约定

维护 [CHANGELOG.md](../CHANGELOG.md)（[Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 格式）：
- **每次改动**：把变更记录到 `## [Unreleased]` 段，按 `新增` / `修复` / `变更` 分类，条目附关联 issue 链接（`[#N](https://github.com/CSJ608/StreamFrame/issues/N)`）。
- **发版时**：`release.yml` 自动从 CHANGELOG 提取当前 tag 对应的版本段落作为 Release 正文（找不到版本段时回退到 GitHub 自动生成）。发版后把 Unreleased 内容归档为正式版本段落，并更新段落末尾的版本对比链接。

### Issue → 修复 → CHANGELOG 工作流

发现 bug 或想加功能时的建议流程：
1. 用 `gh issue create` 建 issue（附复现步骤/预期行为）。
2. 修复提交的 commit message 写 `Fixes #N` → GitHub 自动关闭 issue 并建立关联。
3. 同步更新 CHANGELOG 的 Unreleased 段，条目带 issue 链接。
4. 发版时该条目随版本段落进入 Release 说明。

## 手动验证（CI 之外的本地兜底）

```bash
dotnet build StreamFrame.slnx -c Release
dotnet test StreamFrame.slnx -c Release
dotnet pack src/StreamFrame/StreamFrame.csproj -c Release -o artifacts
dotnet pack src/StreamFrame.Protocols.Xml/StreamFrame.Protocols.Xml.csproj -c Release -o artifacts
```

## 回退方案：手动 API Key 推送

若不想用 Trusted Publishing，可在 nuget.org 生成 API Key 后手动推：
```bash
dotnet nuget push artifacts/StreamFrame.1.0.0.nupkg \
  --api-key <KEY> --source https://api.nuget.org/v3/index.json
```
（NuGet 官方建议使用 Trusted Publishing 而非长期 API Key。）
