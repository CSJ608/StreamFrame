# 发布指南

StreamFrame 的发布流程：打 `v*` 标签 → GitHub Actions 自动构建、建 GitHub Release、推 nuget.org。

## 一次性的前置配置

### 1. nuget.org 注册 Trusted Publisher（浏览器，一次）
登录 nuget.org → Account → API Keys → **Trusted Publishers** 标签 → Register Publisher：
- **GitHub Account**: `CSJ608`
- **GitHub Repository**: `StreamFrame`
- **Environment**: 留空
- **Subject Identifier**: `repo:CSJ608/StreamFrame:ref:refs/tags/v*`

> 该 subject 仅信任以 `v` 开头的 tag 推送，防止任意分支推送覆盖包。若为个人账号，nuget.org 会要求验证仓库所有权。

### 2. GitHub 仓库变量 `NUGET_USER`（浏览器，一次）
仓库 → Settings → Secrets and variables → Actions → **Variables** → New repository variable：
- **Name**: `NUGET_USER`
- **Value**: 你的 nuget.org 用户名

## 日常发布流程（打 tag 即发布）

```bash
# 1. 提交改动并推送
git add -A && git commit -m "..." && git push origin main

# 2. 打版本标签并推送 → 触发 release + publish-nuget 两个 workflow
git tag -a v1.0.1 -m "描述"
git push origin v1.0.1

# 3. 查看两个 workflow 结果
gh run list --workflow release.yml
gh run list --workflow publish-nuget.yml
```

## 两个 workflow 各做什么

| Workflow | 触发 | 作用 |
|---|---|---|
| `release.yml` | `push tag v*` | build + test + pack，创建 GitHub Release 并附 `*.nupkg` / `*.snupkg`；Release 正文从 CHANGELOG 提取当前版本段 |
| `publish-nuget.yml` | `push tag v*` | build + pack，`NuGet/login@v1` 用 OIDC 换短时 API key，推 nuget.org（`--skip-duplicate` 幂等） |

## 版本号约定

- csproj 中 `<Version>` 与 git tag 需一致（如 `1.0.0` ↔ `v1.0.0`），下次发布手动同步。
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
