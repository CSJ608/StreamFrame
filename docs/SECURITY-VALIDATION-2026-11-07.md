# 依赖安全修复验证（2026-11-07）

## 修复范围

- SDK/发布 SDK 10.0.402 → 10.0.403；NativeAOT ILCompiler、ILLink 和 win/linux 编译器锁定为 10.0.14，消除 GHSA-p3jp-9v83-76w9。
- 删除 coverlet.collector 10.1.0，改用 Test.Sdk 18.11.0 已包含的 Microsoft.CodeCoverage 18.11.0；不再携带其 Pkcs 10.0.12，消除 GHSA-846f-f764-76mx、GHSA-2fmv-rv2c-59x7。
- 删除 Issue #107 的精确延期参数、常量和分支；NuGet 与交叉审计均对所有严重级别阻断。
- 核验 Microsoft.CodeCoverage 官方包签名（Microsoft 作者与 nuget.org 仓库签名通过），解压原始 nupkg 后逐项核对内嵌二进制与清单摘要一致。清单收录 9 个内嵌 NuGet 组件版本；工具自身/原生二进制按 CodeCoverage 包版本跟踪。
- 生产库 API 与五个运行时 PackageReference 保持不变；此次漏洞来自构建与覆盖率工具。

## 本机验证

使用 SHA512 与微软官方发布元数据一致的便携 SDK 10.0.403、运行时 10.0.14/8.0.33，未替换系统 SDK。

- 普通及 EnableCoverage=true 锁定 restore：成功，无漏洞告警。
- 普通及覆盖率完整审计：6 个项目、73 个包版本，NuGet 与 GitHub reviewed 公告无命中，包含工具内嵌依赖。
- `dotnet build StreamFrame.slnx -c Release`：0 警告、0 错误。
- `dotnet test StreamFrame.slnx -c Release --no-build`：net8/net10 各通过 180、跳过 3；net48 通过 172、跳过 3；跳过项为显式启用的长时 Soak。
- Python 审计/覆盖率验证：15 项通过，包含摘要篡改、新增二进制、缺失报告、审计 API 失败和全部严重级别的拒绝检查。
- 工作流引用的 7 个 Actions 包版本经 GitHub reviewed 公告查询无命中。

- Windows 覆盖率：net8/net10 各通过 180、跳过 3；两个生产程序集均出现在报告中，无测试程序集混入。
- net8 行覆盖率 91.8%（936/1020）、分支 87.2%；net10 行覆盖率 91.2%（925/1014）、分支 86.5%。摘要验证通过；采集器口径与旧 Coverlet 不直接比较。
- 跨平台 CI（含 Linux NativeAOT 发布、成功/失败退出码冒烟和覆盖率上传）由本修复 PR 的检查记录提供；合并前必须全部通过。

## 边界

以上是当前公告源下的已知依赖漏洞检查，不等于证明不存在未知漏洞或对生产部署进行扫描。
框架依赖部署必须更新宿主运行时，自包含/NativeAOT 产物必须重新发布。
远端 Dependabot alerts API 返回“此仓库未启用 alerts”（403），本次结果来自直接查询 NuGet/GitHub 公告，不能将远端告警列表当作无漏洞证据。
