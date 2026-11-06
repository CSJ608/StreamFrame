# 安全维护与部署边界

## 受支持的运行环境

生产库继续提供 `netstandard2.0`、`net8.0`、`net10.0` 资产；资产兼容不代表宿主运行时仍获得安全支持。
.NET 8/9 的支持期于 2026-11-10 结束，新宿主和示例优先使用 .NET 10 LTS。
.NET Framework 4.8 的补丁由受支持的 Windows 提供，ReferenceAssemblies NuGet 包只是编译引用。

开发 SDK 基线由根目录 `global.json` 管理，允许同 feature band 的更新补丁，不允许预览版。
安装基线或更高补丁后执行 `dotnet --info`，确认实际选择的 SDK 和运行时。
框架依赖部署更新宿主运行时；自包含和 NativeAOT 部署必须重新发布和替换产物。
本仓库不自动安装 SDK、修改开发机私有包源或重启生产程序。

## 连接安全配置

StreamFrame 是原始 TCP 通信库，不内置 TLS、设备身份认证或业务权限判断。
只应监听受访问控制的可信设备网络；跨信任域使用时，在部署层提供受认证的加密隧道和访问控制。
绑定 `IPAddress.Any` 还会接受 IPv6 流量，防火墙规则应同时覆盖 IPv4/IPv6。
TCP KeepAlive 只能发现部分失联情况，不能验证设备身份。
默认单客户端模式不是访问控制：先连接的非预期客户端同样能占住会话。

复用 `samples/StreamFrame.Ownership/ProductionExample.cs` 的有界配置，并按协议调整：

- 设置 `ReceiveQueueCapacity`、`SendQueueCapacity`，限制调用方同时等待发送的任务数。
- 收紧 Framer 的 `MaxPayloadBytes` 和 `MaxIncompleteFrameBufferBytes`；XML 业务还应显式设置 `MaxDocumentBytes`。
- 半帧超时是后续字节到达的间隔，不是整帧的绝对完成期限；持续缓慢输入仍可能占用连接。
- `ReceiveIdleTimeoutMs` 只适用于业务保证有周期流量的协议。允许正常长期静默时，不应盲目开启。
- 队列按消息数量而非字节计量，解码对象、管线缓冲及等待中的任务仍需纳入内存预算。
- `RawBytesReceived/Sent` 和 `FrameError` 快照可能包含业务秘密；日志应脱敏、限流并限制留存，默认不记录完整原始报文。

XML codec 禁止 DTD 并关闭外部解析器；字节限额不是解析深度或对象内存的硬上限。
新增深度/节点数限制、整帧绝对期限、原生 TLS 均需要独立兼容性设计，不通过补丁暗改公共默认值。

## 可复现恢复与审计

仓库 `NuGet.Config` 仅使用 nuget.org，并显式设置审计源；不继承用户的私有下载源。
所有项目启用直接及传递依赖审计。CI 使用锁定恢复：

```bash
dotnet restore StreamFrame.slnx --locked-mode -p:AuditPipeline=true
python build/security-audit.py
```

审计脚本验证六个项目的扫描结果，并用 GitHub Advisory Database 交叉核对解析的包版本。
运行交叉检查需要已认证的 `gh`；CI 使用只读 `GH_TOKEN`。
任何级别告警、结果缺失或审计服务错误都不能当成通过。
类库锁文件保证本仓库构建可复现，不强制消费者使用相同的传递依赖。

更新依赖后须更新并审核普通与覆盖率两套锁文件；NativeAOT 还要覆盖实际发布 RID。
不能把已有 obj/bin 的旧元数据当作当前包内容，发布前检查实际生成的两个 nupkg。

## 覆盖率工具与内嵌依赖

覆盖率使用测试 SDK 已包含的 `Microsoft.CodeCoverage 18.11.0`，输出 Cobertura；
已移除携带漏洞 Pkcs 的 `coverlet.collector 10.1.0`，不再保留 Issue #107 的告警延期。
`build/coverage.runsettings` 仅统计两个生产程序集，net8/net10 均采集并验证报告。
不同采集器的插桩与统计口径不同，替换前后的覆盖率百分比不直接比较。

Microsoft.CodeCoverage 不提供 deps.json。`build/codecoverage-components.json` 记录从官方
18.11.0 包实际 DLL 产品版本核验的内嵌 NuGet 组件（Mono.Cecil 三个 DLL 属于同一包），
以及全部 DLL/EXE/SO/DYLIB 的 SHA256；工具自身和原生二进制随 Microsoft.CodeCoverage 包版本审计。
普通与覆盖率审计均验证完整二进制文件集合和摘要，再将内嵌包版本加入 GitHub 公告查询。
包升级、文件增加/删除或摘要变化均失败，须重新盘点官方包后更新清单；不能只修改版本或忽略失败。
该检查覆盖已知 NuGet 公告，不等于对所有原生代码进行源码漏洞分析。

CI 对所有严重级别的已知依赖漏洞均失败，不保留豁免；审计接口失败或结果不完整也失败。
本地覆盖率运行需在 restore、build、test 和审计保持同一 `EnableCoverage=true` 属性
（PowerShell 使用 `$env:EnableCoverage='true'`）：

```bash
export EnableCoverage=true
dotnet restore StreamFrame.slnx --locked-mode -p:AuditPipeline=true
dotnet build test/StreamFrame.Tests/StreamFrame.Tests.csproj -c Release --no-restore
dotnet test test/StreamFrame.Tests/StreamFrame.Tests.csproj -c Release --no-build -f net10.0 --collect:"Code Coverage;Format=cobertura" --settings build/coverage.runsettings --results-directory coverage/net10.0
python build/security-audit.py --coverage
```

SDK 基线为 10.0.403，NativeAOT 编译器/运行时为 10.0.14，修复
[CVE-2026-111497](https://github.com/advisories/GHSA-p3jp-9v83-76w9)。
部署方还需更新实际宿主（.NET 8 至少 8.0.33，.NET 10 至少 10.0.14）；
升级项目 NuGet 引用不能替代更新宿主，已有自包含/AOT 产物需重新发布。

## 发布权限与验证

Actions 固定到核验过的完整提交 SHA，由 Dependabot 维护。
测试和构建打包 job 只有读取权限；发布 job 只下载同一次运行的包、核对摘要和提交，随后换取短时 NuGet 凭据。
SHA256 摘要提供产物完整性校验，不等价于独立来源证明或第三方签名。
保留 `release.yml` 和现有 tag OIDC 身份，不擅自改变 environment/Trusted Publisher 配置。

主分支规则、Dependabot alerts、tag 保护属于远端设置，必须单独核实；配置了版本更新不代表安全告警已开启。
涉及连接实现的发布还须满足 `docs/SOAK.md` 的完整长测要求，普通测试或短测不能替代。
