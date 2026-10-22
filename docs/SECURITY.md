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
高危/严重告警、结果缺失或审计服务错误都不能当成通过。
类库锁文件保证本仓库构建可复现，不强制消费者使用相同的传递依赖。

更新依赖后须更新并审核普通与覆盖率两套锁文件；NativeAOT 还要覆盖实际发布 RID。
不能把已有 obj/bin 的旧元数据当作当前包内容，发布前检查实际生成的两个 nupkg。

## 覆盖率工具的已知未解决风险

截至 2026-10-22，`coverlet.collector 10.1.0` 是最新稳定版，但其包内携带
`System.Security.Cryptography.Pkcs 10.0.12`，落在以下受影响版本范围：

- [CVE-2026-84008 / GHSA-846f-f764-76mx](https://github.com/advisories/GHSA-846f-f764-76mx)：高危，SignedCms 签名检查资源消耗。
- [CVE-2026-70162 / GHSA-2fmv-rv2c-59x7](https://github.com/advisories/GHSA-2fmv-rv2c-59x7)：中危，Windows EnvelopedCms 解码资源消耗。

Pkcs 修复版本为 10.0.13。本项目和已检查的 coverlet 源码未发现对应 API 的直接调用，
但这不等于证明所有工具加载路径都不可达。普通 NuGet 图不包含这类内嵌组件。
仅在测试项目额外引用新版 Pkcs 不能保证替换 collector 自带 DLL。

临时措施：collector 仅由 `EnableCoverage=true` 引入，在独立低权限覆盖率 job 中执行；
普通测试、CodeQL 和生产打包默认不引用 collector，发布 job 不构建任何项目。
覆盖率 job 仍生成报告，随后审计其内嵌依赖；高危告警仍使该 job 失败，没有自动豁免。
**隔离不是漏洞修复，也不是风险接受审批。** 在上游更新并核验实际包内组件后才能关闭该项。

本地覆盖率运行需在 restore、build、test 三步保持同一属性：

```bash
EnableCoverage=true dotnet restore StreamFrame.slnx --locked-mode
EnableCoverage=true dotnet build test/StreamFrame.Tests/StreamFrame.Tests.csproj -c Release --no-restore
EnableCoverage=true dotnet test test/StreamFrame.Tests/StreamFrame.Tests.csproj -c Release --no-build -f net10.0 --collect:"XPlat Code Coverage"
```

## 发布权限与验证

Actions 固定到核验过的完整提交 SHA，由 Dependabot 维护。
测试和构建打包 job 只有读取权限；发布 job 只下载同一次运行的包、核对摘要和提交，随后换取短时 NuGet 凭据。
SHA256 摘要提供产物完整性校验，不等价于独立来源证明或第三方签名。
保留 `release.yml` 和现有 tag OIDC 身份，不擅自改变 environment/Trusted Publisher 配置。

主分支规则、Dependabot alerts、tag 保护属于远端设置，必须单独核实；配置了版本更新不代表安全告警已开启。
涉及连接实现的发布还须满足 `docs/SOAK.md` 的完整长测要求，普通测试或短测不能替代。
