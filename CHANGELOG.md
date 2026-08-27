# Changelog

本项目版本变更记录，格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 破坏性变更（将随 2.0.0 发布）
- **命名统一，消灭两个"codec"的混淆**：帧定界侧改名——`IFrameCodec`→`IFramer`、`IStreamingFrameCodec`→`IStreamingFramer`、`LengthPrefixFrameCodec`→`LengthPrefixFramer`、`StxEtxFrameCodec`→`StxEtxFramer`；"codec"一词从此只指帧内编解码 `ICodec<TMessage>`。
- **命名空间收敛**：删除 `StreamFrame.Abstractions`，所有公共类型统一进根命名空间 `StreamFrame`——用户只需一个 `using StreamFrame;`（此前具体实现类住在 "Abstractions" 命名空间里，名不副实）。
- **`Start(ct)` 的 `ct` 升级为生命周期令牌**：取消它会停止连接/重连并拆线（`Disconnected` 终态、`GetMessages` 自然结束、之后需新建连接），与 `DisposeAsync` 共用同一条幂等停机路径。1.x 中取消已连接的连接毫无作用且未如实文档化。
- **`ConnectionState` 新增 `Disconnected` 终态**：停机时广播；1.x 取消/停机后状态停留在 `Connecting`。
- **`DeviceIpAddress`→`RemoteIpAddress`（`string?`）**：语义修正——主动模式为配置的远端地址，被动模式为已连接客户端地址（未连接为 `null`，不再返回魔法值 `"NA"`）；双栈下 IPv4 客户端地址归一显示为 IPv4。
- **`IStreamConnection<TMessage>` 新增 `WaitForConnectedAsync`**（接口实现者需适配；此前 1.2.0 的 `FrameError` 已属破坏，一并归入 2.0）。
- **socket 改为 IPv6 双栈**（同一 socket 同时支持 IPv4/IPv6；监听 `IPAddress.Any` 自动按双栈处理）。1.x 仅 IPv4。

### 修复
- 主动模式连接失败重试不再泄漏 socket：`ConnectAsync` 失败/取消时立即释放本次尝试创建的 socket（此前每次失败重试泄漏一个，等终结器回收）。
- CHANGELOG 底部版本对比链接补齐 `[1.2.0]` 并修正 `[Unreleased]` 指向（v1.2.0 发版时漏更新）。

### 变更
- `Start` 重入防护：重复调用抛 `InvalidOperationException`（此前并发/重复 Start 会各自建立 socket 并互相覆盖，泄漏连接）；重建连接请用 `Reconnect()`。
- 发布管线合并为单条流水线 `release.yml`：tag 触发"版本号校验 → 构建 → 测试 → GitHub Release → 推 nuget.org"，测试未通过不会发包（此前发布与推送是两条独立 workflow，测试失败时包仍会被推出）。`workflow_dispatch` 手动触发仅做构建+测试演练，不发布；需重发版本时在对应 tag 的运行记录上 Re-run（推送带 `--skip-duplicate`，幂等）。
- tag 与两个 csproj 的 `<Version>` 一致性在流水线中显式校验，不一致直接失败（此前会静默发出错误版本号的包）。
- 明确 `Start(ct)` 的取消令牌语义（仅约束建立连接阶段）：接口 XML 注释与 README 同步说明。

### 新增
- 代码质量棘轮：引入 Meziantou.Analyzer（SDK 自带 NetAnalyzers 之上的补充规则）与 `.editorconfig`，现存告警清零并把库项目的 `TreatWarningsAsErrors` 打开（告警即构建失败）；`FrameErrorKind`/`DecodeErrorPolicy` 拆分为独立文件（一类型一文件）。
- `WaitForConnectedAsync`：等待连接进入 Connected（已连接立即完成；取消或 Dispose 以取消结束），替代轮询状态或 `Task.Delay` 盲等；demo 全部改用该 API。
- 可选 `ILogger` 构造参数：连接重试、会话故障、用户回调异常等内部事件输出到日志（此前 `Debug.WriteLine` 在 Release 构建中完全不可见，生产排障只能靠猜）。
- `ReceiveQueueCapacity` 选项：接收消息通道可设上限（默认 0 不限制）；消费慢时解码暂停读取、TCP 背压自然传导到对端，封堵慢消费者导致的内存无限增长。
- 选项构造校验：非法取值（负延迟、0 队列容量等）在构造时抛清晰的 `ArgumentOutOfRangeException`（此前在运行中的重连/收发路径深处抛难定位的异常）。
- LICENSE 文件（MIT 此前只在 README 与包元数据中声明，仓库内无正文）。
- push / PR 持续集成 `ci.yml`（构建 + 测试），回归不再等到发版才暴露。

## [1.2.0] - 2026-08-27

### 修复
- **会话假活（严重）**：消息通道不再被解码循环在会话停止时关闭——此前任何一次断线重连（或一条解码失败的报文）都会永久关闭通道，之后连接看似健康（Connected、字节仍在收发），业务消息却永远不再送达。`GetMessages` 现在是跨重连的稳定消息流，仅在 `DisposeAsync` 后正常结束。
- **已收消息不再丢失**：对端"发完数据立即断开"时，解码循环退出前会投递所有已缓冲的完整帧（此前会话停止的取消可能抢在投递之前）。
- **会话拆除不再泄漏旧 socket**：每次重连都会关闭上一个连接的 socket（此前等终结器收场）。
- **迟到的过期故障不再误杀新会话**：会话故障的重连按会话编号（epoch）判定是否仍有效。
- **发送失败不再静默**：发送 worker 遇到 socket 故障会上抛并触发重连（此前只吞异常退出，队列随后塞满、`SendAsync` 永久阻塞）。
- **用户回调异常全部隔离**：`ConnectionChanged` / `RawBytesReceived` / `RawBytesSent` / `FrameError` 的处理器抛异常不再中断状态机或拆会话（此前 `RawBytesReceived` 抛异常会引发重连风暴）。

### 变更
- 帧内容解码失败（codec 抛异常）默认断线重连（此前解码循环静默死亡、连接假活）；可用 `DecodeErrorPolicy.SkipFrame` 改为丢弃坏帧继续。
- `GetMessages` 不再因会话中断/解码失败向枚举方抛异常（错误改经 `FrameError` 事件上报）。
- `RawBytesSent` 改为按 socket 实际写出的分片回调（此前整帧成功才回调，部分发送不可见）。
- `IStreamConnection<TMessage>` 新增 `FrameError` 事件（自实现该接口的第三方驱动需适配）。

### 新增
- `FrameError` 帧层诊断事件：解码失败、被定界器丢弃的噪声字节、未完成帧超限，均携带已拷贝字节（可安全留存）与原因。
- `IFrameDiscardReporting` 可选接口：定界器精确上报重同步丢弃的字节；`LengthPrefixFrameCodec` / `StxEtxFrameCodec` 已实现。
- `MaxIncompleteFrameBufferBytes` 选项：未完成帧缓冲硬上限（默认 = 帧上限 + 4KB），封堵"声明超长帧永不补齐 / 只有 STX 没有 ETX"的无界内存占用。
- 活性探测选项：`TcpKeepAlive` + `KeepAliveTimeMs` / `KeepAliveIntervalMs`、`ReceiveIdleTimeoutMs`（默认全部关闭，行为与 1.1.0 一致）。
- 连接端到端测试 7 项 + 解码循环测试 4 项（重连送达、解码失败双策略、未完成帧超限、丢弃上报、钩子异常隔离、空闲超时、发送失败重连）。

## [1.1.0] - 2026-08-04

### 变更
- 被动模式新增 `AcceptFirstClientOnly` 选项（默认 `true`）：accept 到第一个客户端后关闭监听 socket，后续连接在 TCP 层被立即拒绝，保证单客户端语义与参考实现一致。

### 新增
- 单客户端连接行为测试：第二个客户端被拒绝、第一个客户端可正常收发数据、开关关闭时保持监听。

## [1.0.1] - 2026-08-04

### 变更
- README 补全项目介绍并打包进 NuGet（`PackageReadmeFile`），nuget.org 包页展示完整说明。

## [1.0.0] - 2026-08-03

首个正式发布。

### 新增
- 核心库 `StreamFrame`：通用 socket 通讯框架，帧边界判定（`IFrameCodec`）与帧内编解码（`ICodec<TMessage>`）插件化。
  - `LengthPrefixFrameCodec`：4 字节大端长度头定界。
  - `StxEtxFrameCodec`：STX/ETX 成对包裹定界。
  - `StreamConnection<TMessage>`：客户端/服务端双模式、自动重连、粘包/半包处理、有界发送队列背压。
  - 可选流式单缓冲编码（`IStreamingFrameCodec`），消除发送侧 memcpy。
  - `RawBytesReceived` / `RawBytesSent` 原始字节调试事件。
- 示例驱动 `StreamFrame.Protocols.Xml`：`XmlDocumentCodec`（含 DTD 拒绝的 XXE 防御）。
- 发布自动化：`release.yml`（tag 自动建 GitHub Release）、`publish-nuget.yml`（OIDC Trusted Publishing 推送 nuget.org）。
- 单元测试 25 个 + 三场景端到端 demo。

[Unreleased]: https://github.com/CSJ608/StreamFrame/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/CSJ608/StreamFrame/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/CSJ608/StreamFrame/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/CSJ608/StreamFrame/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/CSJ608/StreamFrame/releases/tag/v1.0.0
