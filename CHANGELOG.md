# Changelog

本项目版本变更记录，格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 新增
- 暂无

### 修复
- 暂无

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

[Unreleased]: https://github.com/CSJ608/StreamFrame/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/CSJ608/StreamFrame/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/CSJ608/StreamFrame/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/CSJ608/StreamFrame/releases/tag/v1.0.0
