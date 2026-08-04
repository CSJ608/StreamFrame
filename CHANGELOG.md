# Changelog

本项目版本变更记录，格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 新增
- 暂无

### 修复
- 暂无

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
