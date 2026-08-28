<p align="center">
  <img src="https://raw.githubusercontent.com/CSJ608/StreamFrame/main/docs/logo/icon-512.png" width="120" alt="StreamFrame logo" />
</p>

# StreamFrame

[![CI](https://github.com/CSJ608/StreamFrame/actions/workflows/ci.yml/badge.svg)](https://github.com/CSJ608/StreamFrame/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/StreamFrame.svg)](https://www.nuget.org/packages/StreamFrame)
[![NuGet Downloads](https://img.shields.io/nuget/dt/StreamFrame)](https://www.nuget.org/stats/packages/StreamFrame?groupby=Version)

[English](README.en.md) | 简体中文

**通用 socket 通讯框架**：把"帧边界判定"与"帧内数据编解码"插件化，适用于通过 socket 进行数据互换的场景（设备通讯、物流 WMS 对接等）。把场景间的共性（连接管理、重连、读写、粘包/半包、消息分发）收敛到框架核心，把差异性（framing、codec）抽象成可插拔的驱动。

## 为什么用它

| | 传统手写 | StreamFrame |
|---|---|---|
| 接一种新设备/协议 | 重写连接、切帧、编解码 | 只写一个 `ICodec<T>` 驱动 |
| 帧边界（长度前缀 / STX-ETX / 自定义） | 各写各的 | 内置两种 + 可插拔 |
| 粘包/半包 | 手写缓冲拼接 | Pipelines 自动处理 |
| 重连 / 状态机 | 手写 | 内置自动重连 |
| 发送性能 | 多次拷贝 | 可选流式零拷贝 |

## 安装

```
dotnet add package StreamFrame
```

XML 报文驱动（可选）：

```
dotnet add package StreamFrame.Protocols.Xml
```

## 快速上手

一条连接 = 一个**帧定界策略**（framing）+ 一个**编解码器**（codec）+ 地址/端口/模式。framing 与 codec 均**连接级固定**，一条连接只流通一种消息类型、一种帧格式。

```csharp
using System.Net;
using System.Xml.Linq;
using StreamFrame;
using StreamFrame.Protocols.Xml;

// 服务端（被动监听）
var server = new StreamConnection<XDocument>(
    new LengthPrefixFramer(),              // 4 字节大端长度头；也可用 StxEtxFramer
    new XmlDocumentCodec(),                // 换成自己的 ICodec<T> 即可支持自定义协议
    IPAddress.Any, 5100, isActive: false);
server.Start(ct);

await foreach (var doc in server.GetMessages(ct))
{
    var id = doc.Root?.Element("Id")?.Value;
    await server.SendAsync(XDocument.Parse($"<Reply><Echo>{id}</Echo></Reply>"), ct);
}

// 客户端（主动连接）
var client = new StreamConnection<XDocument>(
    new LengthPrefixFramer(),
    new XmlDocumentCodec(),
    IPAddress.Parse("127.0.0.1"), 5100, isActive: true);
client.Start(ct);
await client.SendAsync(XDocument.Parse("<Message><Id>1</Id></Message>"), ct);
```

## 概念

### 帧定界（IFramer）— 怎么从字节流里切出一帧

| 实现 | 定界方式 | 适用 |
|---|---|---|
| `LengthPrefixFramer` | 4 字节**大端**长度头 + 负载 | 通用，二进制安全 |
| `StxEtxFramer` | STX `0x02` … ETX `0x03` 包裹 | XML / 纯文本等已知安全的负载 |

两种默认实现都支持**流式单缓冲编码**（可选，消除发送侧 memcpy）。想自定义帧格式时实现 `IFramer` 即可。

### 编解码（ICodec&lt;T&gt;）— 怎么解析/写入帧内数据

```csharp
public interface ICodec<TMessage>
{
    TMessage Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default);
    void Encode(TMessage message, IBufferWriter<byte> writer, CancellationToken ct = default);
}
```

**接新设备 = 写一个驱动**：实现 `ICodec<TMessage>` + 定义业务消息类，选一个帧策略，其余全部复用。官方示例见 `StreamFrame.Protocols.Xml`。

### 连接（IStreamConnection&lt;T&gt;）— 传输层

- **客户端/服务端双模式**：`isActive: true` 主动连远端，`false` 被动监听；IPv4/IPv6 双栈（监听 `IPAddress.Any` 自动按双栈处理，IPv4 客户端地址归一显示为 IPv4）
- **自动重连**：`Connecting → Connected → Retry` 状态机；可选指数退避（`MaxRetryDelayMs`，连续失败倍增封顶 + ±20% 抖动，连接成功自动复位）；`GetMessages` 是跨重连的稳定消息流——断线重连后已收消息不丢、枚举不中断
- **启动与停止**：`Start(ct)` 的 `ct` 是连接的**生命周期令牌**——取消它会停止连接/重连并拆线（状态进入 `Disconnected` 终态，`GetMessages` 自然结束，之后需新建连接）；`DisposeAsync` 与之等效。停机后 `SendAsync` 抛 `ChannelClosedException`
- **等待连接就绪**：`await conn.WaitForConnectedAsync(ct)`——已连接立即完成，未连接时等到下次连接成功（或取消/Dispose），不用轮询状态、不用 `Task.Delay` 盲等
- **健壮性**：帧内容解码失败、未完成帧超限/超时、发送失败、接收空闲超时都会判定会话失效并自动重建（不再产生"连接看似存活、消息静默消失"的假活）
- **活性探测（可选）**：TCP KeepAlive 与接收空闲超时，兜底半开连接（对端断电/拔线）
- **事件**：`ConnectionChanged` 状态变化、`FrameError` 帧层诊断、`RawBytesReceived/Sent` 原始字节（HEX 调试）
- **发送背压**：有界发送队列，队列满时 `SendAsync` 自动等待；接收侧默认无上限缓冲，可用 `ReceiveQueueCapacity` 设上限——消费慢时解码暂停、TCP 背压自然传导到对端，防内存无限增长

## 诊断与调试

### FrameError — 帧层诊断事件

对端发来坏数据时，`FrameError` 事件把**出问题的字节**和**原因**直接交给上层，不用再拿 HEX 流人工对齐：

```csharp
client.FrameError += (_, e) =>
{
    // e.Kind: DecodeFailed（帧完整但内容解析失败）
    //         DiscardedByResync（被定界器当作噪声丢弃的字节）
    //         IncompleteFrameOverflow（未完成帧缓冲超限）
    //         IncompleteFrameTimeout（未完成帧超时：半帧迟迟收不齐）
    // e.Bytes: 已拷贝，可安全长期留存
    // e.Exception: DecodeFailed 时的原始异常
    Console.WriteLine($"[{e.Kind}] {Convert.ToHexString(e.Bytes.Span)} {e.Exception?.Message}");
};
```

帧内容解码失败的策略由 `StreamConnectionOptions.DecodeErrorPolicy` 决定：

| 策略 | 行为 |
|---|---|
| `Disconnect`（默认） | 断线重连——协议内容错乱后流状态通常不可信 |
| `SkipFrame` | 丢弃坏帧继续，适合噪声多的线路 |

### RawBytesReceived / RawBytesSent — 原始字节流

socket 层全量输出（含被丢弃的噪声字节），发送侧按实际写出的分片回调（部分发送失败时已上线字节也可见）。**内存契约**：回调参数是内部缓冲的切片，仅在回调同步执行期间有效，需要留存必须自行拷贝；回调抛异常会被隔离，不影响会话。

### 未完成帧防护

对端声明一个超长帧却永远不补齐（或 STX/ETX 流中只有 STX 没有闭合），会无限占用缓冲。两条互补的防线：

- `MaxIncompleteFrameBufferBytes`（默认 = 帧上限 + 4KB）——**字节**上限：半帧超过即断线，防内存攻击；
- `IncompleteFrameTimeoutMs`（默认 0 = 关闭）——**时间**上限：半帧开始后连续这么久收不到后续字节即断线，`FrameError` 上报 `IncompleteFrameTimeout` 并携带受 8KB 上限保护的缓冲快照。

未完成帧超时只计"帧已开头、迟迟收不齐"的时间：缓冲为空的静默连接不计时（收到字节即重置，整帧切尽后归零）。它与 `ReceiveIdleTimeoutMs`（见下节）互补——后者在完全没流量时也计时，适合有周期报文的协议；对允许长时间空闲、但半帧卡死必须判死的协议（如 HSMS T8），用未完成帧超时。

### 日志（可选）

构造连接时传入 `ILogger`，内部事件（连接重试、会话故障、用户回调异常等）输出到日志，生产环境不再静默：

```csharp
var conn = new StreamConnection<XDocument>(..., logger: loggerFactory.CreateLogger("StreamFrame"));
```

不传则无日志输出（零依赖可用）。

### 活性探测与心跳范式

生产环境建议开启 `TcpKeepAlive = true`；应用层心跳配合 `ReceiveIdleTimeoutMs`（取心跳周期的 3 倍，容忍偶尔丢 1-2 次）是更强的组合——框架不内置心跳（消息形态由协议决定），范式如下，完整可运行示例见 demo 场景 4：

```csharp
var options = new StreamConnectionOptions { ReceiveIdleTimeoutMs = 1500 };

// 一侧周期发心跳；另一侧收到任何消息回 PONG（双向都有字节即可重置双方空闲计时）
_ = Task.Run(async () =>
{
    while (!cts.IsCancellationRequested)
    {
        await client.SendAsync("PING", cts.Token);
        await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token); // 周期 ≈ 超时的 1/3
    }
});
```

对端"猝死"（断电/拔线，无 FIN/RST）时静默超限 → 会话判定死亡 → 自动重连；对端不可达时重连持续失败，状态停留在 `Connecting`/`Retry`。

两种接收超时的取舍：`ReceiveIdleTimeoutMs` 要求连接**必须有周期流量**——协议有心跳/周期上报时用它（还能兜底半开连接）；协议允许长时间静默（只在有帧进行中才该有流量）时改用 `IncompleteFrameTimeoutMs`，静默不算故障、半帧卡死才判死。

## 性能

BenchmarkDotNet 实测（详见 [bench/README.md](bench/README.md)，可本地复现）：

- **流式编码**：每帧堆分配减半（单缓冲 vs 双缓冲），小负载耗时快 25–35%，大负载持平；
- **切帧吞吐**：`LengthPrefixFramer` ≈10 ns/帧，`StxEtxFramer` ≈1.1 µs/帧（逐字节扫描）——高吞吐场景优先长度前缀；
- **端到端**（真实 TCP 回环）：单向吞吐 ≈24 万条/秒（1KB 消息，LengthPrefix），往返延迟 ≈63 µs；XML codec 每条报文 2–16 µs（400B–4KB）——序列化开销远大于定界层。

## 支持框架

| 包目标 | 运行环境 |
|---|---|
| `net10.0`（推荐，LTS 至 2028-11） | .NET 10 |
| `net8.0`（LTS 至 2026-11） | .NET 8 |
| `netstandard2.0` | .NET Framework 4.6.2+、Unity、Mono 等 |

`netstandard2.0` 资产经 **net48 全量测试套件**（真实 TCP 回环）验证；TCP KeepAlive 参数在 .NET Framework 上通过 `SIO_KEEPALIVE_VALS` 设置。CI 在 Ubuntu（net8/net10）与 Windows（net48）双平台运行全部测试。

## 依赖

- [System.IO.Pipelines](https://www.nuget.org/packages/System.IO.Pipelines)
- [System.Threading.Channels](https://www.nuget.org/packages/System.Threading.Channels)
- [Microsoft.Extensions.Logging.Abstractions](https://www.nuget.org/packages/Microsoft.Extensions.Logging.Abstractions)（仅日志抽象，不引入具体日志框架）

## 测试与示例

```bash
dotnet build StreamFrame.slnx
dotnet test
dotnet run --project samples/StreamFrame.Demo          # 四场景端到端 demo
dotnet run -c Release --project bench/StreamFrame.Benchmarks   # 性能基准（约 5-10 分钟）
dotnet test -f net8.0 --collect:"XPlat Code Coverage"  # 覆盖率（CI 亦自动收集并写入运行摘要）
```

## 项目结构

```
src/StreamFrame/                  # 核心库（无业务依赖）
src/StreamFrame.Protocols.Xml/    # XML 报文驱动（示例 codec）
test/StreamFrame.Tests/           # xUnit 单测
samples/StreamFrame.Demo/         # 控制台端到端 demo
bench/StreamFrame.Benchmarks/     # 性能基准
```

## 许可

MIT
