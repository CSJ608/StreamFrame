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

**接新设备 = 写一个驱动**：实现 `ICodec<TMessage>` + 定义业务消息类，选一个帧策略，其余全部复用。官方示例见 `StreamFrame.Protocols.Xml`。JSON 报文最简驱动（System.Text.Json，span 直写、AOT 安全）：

```csharp
sealed class SystemTextJsonCodec : ICodec<JsonElement>
{
    public static readonly SystemTextJsonCodec Instance = new();

    public JsonElement Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
        => JsonDocument.Parse(frame.ToArray()).RootElement;

    public void Encode(JsonElement message, IBufferWriter<byte> writer, CancellationToken ct = default)
    {
        var raw = message.GetRawText(); // 序列化产物（已转义的非 ASCII 安全直写）
        var span = writer.GetSpan(Encoding.UTF8.GetMaxByteCount(raw.Length));
        writer.Advance(Encoding.UTF8.GetBytes(raw, span));
    }
}
```

### 连接（IStreamConnection&lt;T&gt;）— 传输层

- **客户端/服务端双模式**：`isActive: true` 主动连远端，`false` 被动监听；IPv4/IPv6 双栈（监听 `IPAddress.Any` 自动按双栈处理，IPv4 客户端地址归一显示为 IPv4）
- **自动重连**：`Connecting → Connected → Retry` 状态机；可选指数退避（`MaxRetryDelayMs`，连续失败倍增封顶 + ±20% 抖动，连接成功自动复位）；`GetMessages` 是跨重连的稳定消息流——断线重连后已收消息不丢、枚举不中断
- **启动与停止**：`Start(ct)` 的 `ct` 是连接的**生命周期令牌**——取消它会停止连接/重连并拆线（状态进入 `Disconnected` 终态，`GetMessages` 自然结束，之后需新建连接）；`DisposeAsync` 与之等效。停机后 `SendAsync` 抛 `ChannelClosedException`
- **等待连接就绪**：`await conn.WaitForConnectedAsync(ct)`——已连接立即完成，未连接时等到下次连接成功（或取消/Dispose），不用轮询状态、不用 `Task.Delay` 盲等
- **健壮性**：帧内容解码失败、未完成帧超限/超时、发送失败、接收空闲超时都会判定会话失效并自动重建（不再产生"连接看似存活、消息静默消失"的假活）
- **活性探测（可选）**：TCP KeepAlive 与接收空闲超时，兜底半开连接（对端断电/拔线）
- **事件**：`ConnectionChanged` 状态变化、`FrameError` 帧层诊断、`RawBytesReceived/Sent` 原始字节（HEX 调试）
- **内置指标**：`System.Diagnostics.Metrics`（Meter `StreamFrame`）——帧/字节收发计数、重连次数、会话时长、发送队列水位，详见下文"内置指标"
- **发送背压**：有界发送队列，队列满时 `SendAsync` 自动等待；接收侧默认无上限缓冲，可用 `ReceiveQueueCapacity` 设上限——消费慢时解码暂停、TCP 背压自然传导到对端，防内存无限增长
- **会话感知收发（可选高级）**：`CurrentSessionId` / `SendInSessionAsync`（整帧写入 socket 才完成、会话失效即失败、绝不跨会话重放）/ `GetSessionMessages`（消息带会话编号）——为有严格会话边界的协议（如 HSMS）准备，详见下文

## 会话感知收发（高级）

对允许消息重放的一般业务，`SendAsync`（入队即完成、断线后由新会话续发）+ `GetMessages`（跨重连稳定流）就够了。部分协议有严格的会话边界——重连后必须重新握手、旧会话消息禁止重放、协议计时器要从"整帧实际写出"起算（HSMS 的 Select/T3/T6/T8 即是）。为此提供可选能力接口 `ISessionAwareStreamConnection<TMessage>`（`StreamConnection<TMessage>` 已实现；依赖接口抽象的上层用 `is` 探测）：

```csharp
if (connection is ISessionAwareStreamConnection<MyMessage> sessionAware)
{
    long id = sessionAware.CurrentSessionId;          // 每次 TCP 会话建立时分配，单调递增不复用；无会话时为 0

    // 整帧全部写入本机 socket 后才完成；会话在写出前终止 → SessionExpiredException，消息绝不重放
    await sessionAware.SendInSessionAsync(id, message, ct);

    // 接收视图：每条消息携带它所属的会话编号（旧会话解码任务迟到投递的消息带旧编号）
    await foreach (var m in sessionAware.GetSessionMessages(ct))
        Handle(m.SessionId, m.Message);
}
```

语义要点：

- **编号的线性化**：`ConnectionChanged` 回调与 `WaitForConnectedAsync` 完成时读 `CurrentSessionId` 必得有效值（分配先于 Connected 对外发布）；状态离开 Connected（Retry/Disconnected）可见时已归零。
- **"写完"的定义**：整帧字节已交给本机 socket（内核缓冲），不含对端 ACK——应用层可得的最好信号。任务失败时**远端处理结果视为未知**（可能已收到部分/全部字节），由上层协议的事务关联、幂等或恢复流程兜底。
- **调用方取消的提交点**：发送 worker 认领条目之前取消 → 任务取消且消息不再发送；认领之后（帧已开始写出）取消对结果无副作用——取消单条消息不会撕裂帧、不会杀死连接。
- **与 `SendAsync` 的关系**：两类发送共享同一条 FIFO（按入队顺序串行化）；普通 `SendAsync` 的跨会话续发行为不变。会话绑定发送在任何失败路径下都不转移到新会话。
- **两个接收 API 不是广播**：`GetMessages` 与 `GetSessionMessages` 是同一通道的两个竞争消费视图，同时枚举会互相分流——请二选一使用。

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
    // e.SessionId: 检测到错误的解码器所属会话编号（与 CurrentSessionId/
    //              SessionMessage.SessionId 同一编号空间；重连后迟到的
    //              旧会话事件仍带旧编号，不会被改写成当前会话）
    // e.ObservedByteCount / e.IsTruncated: 原始观测字节数与快照是否截断。
    //              timeout/overflow 快照有 8KB 上限，Bytes 更长时只是前缀；
    //              IsTruncated == true 时 ObservedByteCount 才是真实规模
    Console.WriteLine($"[session {e.SessionId}] [{e.Kind}] {Convert.ToHexString(e.Bytes.Span)}" +
        (e.IsTruncated ? $"（前 {e.Bytes.Length}/{e.ObservedByteCount} 字节）" : "") +
        $" {e.Exception?.Message}");
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

未完成帧超时只计"帧已开头、迟迟收不齐"的时间：缓冲为空的静默连接不计时（收到字节即重置，整帧切尽后归零）。它与 `ReceiveIdleTimeoutMs`（见下节）互补——后者在完全没流量时也计时，适合有周期报文的协议；对允许长时间空闲、但半帧卡死必须判死的协议（如 HSMS T8），用未完成帧超时。注意其作用域是"等待网络后续字节"期间：若设置了 `ReceiveQueueCapacity` 且消费端完全停滞，解码循环阻塞在消息通道写入上，此期间不计时（内存防线仍由 `MaxIncompleteFrameBufferBytes` 兜底）。

### 内置指标（Metrics）

连接自带 [System.Diagnostics.Metrics](https://learn.microsoft.com/dotnet/core/diagnostics/metrics-instrumentation) 指标（Meter 名 `StreamFrame`，标签 `endpoint`），零外部依赖——生产部署用 `MeterListener` 或 OpenTelemetry 订阅即可观测，不订阅则开销为每次记录纳秒级：

| 指标 | 类型 | 含义 |
|---|---|---|
| `streamframe.frames_sent` / `frames_received` | Counter | 业务帧收发计数 |
| `streamframe.bytes_sent` / `bytes_received` | Counter | 字节收发计数（含帧定界字节/噪声） |
| `streamframe.reconnects` | Counter | 进入重连的次数 |
| `streamframe.session_duration` | Histogram | 单次 TCP 会话存活时长（秒） |
| `streamframe.send_queue_length` | Histogram | 发送队列水位（每次入队采样） |

```csharp
// 最简订阅（示例）：OTel 的 MeterProvider.AddMeter("StreamFrame") 一行即可接入完整体系
using var listener = new MeterListener { InstrumentPublished = (i, l) => l.EnableMeasurementEvents(i) };
listener.SetMeasurementEventCallback<long>((instrument, value, _, _) => { /* 上报 */ });
listener.Start();
```

> netstandard2.0 目标经 `System.Diagnostics.DiagnosticSource` 包提供同款 API（netfx 运行时可用）。

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
- **切帧吞吐**：`LengthPrefixFramer` ≈8.3 ns/帧，`StxEtxFramer` ≈50 ns/帧（net8+ SearchValues 向量化后较逐字节版提速约 22 倍）——两者均可达每秒千万帧级；
- **端到端**（真实 TCP 回环，2026-08-29 两轮区间，含内置指标）：单向吞吐 1KB ≈13–13.5 万条/秒、64B ≈21–24 万条/秒（LengthPrefix）；往返延迟 ≈66–74 µs；XML codec 每条报文 2–16 µs（400B–4KB）——序列化开销远大于定界层。**框架税分尺寸**：小消息下队列流水线反而快于裸 NetworkStream 逐条 Write 的对照（13–52%），64KB 大消息约 3–4×（编码缓冲分配主导，优化方向已记录）；
- **新特性成本实测**：内置指标无监听时 0.5–1.0 ns/次、零分配（可常开）；`SendInSessionAsync` 约为 `SendAsync` 的 2 倍耗时、+≈470 B/条分配（会话语义的代价，不需要时用 `SendAsync`）；接收视图与未完成帧超时（关闭态）均无可测差异。绝对值随机器浮动，完整数据与噪声披露见 [bench/README.md](bench/README.md)。

### 大报文指南（≥64KB）

64KB 级消息下，开销大头在 codec 写法与消息类型而非框架（归因数据见 [bench/README.md](bench/README.md)）。三条建议：

1. **codec 用 span 直写重载**——`Encoding.GetBytes(ReadOnlySpan<char>, IBufferWriter<byte>)` 不产生中间数组（旧写法每条多一次全尺寸分配 + 拷贝）：

   ```csharp
   // 推荐：零中间数组
   public void Encode(string message, IBufferWriter<byte> writer, CancellationToken ct)
       => Encoding.UTF8.GetBytes(message.AsSpan(), writer);
   // 不推荐：writer.Write(Encoding.UTF8.GetBytes(message));  // 每条一次全尺寸 byte[]
   ```

2. **消息类型选 byte[] / ReadOnlyMemory<byte>**：string 消息每条固有 ≈2× 报文大小的 UTF-16 物化分配；字节负载 + 透传 codec 时框架距裸 TCP 仅 ≈20–30%；
3. 保持默认的**流式编码**开启（`UseStreamingEncode`），发送缓冲会按上一帧大小自适应起租（封顶 1MB）。

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
dotnet run --project samples/StreamFrame.Demo          # 五场景端到端 demo
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
