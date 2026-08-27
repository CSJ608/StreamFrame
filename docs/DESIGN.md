# StreamFrame 通用 Socket 通讯框架

一个把"帧边界判定"与"帧内数据编解码"插件化的通用 .NET socket 通讯框架。适用于通过 socket 进行数据互换的场景（设备通讯、WMS 对接等），把场景之间的共性（连接管理、重连、读写、粘包/半包、消息分发）收敛到核心，把差异性（framing、codec）抽象成可插拔的驱动。

参考项目：
- `Multiway.TC.SamSung`（TCP/XML）— 帧定界、连接状态机、重连、Pipe 收发的设计来源
- `secs4net`（SECS/GEM 二进制协议）— producer/consumer 分离、发送串行化、测试范式

## 项目结构

```
StreamFrame/
├─ StreamFrame.slnx
├─ src/StreamFrame/                  # 核心库（无业务依赖）
│  ├─ ConnectionState.cs             # Connecting / Connected / Retry
│  ├─ IStreamConnection.cs           # IStreamConnection<TMessage>
│  ├─ StreamConnectionOptions.cs
│  ├─ Framing/
│  │  ├─ IFrameCodec.cs              # 帧定界接口（EncodeFrame/TryDecodeFrame/MaxPayloadBytes）
│  │  ├─ LengthPrefixFrameCodec.cs   # 4 字节大端长度头
│  │  └─ StxEtxFrameCodec.cs         # STX 0x02 … ETX 0x03（plain，不做转义）
│  ├─ Codec/
│  │  ├─ ICodec.cs                   # ICodec<TMessage>（驱动实现）
│  │  └─ BufferWriterStream.cs       # IBufferWriter→Stream，供 XmlWriter 直写
│  └─ Connection/
│     ├─ StreamConnection.cs         # 连接实现（双模式 + 状态机 + 重连）
│     └─ FrameDecoder.cs             # 解码循环（切帧 + codec 解码）
├─ src/StreamFrame.Protocols.Xml/    # 官方示例驱动：XML 报文
├─ test/StreamFrame.Tests/           # xUnit 单测
└─ samples/StreamFrame.Demo/         # 控制台端到端 demo
```

## 核心概念

### 1. Framing（帧边界，连接级固定）

```csharp
public interface IFrameCodec
{
    int MaxPayloadBytes { get; }
    void EncodeFrame(ReadOnlySpan<byte> payload, IBufferWriter<byte> writer);
    bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload);
}
```

| 实现 | 定界方式 | 约束 |
|---|---|---|
| `LengthPrefixFrameCodec` | 4 字节**大端**长度头 + 负载 | 负载最大 16 MiB（可配）；非法长度丢弃头重同步 |
| `StxEtxFrameCodec` | STX `0x02` … ETX `0x03` | **plain 不做转义**：负载不得含 0x02/0x03，二进制负载请用 LengthPrefix |

两个内置 framing 同时实现 `IStreamingFrameCodec`：支持流式单缓冲编码（见下文），与纯函数 `EncodeFrame` 字节输出完全一致。

STX/ETX 边界处理（与 SamSung 一致）：
- 待定帧的 ETX 前再次遇到 STX → 丢弃旧半包从新 STX 重新开始
- 无前导 STX 的孤立 ETX → 噪声跳过
- 缓冲尾无闭合 ETX → 回到最后一个 STX 等待更多数据

### 2. Codec（帧内编解码，驱动实现）

```csharp
public interface ICodec<TMessage>
{
    TMessage Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default);
    void Encode(TMessage message, IBufferWriter<byte> writer, CancellationToken ct = default);
}
```

驱动＝实现 codec + 定义业务消息类。官方示例：`StreamFrame.Protocols.Xml.XmlDocumentCodec`（XmlSerializer 解码 / BufferWriterStream+XmlWriter 编码）。用户按此骨架替换即可支持自定义二进制协议。

### 3. 连接核心（客户端/服务端双模式）

```csharp
public interface IStreamConnection<TMessage> : IAsyncDisposable
{
    ConnectionState State { get; }
    bool IsActive { get; }                       // true=主动连接  false=被动监听
    IPAddress IpAddress { get; }
    int Port { get; }
    string DeviceIpAddress { get; }
    event EventHandler<ConnectionState>? ConnectionChanged;
    Action<ReadOnlyMemory<byte>>? RawBytesReceived { get; set; }   // HEX 调试
    Action<ReadOnlyMemory<byte>>? RawBytesSent { get; set; }
    void Start(CancellationToken ct);
    void Reconnect();
    Task SendAsync(TMessage message, CancellationToken ct = default);
    IAsyncEnumerable<TMessage> GetMessages(CancellationToken ct);
}
```

构造注入 `(IFrameCodec framing, ICodec<TMessage> codec, IPAddress, int port, bool isActive, options?)` —— framing 与 codec 均连接级固定，一条连接只流通一种消息类型、一种帧格式。

## 内部架构

### 接收流水线（producer/consumer 分离）

```
socket --ReceiveAsync--> pipe.Writer --[Pipe]--> FrameDecoder.RunAsync
   (receive task)                                  (decode task)
                                                      │ while(TryDecodeFrame) → codec.Decode → Channel<TMessage>
```

- **半包**：`reader.AdvanceTo(buffer.Start, buffer.End)` 保留未消费字节
- **粘包**：`while (TryDecodeFrame)` 循环切尽所有完整帧

### 发送流水线（有界队列背压 + 串行化）

业务线程 `SendAsync` 仅入队到有界 `Channel<TMessage>`（capacity 可配）；发送 worker 出队 → 编码加帧 → 持 `_sendLock` 逐段 `socket.SendAsync`。队列满时 `SendAsync` 自然 await 背压，大帧不阻塞调用线程。

**两种帧编码路径**（连接选项 `UseStreamingEncode`，默认开启）：
- **流式**（默认）：若 framing 实现 `IStreamingFrameCodec`，走 `BeginFrame → codec.Encode(写同一缓冲) → EndFrame` 单缓冲路径，`LengthPrefix` 在 Begin 占位、End 原地回填长度。**全程零 memcpy**，与 secs4net 的 `EncodeMessage` 模式一致。
- **纯函数回退**：若 framing 不支持流式（自定义实现未实现 `IStreamingFrameCodec`），自动回退到 `EncodeFrame(payload)` 两段缓冲（含一次 memcpy）。

```csharp
// 自定义 framing 时：实现 IFrameCodec 即可（走纯函数路径）；
// 想消除 memcpy 再额外实现 IStreamingFrameCodec 的可选成员 BeginFrame/EndFrame。
```

### 生命周期

`Connecting → Connected → Retry`。`Start` 失败按 `ConnectRetryDelayMs`/`AcceptRetryDelayMs` 自动重试；运行中断线统一走 `Retry`（`Interlocked` 重入保护防并发双重重连）→ 停会话 → 重新 Start。

**会话模型（1.2.0）**：一次 TCP 连接 = 一个会话（Pipe + 接收/解码/发送三任务，各自带守护）。会话因对端断开、socket 故障、帧解码失败（`DecodeErrorPolicy.Disconnect`）、未完成帧超限、发送失败或接收空闲超时而终结时，守护任务经 `ScheduleReconnect` 调度重建——不能在任务内联调用 `Reconnect`（StopSession 会等待调用方自身，自等待 2s）。每个会话持有单调递增的 epoch；故障重连只在 epoch 仍为最新时执行，迟到的过期故障不会误杀已重建的新会话。

**消息通道所有权（1.2.0）**：`_messageRelay` 归连接所有、跨会话复用，仅 Dispose 时完成——`GetMessages` 是跨重连的稳定消息流。`FrameDecoder` 不完成通道（1.1.0 在此把取消异常写进通道完成状态，导致重连后消息永不到达的假活）。解码循环的 `ReadAsync` 不绑定取消令牌（取消中的 ReadAsync 会把 Pipe 留在"读取进行中"状态，无法再 TryRead）；退出由 `writer.CompleteAsync` / `CancelPendingRead` 驱动，退出前投递所有已缓冲的完整帧——"已收到的字节必达"。

**原始字节与诊断（1.2.0）**：`RawBytesReceived/Sent` 全量输出（含被定界器丢弃的噪声字节），发送侧按实际写出分片回调；两者与 `ConnectionChanged`、`FrameError` 的用户回调异常均被隔离，不反噬会话。`IFrameDiscardReporting`（可选接口，内置两个 codec 实现）让定界器精确上报重同步丢弃的字节，经 `FrameError(DiscardedByResync)` 交上层调试。

### 不内置（上移驱动层）

- 心跳 / 控制消息（泛型下框架不知消息形态）
- 按事务 ID 配对应答、T3/T6 超时
- 消息分流

框架只保证原始收发 + 状态/原始字节事件。

## 测试

`test/StreamFrame.Tests/`：
- Framing 专项：长度越界/负数/超 Max、孤立 ETX、新 STX 丢弃旧半包、超长帧、丢弃字节精确上报
- 流式编码：Begin/EndFrame 与纯函数 EncodeFrame **字节输出一致性**、长度头回填正确、超长拒绝
- 解码循环：整帧粘包、按 23 字节小块分片喂（半包）、解码失败双策略、未完成帧超限、通道不被 decoder 完成
- XML codec：往返、DTD 拒绝（XXE 防御）
- 连接端到端（真实 TCP 回环）：断线重连后消息送达、解码失败断线/跳帧、调试钩子抛异常会话存活、接收空闲超时、发送失败重连、未完成帧超限断线
- 端到端：`samples/StreamFrame.Demo` 演示 XML+LengthPrefix、文本+STX/ETX、断线重连三场景

运行：`dotnet build StreamFrame.slnx`、`dotnet test`、`dotnet run --project samples/StreamFrame.Demo`
