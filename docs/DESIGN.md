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

业务线程 `SendAsync` 仅入队到有界 `Channel<TMessage>`（capacity 可配）；发送 worker 出队 → `codec.Encode` → `framing.EncodeFrame` → 持 `_sendLock` 逐段 `socket.SendAsync`。队列满时 `SendAsync` 自然 await 背压，大帧不阻塞调用线程。

### 生命周期

`Connecting → Connected → Retry`。`Start` 失败按 `ConnectRetryDelayMs`/`AcceptRetryDelayMs` 自动重试；运行中断线统一走 `Retry`（`Interlocked` 重入保护防并发双重重连）→ 停会话 → 重新 Start。

### 不内置（上移驱动层）

- 心跳 / 控制消息（泛型下框架不知消息形态）
- 按事务 ID 配对应答、T3/T6 超时
- 消息分流

框架只保证原始收发 + 状态/原始字节事件。

## 测试

`test/StreamFrame.Tests/`，无需真实 socket：
- Framing 专项：长度越界/负数/超 Max、孤立 ETX、新 STX 丢弃旧半包、超长帧
- 解码循环：整帧粘包、按 23 字节小块分片喂（半包）
- XML codec：往返、DTD 拒绝（XXE 防御）
- 端到端：`samples/StreamFrame.Demo` 演示 XML+LengthPrefix、文本+STX/ETX、断线重连三场景

运行：`dotnet build StreamFrame.slnx`、`dotnet test`、`dotnet run --project samples/StreamFrame.Demo`
