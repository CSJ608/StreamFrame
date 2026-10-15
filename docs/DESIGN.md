# StreamFrame 通用 Socket 通讯框架

一个把"帧边界判定"与"帧内数据编解码"插件化的通用 .NET socket 通讯框架。适用于通过 socket 进行数据互换的场景（设备通讯、WMS 对接等），把场景之间的共性（连接管理、重连、读写、粘包/半包、消息分发）收敛到核心，把差异性（framing、codec）抽象成可插拔的驱动。

参考项目：
- `Multiway.TC.SamSung`（TCP/XML）— 帧定界、连接状态机、重连、Pipe 收发的设计来源
- `secs4net`（SECS/GEM 二进制协议）— producer/consumer 分离、发送串行化、测试范式

## 项目结构

```
StreamFrame/
├─ StreamFrame.slnx
├─ src/StreamFrame/                  # 核心库（无业务依赖）；所有公共类型都在 StreamFrame 命名空间
│  ├─ ConnectionState.cs             # Connecting / Connected / Retry / Disconnected
│  ├─ IStreamConnection.cs           # IStreamConnection<TMessage>
│  ├─ StreamConnectionOptions.cs
│  ├─ Framing/
│  │  ├─ IFramer.cs                  # 帧定界接口（EncodeFrame/TryDecodeFrame/MaxPayloadBytes）
│  │  ├─ LengthPrefixFramer.cs       # 4 字节大端长度头
│  │  ├─ StxEtxFramer.cs             # STX 0x02 … ETX 0x03（plain，不做转义）
│  │  ├─ IStreamingFramer.cs         # 可选：流式单缓冲编码（Begin/EndFrame）
│  │  ├─ IFrameDiscardReporting.cs   # 可选：重同步丢弃字节上报
│  │  └─ IWrittenBufferWriter.cs     # 流式编码用的可回写缓冲抽象
│  ├─ Codec/
│  │  ├─ ICodec.cs                   # ICodec<TMessage>（驱动实现）
│  │  ├─ BufferWriterStream.cs       # IBufferWriter→Stream，供 XmlWriter 直写
│  │  └─ PooledBufferWriter.cs       # 内部：ArrayPool 租用的 IWrittenBufferWriter
│  └─ Connection/
│     ├─ StreamConnection.cs         # 连接实现（双模式 + 状态机 + 重连 + 生命周期）
│     ├─ FrameDecoder.cs             # 解码循环（切帧 + codec 解码）
│     └─ SessionFaultException.cs    # 会话级故障（内部）
├─ src/StreamFrame.Protocols.Xml/    # 官方示例驱动：XML 报文
├─ test/StreamFrame.Tests/           # xUnit 单测
└─ samples/StreamFrame.Demo/         # 控制台端到端 demo
```

命名空间约定：公共类型统一收敛在根命名空间 `StreamFrame`（2.0 起不再有 `StreamFrame.Abstractions`），用户只需一个 `using StreamFrame;`；目录仅按角色组织文件。

## 核心概念

### 1. Framing（帧边界，连接级固定）

```csharp
public interface IFramer
{
    int MaxPayloadBytes { get; }
    void EncodeFrame(ReadOnlySpan<byte> payload, IBufferWriter<byte> writer);
    bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload);
}
```

| 实现 | 定界方式 | 约束 |
|---|---|---|
| `LengthPrefixFramer` | 4 字节**大端**长度头 + 负载 | 负载最大 16 MiB（可配）；非法长度丢弃头重同步 |
| `StxEtxFramer` | STX `0x02` … ETX `0x03` | **plain 不做转义**：负载不得含 0x02/0x03，二进制负载请用 LengthPrefix |

两个内置 framing 同时实现 `IStreamingFramer`：支持流式单缓冲编码（见下文），与纯函数 `EncodeFrame` 字节输出完全一致。

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
    string? RemoteIpAddress { get; }             // 主动=配置远端；被动=已连客户端（未连为 null）
    event EventHandler<ConnectionState>? ConnectionChanged;
    event EventHandler<FrameErrorEventArgs>? FrameError;            // 帧层诊断
    Action<ReadOnlyMemory<byte>>? RawBytesReceived { get; set; }   // HEX 调试
    Action<ReadOnlyMemory<byte>>? RawBytesSent { get; set; }
    void Start(CancellationToken ct);            // ct = 生命周期令牌（2.0）
    void Reconnect();
    Task WaitForConnectedAsync(CancellationToken ct = default);    // 等待连接就绪
    Task SendAsync(TMessage message, CancellationToken ct = default);
    IAsyncEnumerable<TMessage> GetMessages(CancellationToken ct = default);
}
```

构造注入 `(IFramer framing, ICodec<TMessage> codec, IPAddress, int port, bool isActive, options?, logger?)` —— framing 与 codec 均连接级固定，一条连接只流通一种消息类型、一种帧格式。

socket 为 IPv6 双栈（DualMode）：同一 socket 同时支持 IPv4/IPv6 连接与监听；监听 `IPAddress.Any` 自动归一为 `IPv6Any`，IPv4 客户端地址显示时映射回 IPv4。

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
- **流式**（默认）：若 framing 实现 `IStreamingFramer`，走 `BeginFrame → codec.Encode(写同一缓冲) → EndFrame` 单缓冲路径，`LengthPrefix` 在 Begin 占位、End 原地回填长度。**全程零 memcpy**，与 secs4net 的 `EncodeMessage` 模式一致。
- **纯函数回退**：若 framing 不支持流式（自定义实现未实现 `IStreamingFramer`），自动回退到 `EncodeFrame(payload)` 两段缓冲（含一次 memcpy）。

```csharp
// 自定义 framing 时：实现 IFramer 即可（走纯函数路径）；
// 想消除 memcpy 再额外实现 IStreamingFramer 的可选成员 BeginFrame/EndFrame。
```

### 生命周期

`Connecting → Connected → Retry → … → Disconnected`。`Start` 失败按 `ConnectRetryDelayMs`/`AcceptRetryDelayMs` 自动重试（`RetryDelayScheduler`：设 `MaxRetryDelayMs` 后连续失败按 ×2 倍增封顶并叠加 ±20% 抖动，连接成功复位——默认 0 不启用，行为与历史一致）；运行中断线统一走 `Retry`（`Interlocked` 重入保护防并发双重重连）→ 停会话 → 重新 Start。

**生命周期令牌（2.0）**：`Start(ct)` 把 `ct` 链接进 `_lifetimeCts`；取消 `ct`（或 DisposeAsync）走同一条幂等 `Shutdown` 路径——广播 `Disconnected` 终态 → 停止重连循环 → 拆会话 → 完成收发通道（`GetMessages` 自然结束、后续 `SendAsync` 抛 `ChannelClosedException`）。`Start` 仅可调用一次（重入抛 `InvalidOperationException`），重连内部走 `StartCore`。

**等待连接就绪**：`WaitForConnectedAsync` 基于"当前代"的 TaskCompletionSource——已连接立即完成；否则挂起，进入 `Connected` 时完成并清空（供下轮复用），`Shutdown` 时以取消收尾。

**会话模型（1.2.0；Session 对象化见 #68）**：一次 TCP 连接 = 一个会话（内部 Session 对象集中持有 socket、Pipe、取消源与接收/解码/发送三任务，各自带守护）。会话因对端断开、socket 故障、帧解码失败（`DecodeErrorPolicy.Disconnect`）、未完成帧超限、发送失败或接收空闲超时而终结时，守护任务经 `ScheduleReconnect` 调度重建——不能在任务内联调用 `Reconnect`（StopSession 会等待调用方自身，自等待 2s）。每个会话持有单调递增的 epoch；故障重连只在 epoch 仍为最新时执行，迟到的过期故障不会误杀已重建的新会话。

**消息通道所有权（1.2.0）**：`_messageRelay` 归连接所有、跨会话复用，仅停机（Shutdown）时完成——`GetMessages` 是跨重连的稳定消息流。默认无上限；`ReceiveQueueCapacity > 0` 时改为有界通道（FullMode=Wait），消费慢时解码循环暂停读取，TCP 背压自然传导到对端。`FrameDecoder` 不完成通道（1.1.0 在此把取消异常写进通道完成状态，导致重连后消息永不到达的假活）。解码循环的 `ReadAsync` 不绑定取消令牌（取消中的 ReadAsync 会把 Pipe 留在"读取进行中"状态，无法再 TryRead）；退出由 `writer.CompleteAsync` / `CancelPendingRead` 驱动，退出前投递所有已缓冲的完整帧——"已收到的字节必达"。

**日志**：内部事件（连接/监听重试、会话故障、用户回调异常、socket 关闭失败）经可选注入的 `ILogger` 输出；不传则 NullLogger 零开销。

**原始字节与诊断（1.2.0）**：`RawBytesReceived/Sent` 全量输出（含被定界器丢弃的噪声字节），发送侧按实际写出分片回调；两者与 `ConnectionChanged`、`FrameError` 的用户回调异常均被隔离，不反噬会话。`IFrameDiscardReporting`（可选接口，内置两个 codec 实现）让定界器精确上报重同步丢弃的字节，经 `FrameError(DiscardedByResync)` 交上层调试。

### 状态机与所有权规范（#68 行为基线）

> 本节是 `StreamConnection` 状态转换、资源所有权、锁范围与允许重入点的权威清单，
> 与 `StateOwnershipBaselineTests` 共同构成后续会话所有权重构的行为底线。

**状态转换全表**（`CommunicationStateChanging` 是唯一发布入口；`State` 只在发布段写入）：

| 目标状态 | 触发源 | 发布点 |
|---|---|---|
| `Connecting` | `Start`/重连的 `StartAsync` 循环头（每轮尝试前） | 连接循环 |
| `Connected` | 传输建立成功（connect/accept 返回后、过停机守门） | 连接循环（唯一发布点） |
| `Retry` | 用户 `Reconnect()`；会话任务故障经 `ScheduleReconnect`（携带 epoch，过期即丢） | 用户线程 / 线程池 |
| `Disconnected` | `Shutdown`（Dispose 或生命周期令牌取消）唯一来源 | 停机路径 |

发布段规则（`_publishGate` 锁内，原子）：① 停机后到达的非终态发布**整体丢弃**——终态必须是最后对外可见的状态（并发 Reconnect/连接成功与 Dispose 竞速时不被翻转）；② 过期纪元的故障发布在任何对外可见效果之前丢弃；③ 会话编号分配/归零与纪元递增先于 `State` 写入（线性化点：`ConnectionChanged` 回调与 `WaitForConnectedAsync` 完成时刻读取 `CurrentSessionId` 必得一致值）。通知段（锁外）：用户回调（异常隔离）、等待器完成、会话创建/拆除派发——回调可能重入，不得持有发布锁。

**会话创建的纪元现行性门控**：`StartSession` 只在"发布帧的纪元仍是最新、编号非零"时装配会话对象。Connected 回调内同步 `Reconnect()` 或发布窗口内的现行纪元故障会让位于取代者（其循环重建连接），迟到帧不再"兜底重建"——那曾以过期编号复用、拆掉新会话刚建立的 socket（撕裂会话）。被取代帧的 socket 在新连接覆盖 `_socket` 时当场释放。

**拆除分段（PR-2）**：会话拆除 = 锁内快速中止（`DetachSession`：结算指标 → 取消令牌 → 唤醒解码读取 → 关闭本会话 socket——netfx 上取消令牌无法中止已提交的接收，只有关 socket 能立即解锁阻塞 I/O，即"先中止 I/O 再等待"）+ 锁外收尾（`WaitAndDisposeSession`：有界等待三任务 ≤2s、释放 CTS）。会话锁内无任何等待：并发的会话建立/拆除互不排队；停机与重连的总阻塞时长与旧实现一致（≤2s）。

**资源所有权**：

| 资源 | 创建者 | 释放者 | 备注 |
|---|---|---|---|
| 会话对象（内部 `Session`：socket + Pipe + CTS + 接收/解码/发送三任务 + 编号/纪元/建立时刻） | `StartSession`（纪元现行性守门后装配） | `DetachSession`（锁内快速中止：结算时长指标 → 取消令牌 → 唤醒解码读取 → **先关本会话 socket 解锁 I/O**）+ `WaitAndDisposeSession`（锁外有界等待 ≤2s、释放 CTS） | 不可变对象：任务只操作所属会话的资源，直连迁移/拆除竞速中新旧会话互不干扰；发布到 `_session` 前（`_sessionGate` 内）完成装配 |
| `_socket`（当前连接 socket） | 连接循环（connect/accept，停机守门 + 与关闭块互斥） | 会话拆除时若指向该会话 socket 则置空；新连接覆盖时释放旧值；`Shutdown` 末尾兜底 | "当前值"镜像：诊断（`RemoteIpAddress`）与建立交接用；I/O 一律走会话自己的 socket |
| `_server`（监听 socket） | `InitServer`（Bind/Listen 全部成功才发布） | 单客户端接受后关闭；`Shutdown` 释放；bind 失败自清理 | 归当代接受循环所有（代次门控，#47） |
| `_messageRelay`（消息通道） | 构造 | 仅 `Shutdown`（连接级、跨会话存活） | 会话重建不完成它（1.2.0 假活修复） |
| `_sendQueue` / 挂起的会话绑定发送条目 | 业务侧入队 | `Shutdown` 完成通道；拆除时 `FaultPendingSessionSends` | 条目三态 CAS：Queued/Claimed/Cancelled/Expired |

**锁与同步原语清单**：`_publishGate`（状态发布段，锁内无 await/回调）；`_sessionGate`（会话装配/拆除、`_socket` 赋值互斥；**锁内无等待**——任务收尾在锁外，拆除先关 socket 再有界等待 ≤2s）；`_listenerGate`（监听器发布与停机释放互斥）；`_acceptLock`（接受尝试串行化 + 代次门控落点）；`_sendLock`（socket 写串行化，连接级）；CAS 字段：`_retryInProgress`（重拆防重入）、`_acceptLoopId`（接受循环代次）、`_disposeStage`/`_started`、发送条目状态字。

**允许的重入点**（用户回调可调用任意公共 API，异常一律隔离）：

- `Connected` 回调内 `Reconnect()`：合法（病态但支持）。回调内 Retry 先行（纪元递增 + 派发新循环），原发布帧的会话创建被纪元门控放弃；新连接由新循环完成，编号恒非零且不复用。
- `Connected` 回调内 `DisposeAsync()`：合法。停机同步完成（会话尚未创建则无拆除等待）；原发布帧随后的会话创建被停机守门放弃；挂起等待器以取消收尾。
- 停机后的任何调用：`Reconnect()`/`SendInSessionAsync` 等抛 `ObjectDisposedException` 或以会话失效收尾；迟到的内部状态发布被发布段丢弃。
- 迟到的旧会话任务/故障：入口过期纪元检查丢弃，不误杀新会话（1.2.0/2.3.1 既有保证，重入路径由纪元门控补齐）。

**已知限制/明确的竞速语义（#68 定稿）**：① 停机与重连仍**同步**等待会话任务退出（锁外、有界 ≤2s）——公共 API 的同步/异步行为按 #68 约束保持不变；若未来允许 `DisposeAsync` 异步化，可把该等待移入后台收尾。② 被取代循环建成的连接（被动端的迟到 accept、主动端的迟到 connect）**照常收编**：监听器是连接级共享资源，客户端向服务器端口发起的接入本身合法——被取代循环迟到完成的 accept 发布 Connected 并建立会话，由纪元机制与会话隔离收敛（取代者循环取锁后经代次检查静默退出）。曾评估"迟到连接当场释放"方案：会重置真实客户端、违背接入契约，已否决。代次门控的边界因此是**监听器生命周期（创建/关闭/接受串行化）与 Connecting 发布**（循环头复查前置——被取代循环不再发布 Connecting），而非客户端接入。

### netstandard2.0 兼容层（2.1.0）

多目标 `netstandard2.0;net8.0;net10.0`：
- **PolySharp**（PrivateAssets）：Range/Index/init 等语言级 polyfill，不进包。
- **`Compatibility/NetStandard20Shims.cs`**：`Task.WaitAsync` 等价物；Socket 的 Memory 收发回退到 `SocketTaskExtensions`——System.Net.Sockets 4.3.0 没有带 CancellationToken 的重载，用 3 参重载 + "可取消等待"包装（netfx 本就无法中止已提交的 socket I/O，取消仅放弃等待）；TCP KeepAlive 参数经 `SIO_KEEPALIVE_VALS` 设置（netfx 无 TcpKeepAliveTime/Interval 选项名）。
- **StxEtx 定界器改为手工扫描**：`SequenceReader<T>` 从未发布 netstandard2.0 包资产。单段缓冲走 Span 快路径（零分配），多段租借临时数组；所有 TFM 共用同一实现（`ReadOnlySequence.FirstSpan` 同理改用 `First.Span`）。
- ns2.0 目标额外引用 `System.Threading.Channels` + `Microsoft.Bcl.AsyncInterfaces`（现代 .NET 内置于共享框架，NU1510 要求移除显式引用）。
- **验证矩阵**：测试项目多目标 `net8.0;net10.0;net48`；CI 在 Ubuntu 跑 net8/net10、Windows 跑 net48（真实 .NET Framework 运行时加载 ns2.0 资产执行全量测试）。

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
- 连接端到端（真实 TCP 回环）：断线重连后消息送达、解码失败断线/跳帧、调试钩子抛异常会话存活、接收空闲超时、发送失败重连、未完成帧超限断线、单客户端拒绝
- 易用性与生命周期：等待连接就绪（跨重试/挂起/Dispose 取消）、Start 重入防护、选项构造校验、慢消费者有界接收队列、Start 令牌取消停机（Disconnected 终态/通道关闭）、对端地址语义、双栈监听 IPv4 接入
- 端到端：`samples/StreamFrame.Demo` 演示 XML+LengthPrefix、文本+STX/ETX、断线重连三场景

运行：`dotnet build StreamFrame.slnx`、`dotnet test`、`dotnet run --project samples/StreamFrame.Demo`
