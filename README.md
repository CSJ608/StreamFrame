# StreamFrame

[![NuGet](https://img.shields.io/nuget/v/StreamFrame.svg)](https://www.nuget.org/packages/StreamFrame)
[![NuGet Downloads](https://img.shields.io/nuget/dt/StreamFrame)](https://www.nuget.org/stats/packages/StreamFrame?groupby=Version)

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
using StreamFrame.Abstractions;
using StreamFrame.Protocols.Xml;

// 服务端（被动监听）
var server = new StreamConnection<XDocument>(
    new LengthPrefixFrameCodec(),          // 4 字节大端长度头；也可用 StxEtxFrameCodec
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
    new LengthPrefixFrameCodec(),
    new XmlDocumentCodec(),
    IPAddress.Parse("127.0.0.1"), 5100, isActive: true);
client.Start(ct);
await client.SendAsync(XDocument.Parse("<Message><Id>1</Id></Message>"), ct);
```

## 概念

### 帧定界（IFrameCodec）— 怎么从字节流里切出一帧

| 实现 | 定界方式 | 适用 |
|---|---|---|
| `LengthPrefixFrameCodec` | 4 字节**大端**长度头 + 负载 | 通用，二进制安全 |
| `StxEtxFrameCodec` | STX `0x02` … ETX `0x03` 包裹 | XML / 纯文本等已知安全的负载 |

两种默认实现都支持**流式单缓冲编码**（可选，消除发送侧 memcpy）。想自定义帧格式时实现 `IFrameCodec` 即可。

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

- **客户端/服务端双模式**：`isActive: true` 主动连远端，`false` 被动监听
- **自动重连**：`Connecting → Connected → Retry` 状态机
- **事件**：`ConnectionChanged` 状态变化、`RawBytesReceived/Sent` 原始字节（HEX 调试）
- **发送背压**：有界发送队列，队列满时 `SendAsync` 自动等待

## 依赖

- [System.IO.Pipelines](https://www.nuget.org/packages/System.IO.Pipelines)
- [System.Threading.Channels](https://www.nuget.org/packages/System.Threading.Channels)

## 测试与示例

```bash
dotnet build StreamFrame.slnx
dotnet test
dotnet run --project samples/StreamFrame.Demo
```

## 项目结构

```
src/StreamFrame/                  # 核心库（无业务依赖）
src/StreamFrame.Protocols.Xml/    # XML 报文驱动（示例 codec）
test/StreamFrame.Tests/           # xUnit 单测
samples/StreamFrame.Demo/         # 控制台端到端 demo
```

## 许可

MIT
