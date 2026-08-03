using System.Net;
using StreamFrame.Abstractions;

namespace StreamFrame;

/// <summary>
/// 一条类型化 socket 通讯连接：客户端/服务端双模式，连接级固定 framing 与 codec。
/// </summary>
/// <typeparam name="TMessage">业务消息类型，贯穿这条连接。</typeparam>
public interface IStreamConnection<TMessage> : IAsyncDisposable
{
    /// <summary>当前连接状态。</summary>
    ConnectionState State { get; }

    /// <summary>true = 主动连接远端；false = 被动监听等待连接。</summary>
    bool IsActive { get; }

    /// <summary>主动模式：远端地址；被动模式：监听地址。</summary>
    IPAddress IpAddress { get; }

    /// <summary>主动模式：远端端口；被动模式：监听端口。</summary>
    int Port { get; }

    /// <summary>对端地址（被动模式下为已连接客户端的地址）。</summary>
    string DeviceIpAddress { get; }

    /// <summary>连接状态变更事件。</summary>
    event EventHandler<ConnectionState>? ConnectionChanged;

    /// <summary>从 Socket 收到原始字节时触发（HEX 日志等调试用途）。</summary>
    Action<ReadOnlyMemory<byte>>? RawBytesReceived { get; set; }

    /// <summary>向 Socket 发送原始字节时触发（HEX 日志等调试用途）。</summary>
    Action<ReadOnlyMemory<byte>>? RawBytesSent { get; set; }

    /// <summary>启动连接（主动连接/被动监听），异常时自动重试直到取消。</summary>
    void Start(CancellationToken ct);

    /// <summary>立即进入重连流程。</summary>
    void Reconnect();

    /// <summary>发送一条业务消息。仅入队，由发送 worker 编码加帧后写出；队列满时背压等待。</summary>
    Task SendAsync(TMessage message, CancellationToken ct = default);

    /// <summary>以异步流方式消费收到的业务消息。</summary>
    IAsyncEnumerable<TMessage> GetMessages(CancellationToken ct);
}
