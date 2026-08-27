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

    /// <summary>
    /// 帧层诊断事件：帧内容解码失败（<see cref="FrameErrorKind.DecodeFailed"/>）、被定界器
    /// 丢弃的噪声字节（<see cref="FrameErrorKind.DiscardedByResync"/>）、未完成帧缓冲超限
    /// （<see cref="FrameErrorKind.IncompleteFrameOverflow"/>）。
    /// 事件携带的 <see cref="FrameErrorEventArgs.Bytes"/> 是已拷贝的字节，回调后可安全留存。
    /// </summary>
    event EventHandler<FrameErrorEventArgs>? FrameError;

    /// <summary>
    /// 从 Socket 收到原始字节时触发（HEX 日志等调试用途）。每个接收块都会触发，
    /// 包括后来被帧定界器丢弃的噪声字节。
    ///
    /// 内存契约：回调参数是接收管线内部缓冲的切片，仅在回调同步执行期间有效，
    /// 随后会被复用/覆盖；需要留存（异步落盘、事后 dump）必须自行拷贝。
    /// 回调内抛出的异常会被隔离，不会影响会话。
    /// </summary>
    Action<ReadOnlyMemory<byte>>? RawBytesReceived { get; set; }

    /// <summary>
    /// 向 Socket 写入原始字节时触发（HEX 日志等调试用途）。按 socket 实际写出的分片触发，
    /// 发送中途失败时已成功写出的部分同样可见。
    ///
    /// 内存契约：与 <see cref="RawBytesReceived"/> 相同，仅在回调同步执行期间有效。
    /// 回调内抛出的异常会被隔离，不会影响会话。
    /// </summary>
    Action<ReadOnlyMemory<byte>>? RawBytesSent { get; set; }

    /// <summary>启动连接（主动连接/被动监听），连接/监听失败时自动重试。</summary>
    /// <remarks>
    /// 只能调用一次；重复调用抛 <see cref="InvalidOperationException"/>，重建连接请用 <see cref="Reconnect"/>。
    /// <paramref name="ct"/> 仅约束"建立连接"阶段：连接建立前取消它会停止连接/监听重试。
    /// 连接建立后的收发与断线自动重连由连接自身管理——取消该 token 不会断开已建立的
    /// 连接，也不会停止后续重连；要停止一切请调用 DisposeAsync。
    /// </remarks>
    void Start(CancellationToken ct);

    /// <summary>立即进入重连流程。</summary>
    void Reconnect();

    /// <summary>
    /// 等待连接进入 <see cref="ConnectionState.Connected"/>：已连接时立即完成；否则等到
    /// 下一次连接成功、<paramref name="ct"/> 取消或连接 Dispose（任务以取消结束）。
    /// 替代轮询 <see cref="State"/> 或 Task.Delay 式等待。
    /// </summary>
    Task WaitForConnectedAsync(CancellationToken ct = default);

    /// <summary>发送一条业务消息。仅入队，由发送 worker 编码加帧后写出；队列满时背压等待。</summary>
    Task SendAsync(TMessage message, CancellationToken ct = default);

    /// <summary>以异步流方式消费收到的业务消息。</summary>
    IAsyncEnumerable<TMessage> GetMessages(CancellationToken ct);
}
