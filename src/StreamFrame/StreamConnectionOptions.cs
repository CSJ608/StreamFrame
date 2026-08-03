namespace StreamFrame;

/// <summary>
/// <see cref="IStreamConnection{TMessage}"/> 的可调参数。
/// </summary>
public sealed class StreamConnectionOptions
{
    /// <summary>主动模式下 TCP 连接失败的等待重试间隔（毫秒）。</summary>
    public int ConnectRetryDelayMs { get; set; } = 3000;

    /// <summary>被动模式下 accept 失败的等待重试间隔（毫秒）。</summary>
    public int AcceptRetryDelayMs { get; set; } = 2000;

    /// <summary>Socket 接收缓冲区大小（字节）。</summary>
    public int SocketReceiveBufferSize { get; set; } = 65536;

    /// <summary>发送队列容量；超过容量时 SendAsync 将等待（背压）。</summary>
    public int SendQueueCapacity { get; set; } = 1024;

    /// <summary>编码缓冲区的初始大小（字节）。</summary>
    public int EncodeBufferInitialSize { get; set; } = 1024;

    /// <summary>
    /// 是否使用流式帧编码（单缓冲、零 memcpy）。仅在 framing 实现
    /// <see cref="IStreamingFrameCodec"/> 时生效；否则自动回退到"序列化→帧"两段缓冲。
    /// </summary>
    public bool UseStreamingEncode { get; set; } = true;
}
