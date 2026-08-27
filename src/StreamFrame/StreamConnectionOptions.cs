namespace StreamFrame;

/// <summary>
/// 帧结构完整但内容解码失败（codec 抛异常）时的处理策略。
/// </summary>
public enum DecodeErrorPolicy
{
    /// <summary>
    /// 断开连接并走自动重连（默认）。协议内容错乱后流状态通常不可信，
    /// 重连是对端与本地同时回到一致状态的最短路径。
    /// </summary>
    Disconnect,

    /// <summary>
    /// 丢弃坏帧、继续解析后续帧。适合线路噪声多、流定界自身可靠（如长度前缀）的场景；
    /// 坏帧会通过 FrameError 事件上报。
    /// </summary>
    SkipFrame,
}

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

    /// <summary>
    /// 被动模式（isActive=false）下，accept 到第一个客户端后是否关闭监听 socket。
    /// 默认 true：同一时间仅接受一个客户端，后续连接被 TCP 层立即拒绝（与单客户端设备对接场景匹配）。
    /// 设为 false 可保持监听，但当前框架仍只处理第一个已连接客户端。
    /// </summary>
    public bool AcceptFirstClientOnly { get; set; } = true;

    /// <summary>
    /// 帧内负载解码失败（codec 抛异常）时的处理策略。默认
    /// <see cref="DecodeErrorPolicy.Disconnect"/>：断线重连。
    /// 两种策略下坏帧都会通过 FrameError 事件（Kind=DecodeFailed）上报。
    /// </summary>
    public DecodeErrorPolicy DecodeErrorPolicy { get; set; } = DecodeErrorPolicy.Disconnect;

    /// <summary>
    /// 未完成帧允许缓冲的最大字节数；超过即判定流不可恢复，断线重连（并通过
    /// FrameError 事件上报 IncompleteFrameOverflow）。
    /// 默认 0 = 取 framing 的 MaxPayloadBytes + 4096（保证合法的最大帧仍可通过）。
    /// 若业务消息远小于帧上限，可显式设小（如 64KB）以收紧对端喂半帧的内存攻击面。
    /// </summary>
    public int MaxIncompleteFrameBufferBytes { get; set; }

    /// <summary>
    /// 是否开启 TCP KeepAlive（默认 false）。开启后内核在连接静默期主动探测对端，
    /// 半开连接（对端断电/拔线，无 FIN/RST）会被及时判定为断线。
    /// 需要 Windows 10 1709+ / Linux。生产环境建议开启。
    /// </summary>
    public bool TcpKeepAlive { get; set; }

    /// <summary>TCP KeepAlive 首次探测前的静默时长（毫秒），仅 <see cref="TcpKeepAlive"/> 开启时生效。</summary>
    public int KeepAliveTimeMs { get; set; } = 30_000;

    /// <summary>TCP KeepAlive 探测间隔（毫秒），仅 <see cref="TcpKeepAlive"/> 开启时生效。</summary>
    public int KeepAliveIntervalMs { get; set; } = 5_000;

    /// <summary>
    /// 接收空闲超时（毫秒）：连续这么久没收到任何字节即判定连接死亡，断线重连。
    /// 默认 0 = 关闭。与 <see cref="TcpKeepAlive"/> 互补：这是应用层的"多久必须有流量"约束，
    /// 适合有周期性报文的协议（如设备心跳）。
    /// </summary>
    public int ReceiveIdleTimeoutMs { get; set; }
}
