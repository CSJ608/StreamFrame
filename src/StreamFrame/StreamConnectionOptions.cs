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

    /// <summary>
    /// 连接/监听重试等待的封顶（毫秒）。默认 0 = 不启用退避（固定间隔，与历史行为一致）。
    /// 设为大于基础间隔的值后：连续失败按基础间隔指数倍增（×2）封顶至此值并叠加 ±20% 抖动，
    /// 连接成功后自动复位——对端长时间宕机时避免以固定间隔永久敲击。
    /// </summary>
    public int MaxRetryDelayMs { get; set; }

    /// <summary>Socket 接收缓冲区大小（字节）。</summary>
    public int SocketReceiveBufferSize { get; set; } = 65536;

    /// <summary>发送队列容量；超过容量时 SendAsync 将等待（背压）。</summary>
    public int SendQueueCapacity { get; set; } = 1024;

    /// <summary>编码缓冲区的初始大小（字节）。</summary>
    public int EncodeBufferInitialSize { get; set; } = 1024;

    /// <summary>
    /// 是否使用流式帧编码（单缓冲、零 memcpy）。仅在 framing 实现
    /// <see cref="IStreamingFramer"/> 时生效；否则自动回退到"序列化→帧"两段缓冲。
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
    /// 接收消息通道容量（默认 0 = 不限制）：解码出的业务消息先入通道，再由 GetMessages 消费。
    /// 设为正数后，消费慢时解码循环暂停读取（TCP 背压自然传导到对端），防止慢消费者撑爆内存。
    /// 注意：消费完全停滞期间，会话拆除最多等待 2 秒（内部超时）后继续。
    /// </summary>
    public int ReceiveQueueCapacity { get; set; }

    /// <summary>
    /// 接收空闲超时（毫秒）：连续这么久没收到任何字节即判定连接死亡，断线重连。
    /// 默认 0 = 关闭。与 <see cref="TcpKeepAlive"/> 互补：这是应用层的"多久必须有流量"约束，
    /// 适合有周期性报文的协议（如设备心跳）。
    /// </summary>
    public int ReceiveIdleTimeoutMs { get; set; }

    /// <summary>
    /// 未完成帧超时（毫秒）：缓冲里已有半帧字节、但后续字节连续这么久未到达，即判定会话失效，
    /// 断线重连（并通过 FrameError 事件上报 <see cref="FrameErrorKind.IncompleteFrameTimeout"/>，
    /// 携带受上限保护的缓冲快照）。默认 0 = 关闭。
    ///
    /// 与 <see cref="ReceiveIdleTimeoutMs"/> 的区别：后者在"完全没有字节"时也计时（要求连接
    /// 必须有周期流量）；本选项只在"帧已开头、迟迟收不齐"时计时——缓冲为空的正常静默不会触发，
    /// 一帧完整切出后重新归零。适合允许长时间空闲、但半帧卡死必须判死的长度前缀协议（如 HSMS T8）。
    ///
    /// 作用域限制：计时窗口是解码循环等待网络后续字节期间。若设置了
    /// <see cref="ReceiveQueueCapacity"/> 且消费端完全停滞，解码循环会阻塞在消息通道写入上、
    /// 不在等待网络字节的状态——此期间本超时不计时（半帧的内存防线仍由
    /// <see cref="MaxIncompleteFrameBufferBytes"/> 兜底）。
    /// </summary>
    public int IncompleteFrameTimeoutMs { get; set; }

    /// <summary>
    /// 校验所有参数取值的合法性（由 <see cref="StreamConnection{TMessage}"/> 构造时调用）。
    /// 非法值在构造时立即失败，而不是在运行中的重连/收发路径深处抛出难定位的异常。
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">任一参数取值非法。</exception>
    internal void Validate()
    {
        if (ConnectRetryDelayMs < 0)
            throw new ArgumentOutOfRangeException(nameof(ConnectRetryDelayMs), ConnectRetryDelayMs, "不能为负数。");
        if (AcceptRetryDelayMs < 0)
            throw new ArgumentOutOfRangeException(nameof(AcceptRetryDelayMs), AcceptRetryDelayMs, "不能为负数。");
        if (MaxRetryDelayMs < 0)
            throw new ArgumentOutOfRangeException(nameof(MaxRetryDelayMs), MaxRetryDelayMs, "不能为负数（0 = 不启用退避）。");
        if (SocketReceiveBufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(SocketReceiveBufferSize), SocketReceiveBufferSize, "必须为正数。");
        if (SendQueueCapacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(SendQueueCapacity), SendQueueCapacity, "必须为正数。");
        if (EncodeBufferInitialSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(EncodeBufferInitialSize), EncodeBufferInitialSize, "必须为正数。");
        if (MaxIncompleteFrameBufferBytes < 0)
            throw new ArgumentOutOfRangeException(nameof(MaxIncompleteFrameBufferBytes), MaxIncompleteFrameBufferBytes, "不能为负数（0 = 取默认值）。");
        if (ReceiveQueueCapacity < 0)
            throw new ArgumentOutOfRangeException(nameof(ReceiveQueueCapacity), ReceiveQueueCapacity, "不能为负数（0 = 不限制）。");
        if (ReceiveIdleTimeoutMs < 0)
            throw new ArgumentOutOfRangeException(nameof(ReceiveIdleTimeoutMs), ReceiveIdleTimeoutMs, "不能为负数（0 = 关闭）。");
        if (IncompleteFrameTimeoutMs < 0)
            throw new ArgumentOutOfRangeException(nameof(IncompleteFrameTimeoutMs), IncompleteFrameTimeoutMs, "不能为负数（0 = 关闭）。");
        if (TcpKeepAlive && KeepAliveTimeMs <= 0)
            throw new ArgumentOutOfRangeException(nameof(KeepAliveTimeMs), KeepAliveTimeMs, "TcpKeepAlive 开启时必须为正数。");
        if (TcpKeepAlive && KeepAliveIntervalMs <= 0)
            throw new ArgumentOutOfRangeException(nameof(KeepAliveIntervalMs), KeepAliveIntervalMs, "TcpKeepAlive 开启时必须为正数。");
    }
}
