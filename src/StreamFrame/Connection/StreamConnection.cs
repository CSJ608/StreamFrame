using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamFrame;

/// <summary>
/// 通用 socket 通讯连接实现：Socket + Pipelines + 类型化消息通道。
///
/// 架构（采纳两个参考项目的 producer/consumer 分离）：
/// <code>
/// socket --ReceiveAsync--> pipe.Writer --[Pipe]--> 解码循环
///   (producer task)                                  (consumer task)
///                                                      │ while(TryDecodeFrame) → codec.Decode → Channel&lt;SessionMessage&gt;
/// 发送：业务 SendAsync/SendInSessionAsync 入队 --有界Channel--> 发送worker → codec.Encode → framing.EncodeFrame → _sendLock → socket
/// </code>
///
/// 会话模型：一次 TCP 连接 = 一个会话（内部 Session 对象集中持有 socket + Pipe + 三个
/// 会话任务 + 取消源）；断线或会话故障时整个会话作废重建，而 <c>_messageRelay</c>
/// 消息通道是连接级的、跨会话存活——业务侧的 <see cref="GetMessages"/> 枚举在重连前后
/// 是同一条稳定流，仅在 Dispose 时正常结束。
/// 会话感知收发（<see cref="ISessionAwareStreamConnection{TMessage}"/>）在此之上提供
/// 绑定会话的发送与带会话编号的接收视图。
/// </summary>
public sealed class StreamConnection<TMessage> : ISessionAwareStreamConnection<TMessage>
{
    /// <inheritdoc />
    public event EventHandler<ConnectionState>? ConnectionChanged;

    /// <summary>帧层诊断事件：解码失败、被定界器丢弃的字节、不完整帧超限、未完成帧超时。
    /// 字节已拷贝、可留存；<see cref="FrameErrorEventArgs.SessionId"/> 为检测会话（旧会话延迟事件带旧编号），
    /// <see cref="FrameErrorEventArgs.ObservedByteCount"/>/<see cref="FrameErrorEventArgs.IsTruncated"/>
    /// 描述快照完整性。详见接口文档。</summary>
    public event EventHandler<FrameErrorEventArgs>? FrameError;

    /// <inheritdoc />
    public Action<ReadOnlyMemory<byte>>? RawBytesReceived { get; set; }
    /// <inheritdoc />
    public Action<ReadOnlyMemory<byte>>? RawBytesSent { get; set; }

    /// <inheritdoc />
    public ConnectionState State { get; private set; }
    /// <inheritdoc />
    public bool IsActive { get; }
    /// <inheritdoc />
    public IPAddress IpAddress { get; }
    /// <inheritdoc />
    public int Port { get; }

    /// <summary>对端 IP 地址：主动模式为配置的远端地址；被动模式为已连接客户端的地址（未连接时为 null）。
    /// 双栈 socket 收到的 IPv4 客户端地址归一显示为 IPv4（::ffff:127.0.0.1 → 127.0.0.1）。</summary>
    public string? RemoteIpAddress
        => IsActive
            ? IpAddress.ToString()
            : FormatRemoteAddress((_socket?.RemoteEndPoint as IPEndPoint)?.Address);

    private static string? FormatRemoteAddress(IPAddress? address)
        => address is null
            ? null
            : (address.IsIPv4MappedToIPv6 ? address.MapToIPv4() : address).ToString();

    private const int DisposalNotStarted = 0;
    private const int DisposalComplete = 1;
    private int _disposeStage;

    /// <summary>当前连接 socket：建立路径向会话交接用，兼作诊断（RemoteIpAddress）。
    /// I/O 一律走所属会话的 <see cref="Session.Socket"/>；本字段是"当前值"镜像，
    /// 会话拆除时若仍指向该会话的 socket 则同步置空。</summary>
    private Socket? _socket;
    private Socket? _server;

    private readonly IFramer _framing;
    private readonly ICodec<TMessage> _codec;
    private readonly StreamConnectionOptions _options;

#if NET9_0_OR_GREATER
    private readonly Lock _listenerGate = new(); // 监听器发布与停机释放互斥，不跨 await 持有。
    private readonly Lock _sessionGate = new(); // System.Threading.Lock（net9+，比 monitor 锁更轻量）
    private readonly Lock _publishGate = new(); // 状态发布段互斥（见 CommunicationStateChanging）
#else
    private readonly object _listenerGate = new();
    private readonly object _sessionGate = new();
    private readonly object _publishGate = new();
#endif

    /// <summary>
    /// 接受循环代次（#47）：每次 StartCore 递增。用户重连与自动重连竞速会产生多个并发的
    /// StartAsync 接受循环——旧的"僵尸"循环完成 accept 后可能看到 _server 已被新循环替换，
    /// 跳过单客户端的监听器关闭，泄漏一个"仍在监听但永无人 accept"的 socket（客户端 SYN
    /// 进入无人消费的 backlog，ConnectAsync 永久挂起）。代次门控让被取代的循环在取得
    /// _acceptLock 后静默退出：监听器的创建/关闭/accept 只归属当代循环。
    /// </summary>
    private int _acceptLoopId;

    /// <summary>当前会话（#68）：一次 TCP 连接的全部会话资源——socket、Pipe、取消源、
    /// 接收/解码/发送三个任务与编号/纪元/建立时刻——集中在一个不可变对象上，任务只操作
    /// 所属会话的资源，新旧会话互不干扰（直连迁移不再关错 socket、发错连接）。
    /// null = 无会话；读写均持 <see cref="_sessionGate"/>。连接级状态（编号计数器、
    /// 当前编号、纪元）留在连接上——它们跨会话单调。</summary>
    private Session? _session;
    private int _sessionEpoch;

    /// <inheritdoc cref="ISessionAwareStreamConnection{TMessage}.CurrentSessionId" />
    public long CurrentSessionId => Volatile.Read(ref _currentSessionId);

    /// <summary>公共会话编号的分配计数器：仅在真实 TCP 会话建立时递增（有间隔、单调、不复用）；checked 防静默回绕。</summary>
    private long _sessionCounter;

    /// <summary>当前会话编号：0 = 无会话。分配/归零都必须先于对应状态对外发布（线性化点，见 CommunicationStateChanging）。</summary>
    private long _currentSessionId;

    private readonly Channel<SessionSendEntry> _sendQueue;
    private readonly Channel<SessionMessage<TMessage>> _messageRelay;
    private readonly ConnectionMetrics _metrics;

    /// <summary>已注册、尚未终结的会话绑定发送条目：会话拆除时立即 fault，避免调用方空等重连全程。</summary>
    private readonly ConcurrentDictionary<SessionSendEntry, byte> _pendingSessionSends = new();
    private readonly SemaphoreSlim _sendLock = new(initialCount: 1);
    private readonly SemaphoreSlim _acceptLock = new(1, 1);
    private readonly ILogger _logger;
    private int _retryInProgress;
    private readonly RetryDelayScheduler _retryScheduler;
    private int _started;

    /// <summary>连接生命周期令牌源：Start 时与用户令牌链接；取消（用户取消或 Dispose）即拆线停机。</summary>
    private CancellationTokenSource? _lifetimeCts;
    private CancellationTokenRegistration _lifetimeRegistration;

    /// <summary>等待连接就绪的挂起等待者；进入 Connected 时完成并清空，Dispose 时取消。</summary>
    private TaskCompletionSource<bool>? _whenConnected;

    /// <summary>
    /// 创建一条连接。
    /// </summary>
    /// <param name="framing">帧定界策略（连接级固定）。</param>
    /// <param name="codec">帧内编解码（连接级固定）。</param>
    /// <param name="ipAddress">主动模式为远端地址；被动模式为监听地址。</param>
    /// <param name="port">端口。</param>
    /// <param name="isActive">true 主动连接，false 被动监听。</param>
    /// <param name="options">可调参数（构造时校验，非法取值立即抛异常）。</param>
    /// <param name="logger">可选日志：连接重试、会话故障、用户回调异常等内部事件输出到日志。</param>
    public StreamConnection(
        IFramer framing,
        ICodec<TMessage> codec,
        IPAddress ipAddress,
        int port,
        bool isActive,
        StreamConnectionOptions? options = null,
        ILogger? logger = null)
    {
        _framing = framing ?? throw new ArgumentNullException(nameof(framing));
        _codec = codec ?? throw new ArgumentNullException(nameof(codec));
        IpAddress = ipAddress ?? throw new ArgumentNullException(nameof(ipAddress));
        Port = port;
        IsActive = isActive;
        _options = options ?? new StreamConnectionOptions();
        _options.Validate();
        _logger = logger ?? NullLogger.Instance;

        _sendQueue = Channel.CreateBounded<SessionSendEntry>(new BoundedChannelOptions(_options.SendQueueCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });

        // 消息通道归连接所有、跨会话复用：会话重建偶发新旧解码循环并存，不能声明 SingleWriter。
        if (_options.ReceiveQueueCapacity > 0)
        {
            _messageRelay = Channel.CreateBounded<SessionMessage<TMessage>>(new BoundedChannelOptions(_options.ReceiveQueueCapacity)
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait, // 消费慢时解码暂停，TCP 背压传导到对端
            });
        }
        else
        {
            _messageRelay = Channel.CreateUnbounded<SessionMessage<TMessage>>(new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            });
        }

        _retryScheduler = new RetryDelayScheduler(
            isActive ? _options.ConnectRetryDelayMs : _options.AcceptRetryDelayMs,
            _options.MaxRetryDelayMs);

        _metrics = new ConnectionMetrics($"{IpAddress}:{Port}");
    }

    /// <summary>连接是否已停机（Dispose 或 Start 令牌取消）。停机后不可再用，需新建连接。</summary>
    public bool IsDisposed
        => Volatile.Read(ref _disposeStage) == DisposalComplete;

    private void ThrowIfDisposed()
    {
        if (IsDisposed)
            throw new ObjectDisposedException(nameof(StreamConnection<TMessage>));
    }

    /// <inheritdoc />
    public void Start(CancellationToken ct)
    {
        ThrowIfDisposed();

        // 只允许启动一次：并发/重复 Start 会各自建立 socket 并互相覆盖（泄漏连接）
        if (Interlocked.Exchange(ref _started, 1) != 0)
            throw new InvalidOperationException("连接已启动；如需重建连接请调用 Reconnect()。");

        // ct 是连接的生命周期令牌：取消它会停止连接/重连并拆线（进入 Disconnected 终态）
        _lifetimeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _lifetimeRegistration = _lifetimeCts.Token.Register(
            static self => ((StreamConnection<TMessage>)self!).Shutdown(), this);

        StartCore(_lifetimeCts.Token);
    }

    /// <summary>内部启动入口（重连流程复用），不做一次性启动校验。</summary>
    private void StartCore(CancellationToken ct)
        => _ = StartAsync(ct, Interlocked.Increment(ref _acceptLoopId));

    private async Task StartAsync(CancellationToken ct, int loopId)
    {
        var connected = false;
        try
        {
            while (!connected && !ct.IsCancellationRequested && !IsDisposed)
            {
                CommunicationStateChanging(ConnectionState.Connecting);
                try
                {
                    if (!IsActive && Volatile.Read(ref _acceptLoopId) != loopId)
                        return; // 已被更新的接受循环取代：静默退出，不再竞争 accept

                    var socket = IsActive ? await ConnectAsync(ct).ConfigureAwait(false) : await AcceptAsync(ct, loopId).ConfigureAwait(false);
                    if (socket is null)
                        return; // 排队等到锁时已被取代（AcceptAsync 内部的代次检查）

                    // 停机与"连接已建立"的竞速守门（#68）：Shutdown 可能在传输建立完成后、
                    // 发布前的窗口内完成——终态已广播、会话已拆除。此时发布 Connected 会翻转
                    // 终态并重建会话；把 socket 留进 _socket 则无人负责关闭（Shutdown 已走过
                    // 关闭点），泄漏到终结器。判定与赋值同锁于 Shutdown 末尾的关闭块：窗口闭合。
                    lock (_sessionGate)
                    {
                        if (ct.IsCancellationRequested || IsDisposed)
                        {
                            socket.Dispose();
                            return;
                        }

                        // 被取代的旧 socket（回调内 Reconnect 重入、并发双循环的直连迁移）：
                        // 新连接接管字段时旧值已无人认领（其会话从未建立或已被拆除），
                        // 当场释放不泄漏。正常路径此处恒为 null（会话拆除时已清理）。
                        _socket?.Dispose();
                        _socket = socket;
                    }

                    _retryScheduler.Reset();
                    CommunicationStateChanging(ConnectionState.Connected);
                    connected = true;
                }
                catch (Exception ex) when (!ct.IsCancellationRequested && !IsDisposed)
                {
                    var delay = _retryScheduler.NextDelayMs();
                    _logger.LogInformation("Connect to {Remote} failed (attempt {Attempt}): {Message}; retry in {Delay}ms (active={IsActive})",
                        $"{IpAddress}:{Port}", _retryScheduler.Attempt, ex.Message, delay, IsActive);
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // 用户取消连接流程
        }
    }

    /// <summary>IPv6 双栈 socket：同一 socket 同时支持 IPv4 与 IPv6 的连接/监听。</summary>
    private static Socket CreateTcpSocket()
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        try
        {
            socket.DualMode = true;
            return socket;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    /// <summary>双栈监听地址归一：0.0.0.0 绑定到 IPv6 的 ::（v4 流量经映射地址到达）。</summary>
    private static IPAddress NormalizeListenAddress(IPAddress address)
        => address.Equals(IPAddress.Any) ? IPAddress.IPv6Any : address;

    private async Task<Socket> ConnectAsync(CancellationToken ct)
    {
        // 连接侧按目标地址族建 socket：目标是 IPv4 字面量时用纯 IPv4 socket
        // （双栈 socket 对 v4 映射地址的连接在部分 Windows 环境下有额外延迟，且双栈对指定 v4 目标无收益）
        var socket = IpAddress.AddressFamily == AddressFamily.InterNetwork
            ? new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            : CreateTcpSocket();
        try
        {
#if NETSTANDARD2_0
            // ns2.0 的 SocketTaskExtensions 无带 ct 的重载：IPAddress[] 形式直连（无 DNS）+ 可取消等待
            await SocketTaskExtensions.ConnectAsync(socket, new[] { IpAddress }, Port).WaitAsync(ct).ConfigureAwait(false);
#else
            await socket.ConnectAsync(IpAddress, Port, ct).ConfigureAwait(false);
#endif
        }
        catch
        {
            // 连接失败/取消同样释放本次尝试的 socket，否则失败重试一次泄漏一个
            socket.Dispose();
            throw;
        }

        ConfigureSocket(socket);
        return socket;
    }

    private async Task<Socket?> AcceptAsync(CancellationToken ct, int loopId)
    {
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            // 逐次尝试获取锁、重试延迟在锁外等待：绑定/接受持续失败期间不堵死其它
            // 接受循环（#47 防御性修复——旧实现整个重试循环持锁，故障期被放大为全停）
            await _acceptLock.WaitAsync(ct).ConfigureAwait(false);
            Socket? accepted = null;
            int? retryDelay = null;
            try
            {
                // 代次门控（#47 阶段二）：排队等到锁时若已被更新的循环取代，静默退出——
                // 监听器的创建/关闭/accept 只归属当代循环，杜绝僵尸循环泄漏监听器
                // （泄漏的监听器无人 accept，客户端 SYN 进入 backlog 后 ConnectAsync 永久挂起）
                if (Volatile.Read(ref _acceptLoopId) != loopId)
                    return null;

                var listener = InitServer(ct);

                try
                {
#if NETSTANDARD2_0
                    // FromAsync 无法取消；停机时 listener 被释放，挂起的 accept 以异常收尾
                    accepted = await Task.Factory.FromAsync(listener.BeginAccept, listener.EndAccept, null).ConfigureAwait(false);
#else
                    accepted = await listener.AcceptAsync(ct).ConfigureAwait(false);
#endif
                }
                catch (Exception ex) when (!ct.IsCancellationRequested && !IsDisposed)
                {
                    retryDelay = _retryScheduler.NextDelayMs();
                    _logger.LogWarning(ex, "Accept on {Local} failed (attempt {Attempt}); retry in {Delay}ms.",
                        $"{IpAddress}:{Port}", _retryScheduler.Attempt, retryDelay.Value);
                }

                if (accepted is not null)
                {
                    ConfigureSocket(accepted);

                    // 单客户端模式：accept 到第一个客户端后关闭监听 socket，
                    // 后续连接在 TCP 层被立即拒绝。代次门控保证此处 _server 属于当代循环，
                    // 不会误关/漏关其它循环的监听器。
                    if (_options.AcceptFirstClientOnly)
                    {
                        lock (_listenerGate)
                        {
                            _server?.Dispose();
                            _server = null;
                        }
                    }
                }
            }
            finally
            {
                lock (_listenerGate)
                {
                    if (!IsDisposed)
                        _acceptLock.Release();
                }
            }

            if (accepted is not null)
                return accepted;

            await Task.Delay(retryDelay ?? _retryScheduler.NextDelayMs(), ct).ConfigureAwait(false);
        }
    }

    private Socket InitServer(CancellationToken ct)
    {
        lock (_listenerGate)
        {
            ct.ThrowIfCancellationRequested();
            // Shutdown 先标记终态，再取得同一把锁释放；迟到的初始化不得发布监听器。
            if (IsDisposed)
                throw new OperationCanceledException(ct);
            if (_server != null)
                return _server;

            var listener = CreateTcpSocket();
            try
            {
                listener.Blocking = false;
                // 允许重绑覆盖遗留的 TIME_WAIT：服务端主动关闭（用户 Reconnect/停机）后立即重新
                // 监听不受 2MSL 限制（Linux 上没有该选项会遇到 EADDRINUSE；Windows 实测宽松，
                // 一并设置保持跨平台行为一致——#47 防御性修复）
                listener.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                listener.Bind(new IPEndPoint(NormalizeListenAddress(IpAddress), Port));
                listener.Listen(0);
                _server = listener; // 全部成功才转移所有权；失败保持字段为空，下一轮可重试。
                return listener;
            }
            catch
            {
                listener.Dispose();
                throw;
            }
        }
    }

    /// <summary>统一配置已连接 socket：非阻塞、接收缓冲、可选 TCP KeepAlive（半开连接探测）。</summary>
    private void ConfigureSocket(Socket socket)
    {
        socket.Blocking = false;
        socket.ReceiveBufferSize = _options.SocketReceiveBufferSize;

        if (!_options.TcpKeepAlive)
            return;

        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
#if NETSTANDARD2_0
        // netfx 没有TcpKeepAliveTime/Interval 选项名：Windows 上用 SIO_KEEPALIVE_VALS 设置等价参数。
        // netstandard2.0 资产实际运行环境主要是 .NET Framework（仅 Windows）；失败只记日志，
        // 保留系统默认 KeepAlive 参数（2 小时）兜底。
        try
        {
            const int SioKeepAliveValues = -1744830460;
            var values = new byte[12];
            BitConverter.GetBytes(1).CopyTo(values, 0);                               // onOff = 1
            BitConverter.GetBytes(_options.KeepAliveTimeMs).CopyTo(values, 4);        // 首次探测前静默 ms
            BitConverter.GetBytes(_options.KeepAliveIntervalMs).CopyTo(values, 8);    // 探测间隔 ms
            socket.IOControl(SioKeepAliveValues, values, null);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "SIO_KEEPALIVE_VALS 设置失败（非 Windows 平台？），回退系统默认 KeepAlive 参数。");
        }
#else
        socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, KeepAliveSeconds(_options.KeepAliveTimeMs));
        socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, KeepAliveSeconds(_options.KeepAliveIntervalMs));
#endif
    }

#if !NETSTANDARD2_0
    // 选项已校验为正数；先减后除避免 int.MaxValue 向上取整时溢出。
    internal static int KeepAliveSeconds(int milliseconds) => (milliseconds - 1) / 1000 + 1;
#endif

    /// <summary>立即进入重连流程。</summary>
    /// <exception cref="InvalidOperationException">连接尚未 <see cref="Start"/>（无可重连的会话）。</exception>
    public void Reconnect()
    {
        ThrowIfDisposed();
        if (_lifetimeCts is null)
            throw new InvalidOperationException("连接尚未 Start，无法 Reconnect。");
        CommunicationStateChanging(ConnectionState.Retry);
    }

    /// <summary>
    /// 会话任务内部的故障重连入口。<paramref name="observedEpoch"/> 是故障发生时所属会话的
    /// 编号；执行时若会话已被替换（编号过期），说明故障会话早已被拆除重建，跳过——
    /// 否则迟到的重连会误杀刚建立的新会话。用户显式调用 <see cref="Reconnect"/> 无此约束。
    /// </summary>
    private void ReconnectStale(int observedEpoch)
        => CommunicationStateChanging(ConnectionState.Retry, observedEpoch);

    /// <summary>
    /// 从会话任务内部调度重连。不能在任务内直接调 <see cref="Reconnect"/>：
    /// StopSession 会等待各会话任务退出，而调用方正卡在 Reconnect 里（自等待 2 秒超时）。
    /// 派发到线程池后调用方任务可立即返回；<c>_retryInProgress</c> 的 CAS 防重入风暴。
    /// </summary>
    private void ScheduleReconnect(int epoch)
    {
        if (IsDisposed)
            return;

        _ = Task.Run(() =>
        {
            try
            {
                ReconnectStale(epoch);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Scheduled reconnect failed.");
            }
        });
    }

    private void CommunicationStateChanging(ConnectionState newState, int? observedEpoch = null)
    {
        // 发布段（_publishGate 锁内）：迟到/过期判定 + 编号纪元簿记 + State 写入。
        // 判定与写入必须原子：否则"已通过停机检查、尚未写入 State"的发布线程被抢占，
        // 停机线程在此间隙写入 Disconnected 后，迟到的 Retry/Connected 会把终态翻转
        // （并发 Reconnect 与 Dispose 竞速实测可命中）。锁内无 await、无用户回调。
        lock (_publishGate)
        {
            // 停机后到达的迟到发布（竞速中的连接成功、用户 Reconnect、会话故障重连）整体
            // 丢弃：其后任何非终态发布都会把对外 State 翻成 Connected/Retry、重分配/归零
            // 会话编号——终态必须是最后对外可见的状态。Shutdown 自身的 Disconnected 发布
            // 不受影响（停机标记先于本调用置位，且停机是 Disconnected 的唯一来源）。
            if (IsDisposed && newState != ConnectionState.Disconnected)
                return;

            // 迟到的过期故障（会话已被替换，epoch 不匹配）：在任何对外可见的发布之前丢弃。
            // 若放到 Retry 分支的 gate 内才检查（旧实现），State=Retry 的发布与 CurrentSessionId
            // 归零已经污染了完全存活的会话——WaitForConnectedAsync 悬挂、活会话编号被误判失效。
            // gate 内的权威复查仍然保留，防检查与发布之间 epoch 变化的窄竞态。
            if (newState == ConnectionState.Retry && observedEpoch is { } staleEpoch
                && Volatile.Read(ref _sessionEpoch) != staleEpoch)
                return;

            // 公共会话编号的线性化点：Connected 对外发布（事件回调/等待器完成）之前完成分配；
            // 会话纪元（epoch）同样在发布之前递增——发布后到达的旧纪元幽灵故障在函数入口
            // 即被判为过期丢弃，不会卷进"已发布、任务未创建"的新生会话（评审附带发现收口）。
            if (newState == ConnectionState.Connected)
            {
                var id = checked(Interlocked.Increment(ref _sessionCounter));
                Volatile.Write(ref _currentSessionId, id);
                Interlocked.Increment(ref _sessionEpoch);
            }
            else if (newState is ConnectionState.Retry or ConnectionState.Disconnected)
            {
                Volatile.Write(ref _currentSessionId, 0);
                if (newState == ConnectionState.Retry)
                    _metrics.Reconnect(); // 过期幽灵已在锁内被丢弃，不会计入
            }

            State = newState;
        }

        // 通知段（锁外）：用户回调可能重入（回调内 Reconnect/Dispose），不得持有发布锁。
        try
        {
            ConnectionChanged?.Invoke(this, newState);
        }
        catch (Exception ex)
        {
            // 用户事件处理器抛异常不能中断状态机推进（否则 State 已变而会话未重启）
            _logger.LogWarning(ex, "ConnectionChanged handler threw.");
        }

        if (newState == ConnectionState.Connected)
        {
            // 唤醒所有 WaitForConnectedAsync 等待者；下次离开 Connected 再等时重新创建
            Interlocked.Exchange(ref _whenConnected, null)?.TrySetResult(true);
        }

        switch (newState)
        {
            case ConnectionState.Connected:
                StartSession(Volatile.Read(ref _currentSessionId), Volatile.Read(ref _sessionEpoch));
                break;

            case ConnectionState.Retry:
                if (IsDisposed)
                    return;

                if (Interlocked.CompareExchange(ref _retryInProgress, 1, 0) != 0)
                    return;

                try
                {
                    Session? session = null;
                    lock (_sessionGate)
                    {
                        // 取锁等待期间停机可能已完成：不拆除、不派发新循环（_lifetimeCts
                        // 已被 Shutdown 置空，继续执行会对 null 解引用）
                        if (IsDisposed)
                            return;

                        if (observedEpoch is { } epoch && Volatile.Read(ref _sessionEpoch) != epoch)
                            return; // 故障会话已被替换，避免误杀新会话

                        _sessionEpoch++; // 同会话的其它迟到故障立即过期，防止二次拆建
                        FaultPendingSessionSends(); // 拆除时立即失败挂起的会话绑定发送（先于任务拆除）
                        session = DetachSession();
                    }

                    // 锁外收尾被拆除会话（有界 ≤2s），再派发新循环——顺序与旧实现一致
                    WaitAndDisposeSession(session);

                    // 取锁后停机仍可能已完成（_lifetimeCts 被 Shutdown 回收）：放弃派发，
                    // 静默让位终态；令牌源在取消与回收之间的窗口取 Token 可能抛 ODE，一并容忍。
                    var lifetime = _lifetimeCts;
                    if (lifetime is null)
                        return;
                    try
                    {
                        StartCore(lifetime.Token);
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _retryInProgress, 0);
                }

                break;
        }
    }

    private void StartSession(long sessionId, int epoch)
    {
        Session? superseded;
        lock (_sessionGate)
        {
            // 停机守门（#68）：本方法唯一的迟到路径是"Connected 回调内同步 Dispose"——
            // 发布已过守门、停机随后完成，外层才轮到创建会话。终态后不再创建会话任务
            //（此时 _sendLock/_acceptLock 已释放，迟到的 worker 只会在已释放的锁上故障）。
            if (IsDisposed)
                return;

            // 纪元现行性守门（#68）：本发布帧的纪元已被更新的重连取代——典型是
            // Connected 回调内同步 Reconnect()（回调里 Retry 已递增纪元并派发新循环），
            // 或发布窗口内现行纪元的故障重连。迟到帧不得以过期编号/纪元重建会话：
            // 编号会被复用、会拆掉新会话刚建立的资源（旧"兜底重建"曾在该路径撕裂会话）。
            // 放弃创建，连接由取代者的循环完成；被取代帧的 socket 在新循环覆盖 _socket
            // 时释放。sessionId==0 同理：编号已被归零说明拆除正在并发进行。
            if (Volatile.Read(ref _sessionEpoch) != epoch || sessionId == 0)
                return;

            var socket = _socket;
            if (socket is null)
                return; // 防御：无交接 socket（正常路径不会发生——发布前必有赋值）

            superseded = DetachSession();

            var cts = new CancellationTokenSource();
            // 段尺寸对齐接收缓冲：大报文（64KB 级）在默认 4KB 段下跨 16 段重组，
            // 对齐后整帧常驻单段（64KB 基准实测见 bench/README）
            var pipe = new Pipe(new PipeOptions(minimumSegmentSize: _options.SocketReceiveBufferSize));

            var session = new Session(sessionId, epoch, socket, pipe, cts, Stopwatch.GetTimestamp());

            var maxIncompleteFrameBytes = _options.MaxIncompleteFrameBufferBytes > 0
                ? _options.MaxIncompleteFrameBufferBytes
                : _framing.MaxPayloadBytes + 4096;

            var decoder = new FrameDecoder<TMessage>(
                pipe.Reader, _framing, _codec, _messageRelay, _metrics, sessionId,
                maxIncompleteFrameBytes, _options.IncompleteFrameTimeoutMs,
                _options.DecodeErrorPolicy, RaiseFrameError);

            session.SetTasks(
                WatchSessionFaultsAsync(ReceiveLoopAsync(session), session, epoch),
                WatchSessionFaultsAsync(decoder.RunAsync(cts.Token), session, epoch),
                WatchSessionFaultsAsync(SendWorkerAsync(session), session, epoch));
            _session = session;
        }

        // 锁外收尾被取代会话（有界等待 ≤2s + 释放 CTS）：拆除等待不再阻塞 _sessionGate，
        // 并发的新会话建立/拆除不必排队（#68 已知限制①收口）。
        WaitAndDisposeSession(superseded);
    }

    /// <summary>
    /// 会话任务守护：会话停止引发的取消视为正常退出；其余异常（socket 故障、帧解码失败、
    /// 不完整帧超限、未完成帧超时、发送失败）统一转为断线重连（按 <paramref name="epoch"/>
    /// 防止过期故障误杀新会话）。
    /// </summary>
    private async Task WatchSessionFaultsAsync(Task sessionTask, Session session, int epoch)
    {
        try
        {
            await sessionTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (session.Cts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Session loop faulted; scheduling reconnect.");
            ScheduleReconnect(epoch);
        }
    }

    /// <summary>
    /// 会话拆除（Retry/停机）时立即 fault 所有挂起的会话绑定发送：排队中未写出的条目以
    /// <see cref="SessionExpiredException"/> 结束，调用方不必空等重连；已认领（正在写出）的条目
    /// 由写出路径给出结果。普通条目不受影响（按既有语义留给新会话续发）。
    /// 只在真正的拆除点调用——<see cref="StartSession"/> 开头对旧会话的清理不能调用，
    /// 否则会误杀"Connected 已发布、新会话任务尚未就绪"窗口内注册的新条目；该路径残留的
    /// 旧会话条目由发送 worker 认领时的编号校验兜底（见 <see cref="SendWorkerAsync"/>）。
    /// </summary>
    private void FaultPendingSessionSends()
    {
        foreach (var entry in _pendingSessionSends.Keys)
            entry.TryExpire(new SessionExpiredException(
                entry.SessionId, $"会话 {entry.SessionId} 已终止（断线重连/停机），消息未写出。"));
    }

    private void StopSession()
    {
        Session? session;
        lock (_sessionGate)
        {
            session = DetachSession();
        }

        // 锁外收尾：停机的总阻塞时长与旧实现一致（有界 ≤2s），但不再占住会话锁
        WaitAndDisposeSession(session);
    }

    /// <summary>
    /// 取走并快速中止当前会话（须持 <see cref="_sessionGate"/>）：结算时长指标、取消会话
    /// 令牌、唤醒解码读取、关闭会话 socket。关 socket 在等待任务<strong>之前</strong>——
    /// netfx 上取消令牌无法中止已提交的接收，只有关 socket 能立即解锁阻塞 I/O（先中止
    /// I/O 再等待任务，#68）。锁内无任何等待；任务收尾由 <see cref="WaitAndDisposeSession"/>
    /// 在锁外完成。
    /// </summary>
    private Session? DetachSession()
    {
        var session = _session;
        _session = null;
        if (session is null)
            return null;

        // 会话时长指标：从建立到拆除的真实存活时间
        _metrics.SessionEnded((Stopwatch.GetTimestamp() - session.StartedAt) / (double)Stopwatch.Frequency);

        if (!session.Cts.IsCancellationRequested)
            session.Cts.Cancel();

        // 唤醒可能阻塞在 ReadAsync 的解码循环（writer.CompleteAsync 由接收循环的
        // finally 负责；这里是兜底，二者都会让解码循环自然收尾退出）
        session.Pipe.Reader.CancelPendingRead();

        // 只关闭本会话的 socket（不关闭则每次重连泄漏一个）：直连迁移中 _socket 字段
        // 可能已指向新会话的连接，字段与旧 socket 相同时才同步置空。
        try
        {
            if (session.Socket.Connected)
                session.Socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Socket shutdown on session stop failed.");
        }

        session.Socket.Dispose();
        if (ReferenceEquals(_socket, session.Socket))
            _socket = null;

        return session;
    }

    /// <summary>锁外有界等待会话任务退出并释放取消源（≤2 秒；任务异常仅观察，不外抛）。</summary>
    private void WaitAndDisposeSession(Session? session)
    {
        if (session is null)
            return;

        try
        {
            Task.WhenAll(session.Tasks).Wait(TimeSpan.FromSeconds(2));
        }
        catch (AggregateException)
        {
            // 会话任务在取消/断连时可能以异常结束，此处仅等待其退出。
        }

        session.Cts.Dispose();
    }

    /// <summary>接收循环：只操作所属会话的资源（socket/Pipe/令牌）——直连迁移中字段被
    /// 新会话接管也不受影响，退出仅由本会话的取消或 socket 关闭驱动。</summary>
    private async Task ReceiveLoopAsync(Session session)
    {
        var socket = session.Socket;
        var writer = session.Pipe.Writer;
        var ct = session.Cts.Token;
        var epoch = session.Epoch;
        try
        {
            while (true)
            {
                var memory = writer.GetMemory(_options.SocketReceiveBufferSize);
                var count = await ReceiveWithIdleTimeoutAsync(socket, memory, ct).ConfigureAwait(false);
                writer.Advance(count);

                if (count > 0)
                {
                    InvokeRawBytesReceived(memory[..count]);
                    _metrics.AddBytesReceived(count);
                    await writer.FlushAsync(ct).ConfigureAwait(false);
                }
                else
                {
                    // 对端正常关闭（收到 FIN）
                    ScheduleReconnect(epoch);
                    break;
                }
            }
        }
        finally
        {
            // 字节流结束：解码循环把已缓冲的完整帧投递完后退出
            await writer.CompleteAsync().ConfigureAwait(false);
        }
    }

    /// <summary>接收一字节块；开启接收空闲超时后，静默超时判定为连接死亡（半开兜底）。</summary>
    private async Task<int> ReceiveWithIdleTimeoutAsync(Socket socket, Memory<byte> memory, CancellationToken ct)
    {
        if (_options.ReceiveIdleTimeoutMs <= 0)
            return await socket.ReceiveAsync(memory, SocketFlags.None, ct).ConfigureAwait(false);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        linked.CancelAfter(_options.ReceiveIdleTimeoutMs);
        try
        {
            var count = await socket.ReceiveAsync(memory, SocketFlags.None, linked.Token).ConfigureAwait(false);
            if (count == 0 && linked.IsCancellationRequested)
            {
                // 部分平台（Windows）把取消中的接收折算成 0 字节完成而非抛取消异常——
                // 不加区分会误判为对端正常关闭（FIN），语义应为空闲超时会话故障
                throw new SessionFaultException($"接收空闲超过 {_options.ReceiveIdleTimeoutMs}ms，判定连接死亡。");
            }

            return count;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new SessionFaultException($"接收空闲超过 {_options.ReceiveIdleTimeoutMs}ms，判定连接死亡。");
        }
    }

    /// <summary>发送 worker：只向所属会话的 socket 写出（编号校验防跨会话错发之外，
    /// 连接目标也随会话固定——直连迁移不会把旧条目发到新连接上）。</summary>
    private async Task SendWorkerAsync(Session session)
    {
        // 发送失败（socket 故障/会话失效）不在此吞掉：上抛给会话守护 → 断线重连，
        // 未发送的普通消息留在连接级队列，由重连后的新 worker 继续发送。
        var socket = session.Socket;
        var sessionId = session.Id;
        var ct = session.Cts.Token;
        var reader = _sendQueue.Reader;
        while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            while (true)
            {
                // 上一帧写完后也可能已拆除会话；取消的 worker 不再认领下一条，
                // 否则它会出队后在写入处抛取消，丢掉本应由新会话续发的普通条目。
                ct.ThrowIfCancellationRequested();
                if (!reader.TryRead(out var entry))
                    break;

                if (!entry.IsSessionBound)
                {
                    await SendFramedAsync(entry.Message, ct, socket).ConfigureAwait(false);
                    _metrics.FrameSent();
                    continue;
                }

                // 编号校验（防跨会话错发）：绑定旧会话的残留条目绝不能发到当前会话的 socket 上。
                // 覆盖 Connected→Connected 直连拆除（StartSession 内的会话清理刻意不
                // 清扫，保护"刚注册的新条目"）等一切条目跨会话存活的路径——
                // "会话绑定消息绝不转移到新会话重放"的保证在此闭合
                if (entry.SessionId != sessionId)
                {
                    entry.TryExpire(new SessionExpiredException(
                        entry.SessionId, $"会话 {entry.SessionId} 已失效（条目跨会话残留），消息未写出。"));
                    continue;
                }

                // 认领 = 调用方取消的提交点：认领成功后写入只受会话令牌控制
                if (!entry.TryClaimForSend())
                    continue;

                try
                {
                    await SendFramedAsync(entry.Message, ct, socket).ConfigureAwait(false);
                    entry.Complete(); // 整帧已写入本机 socket，任务成功完成
                    _metrics.FrameSent();
                }
                catch (OperationCanceledException ex) when (ct.IsCancellationRequested)
                {
                    entry.Fault(new SessionExpiredException(
                        entry.SessionId, $"会话 {entry.SessionId} 在整帧写出前终止。", ex));
                    throw;
                }
                catch (Exception ex)
                {
                    // 对调用方只暴露文档承诺的失败类型：internal 的 SessionFaultException
                    // （会话拆除竞速时 SendRawAsync 的"无可用连接"）与 ObjectDisposedException
                    // （socket 已释放）统一收敛为会话失效
                    entry.Fault(ex is SessionFaultException or ObjectDisposedException
                        ? new SessionExpiredException(entry.SessionId, $"会话 {entry.SessionId} 在整帧写出前终止。", ex)
                        : ex);
                    throw;
                }
            }
        }
    }

    /// <summary>
    /// 发送编码缓冲的自适应初始尺寸（高水位记忆）：稳态消息尺寸相近的协议（设备周期上报）
    /// 直接按上一帧大小起租，消掉从 EncodeBufferInitialSize（默认 1KB）起的几何增长梯子——
    /// 大报文场景的反复跨桶租借与其中间拷贝显著减少。发送 worker 单线程访问，无需同步。
    /// </summary>
    private int _encodeBufferHighWater;

    private int AdaptiveEncodeBufferSize
    {
        get
        {
            // netstandard2.0 无 Math.Clamp：显式收拢到 [初始配置, 1MB]
            var size = _encodeBufferHighWater;
            if (size < _options.EncodeBufferInitialSize)
                size = _options.EncodeBufferInitialSize;
            if (size > MaxAdaptiveEncodeBufferBytes)
                size = MaxAdaptiveEncodeBufferBytes;
            return size;
        }
    }

    /// <summary>自适应缓冲封顶：超过此值回退到配置初始值（防御异常尺寸的偶发大帧抬高常态水位）。</summary>
    private const int MaxAdaptiveEncodeBufferBytes = 1024 * 1024;

    private async Task SendFramedAsync(TMessage message, CancellationToken ct, Socket socket)
    {
        // 流式编码：单缓冲、零 memcpy（BeginFrame 占位 → codec 直接写 → EndFrame 回填/收尾）
        if (_options.UseStreamingEncode && _framing is IStreamingFramer streaming)
        {
            using var frame = new PooledBufferWriter(AdaptiveEncodeBufferSize);
            streaming.BeginFrame(frame);
            _codec.Encode(message, frame, ct);
            streaming.EndFrame(frame);
            RememberEncodeHighWater(frame.WrittenCount);
            await SendRawAsync(frame.WrittenMemory, ct, socket).ConfigureAwait(false);
            return;
        }

        // 纯函数编码：序列化产物 → 帧，两段缓冲（含一次 memcpy）
        using var payload = new PooledBufferWriter(AdaptiveEncodeBufferSize);
        _codec.Encode(message, payload, ct);
        using var frame2 = new PooledBufferWriter(payload.WrittenCount + 16);
        _framing.EncodeFrame(payload.WrittenSpan, frame2);
        RememberEncodeHighWater(frame2.WrittenCount);
        await SendRawAsync(frame2.WrittenMemory, ct, socket).ConfigureAwait(false);
    }

    private void RememberEncodeHighWater(int frameBytes)
    {
        if (frameBytes > _encodeBufferHighWater && frameBytes <= MaxAdaptiveEncodeBufferBytes)
            _encodeBufferHighWater = frameBytes;
    }

    /// <summary>向指定会话的 socket 逐段写出；socket 归会话所有（拆除时关闭），
    /// 写出途中被关闭以 ObjectDisposedException 收尾——由 worker 收敛为会话失效。</summary>
    private async Task SendRawAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct, Socket socket)
    {
        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            while (!buffer.IsEmpty)
            {
                var length = await socket.SendAsync(buffer, SocketFlags.None, ct).ConfigureAwait(false);

                // 按实际写出的分片回调：部分发送失败时，已上线字节也可见
                InvokeRawBytesSent(buffer[..length]);
                _metrics.AddBytesSent(length);
                buffer = buffer[length..];
            }
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private void InvokeRawBytesReceived(ReadOnlyMemory<byte> bytes)
    {
        try
        {
            RawBytesReceived?.Invoke(bytes);
        }
        catch (Exception ex)
        {
            // 调试钩子抛异常不能反噬会话（否则会导致重连风暴）
            _logger.LogWarning(ex, "RawBytesReceived handler threw.");
        }
    }

    private void InvokeRawBytesSent(ReadOnlyMemory<byte> bytes)
    {
        try
        {
            RawBytesSent?.Invoke(bytes);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RawBytesSent handler threw.");
        }
    }

    private void RaiseFrameError(FrameErrorEventArgs args)
    {
        try
        {
            FrameError?.Invoke(this, args);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "FrameError handler threw.");
        }
    }

    /// <inheritdoc />
    public Task SendAsync(TMessage message, CancellationToken ct = default)
    {
        _metrics.SendQueueObserved(_sendQueue.Reader.Count);
        return _sendQueue.Writer.WriteAsync(new SessionSendEntry(message), ct).AsTask();
    }

    /// <inheritdoc cref="ISessionAwareStreamConnection{TMessage}.SendInSessionAsync" />
    public async Task SendInSessionAsync(long sessionId, TMessage message, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        // 快速失败（非权威判定；权威在 worker 出队认领与会话拆除 fault）
        if (Volatile.Read(ref _currentSessionId) != sessionId)
            throw NewSessionExpired(sessionId);

        var entry = new SessionSendEntry(sessionId, message);
        _metrics.SendQueueObserved(_sendQueue.Reader.Count);
        _pendingSessionSends.TryAdd(entry, 0);
        try
        {
            // 双检：注册与"拆除归零编号"的竞态——先注册、再复核，失效则以条目携带的同一
            // 异常收尾（同时观察 TCS，避免留下未观察的任务异常）
            if (Volatile.Read(ref _currentSessionId) != sessionId)
            {
                entry.TryExpire(NewSessionExpired(sessionId));
                await entry.CompletionTask.ConfigureAwait(false);
                return;
            }

            // 调用方取消：提交点（worker 认领）之前使条目退出；认领之后无副作用
            using var ctReg = ct.Register(
                static state => ((SessionSendEntry)state!).TryCancelByCaller(), entry);

            // 入队（队列满时等待）。期间允许两类及时结束：调用方取消 → WriteAsync 取消；
            // 会话失效 → 条目被拆除 fault（CompletionTask 先完成，迟到的入队由 worker 跳过）
            var enqueueTask = _sendQueue.Writer.WriteAsync(entry, ct).AsTask();
            var first = await Task.WhenAny(enqueueTask, entry.CompletionTask).ConfigureAwait(false);

            if (first == enqueueTask && enqueueTask.Status == TaskStatus.RanToCompletion)
            {
                // 正常入队：等待写出结果（整帧写完成功 / 会话失效 / 提交点前取消）
                await entry.CompletionTask.ConfigureAwait(false);
                return;
            }

            if (entry.CompletionTask.IsCompleted)
            {
                // 会话拆除已 fault（或提交点前已取消）：以条目结果为准（统一失败类型）；
                // 迟到的入队可能仍会成功，旧信封由 worker 跳过——观察其异常避免未观察任务异常
                ObserveLater(enqueueTask);
                await entry.CompletionTask.ConfigureAwait(false);
                return;
            }

            // 入队失败且与会话无关（调用方取消 / 通道已关闭）：传播入队异常
            await enqueueTask.ConfigureAwait(false);
        }
        finally
        {
            _pendingSessionSends.TryRemove(entry, out _);
        }
    }

    private static SessionExpiredException NewSessionExpired(long sessionId)
        => new(sessionId, $"会话 {sessionId} 已失效（当前无此会话）。");

    /// <summary>后台观察一个可能迟到完成的入队任务的异常，避免未观察任务异常。</summary>
    private static void ObserveLater(Task task)
        => _ = task.ContinueWith(static completed => _ = completed.Exception, TaskScheduler.Default);

    /// <summary>
    /// 一次会话的资源集合（#68）：一次 TCP 连接对应一个不可变的会话对象，集中持有该会话的
    /// socket、Pipe、取消源、接收/解码/发送三个任务与编号/纪元/建立时刻。会话任务只操作
    /// 所属会话的资源——直连迁移、拆除竞速中新旧会话互不干扰；连接级状态（编号计数器、
    /// 当前编号、纪元）不在此处（跨会话单调，归连接所有）。
    /// </summary>
    private sealed class Session
    {
        public Session(long id, int epoch, Socket socket, Pipe pipe, CancellationTokenSource cts, long startedAt)
        {
            Id = id;
            Epoch = epoch;
            Socket = socket;
            Pipe = pipe;
            Cts = cts;
            StartedAt = startedAt;
        }

        public long Id { get; }
        public int Epoch { get; }

        /// <summary>本会话的连接 socket：I/O 唯一入口，拆除时由 <see cref="StreamConnection{TMessage}.DetachSession"/> 关闭。</summary>
        public Socket Socket { get; }

        public Pipe Pipe { get; }
        public CancellationTokenSource Cts { get; }
        public long StartedAt { get; }

        /// <summary>三个会话任务（含守护包装）。在 <c>_sessionGate</c> 内、会话发布到 <c>_session</c> 之前装配。</summary>
        public Task[] Tasks { get; private set; } = Array.Empty<Task>();

        public void SetTasks(Task receive, Task decode, Task sendWorker)
            => Tasks = new[] { receive, decode, sendWorker };
    }

    /// <summary>
    /// 发送队列条目。普通条目（<see cref="SendAsync"/>）无完成源：入队即完成调用方任务，
    /// 未发送的消息跨会话续发（既有语义）。会话绑定条目（<see cref="SendInSessionAsync"/>）
    /// 携带完成源：整帧写入 socket 后成功完成；会话终止时失败、绝不重放。
    ///
    /// 状态字三向互斥（提交点/取消/失效的线性化）：排队中 → 已认领（worker 正在写出，
    /// 只有写出路径能给结果）/ 已取消（调用方在提交点前取消）/ 已失效（会话拆除 fault）。
    /// </summary>
    private sealed class SessionSendEntry
    {
        private const int Queued = 0;
        private const int Claimed = 1;
        private const int CancelledByCaller = 2;
        private const int Expired = 3;

        private readonly TaskCompletionSource<bool>? _completion;
        private int _state;

        public SessionSendEntry(TMessage message)
            => Message = message;

        public SessionSendEntry(long sessionId, TMessage message)
        {
            SessionId = sessionId;
            Message = message;
            _completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public TMessage Message { get; }
        public long SessionId { get; }
        public bool IsSessionBound => _completion is not null;
        public Task CompletionTask => _completion?.Task ?? Task.CompletedTask;

        /// <summary>worker 认领（发送提交点）：仅排队中的条目可发送；已取消/已失效一律跳过。</summary>
        public bool TryClaimForSend()
            => Interlocked.CompareExchange(ref _state, Claimed, Queued) == Queued;

        /// <summary>调用方在提交点前取消：条目退出（worker 跳过），完成源以取消收尾。</summary>
        public void TryCancelByCaller()
        {
            if (Interlocked.CompareExchange(ref _state, CancelledByCaller, Queued) == Queued)
                _completion?.TrySetCanceled(CancellationToken.None);
        }

        /// <summary>会话拆除 fault：仅对排队中的条目生效；已认领的由写出路径给出结果。</summary>
        public void TryExpire(Exception reason)
        {
            if (Interlocked.CompareExchange(ref _state, Expired, Queued) == Queued)
                _completion?.TrySetException(reason);
        }

        public void Complete()
            => _completion?.TrySetResult(true);

        public void Fault(Exception exception)
            => _completion?.TrySetException(exception);
    }

    /// <inheritdoc />
    public Task WaitForConnectedAsync(CancellationToken ct = default)
    {
        if (!IsDisposed && State == ConnectionState.Connected)
            return Task.CompletedTask;

        var tcs = GetOrCreateWhenConnected();
        // 必须在注册后复查：Shutdown 可能已清空旧等待器，随后本次才发布新等待器。
        // 若终态发生在复查之后，Shutdown 会取得已发布的等待器并负责取消。
        if (IsDisposed)
        {
            tcs.TrySetCanceled();
            return tcs.Task;
        }

        return State == ConnectionState.Connected
            ? Task.CompletedTask // 获取等待器的间隙恰好连上了
            : tcs.Task.WaitAsync(ct);
    }

    /// <summary>获取或创建"等待 Connected"的完成源；Connected 时完成并置空，可重复使用。</summary>
    private TaskCompletionSource<bool> GetOrCreateWhenConnected()
    {
        while (true)
        {
            var existing = Volatile.Read(ref _whenConnected);
            if (existing is not null)
                return existing;

            var created = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (Interlocked.CompareExchange(ref _whenConnected, created, null) is null)
                return created;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TMessage> GetMessages(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var reader = _messageRelay.Reader;
        while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            while (reader.TryRead(out var envelope))
                yield return envelope.Message;
        }
    }

    /// <inheritdoc cref="ISessionAwareStreamConnection{TMessage}.GetSessionMessages" />
    public async IAsyncEnumerable<SessionMessage<TMessage>> GetSessionMessages(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var reader = _messageRelay.Reader;
        while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            while (reader.TryRead(out var envelope))
                yield return envelope;
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Shutdown();
        return new ValueTask();
    }

    /// <summary>
    /// 终态拆线（幂等）：DisposeAsync 与"Start 的取消令牌被取消"共用同一停机路径。
    /// 先广播 <see cref="ConnectionState.Disconnected"/>，再停止重连循环、拆除会话、
    /// 完成收发通道（GetMessages 自然结束、后续 SendAsync 抛 ChannelClosedException）。
    /// </summary>
    private void Shutdown()
    {
        if (Interlocked.Exchange(ref _disposeStage, DisposalComplete) != DisposalNotStarted)
            return;

        try
        {
            // 广播终态（用户处理器抛异常由 CommunicationStateChanging 隔离，不影响拆线）
            CommunicationStateChanging(ConnectionState.Disconnected);
        }
        finally
        {
            ConnectionChanged = null;
            FrameError = null;
        }

        // 连接终止：所有连接就绪等待以取消收尾
        Interlocked.Exchange(ref _whenConnected, null)?.TrySetCanceled();

        _lifetimeRegistration.Dispose();
        _lifetimeCts?.Cancel(); // 停止连接/监听重试循环
        FaultPendingSessionSends(); // 停机：挂起的会话绑定发送全部以会话失效收尾（先于通道完成，统一失败类型）
        _sendQueue.Writer.TryComplete();

        StopSession();

        // 消息通道归连接所有：停机是它唯一的完成点（正常完成，枚举端自然结束）
        _messageRelay.Writer.TryComplete();

        // 与 StartAsync 的"停机守门 + 赋值"共用 _sessionGate：连接建立与停机互斥，
        // 刚建立的 socket 不会漏进已完成的停机（#68）。
        lock (_sessionGate)
        {
            if (_socket is { } socket)
            {
                try
                {
                    if (socket.Connected)
                        socket.Shutdown(SocketShutdown.Both);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Socket shutdown on disconnect failed.");
                }
                socket.Dispose();
                _socket = null;
            }
        }

        lock (_listenerGate)
        {
            _server?.Dispose();
            _server = null;
            _acceptLock.Dispose();
        }

        _sendLock.Dispose();
        _lifetimeCts?.Dispose();
        _lifetimeCts = null;
    }
}
