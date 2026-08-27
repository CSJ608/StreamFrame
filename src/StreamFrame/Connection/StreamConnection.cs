using System.Buffers;
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
///                                                      │ while(TryDecodeFrame) → codec.Decode → Channel&lt;TMessage&gt;
/// 发送：业务 SendAsync 入队 --有界Channel--> 发送worker → codec.Encode → framing.EncodeFrame → _sendLock → socket
/// </code>
///
/// 会话模型：一次 TCP 连接 = 一个会话（Pipe + 三个会话任务）；断线或会话故障时整个会话
/// 作废重建，而 <c>_messageRelay</c> 消息通道是连接级的、跨会话存活——业务侧的
/// <see cref="GetMessages"/> 枚举在重连前后是同一条稳定流，仅在 Dispose 时正常结束。
/// </summary>
public sealed class StreamConnection<TMessage> : IStreamConnection<TMessage>
{
    public event EventHandler<ConnectionState>? ConnectionChanged;

    /// <summary>帧层诊断事件：解码失败、被定界器丢弃的字节、不完整帧超限。字节已拷贝、可留存。</summary>
    public event EventHandler<FrameErrorEventArgs>? FrameError;

    public Action<ReadOnlyMemory<byte>>? RawBytesReceived { get; set; }
    public Action<ReadOnlyMemory<byte>>? RawBytesSent { get; set; }

    public ConnectionState State { get; private set; }
    public bool IsActive { get; }
    public IPAddress IpAddress { get; }
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

    private Socket? _socket;
    private Socket? _server;

    private readonly IFramer _framing;
    private readonly ICodec<TMessage> _codec;
    private readonly StreamConnectionOptions _options;

#if NET9_0_OR_GREATER
    private readonly Lock _sessionGate = new(); // System.Threading.Lock（net9+，比 monitor 锁更轻量）
#else
    private readonly object _sessionGate = new();
#endif
    private Pipe? _pipe;
    private CancellationTokenSource? _sessionCts;
    private Task? _receiveTask;
    private Task? _decodeTask;
    private Task? _sendWorkerTask;
    private int _sessionEpoch;

    private readonly Channel<TMessage> _sendQueue;
    private readonly Channel<TMessage> _messageRelay;
    private readonly SemaphoreSlim _sendLock = new(initialCount: 1);
    private readonly SemaphoreSlim _acceptLock = new(1, 1);
    private readonly ILogger _logger;
    private int _retryInProgress;
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

        _sendQueue = Channel.CreateBounded<TMessage>(new BoundedChannelOptions(_options.SendQueueCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });

        // 消息通道归连接所有、跨会话复用：会话重建偶发新旧解码循环并存，不能声明 SingleWriter。
        if (_options.ReceiveQueueCapacity > 0)
        {
            _messageRelay = Channel.CreateBounded<TMessage>(new BoundedChannelOptions(_options.ReceiveQueueCapacity)
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait, // 消费慢时解码暂停，TCP 背压传导到对端
            });
        }
        else
        {
            _messageRelay = Channel.CreateUnbounded<TMessage>(new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            });
        }
    }

    public bool IsDisposed
        => Volatile.Read(ref _disposeStage) == DisposalComplete;

    private void ThrowIfDisposed()
    {
        if (IsDisposed)
            throw new ObjectDisposedException(nameof(StreamConnection<TMessage>));
    }

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
        => _ = StartAsync(ct);

    private async Task StartAsync(CancellationToken ct)
    {
        var connected = false;
        try
        {
            while (!connected && !ct.IsCancellationRequested && !IsDisposed)
            {
                CommunicationStateChanging(ConnectionState.Connecting);
                try
                {
                    _socket = IsActive ? await ConnectAsync(ct).ConfigureAwait(false) : await AcceptAsync(ct).ConfigureAwait(false);
                    CommunicationStateChanging(ConnectionState.Connected);
                    connected = true;
                }
                catch (Exception ex) when (!ct.IsCancellationRequested && !IsDisposed)
                {
                    var delay = IsActive ? _options.ConnectRetryDelayMs : _options.AcceptRetryDelayMs;
                    _logger.LogInformation("Connect to {Remote} failed: {Message}; retry in {Delay}ms (active={IsActive})",
                        $"{IpAddress}:{Port}", ex.Message, delay, IsActive);
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
        socket.DualMode = true;
        return socket;
    }

    /// <summary>双栈监听地址归一：0.0.0.0 绑定到 IPv6 的 ::（v4 流量经映射地址到达）。</summary>
    private static IPAddress NormalizeListenAddress(IPAddress address)
        => address.Equals(IPAddress.Any) ? IPAddress.IPv6Any : address;

    private async Task<Socket> ConnectAsync(CancellationToken ct)
    {
        var socket = CreateTcpSocket();
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

    private async Task<Socket> AcceptAsync(CancellationToken ct)
    {
        await _acceptLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                if (_server == null)
                    InitServer();

                try
                {
#if NETSTANDARD2_0
                    var listener = _server!;
                    // FromAsync 无法取消；停机时 listener 被释放，挂起的 accept 以异常收尾
                    var socket = await Task.Factory.FromAsync(listener.BeginAccept, listener.EndAccept, null).ConfigureAwait(false);
#else
                    var socket = await _server!.AcceptAsync(ct).ConfigureAwait(false);
#endif
                    ConfigureSocket(socket);

                    // 单客户端模式：accept 到第一个客户端后关闭监听 socket，
                    // 后续连接在 TCP 层被立即拒绝。
                    if (_options.AcceptFirstClientOnly && _server != null)
                    {
                        _server.Dispose();
                        _server = null;
                    }

                    return socket;
                }
                catch (Exception ex) when (!ct.IsCancellationRequested && !IsDisposed)
                {
                    _logger.LogWarning(ex, "Accept on {Local} failed; retry in {Delay}ms.", $"{IpAddress}:{Port}", _options.AcceptRetryDelayMs);
                    await Task.Delay(_options.AcceptRetryDelayMs, ct).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            _acceptLock.Release();
        }
    }

    private void InitServer()
    {
        if (_server != null)
        {
            _server.Dispose();
            _server = null;
        }

        _server = CreateTcpSocket();
        _server.Blocking = false;
        _server.Bind(new IPEndPoint(NormalizeListenAddress(IpAddress), Port));
        _server.Listen(0);
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
        socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, _options.KeepAliveTimeMs);
        socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, _options.KeepAliveIntervalMs);
#endif
    }

    /// <summary>立即进入重连流程。</summary>
    public void Reconnect()
    {
        ThrowIfDisposed();
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
        State = newState;

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
                StartSession();
                break;

            case ConnectionState.Retry:
                if (IsDisposed)
                    return;

                if (Interlocked.CompareExchange(ref _retryInProgress, 1, 0) != 0)
                    return;

                try
                {
                    lock (_sessionGate)
                    {
                        if (observedEpoch is { } epoch && Volatile.Read(ref _sessionEpoch) != epoch)
                            return; // 故障会话已被替换，避免误杀新会话

                        _sessionEpoch++; // 同会话的其它迟到故障立即过期，防止二次拆建
                        StopSessionCore();
                    }

                    StartCore(_lifetimeCts!.Token);
                }
                finally
                {
                    Interlocked.Exchange(ref _retryInProgress, 0);
                }

                break;
        }
    }

    private void StartSession()
    {
        lock (_sessionGate)
        {
            StopSessionCore();

            var epoch = ++_sessionEpoch;
            var cts = new CancellationTokenSource();
            var token = cts.Token;
            var pipe = new Pipe();

            _sessionCts = cts;
            _pipe = pipe;

            var maxIncompleteFrameBytes = _options.MaxIncompleteFrameBufferBytes > 0
                ? _options.MaxIncompleteFrameBufferBytes
                : _framing.MaxPayloadBytes + 4096;

            var decoder = new FrameDecoder<TMessage>(
                pipe.Reader, _framing, _codec, _messageRelay,
                maxIncompleteFrameBytes, _options.DecodeErrorPolicy, RaiseFrameError);

            _receiveTask = WatchSessionFaultsAsync(ReceiveLoopAsync(pipe.Writer, token, epoch), cts, epoch);
            _decodeTask = WatchSessionFaultsAsync(decoder.RunAsync(token), cts, epoch);
            _sendWorkerTask = WatchSessionFaultsAsync(SendWorkerAsync(token), cts, epoch);
        }
    }

    /// <summary>
    /// 会话任务守护：会话停止引发的取消视为正常退出；其余异常（socket 故障、帧解码失败、
    /// 不完整帧超限、发送失败）统一转为断线重连（按 <paramref name="epoch"/> 防止过期故障误杀新会话）。
    /// </summary>
    private async Task WatchSessionFaultsAsync(Task sessionTask, CancellationTokenSource sessionCts, int epoch)
    {
        try
        {
            await sessionTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (sessionCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Session loop faulted; scheduling reconnect.");
            ScheduleReconnect(epoch);
        }
    }

    private void StopSession()
    {
        lock (_sessionGate)
        {
            StopSessionCore();
        }
    }

    private void StopSessionCore()
    {
        if (_sessionCts is not { } cts)
            return;

        var pipe = _pipe;

        if (!cts.IsCancellationRequested)
            cts.Cancel();

        // 唤醒可能阻塞在 ReadAsync 的解码循环（writer.CompleteAsync 由接收循环的
        // finally 负责；这里是兜底，二者都会让解码循环自然收尾退出）
        pipe?.Reader.CancelPendingRead();

        var tasks = new[] { _receiveTask, _decodeTask, _sendWorkerTask };

        _sessionCts = null;
        _receiveTask = null;
        _decodeTask = null;
        _sendWorkerTask = null;
        _pipe = null;

        var pending = tasks.Where(static task => task is not null).Cast<Task>().ToArray();
        if (pending.Length > 0)
        {
            try
            {
                Task.WhenAll(pending).Wait(TimeSpan.FromSeconds(2));
            }
            catch (AggregateException)
            {
                // 会话任务在取消/断连时可能以异常结束，此处仅等待其退出。
            }
        }

        // 会话已死，关闭旧连接的 socket（不关闭则每次重连泄漏一个，等终结器收场）
        if (_socket is { } socket)
        {
            try
            {
                if (socket.Connected)
                    socket.Shutdown(SocketShutdown.Both);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Socket shutdown on session stop failed.");
            }

            socket.Dispose();
            _socket = null;
        }

        cts.Dispose();
    }

    private async Task ReceiveLoopAsync(PipeWriter writer, CancellationToken ct, int epoch)
    {
        try
        {
            while (true)
            {
                var socket = _socket;
                if (socket is null)
                    break;

                var memory = writer.GetMemory(_options.SocketReceiveBufferSize);
                var count = await ReceiveWithIdleTimeoutAsync(socket, memory, ct).ConfigureAwait(false);
                writer.Advance(count);

                if (count > 0)
                {
                    InvokeRawBytesReceived(memory[..count]);
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
            return await socket.ReceiveAsync(memory, SocketFlags.None, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new SessionFaultException($"接收空闲超过 {_options.ReceiveIdleTimeoutMs}ms，判定连接死亡。");
        }
    }

    private async Task SendWorkerAsync(CancellationToken ct)
    {
        // 发送失败（socket 故障/会话失效）不在此吞掉：上抛给会话守护 → 断线重连，
        // 未发送的消息留在连接级队列，由重连后的新 worker 继续发送。
        var reader = _sendQueue.Reader;
        while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            while (reader.TryRead(out var message))
                await SendFramedAsync(message, ct).ConfigureAwait(false);
        }
    }

    private async Task SendFramedAsync(TMessage message, CancellationToken ct)
    {
        // 流式编码：单缓冲、零 memcpy（BeginFrame 占位 → codec 直接写 → EndFrame 回填/收尾）
        if (_options.UseStreamingEncode && _framing is IStreamingFramer streaming)
        {
            using var frame = new PooledBufferWriter(_options.EncodeBufferInitialSize);
            streaming.BeginFrame(frame);
            _codec.Encode(message, frame, ct);
            streaming.EndFrame(frame);
            await SendRawAsync(frame.WrittenMemory, ct).ConfigureAwait(false);
            return;
        }

        // 纯函数编码：序列化产物 → 帧，两段缓冲（含一次 memcpy）
        using var payload = new PooledBufferWriter(_options.EncodeBufferInitialSize);
        _codec.Encode(message, payload, ct);
        using var frame2 = new PooledBufferWriter(payload.WrittenCount + 16);
        _framing.EncodeFrame(payload.WrittenSpan, frame2);
        await SendRawAsync(frame2.WrittenMemory, ct).ConfigureAwait(false);
    }

    private async Task SendRawAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct)
    {
        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            while (!buffer.IsEmpty)
            {
                var socket = _socket;
                if (socket is null)
                    throw new SessionFaultException("会话已结束，无可用连接。");

                var length = await socket.SendAsync(buffer, SocketFlags.None, ct).ConfigureAwait(false);

                // 按实际写出的分片回调：部分发送失败时，已上线字节也可见
                InvokeRawBytesSent(buffer[..length]);
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

    public Task SendAsync(TMessage message, CancellationToken ct = default)
        => _sendQueue.Writer.WriteAsync(message, ct).AsTask();

    public Task WaitForConnectedAsync(CancellationToken ct = default)
    {
        if (State == ConnectionState.Connected)
            return Task.CompletedTask;

        var tcs = GetOrCreateWhenConnected();
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

    public async IAsyncEnumerable<TMessage> GetMessages(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var reader = _messageRelay.Reader;
        while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            while (reader.TryRead(out var message))
                yield return message;
        }
    }

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
        _sendQueue.Writer.TryComplete();

        StopSession();

        // 消息通道归连接所有：停机是它唯一的完成点（正常完成，枚举端自然结束）
        _messageRelay.Writer.TryComplete();

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

        _server?.Dispose();
        _server = null;

        _sendLock.Dispose();
        _acceptLock.Dispose();
        _lifetimeCts?.Dispose();
        _lifetimeCts = null;
    }
}
