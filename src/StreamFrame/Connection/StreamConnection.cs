using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using StreamFrame.Abstractions;

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

    public string DeviceIpAddress
        => IsActive
            ? IpAddress.ToString()
            : (_socket?.RemoteEndPoint as IPEndPoint)?.Address.ToString() ?? "NA";

    private const int DisposalNotStarted = 0;
    private const int DisposalComplete = 1;
    private int _disposeStage;

    private Socket? _socket;
    private Socket? _server;

    private readonly IFrameCodec _framing;
    private readonly ICodec<TMessage> _codec;
    private readonly StreamConnectionOptions _options;

    private readonly object _sessionGate = new();
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
    private readonly CancellationTokenSource _lifetimeCts = new();
    private int _retryInProgress;

    /// <summary>
    /// 创建一条连接。
    /// </summary>
    /// <param name="framing">帧定界策略（连接级固定）。</param>
    /// <param name="codec">帧内编解码（连接级固定）。</param>
    /// <param name="ipAddress">主动模式为远端地址；被动模式为监听地址。</param>
    /// <param name="port">端口。</param>
    /// <param name="isActive">true 主动连接，false 被动监听。</param>
    /// <param name="options">可调参数。</param>
    public StreamConnection(
        IFrameCodec framing,
        ICodec<TMessage> codec,
        IPAddress ipAddress,
        int port,
        bool isActive,
        StreamConnectionOptions? options = null)
    {
        _framing = framing ?? throw new ArgumentNullException(nameof(framing));
        _codec = codec ?? throw new ArgumentNullException(nameof(codec));
        IpAddress = ipAddress ?? throw new ArgumentNullException(nameof(ipAddress));
        Port = port;
        IsActive = isActive;
        _options = options ?? new StreamConnectionOptions();

        _sendQueue = Channel.CreateBounded<TMessage>(new BoundedChannelOptions(_options.SendQueueCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });
        _messageRelay = Channel.CreateUnbounded<TMessage>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });
    }

    public bool IsDisposed
        => Interlocked.CompareExchange(ref _disposeStage, DisposalComplete, DisposalComplete) == DisposalComplete;

    public void Start(CancellationToken ct)
    {
        if (IsDisposed)
            throw new ObjectDisposedException(nameof(StreamConnection<TMessage>));

        _ = StartAsync(ct);
    }

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
                    Debug.WriteLine($"Connect failed: {ex.Message}; retry in {delay}ms.");
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // 用户取消连接流程
        }
    }

    private async Task<Socket> ConnectAsync(CancellationToken ct)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await socket.ConnectAsync(IpAddress, Port, ct).ConfigureAwait(false);
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
                    var socket = await _server!.AcceptAsync(ct).ConfigureAwait(false);
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
                    Debug.WriteLine($"Accept failed: {ex.Message}; retry in {_options.AcceptRetryDelayMs}ms.");
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

        _server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
        {
            Blocking = false,
        };
        _server.Bind(new IPEndPoint(IpAddress, Port));
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
        socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, _options.KeepAliveTimeMs);
        socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, _options.KeepAliveIntervalMs);
    }

    /// <summary>立即进入重连流程。</summary>
    public void Reconnect()
        => CommunicationStateChanging(ConnectionState.Retry);

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
                Debug.WriteLine($"Scheduled reconnect failed: {ex.Message}");
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
            Debug.WriteLine($"ConnectionChanged handler threw: {ex.Message}");
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

                    Start(_lifetimeCts.Token);
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
            Debug.WriteLine($"Session loop faulted: {ex.Message}");
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
                Debug.WriteLine($"Socket shutdown on session stop: {ex.Message}");
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
        await foreach (var message in _sendQueue.Reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            await SendFramedAsync(message, ct).ConfigureAwait(false);
        }
    }

    private async Task SendFramedAsync(TMessage message, CancellationToken ct)
    {
        // 流式编码：单缓冲、零 memcpy（BeginFrame 占位 → codec 直接写 → EndFrame 回填/收尾）
        if (_options.UseStreamingEncode && _framing is IStreamingFrameCodec streaming)
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
            Debug.WriteLine($"RawBytesReceived handler threw: {ex.Message}");
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
            Debug.WriteLine($"RawBytesSent handler threw: {ex.Message}");
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
            Debug.WriteLine($"FrameError handler threw: {ex.Message}");
        }
    }

    public Task SendAsync(TMessage message, CancellationToken ct = default)
        => _sendQueue.Writer.WriteAsync(message, ct).AsTask();

    public IAsyncEnumerable<TMessage> GetMessages(CancellationToken ct = default)
        => _messageRelay.Reader.ReadAllAsync(ct);

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStage, DisposalComplete) != DisposalNotStarted)
            return;

        ConnectionChanged = null;
        FrameError = null;

        _lifetimeCts.Cancel();
        _lifetimeCts.Dispose();

        _sendQueue.Writer.TryComplete();

        StopSession();

        // 消息通道归连接所有：Dispose 是它唯一的完成点（正常完成，枚举端自然结束）
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
                Debug.WriteLine($"Socket shutdown on dispose: {ex.Message}");
            }
            socket.Dispose();
            _socket = null;
        }

        _server?.Dispose();
        _server = null;

        _sendLock.Dispose();
        _acceptLock.Dispose();
    }
}

/// <summary>
/// 按初始大小租用 ArrayPool 缓冲、可动态扩容的 IWrittenBufferWriter，用于编码中间字节。
/// GetSpan 保证返回至少 sizeHint 长的段（不足时先扩容），写入的数据可立即通过 WrittenSpan 读取。
/// </summary>
internal sealed class PooledBufferWriter : IWrittenBufferWriter, IDisposable
{
    private byte[] _buffer;
    private int _written;

    public PooledBufferWriter(int capacity)
        => _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(capacity, 1));

    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _written);
    public Span<byte> WrittenSpan => _buffer.AsSpan(0, _written);
    public int WrittenCount => _written;

    public void Advance(int count)
    {
        if (count < 0 || _written + count > _buffer.Length)
            throw new ArgumentOutOfRangeException(nameof(count));
        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    private void EnsureCapacity(int sizeHint)
    {
        // 剩余空间必须至少容纳 max(sizeHint, 1) 字节——GetSpan 不允许返回空 span
        // （BuffersExtensions.Write 内部以 GetSpan(0) 多段循环拷贝，依赖非空返回值）。
        var required = _written + Math.Max(sizeHint, 1);
        if (required <= _buffer.Length)
            return;

        var newSize = Math.Max(required, _buffer.Length * 2);
        var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
        Array.Copy(_buffer, newBuffer, _written);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = newBuffer;
    }

    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = Array.Empty<byte>();
    }
}
