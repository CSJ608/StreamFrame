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
/// </summary>
public sealed class StreamConnection<TMessage> : IStreamConnection<TMessage>
{
    public event EventHandler<ConnectionState>? ConnectionChanged;

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
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
        {
            Blocking = false,
            ReceiveBufferSize = _options.SocketReceiveBufferSize,
        };
        await socket.ConnectAsync(IpAddress, Port, ct).ConfigureAwait(false);
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
                    socket.Blocking = false;
                    socket.ReceiveBufferSize = _options.SocketReceiveBufferSize;

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

    public void Reconnect()
        => CommunicationStateChanging(ConnectionState.Retry);

    private void CommunicationStateChanging(ConnectionState newState)
    {
        State = newState;
        ConnectionChanged?.Invoke(this, State);

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
                    StopSession();
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

            var cts = new CancellationTokenSource();
            _sessionCts = cts;
            var pipe = new Pipe();
            _pipe = pipe;

            _receiveTask = ReceiveLoopAsync(pipe.Writer, cts.Token);
            var decoder = new FrameDecoder<TMessage>(pipe.Reader, _framing, _codec, _messageRelay);
            _decodeTask = decoder.RunAsync(cts.Token);
            _sendWorkerTask = SendWorkerAsync(cts.Token);
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
        if (_sessionCts is { } cts)
        {
            if (!cts.IsCancellationRequested)
                cts.Cancel();

            var tasks = new[] { _receiveTask, _decodeTask, _sendWorkerTask };

            _sessionCts = null;
            _receiveTask = null;
            _decodeTask = null;
            _sendWorkerTask = null;
            _pipe = null;

            cts.Dispose();

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
        }
    }

    private async Task ReceiveLoopAsync(PipeWriter writer, CancellationToken ct)
    {
        try
        {
            while (true)
            {
                var socket = _socket;
                if (socket is null)
                    break;

                var memory = writer.GetMemory(_options.SocketReceiveBufferSize);
                var count = await socket.ReceiveAsync(memory, SocketFlags.None, ct).ConfigureAwait(false);
                writer.Advance(count);

                if (count > 0)
                    RawBytesReceived?.Invoke(memory[..count]);

                await writer.FlushAsync(ct).ConfigureAwait(false);

                if (count == 0)
                {
                    Reconnect();
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            if (!ct.IsCancellationRequested)
            {
                Debug.WriteLine($"ReceiveLoop failed: {ex.Message}");
                Reconnect();
            }
        }
    }

    private async Task SendWorkerAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var message in _sendQueue.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                await SendFramedAsync(message, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"SendWorker failed: {ex.Message}");
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
        var sent = buffer;
        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            while (!buffer.IsEmpty)
            {
                var socket = _socket;
                if (socket is null)
                    throw new InvalidOperationException("No connected socket.");

                var length = await socket.SendAsync(buffer, SocketFlags.None, ct).ConfigureAwait(false);
                buffer = buffer[length..];
            }
        }
        finally
        {
            _sendLock.Release();
        }

        RawBytesSent?.Invoke(sent);
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

        _lifetimeCts.Cancel();
        _lifetimeCts.Dispose();

        _sendQueue.Writer.TryComplete();
        _messageRelay.Writer.TryComplete();

        StopSession();

        if (_socket is { } socket)
        {
            try
            {
                if (socket.Connected)
                    socket.Shutdown(SocketShutdown.Both);
            }
            catch (SocketException)
            {
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
