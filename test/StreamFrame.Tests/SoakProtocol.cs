using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using StreamFrame;

namespace StreamFrame.Tests;

#pragma warning disable MA0048 // Cohesive test-only protocol helpers, kept out of the library API.

internal sealed class SoakFactAttribute : FactAttribute
{
    public SoakFactAttribute()
    {
        var raw = Environment.GetEnvironmentVariable("STREAMFRAME_SOAK_SECONDS");
        if (!double.TryParse(raw, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var seconds) || seconds <= 0 || double.IsInfinity(seconds) || double.IsNaN(seconds))
            Skip = "Set STREAMFRAME_SOAK_SECONDS to execute the long-running TCP suite.";
    }
}

internal sealed class SoakTrace
{
    private readonly Xunit.Abstractions.ITestOutputHelper _output;
    private readonly long _start = TestClock.TickCount64;
    private readonly bool _verbose;
    public SoakTrace(Xunit.Abstractions.ITestOutputHelper output, bool verbose = true)
    {
        _output = output;
        _verbose = verbose;
    }
    public void Log(string text)
    {
        if (_verbose || text.StartsWith("phase=", StringComparison.Ordinal) || text.StartsWith("state=", StringComparison.Ordinal) || text.StartsWith("long-stream", StringComparison.Ordinal))
            _output.WriteLine($"[{TestClock.TickCount64 - _start}ms] {text}");
    }
}

internal sealed class SoakAttempt
{
    public string Wire { get; }
    public long ClaimedSession;
    public int Claimed;
    public int RawComplete;
    public int Sequence { get; }
    public SoakAttempt(string wire, int sequence) { Wire = wire; Sequence = sequence; }
}

/// <summary>Test-only protocol: wire attempt IDs, application ACK, retry and receiver deduplication.
/// Codec entry observes dequeue; RawBytesSent observes local bytes, never a remote acknowledgement.</summary>
internal sealed class SoakRun : IAsyncDisposable
{
    private readonly SoakTrace _trace;
    private readonly List<SoakPeer> _peers = new();
    private readonly ConcurrentDictionary<int, int> _retry = new();
    private readonly ConcurrentDictionary<string, byte> _acks = new();
    private readonly CancellationTokenSource _ackStop = new();
    private readonly Task _ackReader;
    private readonly List<byte> _raw = new();
    private long _rawSession;
    private int _barrier;
    private int _sequence;
    private readonly List<Task> _sends = new();
    private bool _stopped;
    private readonly int _port;
    public StreamConnection<string> Server { get; }
    public ConcurrentDictionary<string, SoakAttempt> Attempts { get; } = new();
    public ConcurrentQueue<(string Wire, long SessionId)> Received { get; } = new();
    public ConcurrentDictionary<int, byte> Delivered { get; } = new();
    public ConcurrentDictionary<int, byte> Acked { get; } = new();

    public SoakRun(SoakTrace trace)
    {
        _trace = trace;
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        _port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        Server = new StreamConnection<string>(new LengthPrefixFramer(), new ObservedCodec(this),
            IPAddress.Loopback, _port, isActive: false,
            new StreamConnectionOptions { SendQueueCapacity = 256, AcceptRetryDelayMs = 200 },
            logger: new FaultLogger(trace));
        Server.ConnectionChanged += (_, state) => trace.Log($"state={state} sid={Server.CurrentSessionId}");
        Server.RawBytesSent += ObserveRaw;
        _ackReader = ReadAcksAsync();
        Server.Start(CancellationToken.None);
    }

    private sealed class FaultLogger : Microsoft.Extensions.Logging.ILogger
    {
        private readonly SoakTrace _trace;
        public FaultLogger(SoakTrace trace) => _trace = trace;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => logLevel >= Microsoft.Extensions.Logging.LogLevel.Warning;
        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
            TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => _trace.Log($"fault level={logLevel} {formatter(state, exception)} {exception}");
    }

    private sealed class ObservedCodec : ICodec<string>
    {
        private readonly SoakRun _run;
        public ObservedCodec(SoakRun run) => _run = run;
        public string Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default) => StringCodec.Instance.Decode(frame, ct);
        public void Encode(string message, IBufferWriter<byte> writer, CancellationToken ct = default)
        {
            var attempt = _run.Attempts[message];
            attempt.ClaimedSession = _run.Server.CurrentSessionId;
            Interlocked.Exchange(ref attempt.Claimed, 1);
            _run._trace.Log($"claim id={message} sid-snapshot={attempt.ClaimedSession} cancelled={ct.IsCancellationRequested}");
            StringCodec.Instance.Encode(message, writer, ct);
        }
    }

    private void ObserveRaw(ReadOnlyMemory<byte> bytes)
    {
        lock (_raw)
        {
            var sid = Server.CurrentSessionId;
            if (_rawSession != sid)
            {
                if (_raw.Count > 0) _trace.Log($"raw-partial sid-snapshot={_rawSession} bytes={_raw.Count}");
                _raw.Clear();
                _rawSession = sid;
            }
            _trace.Log($"raw bytes={bytes.Length} sid-snapshot={sid}");
            _raw.AddRange(bytes.ToArray());
            while (_raw.Count >= 4)
            {
                var length = BinaryPrimitives.ReadInt32BigEndian(_raw.Take(4).ToArray());
                // The public raw callback has a current-state snapshot, not immutable session metadata.
                // A teardown in a partial write may change that snapshot: retain uncertainty in the log.
                if (length < 1 || length > 4096)
                {
                    _trace.Log("raw snapshot changed within a frame; attribution unavailable");
                    _raw.Clear();
                    return;
                }
                if (_raw.Count < length + 4) return;
                var wire = Encoding.UTF8.GetString(_raw.Skip(4).Take(length).ToArray());
                if (Attempts.TryGetValue(wire, out var attempt)) Interlocked.Exchange(ref attempt.RawComplete, 1);
                _trace.Log($"local-frame id={wire} sid-snapshot={sid}");
                _raw.RemoveRange(0, length + 4);
            }
        }
    }

    public void Register(string wire) => Assert.True(Attempts.TryAdd(wire, new SoakAttempt(wire, _sequence++)), $"duplicate attempt {wire}");

    public void SendBound(long sid, string wire)
    {
        Register(wire);
        _sends.Add(ObserveAsync());
        async Task ObserveAsync()
        {
            try
            {
                await Server.SendInSessionAsync(sid, wire);
                _trace.Log($"bound-result id={wire} full-local-write");
            }
            catch (Exception ex) when (ex is SessionExpiredException or SocketException or OperationCanceledException)
            {
                _trace.Log($"bound-result id={wire} {ex.GetType().Name}");
            }
        }
    }

    public Task ObserveSendsAsync() => BoundedAsync(Task.WhenAll(_sends), "all bound sends");

    public async Task EnqueueAsync(string wire)
    {
        Register(wire);
        using var timeout = new CancellationTokenSource(15000);
        var send = Server.SendAsync(wire, timeout.Token);
        try { await BoundedAsync(send, $"enqueue {wire}"); }
        finally { _trace.Log($"enqueue-result id={wire} status={send.Status} sid={Server.CurrentSessionId}"); }
    }

    public Task SendBusinessAsync(int id)
    {
        var attempt = _retry.AddOrUpdate(id, 0, (_, previous) => previous + 1);
        return EnqueueAsync($"p{id}/{attempt}");
    }

    public async Task<SoakPeer> ConnectAsync()
    {
        _trace.Log("phase=connect begin");
        var end = TestClock.TickCount64 + 15000;
        while (true)
        {
            var client = new TcpClient();
            try
            {
                await BoundedAsync(client.ConnectAsync(IPAddress.Loopback, _port), "TCP connect", 5000);
                await WaitAsync(() => Server.CurrentSessionId != 0, "session publication");
                var peer = new SoakPeer(this, client, Server.CurrentSessionId, _trace);
                _peers.Add(peer);
                _trace.Log($"phase=connect done sid={peer.SessionId}");
                return peer;
            }
            catch (SocketException) when (TestClock.TickCount64 < end)
            {
                client.Dispose();
                await Task.Delay(100);
            }
            catch { client.Dispose(); throw; }
        }
    }

    public async Task BarrierAsync(SoakPeer peer)
    {
        var wire = $"z{_barrier++}";
        _trace.Log($"phase=drain begin id={wire} sid={peer.SessionId}");
        await EnqueueAsync(wire);
        await WaitAsync(() => _acks.ContainsKey(wire), $"drain ACK {wire}");
        _trace.Log($"phase=drain done id={wire}");
    }

    private async Task ReadAcksAsync()
    {
        try
        {
            await foreach (var message in Server.GetSessionMessages(_ackStop.Token))
            {
                var ack = message.Message;
                Assert.StartsWith("ack/", ack, StringComparison.Ordinal);
                var wire = ack.Substring(4);
                Assert.True(Attempts.ContainsKey(wire), $"unknown ACK {wire}");
                _acks.TryAdd(wire, 0);
                if (wire.StartsWith("p", StringComparison.Ordinal)) Acked.TryAdd(BusinessId(wire), 0);
                _trace.Log($"ACK id={wire} sid={message.SessionId}");
            }
        }
        catch (OperationCanceledException) when (_ackStop.IsCancellationRequested) { }
    }

    public static int BusinessId(string wire) => int.Parse(wire.Substring(1).Split('/')[0], System.Globalization.CultureInfo.InvariantCulture);

    public void OnFrame(string wire, long sid)
    {
        Assert.True(Attempts.ContainsKey(wire), $"unknown peer frame {wire}");
        Received.Enqueue((wire, sid));
        if (wire.StartsWith("p", StringComparison.Ordinal))
        {
            var first = Delivered.TryAdd(BusinessId(wire), 0);
            _trace.Log($"business id={wire} sid={sid} applied={first}");
        }
        _trace.Log($"peer-frame id={wire} sid={sid}");
    }

    public string[] UnclaimedPlain() => Attempts.Values
        .Where(x => x.Wire.StartsWith("p", StringComparison.Ordinal) && Volatile.Read(ref x.Claimed) == 0)
        .Select(x => x.Wire).ToArray();

    public void AssertQueueContinuation(string[] unclaimed)
    {
        var seen = new HashSet<string>(Received.Select(x => x.Wire));
        foreach (var wire in unclaimed) Assert.Contains(wire, seen);
        foreach (var attempt in Attempts.Values.Where(x => x.Wire.StartsWith("p", StringComparison.Ordinal)))
            Assert.Equal(1, Volatile.Read(ref attempt.Claimed));
    }

    public void ReportUnconfirmed()
    {
        var seen = new HashSet<string>(Received.Select(x => x.Wire));
        foreach (var attempt in Attempts.Values.Where(x => x.Wire.StartsWith("p", StringComparison.Ordinal) && !_acks.ContainsKey(x.Wire)))
        {
            var stage = seen.Contains(attempt.Wire) ? "peer-collected/ACK-missing" :
                Volatile.Read(ref attempt.RawComplete) != 0 ? "local-write-complete/remote-unconfirmed" :
                Volatile.Read(ref attempt.Claimed) != 0 ? "claimed/write-incomplete-or-raw-attribution-unavailable" : "unclaimed";
            _trace.Log($"unconfirmed id={attempt.Wire} stage={stage} claim-sid-snapshot={attempt.ClaimedSession}");
        }
    }

    public async Task WaitAsync(Func<bool> condition, string phase)
    {
        var end = TestClock.TickCount64 + 15000;
        while (!condition() && TestClock.TickCount64 < end)
        {
            if (_ackReader.IsFaulted) await _ackReader;
            foreach (var peer in _peers) if (peer.Reader.IsFaulted) await peer.Reader;
            await Task.Delay(10);
        }
        Assert.True(condition(), $"phase={phase} timed out, state={Server.State} sid={Server.CurrentSessionId}");
    }

    public async Task ClosePeerAsync(SoakPeer peer)
    {
        _trace.Log($"phase=reader-stop sid={peer.SessionId}");
        peer.Close(); // NetworkStream.ReadAsync cancellation alone does not abort pending net48 I/O.
        await BoundedAsync(peer.Reader, $"reader stop sid={peer.SessionId}");
        _trace.Log($"phase=reader-observed sid={peer.SessionId} status={peer.Reader.Status}");
    }

    public async Task StopAsync()
    {
        if (_stopped) return;
        _stopped = true;
        foreach (var peer in _peers) peer.Close();
        try { await BoundedAsync(Task.WhenAll(_peers.Select(x => x.Reader)), "all peer readers"); }
        finally
        {
            _ackStop.Cancel();
            try { await BoundedAsync(_ackReader, "ACK reader stop"); }
            finally
            {
                await BoundedAsync(Task.Run(async () => await Server.DisposeAsync()), "server dispose");
                await ObserveSendsAsync();
                _ackStop.Dispose();
                _trace.Log("phase=server-disposed");
            }
        }
    }

    public ValueTask DisposeAsync() => new(StopAsync());

    public static async Task BoundedAsync(Task task, string phase, int milliseconds = 15000)
    {
        using var timer = new CancellationTokenSource();
        if (await Task.WhenAny(task, Task.Delay(milliseconds, timer.Token)) != task)
        {
            // Observe a late fault even when the bounded failure has already unwound the owner.
            _ = task.ContinueWith(t => { _ = t.Exception; }, CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            throw new TimeoutException($"phase={phase} exceeded {milliseconds}ms, status={task.Status}");
        }
        timer.Cancel();
        await task;
    }
}

internal sealed class SoakPeer
{
    private readonly SoakRun _run;
    private readonly SoakTrace _trace;
    private readonly NetworkStream _stream;
    private readonly SemaphoreSlim _write = new(1, 1);
    private int _closing;
    private bool _poisoned;
    public TcpClient Client { get; }
    public long SessionId { get; }
    public Task Reader { get; }
    public SoakPeer(SoakRun run, TcpClient client, long sessionId, SoakTrace trace)
    {
        _run = run;
        _trace = trace;
        Client = client;
        SessionId = sessionId;
        _stream = client.GetStream();
        Reader = ReadAsync();
    }
    public void Close()
    {
        Interlocked.Exchange(ref _closing, 1);
        Client.Dispose();
    }
    public async Task InjectPartialAsync()
    {
        await SoakRun.BoundedAsync(_write.WaitAsync(), "partial write lock");
        try
        {
            _poisoned = true; // No ACK writes behind a deliberately incomplete incoming frame.
            var junk = new byte[] { 0, 0, 15, 160 };
            await SoakRun.BoundedAsync(_stream.WriteAsync(junk, 0, junk.Length), "partial write");
            _trace.Log($"partial-frame sid={SessionId}");
        }
        finally { _write.Release(); }
    }
    private async Task ReadAsync()
    {
        try
        {
            while (true)
            {
                var header = await ReadBytesAsync(4);
                if (header is null) return;
                var length = BinaryPrimitives.ReadInt32BigEndian(header);
                Assert.InRange(length, 1, 4096);
                var payload = await ReadBytesAsync(length);
                if (payload is null) return; // Fault injection may truncate an in-flight frame.
                var wire = Encoding.UTF8.GetString(payload);
                _run.OnFrame(wire, SessionId);
                await SoakRun.BoundedAsync(_write.WaitAsync(), "ACK write lock");
                try
                {
                    if (_poisoned) continue;
                    var ack = Encoding.UTF8.GetBytes("ack/" + wire);
                    var frame = new byte[ack.Length + 4];
                    BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(0, 4), ack.Length);
                    ack.CopyTo(frame, 4);
                    await SoakRun.BoundedAsync(_stream.WriteAsync(frame, 0, frame.Length), $"ACK write {wire}");
                }
                finally { _write.Release(); }
            }
        }
        catch (Exception ex) when (ex is IOException or SocketException || ex is ObjectDisposedException && Volatile.Read(ref _closing) != 0)
        {
            _trace.Log($"reader-end sid={SessionId} exception={ex.GetType().Name} closing={_closing}");
        }
    }
    private async Task<byte[]?> ReadBytesAsync(int length)
    {
        var bytes = new byte[length];
        var offset = 0;
        while (offset < length)
        {
            var count = await _stream.ReadAsync(bytes, offset, length - offset);
            if (count == 0) return null;
            offset += count;
        }
        return bytes;
    }
}
