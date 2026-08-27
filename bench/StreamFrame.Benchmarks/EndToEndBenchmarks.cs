using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;

namespace StreamFrame.Benchmarks;

/// <summary>
/// 端到端管道基准（真实 TCP 回环）：完整连接（收发队列 + Pipe + 定界 + codec + socket）的
/// 单向吞吐（按 framer 分组）与往返延迟。回答"整库每秒能打多少消息、一条消息来回要多久"。
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 10)]
public class EndToEndBenchmarks
{
    private const int ThroughputMessages = 10_000;
    private const int PingPongRounds = 2_000;

    [Params("LengthPrefix", "StxEtx")]
    public string Framer { get; set; } = "LengthPrefix";

    private StreamConnection<string> _server = null!;
    private StreamConnection<string> _client = null!;
    private CancellationTokenSource _cts = null!;

    // 完成信号：消费端收到目标条数时置位（吞吐）/客户端收到回显时置位（乒乓）
    private long _serverReceived;
    private long _clientReceived;
    private volatile TaskCompletionSource _serverDrained = NewTcs();
    private volatile TaskCompletionSource _clientDrained = NewTcs();

    private static TaskCompletionSource NewTcs()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    [GlobalSetup]
    public async Task Setup()
    {
        var port = GetFreePort();
        var options = new StreamConnectionOptions { ConnectRetryDelayMs = 200 };
        StreamConnection<string> Create(bool isActive)
            => new(
                Framer == "LengthPrefix" ? new LengthPrefixFramer() : new StxEtxFramer(),
                Utf8TextCodec.Instance,
                IPAddress.Loopback,
                port,
                isActive,
                options);

        _server = Create(isActive: false);
        _client = Create(isActive: true);
        _cts = new CancellationTokenSource();

        // 服务端：回显（乒乓用）；同时计数（吞吐用）
        _ = Task.Run(async () =>
        {
            await foreach (var message in _server.GetMessages(_cts.Token))
            {
                if (Interlocked.Increment(ref _serverReceived) == ThroughputMessages)
                    _serverDrained.TrySetResult();
                await _server.SendAsync(message, _cts.Token);
            }
        });
        // 客户端：只计数收到的回显
        _ = Task.Run(async () =>
        {
            await foreach (var _ in _client.GetMessages(_cts.Token))
            {
                if (Interlocked.Increment(ref _clientReceived) == PingPongRounds)
                    _clientDrained.TrySetResult();
            }
        });

        _server.Start(default);
        _client.Start(default);
        await Task.WhenAll(_server.WaitForConnectedAsync(), _client.WaitForConnectedAsync());
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _cts.Cancel();
        _server.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _cts.Dispose();
    }

    /// <summary>单向吞吐：客户端连发 1 万条 1KB 消息，计到服务端全部收到。Mean 已按 OperationsPerInvoke 折算为每消息。</summary>
    [Benchmark(OperationsPerInvoke = ThroughputMessages)]
    public async Task OneWayThroughput_10k_1KB()
    {
        Interlocked.Exchange(ref _serverReceived, 0);
        _serverDrained = NewTcs();

        var payload = new string('x', 1024);
        for (var i = 0; i < ThroughputMessages; i++)
            await _client.SendAsync(payload);

        // 等全部送达（30 秒兜底，异常时让基准响亮失败而不是挂死）
        await _serverDrained.Task.WaitAsync(TimeSpan.FromSeconds(30));
    }

    /// <summary>往返延迟：客户端逐条发送并等回显（串行化每一条的完整 RTT）。Mean 已折算为每次往返。</summary>
    [Benchmark(OperationsPerInvoke = PingPongRounds)]
    public async Task RoundTripLatency()
    {
        Interlocked.Exchange(ref _clientReceived, 0);
        _clientDrained = NewTcs();

        for (var i = 0; i < PingPongRounds; i++)
        {
            await _client.SendAsync("ping");
            // 串行等待这一条的回显：单条完整往返
            while (Volatile.Read(ref _clientReceived) <= i)
                await Task.Yield();
        }
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
