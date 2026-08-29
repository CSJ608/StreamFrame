using System.Net;
using System.Net.Sockets;
using BenchmarkDotNet.Attributes;

namespace StreamFrame.Benchmarks;

/// <summary>
/// 端到端管道基准（真实 TCP 回环）：完整连接（收发队列 + Pipe + 定界 + codec + socket）。
/// 原单类按关注点拆分：单向吞吐（Framer × 负载尺寸）与往返延迟（Framer）分开参数化，
/// 避免延迟基准在尺寸参数下空转重复。
/// </summary>
public abstract class EndToEndBenchmarkBase
{
    protected const int ThroughputMessages = 10_000;
    protected const int PingPongRounds = 2_000;

    protected StreamConnection<string> Server { get; private set; } = null!;
    protected StreamConnection<string> Client { get; private set; } = null!;
    protected CancellationTokenSource Cts { get; private set; } = null!;

    protected long ServerReceived;
    protected long ClientReceived;
    protected volatile TaskCompletionSource ServerDrained = NewTcs();
    protected volatile TaskCompletionSource ClientDrained = NewTcs();

    protected static TaskCompletionSource NewTcs()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    protected void SetupCore(IFramer framer)
    {
        var port = GetFreePort();
        var options = new StreamConnectionOptions { ConnectRetryDelayMs = 200 };
        StreamConnection<string> Create(bool isActive)
            => new(framer, Utf8TextCodec.Instance, IPAddress.Loopback, port, isActive, options);

        Server = Create(isActive: false);
        Client = Create(isActive: true);
        Cts = new CancellationTokenSource();

        // 服务端：回显（乒乓用）；同时计数（吞吐用）
        _ = Task.Run(async () =>
        {
            await foreach (var message in Server.GetMessages(Cts.Token))
            {
                if (Interlocked.Increment(ref ServerReceived) == ThroughputMessages)
                    ServerDrained.TrySetResult();
                await Server.SendAsync(message, Cts.Token);
            }
        });
        // 客户端：只计数收到的回显
        _ = Task.Run(async () =>
        {
            await foreach (var _ in Client.GetMessages(Cts.Token))
            {
                if (Interlocked.Increment(ref ClientReceived) == PingPongRounds)
                    ClientDrained.TrySetResult();
            }
        });

        Server.Start(default);
        Client.Start(default);
        Task.WhenAll(Server.WaitForConnectedAsync(), Client.WaitForConnectedAsync()).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        Cts.Cancel();
        Server.DisposeAsync().AsTask().GetAwaiter().GetResult();
        Client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        Cts.Dispose();
    }

    protected static IFramer CreateFramer(string name)
        => name == "LengthPrefix" ? new LengthPrefixFramer() : new StxEtxFramer();

    protected static int PayloadLength(string size)
        => size switch
        {
            "64B" => 64,
            "64KB" => 64 * 1024,
            _ => 1024,
        };

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}

/// <summary>单向吞吐：客户端连发 1 万条消息，计到服务端全部收到。Mean 已按 OperationsPerInvoke 折算为每消息。</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 10)]
public class OneWayThroughputBenchmarks : EndToEndBenchmarkBase
{
    [Params("LengthPrefix", "StxEtx")]
    public string Framer { get; set; } = "LengthPrefix";

    [Params("64B", "1KB", "64KB")]
    public string PayloadSize { get; set; } = "1KB";

    [GlobalSetup]
    public void Setup()
        => SetupCore(CreateFramer(Framer));

    [Benchmark(OperationsPerInvoke = ThroughputMessages)]
    public async Task OneWayThroughput()
    {
        Interlocked.Exchange(ref ServerReceived, 0);
        ServerDrained = NewTcs();

        var payload = new string('x', PayloadLength(PayloadSize));
        for (var i = 0; i < ThroughputMessages; i++)
            await Client.SendAsync(payload);

        await ServerDrained.Task.WaitAsync(TimeSpan.FromSeconds(30));
    }
}

/// <summary>往返延迟：客户端逐条发送并等回显（串行化每一条的完整 RTT）。Mean 已折算为每次往返。</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 10)]
public class RoundTripLatencyBenchmarks : EndToEndBenchmarkBase
{
    [Params("LengthPrefix", "StxEtx")]
    public string Framer { get; set; } = "LengthPrefix";

    [GlobalSetup]
    public void Setup()
        => SetupCore(CreateFramer(Framer));

    [Benchmark(OperationsPerInvoke = PingPongRounds)]
    public async Task RoundTripLatency()
    {
        Interlocked.Exchange(ref ClientReceived, 0);
        ClientDrained = NewTcs();

        for (var i = 0; i < PingPongRounds; i++)
        {
            await Client.SendAsync("ping");
            // 串行等待这一条的回显：单条完整往返
            while (Volatile.Read(ref ClientReceived) <= i)
                await Task.Yield();
        }
    }
}
