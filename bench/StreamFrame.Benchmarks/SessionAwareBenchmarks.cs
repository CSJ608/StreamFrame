using System.Net;
using System.Net.Sockets;
using BenchmarkDotNet.Attributes;
using StreamFrame;

namespace StreamFrame.Benchmarks;

/// <summary>
/// 会话感知/新特性开销的端到端量化（LengthPrefix + 1KB，与单向吞吐同口径）：
/// ① SendInSessionAsync vs SendAsync（信封 + 完成源 + 注册表的每消息成本）；
/// ② GetSessionMessages vs GetMessages（接收视图的成本）；
/// ③ 未完成帧超时 关闭 vs 开启未触发（默认关闭路径是否零开销）。
/// 三个关注点拆成三个基准类，避免参数交叉积膨胀。
/// </summary>
public abstract class SessionAwareBenchmarkBase
{
    protected const int Messages = 10_000;

    protected StreamConnection<string> Server { get; private set; } = null!;
    protected StreamConnection<string> Client { get; private set; } = null!;
    protected CancellationTokenSource Cts { get; private set; } = null!;
    protected long ServerReceived;
    protected volatile TaskCompletionSource ServerDrained = NewTcs();

    protected static TaskCompletionSource NewTcs()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    protected Task RunServerLoop(Func<IAsyncEnumerable<string>> streamFactory)
        => Task.Run(async () =>
        {
            await foreach (var _ in streamFactory())
            {
                if (Interlocked.Increment(ref ServerReceived) == Messages)
                    ServerDrained.TrySetResult();
            }
        });

    protected void SetupCore(StreamConnectionOptions options, Func<IAsyncEnumerable<string>> streamFactory, ICodec<string>? codec = null)
    {
        var port = GetFreePort();
        StreamConnection<string> Create(bool isActive)
            => new(
                new LengthPrefixFramer(),
                codec ?? Utf8TextCodec.Instance,
                IPAddress.Loopback,
                port,
                isActive,
                options);

        Server = Create(isActive: false);
        Client = Create(isActive: true);
        Cts = new CancellationTokenSource();

        _ = RunServerLoop(streamFactory);

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

    protected static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}

/// <summary>① 发送方式：会话绑定发送 vs 普通发送（服务端经 GetMessages 消费）。</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 10)]
public class SessionAwareSendBenchmarks : SessionAwareBenchmarkBase
{
    private long _sessionId;

    [Params("SendAsync", "SendInSessionAsync")]
    public string SendMode { get; set; } = "SendAsync";

    [GlobalSetup]
    public void Setup()
        => SetupCore(new StreamConnectionOptions { ConnectRetryDelayMs = 200 }, () => Server.GetMessages(Cts.Token));

    /// <summary>单向吞吐：连发 1 万条 1KB。Mean 已折算为每消息。</summary>
    [Benchmark(OperationsPerInvoke = Messages)]
    public async Task OneWayThroughput()
    {
        Interlocked.Exchange(ref ServerReceived, 0);
        ServerDrained = NewTcs();

        var payload = new string('x', 1024);
        if (SendMode == "SendInSessionAsync")
        {
            _sessionId = ((ISessionAwareStreamConnection<string>)Client).CurrentSessionId;
            for (var i = 0; i < Messages; i++)
                await Client.SendInSessionAsync(_sessionId, payload);
        }
        else
        {
            for (var i = 0; i < Messages; i++)
                await Client.SendAsync(payload);
        }

        await ServerDrained.Task.WaitAsync(TimeSpan.FromSeconds(30));
    }
}

/// <summary>② 接收视图：GetSessionMessages vs GetMessages（普通发送驱动）。</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 10)]
public class SessionAwareReceiveBenchmarks : SessionAwareBenchmarkBase
{
    [Params("GetMessages", "GetSessionMessages")]
    public string ReceiveView { get; set; } = "GetMessages";

    [GlobalSetup]
    public void Setup()
        => SetupCore(
            new StreamConnectionOptions { ConnectRetryDelayMs = 200 },
            () => ReceiveView == "GetMessages"
                ? Server.GetMessages(Cts.Token)
                : ((ISessionAwareStreamConnection<string>)Server).GetSessionMessages(Cts.Token).Select(m => m.Message));

    /// <summary>单向吞吐：连发 1 万条 1KB。Mean 已折算为每消息。</summary>
    [Benchmark(OperationsPerInvoke = Messages)]
    public async Task OneWayThroughput()
    {
        Interlocked.Exchange(ref ServerReceived, 0);
        ServerDrained = NewTcs();

        var payload = new string('x', 1024);
        for (var i = 0; i < Messages; i++)
            await Client.SendAsync(payload);

        await ServerDrained.Task.WaitAsync(TimeSpan.FromSeconds(30));
    }
}

/// <summary>③ 未完成帧超时：关闭（默认） vs 开启未触发（500ms，帧内不会超时）。</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 10)]
public class IncompleteFrameTimeoutBenchmarks : SessionAwareBenchmarkBase
{
    [Params("Off", "On_Untriggered")]
    public string TimeoutMode { get; set; } = "Off";

    [GlobalSetup]
    public void Setup()
        => SetupCore(
            new StreamConnectionOptions
            {
                ConnectRetryDelayMs = 200,
                IncompleteFrameTimeoutMs = TimeoutMode == "On_Untriggered" ? 500 : 0,
            },
            () => Server.GetMessages(Cts.Token));

    /// <summary>单向吞吐：连发 1 万条 1KB。Mean 已折算为每消息。</summary>
    [Benchmark(OperationsPerInvoke = Messages)]
    public async Task OneWayThroughput()
    {
        Interlocked.Exchange(ref ServerReceived, 0);
        ServerDrained = NewTcs();

        var payload = new string('x', 1024);
        for (var i = 0; i < Messages; i++)
            await Client.SendAsync(payload);

        await ServerDrained.Task.WaitAsync(TimeSpan.FromSeconds(30));
    }
}

file static class AsyncEnumerableLinq
{
    // System.Linq.Async 依赖避免：仅用于把会话视图投影回消息流
    public static async IAsyncEnumerable<string> Select<T>(
        this IAsyncEnumerable<T> source, Func<T, string> selector)
    {
        await foreach (var item in source)
            yield return selector(item);
    }
}
