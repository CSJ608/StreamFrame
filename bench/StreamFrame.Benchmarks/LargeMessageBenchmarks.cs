using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using BenchmarkDotNet.Attributes;
using StreamFrame;

namespace StreamFrame.Benchmarks;

/// <summary>
/// 64KB 大报文的分配归因基准：同一端到端管道（LengthPrefix + 计数消费，无回显）在三种
/// codec/消息类型下的表现——
/// ① String_Alloc：字符串 + 分配中间数组的 GetBytes（现状写法，demo/旧基准同款）；
/// ② String_Span：字符串 + span 直写（Encoding.GetBytes(ReadOnlySpan&lt;char&gt;, IBufferWriter)，零中间数组）；
/// ③ ByteArray：byte[] 消息 + 透传 codec（编码零拷贝、解码一次 ToArray）。
/// 用于把"框架税"精确拆到 框架 / codec 写法 / 消息类型 三个口袋。
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 10)]
public class LargeMessageStringBenchmarks : SessionAwareBenchmarkBase
{
    [Params("String_Alloc", "String_Span")]
    public string CodecMode { get; set; } = "String_Alloc";

    [GlobalSetup]
    public void Setup()
        => SetupCore(
            new StreamConnectionOptions { ConnectRetryDelayMs = 200 },
            () => Server.GetMessages(Cts.Token),
            CodecMode == "String_Span" ? SpanUtf8TextCodec.Instance : Utf8TextCodec.Instance);

    /// <summary>单向吞吐：连发 1 万条 64KB。Mean 已折算为每消息。</summary>
    [Benchmark(OperationsPerInvoke = Messages)]
    public async Task OneWayThroughput_64KB()
    {
        Interlocked.Exchange(ref ServerReceived, 0);
        ServerDrained = NewTcs();

        var payload = new string('x', 64 * 1024);
        for (var i = 0; i < Messages; i++)
            await Client.SendAsync(payload);

        await ServerDrained.Task.WaitAsync(TimeSpan.FromSeconds(60));
    }
}

/// <summary>byte[] 消息 + 透传 codec 的对照：编码零拷贝（同一数组实例发一万次），解码一次 ToArray。</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 10)]
public class LargeMessageByteArrayBenchmarks
{
    private const int Messages = 10_000;

    [Params("64KB", "1KB")]
    public string PayloadSize { get; set; } = "64KB";

    private StreamConnection<byte[]> _server = null!;
    private StreamConnection<byte[]> _client = null!;
    private CancellationTokenSource _cts = null!;
    private byte[] _payload = null!;
    private long _received;
    private volatile TaskCompletionSource _drained = NewTcs();

    private static TaskCompletionSource NewTcs()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    [GlobalSetup]
    public void Setup()
    {
        var port = GetFreePort();
        var options = new StreamConnectionOptions { ConnectRetryDelayMs = 200 };
        StreamConnection<byte[]> Create(bool isActive)
            => new(new LengthPrefixFramer(), ByteArrayPassThroughCodec.Instance, IPAddress.Loopback, port, isActive, options);

        _server = Create(isActive: false);
        _client = Create(isActive: true);
        _cts = new CancellationTokenSource();
        _payload = new byte[PayloadSize == "1KB" ? 1024 : 64 * 1024];

        _ = Task.Run(async () =>
        {
            await foreach (var _ in _server.GetMessages(_cts.Token))
            {
                if (Interlocked.Increment(ref _received) == Messages)
                    _drained.TrySetResult();
            }
        });

        _server.Start(default);
        _client.Start(default);
        Task.WhenAll(_server.WaitForConnectedAsync(), _client.WaitForConnectedAsync()).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _cts.Cancel();
        _server.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _cts.Dispose();
    }

    /// <summary>单向吞吐：连发 1 万条（同一 byte[] 实例）。Mean 已折算为每消息。</summary>
    [Benchmark(OperationsPerInvoke = Messages)]
    public async Task OneWayThroughput()
    {
        Interlocked.Exchange(ref _received, 0);
        _drained = NewTcs();

        for (var i = 0; i < Messages; i++)
            await _client.SendAsync(_payload);

        await _drained.Task.WaitAsync(TimeSpan.FromSeconds(60));
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

/// <summary>span 直写的 UTF-8 文本 codec：Encode 不产生中间数组（大报文指南的推荐写法）。</summary>
internal sealed class SpanUtf8TextCodec : ICodec<string>
{
    public static readonly SpanUtf8TextCodec Instance = new();

    public string Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
        => Encoding.UTF8.GetString(frame);

    public void Encode(string message, IBufferWriter<byte> writer, CancellationToken ct = default)
        => Encoding.UTF8.GetBytes(message.AsSpan(), writer);
}

/// <summary>byte[] 透传 codec：编码原样写入（零拷贝），解码一次 ToArray。</summary>
internal sealed class ByteArrayPassThroughCodec : ICodec<byte[]>
{
    public static readonly ByteArrayPassThroughCodec Instance = new();

    public byte[] Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
        => frame.ToArray();

    public void Encode(byte[] message, IBufferWriter<byte> writer, CancellationToken ct = default)
        => writer.Write(message);
}
