using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using BenchmarkDotNet.Attributes;

namespace StreamFrame.Benchmarks;

/// <summary>
/// 裸 TCP 回环地板值：预生成"4 字节大端长度 + 负载"的帧字节，直接在 NetworkStream 上
/// 逐条 Write/Read（无定界、无 codec、无队列）。用于计算 StreamFrame 的框架税
/// （绝对差值与百分比并列，口径与 OneWayThroughputBenchmarks 完全一致）。
/// 接收循环常驻、每次调用重置计数并换完成源（与 e2e 基准同款模式——初版接收只读一轮，
/// 第二次调用起写侧无人读导致 WriteAsync 永久阻塞，已修正）。
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 10)]
public class RawTcpFloorBenchmarks
{
    private const int Messages = 10_000;

    [Params("64B", "1KB", "64KB")]
    public string PayloadSize { get; set; } = "1KB";

    private TcpListener _listener = null!;
    private TcpClient _sender = null!;
    private TcpClient _receiver = null!;
    private byte[] _frame = null!;
    private long _totalBytesPerRound;
    private long _receivedBytes;
    private volatile TaskCompletionSource _drained = NewTcs();

    private static TaskCompletionSource NewTcs()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    [GlobalSetup]
    public async Task Setup()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        var port = ((IPEndPoint)_listener.LocalEndpoint).Port;

        _sender = new TcpClient();
        await _sender.ConnectAsync(IPAddress.Loopback, port);
        _receiver = await _listener.AcceptTcpClientAsync();
        _listener.Stop();

        var payloadLength = PayloadSize switch
        {
            "64B" => 64,
            "64KB" => 64 * 1024,
            _ => 1024,
        };
        _frame = new byte[4 + payloadLength];
        BinaryPrimitives.WriteInt32BigEndian(_frame, payloadLength);
        for (var i = 4; i < _frame.Length; i++)
            _frame[i] = (byte)'x';
        _totalBytesPerRound = (4L + payloadLength) * Messages;

        // 常驻接收：本轮读满目标字节数即完成当前调用（计数滚动抵扣跨轮余量）
        _ = Task.Run(async () =>
        {
            var stream = _receiver.GetStream();
            var buffer = new byte[64 * 1024];
            while (true)
            {
                var n = await stream.ReadAsync(buffer);
                if (n == 0)
                    return;
                if (Interlocked.Add(ref _receivedBytes, n) >= Volatile.Read(ref _totalBytesPerRound))
                {
                    Interlocked.Add(ref _receivedBytes, -Volatile.Read(ref _totalBytesPerRound));
                    _drained.TrySetResult();
                }
            }
        });
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _sender.Dispose();
        _receiver.Dispose();
        _listener.Stop();
    }

    /// <summary>单向吞吐地板：连发 1 万条帧字节。Mean 已按 OperationsPerInvoke 折算为每消息。</summary>
    [Benchmark(OperationsPerInvoke = Messages)]
    public async Task OneWayThroughput()
    {
        Interlocked.Exchange(ref _receivedBytes, 0);
        _drained = NewTcs();

        var stream = _sender.GetStream();
        for (var i = 0; i < Messages; i++)
            await stream.WriteAsync(_frame);

        await _drained.Task.WaitAsync(TimeSpan.FromSeconds(30));
    }
}
