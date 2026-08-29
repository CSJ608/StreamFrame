using BenchmarkDotNet.Attributes;
using StreamFrame;

namespace StreamFrame.Benchmarks;

/// <summary>
/// 内置指标（ConnectionMetrics）的无监听开销（生产默认态：无人订阅 Meter）。
/// 回答：每次记录到底几纳秒、单条消息的收/发路径合计加多少。
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 15)]
public class MetricsOverheadBenchmarks
{
    private const int Calls = 1_000;

    // 无监听者（生产默认）：ConnectionMetrics 的记录全部走"无订阅"短路
    private readonly ConnectionMetrics _metrics = new("127.0.0.1:0");

    [Benchmark(Baseline = true, OperationsPerInvoke = Calls)]
    public void EmptyLoop_Calibration()
    {
        for (var i = 0; i < Calls; i++)
        {
        }
    }

    [Benchmark(OperationsPerInvoke = Calls)]
    public void FrameSent()
    {
        for (var i = 0; i < Calls; i++)
            _metrics.FrameSent();
    }

    [Benchmark(OperationsPerInvoke = Calls)]
    public void FrameReceived()
    {
        for (var i = 0; i < Calls; i++)
            _metrics.FrameReceived();
    }

    [Benchmark(OperationsPerInvoke = Calls)]
    public void AddBytesReceived_1Chunk_1028B()
    {
        for (var i = 0; i < Calls; i++)
            _metrics.AddBytesReceived(1028);
    }

    [Benchmark(OperationsPerInvoke = Calls)]
    public void SendQueueObserved()
    {
        for (var i = 0; i < Calls; i++)
            _metrics.SendQueueObserved(0);
    }

    [Benchmark(OperationsPerInvoke = Calls)]
    public void Reconnect()
    {
        for (var i = 0; i < Calls; i++)
            _metrics.Reconnect();
    }

    [Benchmark(OperationsPerInvoke = Calls)]
    public void SessionEnded_1s()
    {
        for (var i = 0; i < Calls; i++)
            _metrics.SessionEnded(1.0);
    }

    /// <summary>单条消息的接收路径合计：一个字节块 + 一帧。</summary>
    [Benchmark(OperationsPerInvoke = Calls)]
    public void ReceivePath_PerMessage()
    {
        for (var i = 0; i < Calls; i++)
        {
            _metrics.AddBytesReceived(1028);
            _metrics.FrameReceived();
        }
    }

    /// <summary>单条消息的发送路径合计：入队采样 + 一个字节块 + 一帧。</summary>
    [Benchmark(OperationsPerInvoke = Calls)]
    public void SendPath_PerMessage()
    {
        for (var i = 0; i < Calls; i++)
        {
            _metrics.SendQueueObserved(0);
            _metrics.AddBytesSent(1028);
            _metrics.FrameSent();
        }
    }
}
