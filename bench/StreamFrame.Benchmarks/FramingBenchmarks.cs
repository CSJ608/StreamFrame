using System.Buffers;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace StreamFrame.Benchmarks;

/// <summary>
/// 帧编码两条路径的对比基准：
/// - Streaming：BeginFrame → 写负载 → EndFrame（单缓冲，发送侧默认路径）
/// - Plain：    负载先进缓冲 A，再 EncodeFrame 拷贝进帧缓冲 B（两段缓冲，含一次 memcpy）
/// 以及切帧（TryDecodeFrame）吞吐。
/// 运行：dotnet run -c Release --project bench/StreamFrame.Benchmarks
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 15)]
public class FramingBenchmarks
{
    /// <summary>静态预分配：BDN 在 GlobalSetup 之前取参数，负载必须在字段初始化期就绪。</summary>
    private static readonly Dictionary<string, byte[]> PayloadTable = new()
    {
        ["64B"] = MakeTextPayload(64),
        ["1KB"] = MakeTextPayload(1024),
        ["64KB"] = MakeTextPayload(64 * 1024),
    };

    [Params("64B", "1KB", "64KB")]
    public string PayloadSize { get; set; } = "64B";

    private readonly LengthPrefixFramer _lengthPrefix = new();
    private readonly StxEtxFramer _stxEtx = new();

    private ReadOnlySequence<byte> _gluedLengthPrefix1K = default;
    private ReadOnlySequence<byte> _gluedStxEtx1K = default;

    private byte[] Payload => PayloadTable[PayloadSize];

    [GlobalSetup]
    public void Setup()
    {
        _gluedLengthPrefix1K = Glue(_lengthPrefix, PayloadTable["1KB"], frames: 100);
        _gluedStxEtx1K = Glue(_stxEtx, PayloadTable["1KB"], frames: 100);
    }

    // ----- LengthPrefix：编码两条路径 -----

    [Benchmark]
    public int LengthPrefix_Plain()
    {
        using var payloadBuffer = new PooledBufferWriter(1024);
        payloadBuffer.Write(Payload);
        using var frame = new PooledBufferWriter(payloadBuffer.WrittenCount + 16);
        _lengthPrefix.EncodeFrame(payloadBuffer.WrittenSpan, frame);
        return frame.WrittenCount;
    }

    [Benchmark]
    public int LengthPrefix_Streaming()
    {
        using var frame = new PooledBufferWriter(1024);
        _lengthPrefix.BeginFrame(frame);
        frame.Write(Payload);
        _lengthPrefix.EndFrame(frame);
        return frame.WrittenCount;
    }

    // ----- StxEtx：编码两条路径 -----

    [Benchmark]
    public int StxEtx_Plain()
    {
        using var payloadBuffer = new PooledBufferWriter(1024);
        payloadBuffer.Write(Payload);
        using var frame = new PooledBufferWriter(payloadBuffer.WrittenCount + 16);
        _stxEtx.EncodeFrame(payloadBuffer.WrittenSpan, frame);
        return frame.WrittenCount;
    }

    [Benchmark]
    public int StxEtx_Streaming()
    {
        using var frame = new PooledBufferWriter(1024);
        _stxEtx.BeginFrame(frame);
        frame.Write(Payload);
        _stxEtx.EndFrame(frame);
        return frame.WrittenCount;
    }

    // ----- 切帧吞吐（每批 100 × 1KB 帧）-----

    [Benchmark]
    public int LengthPrefix_Decode100GluedFrames()
        => DecodeAll(_lengthPrefix, _gluedLengthPrefix1K);

    [Benchmark]
    public int StxEtx_Decode100GluedFrames()
        => DecodeAll(_stxEtx, _gluedStxEtx1K);

    // ----- 辅助 -----

    private static int DecodeAll(IFramer framer, in ReadOnlySequence<byte> glued)
    {
        var buffer = glued;
        var frames = 0;
        while (framer.TryDecodeFrame(ref buffer, out _))
            frames++;
        return frames;
    }

    private static byte[] MakeTextPayload(int size)
    {
        // 模拟 XML/文本报文的字节分布（不含 STX/ETX，UTF-8 连续字节 ≥0x80 也不会撞上）
        var text = new StringBuilder(size);
        while (text.Length < size)
            text.Append("<Message><Id>42</Id><Data>0123456789ABCDEF</Data></Message>");
        return Encoding.UTF8.GetBytes(text.ToString(0, Math.Min(text.Length, size)));
    }

    private static ReadOnlySequence<byte> Glue(IFramer framer, byte[] payload, int frames)
    {
        var writer = new ArrayBufferWriter<byte>();
        for (var i = 0; i < frames; i++)
            framer.EncodeFrame(payload, writer);
        return new ReadOnlySequence<byte>(writer.WrittenSpan.ToArray());
    }
}
