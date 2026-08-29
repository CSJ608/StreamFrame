using System.Buffers;
using StreamFrame;

namespace StreamFrame.Tests;

/// <summary>
/// 定向守卫/边界分支测试（覆盖率补测）：帧定界器与缓冲写入器的参数守卫、超限拒绝路径。
/// </summary>
public class GuardAndFramerEdgeTests
{
    // ---- StxEtxFramer ----

    [Fact]
    public void StxEtx_Ctor_NonPositiveMax_Throws()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new StxEtxFramer(maxPayloadBytes: 0));

    [Fact]
    public void StxEtx_EncodeFrame_OversizePayload_Throws()
    {
        var framer = new StxEtxFramer(maxPayloadBytes: 8);
        Assert.Throws<InvalidOperationException>(() => framer.EncodeFrame(new byte[9], new TestWrittenBufferWriter()));
    }

    [Fact]
    public void StxEtx_EncodeFrame_PayloadContainsStxOrEtx_Throws()
    {
        var framer = new StxEtxFramer();
        using var writer = new TestWrittenBufferWriter();

        Assert.Throws<InvalidOperationException>(() => framer.EncodeFrame(new byte[] { 0x02 }, writer));
        Assert.Throws<InvalidOperationException>(() => framer.EncodeFrame(new byte[] { 0x03 }, writer));
    }

    [Fact]
    public void StxEtx_EndFrame_OversizePayload_Throws()
    {
        // BeginFrame(STX) + 20 字节负载 + EndFrame：EndFrame 发现负载超 MaxPayloadBytes=8
        var framer = new StxEtxFramer(maxPayloadBytes: 8);
        using var writer = new TestWrittenBufferWriter();
        framer.BeginFrame(writer);
        writer.GetSpan(20);          // 触发扩容到 20+
        writer.Advance(20);

        Assert.Throws<InvalidOperationException>(() => framer.EndFrame(writer));
    }

    [Fact]
    public void StxEtx_EndFrame_PayloadContainsStxOrEtx_Throws()
    {
        var framer = new StxEtxFramer();
        using var writer = new TestWrittenBufferWriter();
        framer.BeginFrame(writer);
        writer.GetSpan(1)[0] = 0x02; // 负载内的 STX
        writer.Advance(1);

        Assert.Throws<InvalidOperationException>(() => framer.EndFrame(writer));
    }

    [Fact]
    public void StxEtx_Decode_OversizeFrame_DiscardsAndRescans()
    {
        // STX + 9 字节负载 + ETX，MaxPayloadBytes=8：超长帧被丢弃，扫描继续到下一个合法帧
        var framer = new StxEtxFramer(maxPayloadBytes: 8);
        var buffer = new ReadOnlySequence<byte>(new byte[]
        {
            0x02, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x03, // 超长帧（9B 负载）
            0x02, 0x4F, 0x4B, 0x03,                                             // 合法帧（2B 负载）
        });

        Assert.True(framer.TryDecodeFrame(ref buffer, out var payload));
        Assert.Equal(new byte[] { 0x4F, 0x4B }, payload.ToArray());
        Assert.True(buffer.IsEmpty); // 两个帧都被消化
    }

    // ---- LengthPrefixFramer ----

    [Fact]
    public void LengthPrefix_Ctor_NonPositiveMax_Throws()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new LengthPrefixFramer(maxPayloadBytes: 0));

    [Fact]
    public void LengthPrefix_EncodeFrame_OversizePayload_Throws()
    {
        var framer = new LengthPrefixFramer(maxPayloadBytes: 8);
        Assert.Throws<InvalidOperationException>(() => framer.EncodeFrame(new byte[9], new TestWrittenBufferWriter()));
    }

    [Fact]
    public void LengthPrefix_Decode_NegativeLength_DiscardsHeader()
    {
        // 长度头高位为 1 → 负数 → 非法，丢弃 4 字节重同步；后续合法帧可解
        var framer = new LengthPrefixFramer();
        var bytes = new byte[4 + 4 + 2 + 4 + 2];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(0, 4), -1); // 非法头
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(4, 4), 2);  // 合法帧头
        bytes[8] = (byte)'o';
        bytes[9] = (byte)'k';
        _ = bytes.AsSpan(10); // （占位：保持数组形状可读）

        var buffer = new ReadOnlySequence<byte>(bytes);
        // 第一次调用：丢弃非法头后本次无完整帧（数据不足路径）
        if (framer.TryDecodeFrame(ref buffer, out _))
            Assert.Fail("负长度头不应解出帧。");
        // 第二次调用：合法帧就位
        Assert.True(framer.TryDecodeFrame(ref buffer, out var payload));
        Assert.Equal("ok"u8.ToArray(), payload.ToArray());
    }

    // ---- PooledBufferWriter / BufferWriterStream ----

    [Fact]
    public void PooledBufferWriter_AdvanceNegative_Throws()
    {
        using var writer = new PooledBufferWriter(16);
        Assert.Throws<ArgumentOutOfRangeException>(() => writer.Advance(-1));
    }

    [Fact]
    public void PooledBufferWriter_AdvanceBeyondCapacity_Throws()
    {
        using var writer = new PooledBufferWriter(16);
        Assert.Throws<ArgumentOutOfRangeException>(() => writer.Advance(17));
    }

    [Fact]
    public void BufferWriterStream_NullWriter_Throws()
        => Assert.Throws<ArgumentNullException>(() => new BufferWriterStream(null!));

    [Fact]
    public void BufferWriterStream_Write_AppendsBytes()
    {
        using var target = new TestWrittenBufferWriter();
        using var stream = new BufferWriterStream(target);
        stream.Write(new byte[] { 1, 2, 3 }, 0, 3);

        Assert.Equal(new byte[] { 1, 2, 3 }, target.WrittenSpan.ToArray());
    }
}
