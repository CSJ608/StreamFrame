using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace StreamFrame.Tests;

public class StreamingFrameEncodeTests
{
    /// <summary>
    /// 用流式 Begin/EndFrame 编码一段 payload，返回字节。
    /// </summary>
    private static byte[] EncodeStreaming(IStreamingFramer framing, byte[] payload)
    {
        using var writer = new TestWrittenBufferWriter();
        framing.BeginFrame(writer);
        writer.Write(payload);
        framing.EndFrame(writer);
        return writer.WrittenSpan.ToArray();
    }

    /// <summary>
    /// 用纯函数 EncodeFrame 编码同一段 payload，返回字节。
    /// </summary>
    private static byte[] EncodePlain(IFramer framing, byte[] payload)
    {
        var buffer = new TestWrittenBufferWriter();
        framing.EncodeFrame(payload, buffer);
        return buffer.WrittenSpan.ToArray();
    }

    [Fact]
    public void LengthPrefix_StreamingMatchesPlain()
    {
        var framing = new LengthPrefixFramer();
        foreach (var payload in new[]
                 {
                     Array.Empty<byte>(),
                     Encoding.UTF8.GetBytes("hello"),
                     Enumerable.Range(0, 1000).Select(i => (byte)(i % 256)).ToArray(),
                 })
        {
            Assert.Equal(EncodePlain(framing, payload), EncodeStreaming(framing, payload));
        }
    }

    [Fact]
    public void StxEtx_StreamingMatchesPlain()
    {
        var framing = new StxEtxFramer();
        foreach (var payload in new[]
                 {
                     Encoding.UTF8.GetBytes("A"),
                     Encoding.UTF8.GetBytes("hello world"),
                     Encoding.UTF8.GetBytes("带中文的负载"),
                 })
        {
            Assert.Equal(EncodePlain(framing, payload), EncodeStreaming(framing, payload));
        }
    }

    [Fact]
    public void LengthPrefix_StreamingEndFrame_BackfillsCorrectLength()
    {
        var framing = new LengthPrefixFramer();
        var payload = Encoding.UTF8.GetBytes("test-payload");

        using var writer = new TestWrittenBufferWriter();
        framing.BeginFrame(writer);
        writer.Write(payload);
        framing.EndFrame(writer);

        // 前 4 字节应是大端长度
        var length = BinaryPrimitives.ReadInt32BigEndian(writer.WrittenSpan.Slice(0, 4));
        Assert.Equal(payload.Length, length);
        // 随后是负载
        Assert.Equal(payload, writer.WrittenSpan.Slice(4).ToArray());
    }

    [Fact]
    public void LengthPrefix_StreamingRejectsOverlongPayload()
    {
        var framing = new LengthPrefixFramer(maxPayloadBytes: 8);
        using var writer = new TestWrittenBufferWriter();
        framing.BeginFrame(writer);
        writer.Write(new byte[9]);
        Assert.Throws<InvalidOperationException>(() => framing.EndFrame(writer));
    }

    [Fact]
    public void StxEtx_StreamingRejectsPayloadContainingStxOrEtx()
    {
        var framing = new StxEtxFramer();
        using var writer = new TestWrittenBufferWriter();
        framing.BeginFrame(writer);
        writer.Write(new byte[] { 0x41, 0x02, 0x42 });
        Assert.Throws<InvalidOperationException>(() => framing.EndFrame(writer));
    }
}
