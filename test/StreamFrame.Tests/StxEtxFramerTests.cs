using System.Buffers;
using System.Text;

namespace StreamFrame.Tests;

public class StxEtxFramerTests
{
    private const byte STX = 0x02;
    private const byte ETX = 0x03;

    [Fact]
    public void Encode_WrapsPayload()
    {
        var codec = new StxEtxFramer();
        var payload = Encoding.UTF8.GetBytes("hello");

        var writer = new ArrayBufferWriter<byte>();
        codec.EncodeFrame(payload, writer);
        var bytes = writer.WrittenSpan.ToArray();

        Assert.Equal(STX, bytes[0]);
        Assert.Equal(ETX, bytes[^1]);
        Assert.Equal(payload, bytes[1..^1]);
    }

    [Fact]
    public void Encode_RejectsPayloadContainingStxOrEtx()
    {
        var codec = new StxEtxFramer();
        // 负载含 0x02，plain 模式必须拒绝
        Assert.Throws<InvalidOperationException>(() =>
        {
            var writer = new ArrayBufferWriter<byte>();
            codec.EncodeFrame(new byte[] { 0x41, 0x02, 0x42 }, writer);
        });
    }

    [Fact]
    public void Decode_ExtractsPayload()
    {
        var codec = new StxEtxFramer();
        var payload = Encoding.UTF8.GetBytes("payload data");

        var writer = new ArrayBufferWriter<byte>();
        codec.EncodeFrame(payload, writer);
        var buffer = new ReadOnlySequence<byte>(writer.WrittenSpan.ToArray());

        Assert.True(codec.TryDecodeFrame(ref buffer, out var decoded));
        Assert.Equal(payload, decoded.ToArray());
        Assert.True(buffer.IsEmpty);
    }

    [Fact]
    public void Decode_HalfPacket_ReturnsFalse_AndKeepsFromStx()
    {
        var codec = new StxEtxFramer();
        var payload = Encoding.UTF8.GetBytes("hello");

        var writer = new ArrayBufferWriter<byte>();
        codec.EncodeFrame(payload, writer);
        var buffer = new ReadOnlySequence<byte>(writer.WrittenSpan.ToArray());

        // 只给到一半
        var partial = buffer.Slice(0, 5);
        Assert.False(codec.TryDecodeFrame(ref partial, out _));
        // 未闭合帧保留从 STX 开始的全部字节
        Assert.Equal(5, partial.Length);
    }

    [Fact]
    public void Decode_LoneEtx_IsSkippedAsNoise()
    {
        var codec = new StxEtxFramer();
        var payload = Encoding.UTF8.GetBytes("hi");
        var writer = new ArrayBufferWriter<byte>();
        codec.EncodeFrame(payload, writer);

        // 前缀加孤立 ETX 噪声，再接正常帧
        var noiseAndFrame = new byte[1 + writer.WrittenCount];
        noiseAndFrame[0] = ETX;
        writer.WrittenSpan.CopyTo(noiseAndFrame.AsSpan(1));

        var buffer = new ReadOnlySequence<byte>(noiseAndFrame);
        Assert.True(codec.TryDecodeFrame(ref buffer, out var decoded));
        Assert.Equal(payload, decoded.ToArray());
        Assert.True(buffer.IsEmpty);
    }

    [Fact]
    public void Decode_NewStx_DiscardsPreviousHalfPacket()
    {
        var codec = new StxEtxFramer();
        var payload = Encoding.UTF8.GetBytes("complete");
        var writer = new ArrayBufferWriter<byte>();
        codec.EncodeFrame(payload, writer);
        var full = writer.WrittenSpan.ToArray();

        // 半个旧帧（STX + 部分数据）+ 完整新帧，中途插入新 STX
        var bytes = new List<byte> { STX, 0x41, 0x42, STX };
        bytes.AddRange(full);
        var buffer = new ReadOnlySequence<byte>(bytes.ToArray());

        // 第一帧是从新 STX 开始的完整帧
        Assert.True(codec.TryDecodeFrame(ref buffer, out var decoded));
        Assert.Equal(payload, decoded.ToArray());
        Assert.True(buffer.IsEmpty);
    }

    [Fact]
    public void Decode_OverlongFrame_IsDropped()
    {
        var codec = new StxEtxFramer(maxPayloadBytes: 16);
        // STX + 20 字节负载 + ETX
        var bytes = new List<byte> { STX };
        bytes.AddRange(Enumerable.Repeat((byte)0x41, 20));
        bytes.Add(ETX);

        // 后面接一个正常短帧
        var shortPayload = Encoding.UTF8.GetBytes("ok");
        var shortWriter = new ArrayBufferWriter<byte>();
        codec.EncodeFrame(shortPayload, shortWriter);
        bytes.AddRange(shortWriter.WrittenSpan.ToArray());

        var buffer = new ReadOnlySequence<byte>(bytes.ToArray());

        // 超长帧被跳过，扫描到后面的正常帧
        Assert.True(codec.TryDecodeFrame(ref buffer, out var decoded));
        Assert.Equal(shortPayload, decoded.ToArray());
    }
}
