using System.Buffers;
using System.Text;

namespace StreamFrame.Tests;

public class LengthPrefixFramerTests
{
    private static (byte[] frame, ReadOnlySequence<byte> buffer) EncodeSingle(IFramer codec, byte[] payload)
    {
        var writer = new ArrayBufferWriter<byte>();
        codec.EncodeFrame(payload, writer);
        return (writer.WrittenSpan.ToArray(), new ReadOnlySequence<byte>(writer.WrittenSpan.ToArray()));
    }

    [Fact]
    public void Encode_WritesBigEndianLengthHeader()
    {
        var codec = new LengthPrefixFramer();
        var payload = Encoding.UTF8.GetBytes("hello");

        var writer = new ArrayBufferWriter<byte>();
        codec.EncodeFrame(payload, writer);
        var bytes = writer.WrittenSpan.ToArray();

        Assert.Equal(9, bytes.Length);
        // 大端 0x00 0x00 0x00 0x05
        Assert.Equal(new byte[] { 0x00, 0x00, 0x00, 0x05 }, bytes[..4]);
        Assert.Equal(payload, bytes[4..]);
    }

    [Fact]
    public void Decode_ExtractsPayloadAndAdvances()
    {
        var codec = new LengthPrefixFramer();
        var payload = Encoding.UTF8.GetBytes("hello world");
        var (frame, buffer) = EncodeSingle(codec, payload);

        Assert.True(codec.TryDecodeFrame(ref buffer, out var decoded));
        Assert.Equal(payload, decoded.ToArray());
        Assert.True(buffer.IsEmpty);
        Assert.Equal(0, frame.Length - payload.Length - 4);
    }

    [Fact]
    public void Decode_HalfPacket_ReturnsFalse()
    {
        var codec = new LengthPrefixFramer();
        var payload = Encoding.UTF8.GetBytes("hello");
        var (_, buffer) = EncodeSingle(codec, payload);

        // 只给长度头 + 2 字节负载
        var partial = buffer.Slice(0, 6);
        Assert.False(codec.TryDecodeFrame(ref partial, out _));
    }

    [Fact]
    public void Decode_GluedFrames_ExtractsAll()
    {
        var codec = new LengthPrefixFramer();
        var p1 = Encoding.UTF8.GetBytes("first");
        var p2 = Encoding.UTF8.GetBytes("second-message");

        var all = new ArrayBufferWriter<byte>();
        codec.EncodeFrame(p1, all);
        codec.EncodeFrame(p2, all);
        var buffer = new ReadOnlySequence<byte>(all.WrittenSpan.ToArray());

        Assert.True(codec.TryDecodeFrame(ref buffer, out var f1));
        Assert.Equal(p1, f1.ToArray());
        Assert.True(codec.TryDecodeFrame(ref buffer, out var f2));
        Assert.Equal(p2, f2.ToArray());
        Assert.True(buffer.IsEmpty);
    }

    [Fact]
    public void Decode_InvalidLength_DiscardsHeader()
    {
        var codec = new LengthPrefixFramer();
        // 负载长度 16MB+1，超过 MaxPayloadBytes（16MB）
        var bytes = new byte[] { 0x01, 0x00, 0x00, 0x01, 0x41, 0x42 };
        var buffer = new ReadOnlySequence<byte>(bytes);

        Assert.False(codec.TryDecodeFrame(ref buffer, out _));
        // 长度头被丢弃，剩下 0x41 0x42 供重同步
        Assert.Equal(2, buffer.Length);
    }

    [Fact]
    public void Decode_NegativeLength_DiscardsHeader()
    {
        var codec = new LengthPrefixFramer();
        // 0xFF 开头即负数
        var bytes = new byte[] { 0xFF, 0xFF, 0xFF, 0xFE, 0x41 };
        var buffer = new ReadOnlySequence<byte>(bytes);

        Assert.False(codec.TryDecodeFrame(ref buffer, out _));
        Assert.Equal(1, buffer.Length);
    }
}
