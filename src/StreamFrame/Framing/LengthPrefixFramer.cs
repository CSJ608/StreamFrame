using System.Buffers;
using System.Buffers.Binary;

namespace StreamFrame;

/// <summary>
/// 4 字节大端长度前缀 + 负载 的帧定界。负载最大 16 MiB。
/// 实现 <see cref="IStreamingFramer"/>：支持单缓冲原地编码（BeginFrame 预留长度位，
/// EndFrame 回填长度），与 <see cref="IFramer.EncodeFrame"/> 字节输出完全一致。
/// </summary>
public sealed class LengthPrefixFramer : IStreamingFramer, IFrameDiscardReporting
{
    public const int LengthPrefixSize = 4;
    public const int DefaultMaxPayloadBytes = 16 * 1024 * 1024;

    public int MaxPayloadBytes { get; }

    public LengthPrefixFramer(int maxPayloadBytes = DefaultMaxPayloadBytes)
    {
        if (maxPayloadBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxPayloadBytes));

        MaxPayloadBytes = maxPayloadBytes;
    }

    public void EncodeFrame(ReadOnlySpan<byte> payload, IBufferWriter<byte> writer)
    {
        if (payload.Length > MaxPayloadBytes)
            throw new InvalidOperationException($"Frame payload of {payload.Length} bytes exceeds MaxPayloadBytes={MaxPayloadBytes}.");

        var header = writer.GetSpan(LengthPrefixSize);
        BinaryPrimitives.WriteInt32BigEndian(header, payload.Length);
        writer.Advance(LengthPrefixSize);
        writer.Write(payload);
    }

    public void BeginFrame(IWrittenBufferWriter writer)
    {
        var header = writer.GetSpan(LengthPrefixSize);
        header.Clear(); // 占位 4 字节，EndFrame 回填
        writer.Advance(LengthPrefixSize);
    }

    public void EndFrame(IWrittenBufferWriter writer)
    {
        var payloadLength = writer.WrittenCount - LengthPrefixSize;
        if (payloadLength < 0 || payloadLength > MaxPayloadBytes)
            throw new InvalidOperationException($"Frame payload of {payloadLength} bytes exceeds MaxPayloadBytes={MaxPayloadBytes}.");

        var header = writer.WrittenSpan.Slice(0, LengthPrefixSize);
        BinaryPrimitives.WriteInt32BigEndian(header, payloadLength);
    }

    public bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload)
        => TryDecodeFrame(ref buffer, out payload, out _);

    public bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload, out ReadOnlySequence<byte> discarded)
    {
        payload = default;
        discarded = default;
        if (buffer.Length < LengthPrefixSize)
            return false;

        var length = ReadLengthPrefix(buffer);

        // 非法长度（负数 / 超上限）：丢弃长度头，尝试从下一字节重新同步。
        if ((uint)length > (uint)MaxPayloadBytes)
        {
            discarded = buffer.Slice(0, LengthPrefixSize);
            buffer = buffer.Slice(LengthPrefixSize);
            return false;
        }

        var frameLength = LengthPrefixSize + length;
        if (buffer.Length < frameLength)
            return false;

        payload = buffer.Slice(LengthPrefixSize, length);
        buffer = buffer.Slice(frameLength);
        return true;
    }

    private static int ReadLengthPrefix(in ReadOnlySequence<byte> buffer)
    {
        var header = buffer.Slice(0, LengthPrefixSize);
        if (header.IsSingleSegment)
            return BinaryPrimitives.ReadInt32BigEndian(header.First.Span);

        Span<byte> tmp = stackalloc byte[LengthPrefixSize];
        header.CopyTo(tmp);
        return BinaryPrimitives.ReadInt32BigEndian(tmp);
    }
}
