using System.Buffers;

namespace StreamFrame.Abstractions;

/// <summary>
/// STX/ETX 成对包裹的帧定界：0x02 (STX) 起、0x03 (ETX) 止，负载不做转义。
///
/// 注意：plain 模式要求负载不得包含 0x02/0x03，否则会被误判为帧边界。
/// 适合 XML / 纯文本等已知安全的负载；二进制负载请改用 <see cref="LengthPrefixFrameCodec"/>。
///
/// 边界处理（与 SamSung 一致）：
/// <list type="bullet">
/// <item>在待定帧的 ETX 收到之前若再次遇到 STX，则丢弃当前部分帧并从新 STX 处重新开始。</item>
/// <item>无前导 STX 的孤立 ETX 被视为噪声字节跳过。</item>
/// <item>到达缓冲区末尾但无闭合 ETX 时回到最后一个 STX 处等待后续数据。</item>
/// </list>
/// 实现 <see cref="IStreamingFrameCodec"/>：BeginFrame 写 STX、EndFrame 写 ETX，
/// 与 <see cref="IFrameCodec.EncodeFrame"/> 字节输出完全一致。
/// </summary>
public sealed class StxEtxFrameCodec : IStreamingFrameCodec, IFrameDiscardReporting
{
    private const byte STX = 0x02;
    private const byte ETX = 0x03;

    public const int DefaultMaxPayloadBytes = 16 * 1024 * 1024;

    public int MaxPayloadBytes { get; }

    public StxEtxFrameCodec(int maxPayloadBytes = DefaultMaxPayloadBytes)
    {
        if (maxPayloadBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxPayloadBytes));

        MaxPayloadBytes = maxPayloadBytes;
    }

    public void EncodeFrame(ReadOnlySpan<byte> payload, IBufferWriter<byte> writer)
    {
        if (payload.Length > MaxPayloadBytes)
            throw new InvalidOperationException($"Frame payload of {payload.Length} bytes exceeds MaxPayloadBytes={MaxPayloadBytes}.");

        if (payload.IndexOfAny(STX, ETX) >= 0)
            throw new InvalidOperationException(
                "Plain STX/ETX framing cannot carry payload bytes 0x02/0x03. " +
                "Use LengthPrefixFrameCodec for binary payloads.");

        var destination = writer.GetSpan(payload.Length + 2);
        destination[0] = STX;
        payload.CopyTo(destination[1..]);
        destination[payload.Length + 1] = ETX;
        writer.Advance(payload.Length + 2);
    }

    public void BeginFrame(IWrittenBufferWriter writer)
    {
        var destination = writer.GetSpan(1);
        destination[0] = STX;
        writer.Advance(1);
    }

    public void EndFrame(IWrittenBufferWriter writer)
    {
        var payloadLength = writer.WrittenCount - 1; // 减掉 BeginFrame 写入的 STX
        if (payloadLength < 0 || payloadLength > MaxPayloadBytes)
            throw new InvalidOperationException($"Frame payload of {payloadLength} bytes exceeds MaxPayloadBytes={MaxPayloadBytes}.");

        // 校验负载不包含 STX/ETX（plain 协议约束），防止发送不可解析的帧。
        // 解码端遇到负载内的 STX 会截断、ETX 会提前闭合，必须在发送前拒绝。
        var payload = writer.WrittenSpan.Slice(1);
        if (payload.IndexOfAny(STX, ETX) >= 0)
            throw new InvalidOperationException(
                "Plain STX/ETX framing cannot carry payload bytes 0x02/0x03. " +
                "Use LengthPrefixFrameCodec for binary payloads.");

        var destination = writer.GetSpan(1);
        destination[0] = ETX;
        writer.Advance(1);
    }

    public bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload)
        => TryDecodeFrame(ref buffer, out payload, out _);

    public bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload, out ReadOnlySequence<byte> discarded)
    {
        payload = default;
        var original = buffer;

        var reader = new SequenceReader<byte>(buffer);

        long lastStxOffset = -1;
        long lastStxPayloadStart = -1;

        while (reader.TryRead(out var b))
        {
            if (b == STX)
            {
                lastStxOffset = reader.Consumed - 1; // STX 位置
                lastStxPayloadStart = reader.Consumed; // 负载起始位置
            }
            else if (b == ETX)
            {
                if (lastStxPayloadStart < 0)
                    continue; // 孤立 ETX，跳过

                var payloadLength = reader.Consumed - 1 - lastStxPayloadStart;
                if (payloadLength > MaxPayloadBytes)
                {
                    // 超长帧：丢弃当前部分帧并继续扫描
                    lastStxPayloadStart = -1;
                    lastStxOffset = -1;
                    continue;
                }

                payload = buffer.Slice(lastStxPayloadStart, payloadLength);
                buffer = buffer.Slice(reader.Consumed);
                discarded = original.Slice(0, lastStxOffset); // 保留点之前的噪声/被中止半帧
                return true;
            }
        }

        // 缓冲区耗尽但未收到完整帧：保留自最后一个 STX 起的余量等待后续数据。
        // 保留点之前的字节（杂散噪声、被更新的 STX 中止的旧半帧）即为本次丢弃。
        discarded = original.Slice(0, lastStxOffset >= 0 ? lastStxOffset : original.Length);
        buffer = lastStxOffset >= 0
            ? buffer.Slice(lastStxOffset)
            : buffer.Slice(buffer.End);

        return false;
    }
}
