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
/// </summary>
public sealed class StxEtxFrameCodec : IFrameCodec
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

    public bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload)
    {
        payload = default;

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
                return true;
            }
        }

        // 缓冲区耗尽但未收到完整帧
        buffer = lastStxOffset >= 0
            ? buffer.Slice(lastStxOffset) // 回到最后一个 STX 位置等待更多数据
            : buffer.Slice(buffer.End);   // 消费全部噪声字节

        return false;
    }
}
