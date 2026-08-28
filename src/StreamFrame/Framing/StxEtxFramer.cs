using System.Buffers;

namespace StreamFrame;

/// <summary>
/// STX/ETX 成对包裹的帧定界：0x02 (STX) 起、0x03 (ETX) 止，负载不做转义。
///
/// 注意：plain 模式要求负载不得包含 0x02/0x03，否则会被误判为帧边界。
/// 适合 XML / 纯文本等已知安全的负载；二进制负载请改用 <see cref="LengthPrefixFramer"/>。
///
/// 边界处理（与 SamSung 一致）：
/// <list type="bullet">
/// <item>在待定帧的 ETX 收到之前若再次遇到 STX，则丢弃当前部分帧并从新 STX 处重新开始。</item>
/// <item>无前导 STX 的孤立 ETX 被视为噪声字节跳过。</item>
/// <item>到达缓冲区末尾但无闭合 ETX 时回到最后一个 STX 处等待后续数据。</item>
/// </list>
/// 实现 <see cref="IStreamingFramer"/>：BeginFrame 写 STX、EndFrame 写 ETX，
/// 与 <see cref="IFramer.EncodeFrame"/> 字节输出完全一致。
/// </summary>
public sealed class StxEtxFramer : IStreamingFramer, IFrameDiscardReporting
{
    private const byte STX = 0x02;
    private const byte ETX = 0x03;

#if !NETSTANDARD2_0
    /// <summary>向量化候选集：解码扫描时一次跳到最近的 STX/ETX（net8+ 的 SearchValues）。</summary>
    private static readonly System.Buffers.SearchValues<byte> StxEtxSearchValues =
        System.Buffers.SearchValues.Create(new byte[] { STX, ETX });
#endif

    /// <summary>默认负载上限（16 MiB）。</summary>
    public const int DefaultMaxPayloadBytes = 16 * 1024 * 1024;

    /// <inheritdoc />
    public int MaxPayloadBytes { get; }

    /// <summary>创建 STX/ETX 定界器。</summary>
    /// <param name="maxPayloadBytes">单帧负载上限，默认 16 MiB。</param>
    public StxEtxFramer(int maxPayloadBytes = DefaultMaxPayloadBytes)
    {
        if (maxPayloadBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxPayloadBytes));

        MaxPayloadBytes = maxPayloadBytes;
    }

    /// <inheritdoc />
    public void EncodeFrame(ReadOnlySpan<byte> payload, IBufferWriter<byte> writer)
    {
        if (payload.Length > MaxPayloadBytes)
            throw new InvalidOperationException($"Frame payload of {payload.Length} bytes exceeds MaxPayloadBytes={MaxPayloadBytes}.");

        if (payload.IndexOfAny(STX, ETX) >= 0)
            throw new InvalidOperationException(
                "Plain STX/ETX framing cannot carry payload bytes 0x02/0x03. " +
                "Use LengthPrefixFramer for binary payloads.");

        var destination = writer.GetSpan(payload.Length + 2);
        destination[0] = STX;
        payload.CopyTo(destination[1..]);
        destination[payload.Length + 1] = ETX;
        writer.Advance(payload.Length + 2);
    }

    /// <inheritdoc />
    public void BeginFrame(IWrittenBufferWriter writer)
    {
        var destination = writer.GetSpan(1);
        destination[0] = STX;
        writer.Advance(1);
    }

    /// <inheritdoc />
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
                "Use LengthPrefixFramer for binary payloads.");

        var destination = writer.GetSpan(1);
        destination[0] = ETX;
        writer.Advance(1);
    }

    /// <inheritdoc />
    public bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload)
        => TryDecodeFrame(ref buffer, out payload, out _);

    /// <inheritdoc />
    public bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload, out ReadOnlySequence<byte> discarded)
    {
        payload = default;
        var original = buffer;

        // 手工扫描（不依赖 SequenceReader——它从未发布 netstandard2.0 包资产）：
        // 单段缓冲直接扫 Span（零分配）；多段缓冲租借临时数组拼连续后扫。
        byte[]? rented = null;
        ReadOnlySpan<byte> span;
        if (buffer.IsSingleSegment)
        {
            span = buffer.First.Span; // FirstSpan（netcore 专属）不可用；单段时 First.Span 等价
        }
        else
        {
            rented = ArrayPool<byte>.Shared.Rent((int)buffer.Length);
            buffer.CopyTo(rented);
            span = rented.AsSpan(0, (int)buffer.Length);
        }

        try
        {
            long lastStxOffset = -1;
            long lastStxPayloadStart = -1;

#if !NETSTANDARD2_0
            // 向量化扫描（net8+）：SearchValues 让 IndexOfAny 一次跳到最近的 STX/ETX 候选，
            // 不含候选的整段字节直接跳过（旧逐字节实现约 1.1µs/帧）。语义与逐字节版完全一致。
            var i = 0;
            while (i < span.Length)
            {
                var hit = span.Slice(i).IndexOfAny(StxEtxSearchValues);
                if (hit < 0)
                    break;
                i += hit;

                if (span[i] == STX)
                {
                    lastStxOffset = i;           // STX 位置
                    lastStxPayloadStart = i + 1; // 负载起始位置
                }
                else if (lastStxPayloadStart >= 0) // ETX：有已开启的帧才可能闭合
                {
                    var payloadLength = i - lastStxPayloadStart;
                    if (payloadLength <= MaxPayloadBytes)
                    {
                        payload = buffer.Slice(lastStxPayloadStart, payloadLength);
                        buffer = buffer.Slice(i + 1);
                        discarded = original.Slice(0, lastStxOffset); // 保留点之前的噪声/被中止半帧
                        return true;
                    }

                    // 超长帧：丢弃当前部分帧并继续扫描
                    lastStxPayloadStart = -1;
                    lastStxOffset = -1;
                }
                // 孤立 ETX（无已开启的帧）：视为噪声字节跳过

                i++;
            }
#else
            for (var i = 0; i < span.Length; i++)
            {
                var b = span[i];
                if (b == STX)
                {
                    lastStxOffset = i;      // STX 位置
                    lastStxPayloadStart = i + 1; // 负载起始位置
                }
                else if (b == ETX)
                {
                    if (lastStxPayloadStart < 0)
                        continue; // 孤立 ETX，跳过

                    var payloadLength = i - lastStxPayloadStart;
                    if (payloadLength > MaxPayloadBytes)
                    {
                        // 超长帧：丢弃当前部分帧并继续扫描
                        lastStxPayloadStart = -1;
                        lastStxOffset = -1;
                        continue;
                    }

                    payload = buffer.Slice(lastStxPayloadStart, payloadLength);
                    buffer = buffer.Slice(i + 1);
                    discarded = original.Slice(0, lastStxOffset); // 保留点之前的噪声/被中止半帧
                    return true;
                }
            }
#endif

            // 缓冲区耗尽但未收到完整帧：保留自最后一个 STX 起的余量等待后续数据。
            // 保留点之前的字节（杂散噪声、被更新的 STX 中止的旧半帧）即为本次丢弃。
            discarded = original.Slice(0, lastStxOffset >= 0 ? lastStxOffset : original.Length);
            buffer = lastStxOffset >= 0
                ? buffer.Slice(lastStxOffset)
                : buffer.Slice(buffer.End);

            return false;
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
