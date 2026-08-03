using System.Buffers;

namespace StreamFrame.Abstractions;

/// <summary>
/// 数据帧定界策略：负责把业务负载（payload）编成线上帧，以及从字节流中切出完整帧。
///
/// 发送与接收共用同一实现，保证一条连接上帧格式永远一致。
/// </summary>
public interface IFrameCodec
{
    /// <summary>单帧负载允许的最大字节数，用于防御超长帧撑爆缓冲区。</summary>
    int MaxPayloadBytes { get; }

    /// <summary>
    /// 对完整的帧内负载（不含定界字节）加帧定界并写入 <paramref name="writer"/>。
    /// </summary>
    void EncodeFrame(ReadOnlySpan<byte> payload, IBufferWriter<byte> writer);

    /// <summary>
    /// 尝试从 <paramref name="buffer"/> 中切出一帧。
    /// </summary>
    /// <param name="buffer">待解析的字节流；成功时前进到下一帧起点，失败时保留未消费字节。</param>
    /// <param name="payload">切出的帧内负载（不含定界字节）。</param>
    /// <returns>成功切出一帧返回 true；数据不足（半包）返回 false。</returns>
    bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload);
}
