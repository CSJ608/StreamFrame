using System.Buffers;

namespace StreamFrame.Abstractions;

/// <summary>
/// 帧定界器的可选能力：精确上报被当作噪声/垃圾丢弃的字节。
///
/// 与 <see cref="IFrameCodec.TryDecodeFrame"/> 语义完全一致，仅额外通过
/// <paramref name="discarded"/> 返回本次调用中被定界器跳过的字节（如非法长度头重同步
/// 丢弃的头字节、STX/ETX 流中的杂散字节、被新 STX 中止的旧半帧内容）。
/// 正常切帧时 <paramref name="discarded"/> 为空。
///
/// 连接层检测到本接口后，会把这些字节通过 FrameError 事件（Kind=DiscardedByResync）
/// 交给上层调试；不实现本接口的第三方 codec 不受影响，只是没有丢弃上报。
/// </summary>
public interface IFrameDiscardReporting
{
    /// <summary>
    /// 尝试从 <paramref name="buffer"/> 中切出一帧，并返回本次被丢弃的字节。
    /// 语义与 <see cref="IFrameCodec.TryDecodeFrame"/> 一致。
    /// </summary>
    /// <param name="buffer">待解析的字节流；成功时前进到下一帧起点，失败时保留未消费字节。</param>
    /// <param name="payload">切出的帧内负载（不含定界字节）。</param>
    /// <param name="discarded">本次调用中被定界器丢弃的字节；无丢弃时为空序列。</param>
    /// <returns>成功切出一帧返回 true；数据不足（半包）返回 false。</returns>
    bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload, out ReadOnlySequence<byte> discarded);
}
