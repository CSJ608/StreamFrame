namespace StreamFrame;

/// <summary>帧层诊断事件的类别。</summary>
public enum FrameErrorKind
{
    /// <summary>帧结构完整，但帧内负载解码失败（codec 抛出异常）。</summary>
    DecodeFailed,

    /// <summary>字节被帧定界器当作噪声/垃圾丢弃（如非法长度头重同步、STX/ETX 之外的杂散字节）。</summary>
    DiscardedByResync,

    /// <summary>未完成帧的已缓冲字节超过上限，判定流不可恢复。</summary>
    IncompleteFrameOverflow,
}

/// <summary>
/// <see cref="IStreamConnection{TMessage}.FrameError"/> 事件的参数。
///
/// <see cref="Bytes"/> 是事件对应字节的一份拷贝（坏帧负载 / 被丢弃的字节 / 超限缓冲快照），
/// 回调返回后可安全长期留存；<see cref="Exception"/> 为 DecodeFailed 时的原始解码异常。
/// </summary>
public sealed class FrameErrorEventArgs : EventArgs
{
    public FrameErrorEventArgs(FrameErrorKind kind, ReadOnlyMemory<byte> bytes, Exception? exception = null)
    {
        Kind = kind;
        Bytes = bytes;
        Exception = exception;
    }

    /// <summary>事件类别。</summary>
    public FrameErrorKind Kind { get; }

    /// <summary>事件对应的字节（已拷贝，可留存）。</summary>
    public ReadOnlyMemory<byte> Bytes { get; }

    /// <summary>DecodeFailed 时的原始解码异常；其它类别为 null。</summary>
    public Exception? Exception { get; }
}
