namespace StreamFrame;


/// <summary>
/// <see cref="IStreamConnection{TMessage}.FrameError"/> 事件的参数。
///
/// <see cref="Bytes"/> 是事件对应字节的一份拷贝（坏帧负载 / 被丢弃的字节 / 超限缓冲快照），
/// 回调返回后可安全长期留存；<see cref="Exception"/> 为 DecodeFailed 时的原始解码异常。
/// </summary>
public sealed class FrameErrorEventArgs : EventArgs
{
    /// <summary>创建帧诊断事件参数。</summary>
    /// <param name="kind">事件类别。</param>
    /// <param name="bytes">已拷贝的事件字节（可安全留存）。</param>
    /// <param name="exception">DecodeFailed 时的原始异常；其它类别为 null。</param>
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
