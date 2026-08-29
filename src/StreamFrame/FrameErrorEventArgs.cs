namespace StreamFrame;

/// <summary>
/// <see cref="IStreamConnection{TMessage}.FrameError"/> 事件的参数。
///
/// <see cref="Bytes"/> 是事件对应字节的一份拷贝（坏帧负载 / 被丢弃的字节 / 超限缓冲快照），
/// 回调返回后可安全长期留存；<see cref="Exception"/> 为 DecodeFailed 时的原始解码异常；
/// <see cref="SessionId"/> 标记错误由哪次 TCP 会话检出，<see cref="ObservedByteCount"/> 与
/// <see cref="IsTruncated"/> 描述快照相对原始观测数据的完整性。
/// </summary>
public sealed class FrameErrorEventArgs : EventArgs
{
    /// <summary>
    /// 创建帧诊断事件参数（兼容重载：无会话归属，观测字节数按完整数据计，
    /// 因此 <see cref="IsTruncated"/> 恒为 false）。由
    /// <see cref="StreamConnection{TMessage}"/> 发出的事件不走本重载。
    /// </summary>
    /// <param name="kind">事件类别。</param>
    /// <param name="bytes">已拷贝的事件字节（可安全留存）。</param>
    /// <param name="exception">DecodeFailed 时的原始异常；其它类别为 null。</param>
    public FrameErrorEventArgs(FrameErrorKind kind, ReadOnlyMemory<byte> bytes, Exception? exception = null)
        : this(kind, bytes, exception, sessionId: 0, observedByteCount: bytes.Length)
    {
    }

    /// <summary>创建带会话归属与快照完整性的帧诊断事件参数。</summary>
    /// <param name="kind">事件类别。</param>
    /// <param name="bytes">已拷贝的事件字节（可安全留存）；为原始观测数据的前缀（可能截断）。</param>
    /// <param name="exception">DecodeFailed 时的原始异常；其它类别为 null。</param>
    /// <param name="sessionId">检测到错误的解码器绑定的会话编号（正数）。</param>
    /// <param name="observedByteCount">原始观测字节数，须 >= <paramref name="bytes"/> 的长度（含义见 <see cref="ObservedByteCount"/>）。</param>
    public FrameErrorEventArgs(
        FrameErrorKind kind,
        ReadOnlyMemory<byte> bytes,
        Exception? exception,
        long sessionId,
        long observedByteCount)
    {
        Kind = kind;
        Bytes = bytes;
        Exception = exception;
        SessionId = sessionId;
        ObservedByteCount = observedByteCount;
        IsTruncated = bytes.Length < observedByteCount;
    }

    /// <summary>事件类别。</summary>
    public FrameErrorKind Kind { get; }

    /// <summary>事件对应的字节（已拷贝，可留存）。</summary>
    public ReadOnlyMemory<byte> Bytes { get; }

    /// <summary>DecodeFailed 时的原始解码异常；其它类别为 null。</summary>
    public Exception? Exception { get; }

    /// <summary>
    /// 检测到错误的解码器绑定的 TCP 会话编号：与
    /// <see cref="ISessionAwareStreamConnection{TMessage}.CurrentSessionId"/> /
    /// <see cref="SessionMessage{TMessage}.SessionId"/> 同一编号空间（正数、单调递增、
    /// 跨重连不复用），可跨 API 直接关联比对。
    ///
    /// 事件参数在解码器内部构造、构造时绑定：会话重建后延迟到达的旧会话事件仍携带
    /// 旧编号，不会在投递时被改写为当前会话（回调里读 CurrentSessionId 得到的是回调
    /// 时刻的会话，不能证明归属）。由 <see cref="StreamConnection{TMessage}"/> 发出的
    /// 事件恒为正数；0 仅出现在用兼容构造重载手工构造的参数中（表示未提供）。
    /// </summary>
    public long SessionId { get; }

    /// <summary>
    /// 检测到错误时实际观测到的字节数，恒满足 <c>ObservedByteCount &gt;= Bytes.Length</c>。
    /// 含义按 <see cref="Kind"/>：
    /// <list type="table">
    /// <item><term>DecodeFailed</term><description>坏帧的帧内负载字节数（不含定界字节；当前为完整拷贝）。</description></item>
    /// <item><term>DiscardedByResync</term><description>本次被定界器丢弃的字节数（当前为完整拷贝）。</description></item>
    /// <item><term>IncompleteFrameTimeout</term><description>超时判定时缓冲中半帧的全部字节数（含已收到的长度头部分）。</description></item>
    /// <item><term>IncompleteFrameOverflow</term><description>超限判定时的未完成帧缓冲字节数。</description></item>
    /// </list>
    /// </summary>
    public long ObservedByteCount { get; }

    /// <summary>
    /// <see cref="Bytes"/> 是否为截断快照：等价于 <c>Bytes.Length &lt; ObservedByteCount</c>。
    /// IncompleteFrameTimeout / IncompleteFrameOverflow 的快照有 8192 字节上限，原始观测
    /// 数据更长时 <see cref="Bytes"/> 只携带前缀；DecodeFailed / DiscardedByResync 当前为
    /// 完整数据，恒为 false。
    /// </summary>
    public bool IsTruncated { get; }
}
