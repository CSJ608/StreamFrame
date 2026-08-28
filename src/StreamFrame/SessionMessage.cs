namespace StreamFrame;

/// <summary>
/// 带会话归属的业务消息：<see cref="ISessionAwareStreamConnection{TMessage}.GetSessionMessages"/>
/// 的产出元素。旧会话的解码任务在会话拆除后迟到投递的消息，仍携带旧会话的编号。
/// </summary>
/// <typeparam name="TMessage">业务消息类型。</typeparam>
public readonly record struct SessionMessage<TMessage>
{
    /// <summary>创建带会话归属的消息。</summary>
    /// <param name="sessionId">消息所属的 TCP 会话编号（会话建立时分配，单调递增、不复用）。</param>
    /// <param name="message">业务消息。</param>
    public SessionMessage(long sessionId, TMessage message)
    {
        SessionId = sessionId;
        Message = message;
    }

    /// <summary>消息所属的 TCP 会话编号；0 不会出现（0 保留表示"无会话"）。</summary>
    public long SessionId { get; }

    /// <summary>业务消息。</summary>
    public TMessage Message { get; }
}
