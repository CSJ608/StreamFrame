namespace StreamFrame;

/// <summary>
/// 会话绑定发送无法完成：消息绑定的会话在整帧写入 socket 之前终止（断线重连、
/// 对端关闭、Dispose 等）。该消息<b>不会</b>被自动转移到新会话重放。
///
/// 收到此异常时的公共保证：本地未取得"整帧已交给本机 socket"的完成确认；本次发送
/// 以失败结束。<b>远端是否已收到（部分）字节视为未知</b>——TCP 发送失败与取消存在
/// 竞态边界，调用方应把远端处理结果视为未知，由上层协议的事务关联、幂等或恢复流程
/// 兜底，而不是假设远端一定未收到。
/// </summary>
public sealed class SessionExpiredException : Exception
{
    /// <summary>创建会话失效异常。</summary>
    /// <param name="sessionId">已失效的会话编号。</param>
    /// <param name="message">异常消息。</param>
    /// <param name="innerException">导致会话终止的底层异常（可选）。</param>
    public SessionExpiredException(long sessionId, string message, Exception? innerException = null)
        : base(message, innerException)
    {
        SessionId = sessionId;
    }

    /// <summary>已失效的会话编号。</summary>
    public long SessionId { get; }
}
