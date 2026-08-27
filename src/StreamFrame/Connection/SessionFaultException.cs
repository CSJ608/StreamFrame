namespace StreamFrame;

/// <summary>
/// 会话级故障（帧解码失败、不完整帧超限、接收空闲超时等）：当前会话不可恢复，
/// 由连接层统一转为断线重连。仅框架内部控制流使用。
/// </summary>
internal sealed class SessionFaultException : Exception
{
    public SessionFaultException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}
