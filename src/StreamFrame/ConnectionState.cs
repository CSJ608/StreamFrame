namespace StreamFrame;

/// <summary>
/// 连接生命周期状态。
/// </summary>
public enum ConnectionState
{
    /// <summary>正在尝试建立连接（主动 connect 或被动 accept）。</summary>
    Connecting,

    /// <summary>已建立 TCP 连接，可收发数据。</summary>
    Connected,

    /// <summary>连接中断，进入重连流程。</summary>
    Retry,

    /// <summary>连接已终止且不再重连：Dispose，或 Start 的取消令牌被取消。终态。</summary>
    Disconnected,
}
