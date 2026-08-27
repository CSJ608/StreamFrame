namespace StreamFrame;

/// <summary>
/// 帧结构完整但内容解码失败（codec 抛异常）时的处理策略。
/// </summary>
public enum DecodeErrorPolicy
{
    /// <summary>
    /// 断开连接并走自动重连（默认）。协议内容错乱后流状态通常不可信，
    /// 重连是对端与本地同时回到一致状态的最短路径。
    /// </summary>
    Disconnect,

    /// <summary>
    /// 丢弃坏帧、继续解析后续帧。适合线路噪声多、流定界自身可靠（如长度前缀）的场景；
    /// 坏帧会通过 FrameError 事件上报。
    /// </summary>
    SkipFrame,
}
