using System.Diagnostics.Metrics;

namespace StreamFrame;

/// <summary>
/// 连接级内置指标（<see cref="Meter"/> 名 "StreamFrame"，零外部依赖）：
/// 帧收发计数、字节收发计数、重连次数、会话时长、发送队列水位。生产部署用
/// MeterListener / OpenTelemetry 订阅 "StreamFrame" 即可观测，不接入则开销为每次记录
/// 数纳秒级；netstandard2.0 目标为 no-op 兜底（无 Metrics API，记录全部空操作）。
///
/// 标签：单一 "endpoint"（构造时的 ip:port）——每条连接的地址固定，标签基数受控。
/// </summary>
internal sealed class ConnectionMetrics
{
    private static readonly Meter Meter = new("StreamFrame");

    private static readonly Counter<long> FramesSent = Meter.CreateCounter<long>(
        "streamframe.frames_sent", unit: "frames", description: "整帧写入 socket 的业务帧数（含普通与会话绑定发送）。");

    private static readonly Counter<long> FramesReceived = Meter.CreateCounter<long>(
        "streamframe.frames_received", unit: "frames", description: "解码切出的业务帧数。");

    private static readonly Counter<long> BytesSent = Meter.CreateCounter<long>(
        "streamframe.bytes_sent", unit: "bytes", description: "写入 socket 的字节总数（含帧定界字节）。");

    private static readonly Counter<long> BytesReceived = Meter.CreateCounter<long>(
        "streamframe.bytes_received", unit: "bytes", description: "从 socket 收到的字节总数（含噪声与未完成帧）。");

    private static readonly Counter<long> Reconnects = Meter.CreateCounter<long>(
        "streamframe.reconnects", unit: "reconnects", description: "进入重连的次数（故障触发与用户显式 Reconnect 均计数）。");

    private static readonly Histogram<double> SessionDuration = Meter.CreateHistogram<double>(
        "streamframe.session_duration", unit: "s", description: "单次 TCP 会话从建立到拆除的时长。");

    private static readonly Histogram<long> SendQueueLength = Meter.CreateHistogram<long>(
        "streamframe.send_queue_length", unit: "items", description: "发送队列水位（每次入队时采样）。");

    private readonly KeyValuePair<string, object?> _tag;

    public ConnectionMetrics(string endpoint)
        => _tag = new("endpoint", endpoint);

    public void FrameSent()
        => FramesSent.Add(1, _tag);

    public void FrameReceived()
        => FramesReceived.Add(1, _tag);

    public void AddBytesSent(long count)
        => BytesSent.Add(count, _tag);

    public void AddBytesReceived(long count)
        => BytesReceived.Add(count, _tag);

    public void Reconnect()
        => Reconnects.Add(1, _tag);

    public void SessionEnded(double seconds)
        => SessionDuration.Record(seconds, _tag);

    public void SendQueueObserved(int count)
        => SendQueueLength.Record(count, _tag);
}
