using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace StreamFrame;

/// <summary>
/// 解码循环：从 PipeReader 消费字节流，按帧定界切帧、codec 解码，产出业务消息到通道。
/// 半包靠 AdvanceTo 保留未消费字节；粘包靠循环切尽所有完整帧。
///
/// 生命周期约定一：本类型不负责消息通道（relay）的完成——通道归连接所有，
/// 跨会话复用，仅在连接 Dispose 时完成。会话级失败（解码失败、不完整帧超限、
/// 未完成帧超时）以 <see cref="SessionFaultException"/> 上抛，由连接层决定断线重连。
///
/// 生命周期约定二：ReadAsync 不绑定会话取消令牌（取消中的 ReadAsync 会把 Pipe 留在
/// "读取进行中"状态，无法再 TryRead）。退出由流结束（writer.CompleteAsync）或
/// CancelPendingRead 驱动；退出前把已缓冲的完整帧全部投递，保证"收到的字节必达"。
/// 唯一例外：未完成帧超时使用独立的超时令牌（不链会话取消），超时后本循环直接以
/// 会话故障收尾、不再复用该 PipeReader——因此不会有"进行中的读取残留到下一次读取"。
/// </summary>
internal sealed class FrameDecoder<TMessage>
{
    /// <summary>上报 IncompleteFrameOverflow / IncompleteFrameTimeout 时携带的缓冲快照上限，避免事件本身复制超长数据。</summary>
    private const int ErrorSnapshotBytes = 8192;

    private readonly PipeReader _reader;
    private readonly IFramer _framing;
    private readonly ICodec<TMessage> _codec;
    private readonly Channel<TMessage> _relay;
    private readonly int _maxIncompleteFrameBytes;
    private readonly int _incompleteFrameTimeoutMs;
    private readonly DecodeErrorPolicy _decodeErrorPolicy;
    private readonly Action<FrameErrorEventArgs>? _onFrameError;

    public FrameDecoder(
        PipeReader reader,
        IFramer framing,
        ICodec<TMessage> codec,
        Channel<TMessage> relay,
        int maxIncompleteFrameBytes,
        int incompleteFrameTimeoutMs,
        DecodeErrorPolicy decodeErrorPolicy,
        Action<FrameErrorEventArgs>? onFrameError)
    {
        _reader = reader;
        _framing = framing;
        _codec = codec;
        _relay = relay;
        _maxIncompleteFrameBytes = maxIncompleteFrameBytes;
        _incompleteFrameTimeoutMs = incompleteFrameTimeoutMs;
        _decodeErrorPolicy = decodeErrorPolicy;
        _onFrameError = onFrameError;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        // 未完成帧状态：上次 AdvanceTo 时缓冲里留存的半帧字节（快照在挂表前拷贝——
        // AdvanceTo 之后旧 sequence 不再保证有效）。缓冲为空 = 没有进行中的帧，不计时。
        var pendingBytes = 0L;
        var pendingSnapshot = ReadOnlyMemory<byte>.Empty;

        try
        {
            while (true)
            {
                ReadResult result;
                if (pendingBytes > 0 && _incompleteFrameTimeoutMs > 0)
                {
                    // 计时窗口 = 本次 ReadAsync：新字节到达即返回、循环重来（计时自然重置）；
                    // 令牌刻意不链会话取消（生命周期约定二），只有超时会触发它。
                    using var timeoutCts = new CancellationTokenSource(_incompleteFrameTimeoutMs);
                    try
                    {
                        result = await _reader.ReadAsync(timeoutCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                    {
                        throw TimeoutFault(pendingBytes, pendingSnapshot);
                    }
                }
                else
                {
                    result = await _reader.ReadAsync(CancellationToken.None).ConfigureAwait(false);
                }

                var buffer = result.Buffer;

                // 切尽当前缓冲内的所有完整帧；已缓冲的字节即便会话正在停止也要投递完
                while (TryDecodeFrame(ref buffer, out var payload))
                {
                    TMessage message;
                    try
                    {
                        message = _codec.Decode(payload, CancellationToken.None);
                    }
                    catch (Exception ex)
                    {
                        RaiseFrameError(new FrameErrorEventArgs(FrameErrorKind.DecodeFailed, payload.ToArray(), ex));
                        if (_decodeErrorPolicy == DecodeErrorPolicy.Disconnect)
                            throw new SessionFaultException($"帧负载解码失败: {ex.Message}", ex);

                        continue; // SkipFrame：丢弃坏帧，继续后续帧
                    }

                    try
                    {
                        await _relay.Writer.WriteAsync(message, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (ChannelClosedException)
                    {
                        return; // 连接已 Dispose，消息通道关闭
                    }
                }

                if (buffer.Length > _maxIncompleteFrameBytes)
                {
                    // 未完成帧永远等不齐或对端蓄意喂半帧：判定流不可恢复，交由连接层断线。
                    var snapshot = buffer.Slice(0, Math.Min(buffer.Length, ErrorSnapshotBytes)).ToArray();
                    RaiseFrameError(new FrameErrorEventArgs(FrameErrorKind.IncompleteFrameOverflow, snapshot));
                    throw new SessionFaultException(
                        $"未完成帧缓冲 {buffer.Length} 字节超过上限 {_maxIncompleteFrameBytes} 字节。");
                }

                // 挂表前拷贝半帧快照；缓冲为空（帧刚切尽/尚未开始）时清零，下轮不计时
                pendingBytes = buffer.Length;
                pendingSnapshot = buffer.IsEmpty
                    ? ReadOnlyMemory<byte>.Empty
                    : buffer.Slice(0, (int)Math.Min(buffer.Length, ErrorSnapshotBytes)).ToArray();

                _reader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    break; // 字节流结束（对端断开/会话停止）：缓冲已投递完，正常退出

                if (result.IsCanceled)
                {
                    // CancelPendingRead（会话停止时唤醒）：缓冲已投递完，正常退出
                    if (ct.IsCancellationRequested)
                        break;

                    // 非会话取消的 IsCanceled 只能来自未完成帧超时令牌的闩锁路径
                    // （数据与取消同时到达时 ReadAsync 不抛异常、以 IsCanceled 结果返回）
                    if (pendingBytes > 0 && _incompleteFrameTimeoutMs > 0)
                        throw TimeoutFault(pendingBytes, pendingSnapshot);
                }
            }
        }
        finally
        {
            // 无论正常退出还是故障，都释放 Pipe 的池化段
            await _reader.CompleteAsync().ConfigureAwait(false);
        }
    }

    /// <summary>未完成帧超时的会话故障：先上报带快照的 FrameError，再抛出（由连接层断线重连）。</summary>
    private SessionFaultException TimeoutFault(long pendingBytes, ReadOnlyMemory<byte> snapshot)
    {
        RaiseFrameError(new FrameErrorEventArgs(FrameErrorKind.IncompleteFrameTimeout, snapshot));
        return new SessionFaultException(
            $"未完成帧已缓冲 {pendingBytes} 字节，{_incompleteFrameTimeoutMs}ms 内未收齐后续字节，判定会话失效。");
    }

    /// <summary>切一帧；定界器支持丢弃上报时，把被丢弃的字节作为诊断事件上抛。</summary>
    private bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload)
    {
        if (_framing is IFrameDiscardReporting reporting)
        {
            var found = reporting.TryDecodeFrame(ref buffer, out payload, out var discarded);
            if (!discarded.IsEmpty)
                RaiseFrameError(new FrameErrorEventArgs(FrameErrorKind.DiscardedByResync, discarded.ToArray()));
            return found;
        }

        return _framing.TryDecodeFrame(ref buffer, out payload);
    }

    private void RaiseFrameError(FrameErrorEventArgs args)
        => _onFrameError?.Invoke(args);
}
