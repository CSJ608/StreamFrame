using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;
using StreamFrame.Abstractions;

namespace StreamFrame;

/// <summary>
/// 解码循环：从 PipeReader 消费字节流，按帧定界切帧、codec 解码，产出业务消息到通道。
/// 半包靠 AdvanceTo 保留未消费字节；粘包靠循环切尽所有完整帧。
///
/// 生命周期约定一：本类型不负责 <paramref name="relay"/> 的完成——通道归连接所有，
/// 跨会话复用，仅在连接 Dispose 时完成。会话级失败（解码失败、不完整帧超限）以
/// <see cref="SessionFaultException"/> 上抛，由连接层决定断线重连。
///
/// 生命周期约定二：ReadAsync 不绑定会话取消令牌（取消中的 ReadAsync 会把 Pipe 留在
/// "读取进行中"状态，无法再 TryRead）。退出由流结束（writer.CompleteAsync）或
/// CancelPendingRead 驱动；退出前把已缓冲的完整帧全部投递，保证"收到的字节必达"。
/// </summary>
internal sealed class FrameDecoder<TMessage>
{
    /// <summary>上报 IncompleteFrameOverflow 时携带的缓冲快照上限，避免事件本身复制超长数据。</summary>
    private const int OverflowSnapshotBytes = 8192;

    private readonly PipeReader _reader;
    private readonly IFrameCodec _framing;
    private readonly ICodec<TMessage> _codec;
    private readonly Channel<TMessage> _relay;
    private readonly int _maxIncompleteFrameBytes;
    private readonly DecodeErrorPolicy _decodeErrorPolicy;
    private readonly Action<FrameErrorEventArgs>? _onFrameError;

    public FrameDecoder(
        PipeReader reader,
        IFrameCodec framing,
        ICodec<TMessage> codec,
        Channel<TMessage> relay,
        int maxIncompleteFrameBytes,
        DecodeErrorPolicy decodeErrorPolicy,
        Action<FrameErrorEventArgs>? onFrameError)
    {
        _reader = reader;
        _framing = framing;
        _codec = codec;
        _relay = relay;
        _maxIncompleteFrameBytes = maxIncompleteFrameBytes;
        _decodeErrorPolicy = decodeErrorPolicy;
        _onFrameError = onFrameError;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        try
        {
            while (true)
            {
                var result = await _reader.ReadAsync(CancellationToken.None).ConfigureAwait(false);
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
                    var snapshot = buffer.Slice(0, Math.Min(buffer.Length, OverflowSnapshotBytes)).ToArray();
                    RaiseFrameError(new FrameErrorEventArgs(FrameErrorKind.IncompleteFrameOverflow, snapshot));
                    throw new SessionFaultException(
                        $"未完成帧缓冲 {buffer.Length} 字节超过上限 {_maxIncompleteFrameBytes} 字节。");
                }

                _reader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    break; // 字节流结束（对端断开/会话停止）：缓冲已投递完，正常退出

                if (result.IsCanceled)
                {
                    // CancelPendingRead（会话停止时唤醒）：缓冲已投递完，正常退出
                    if (ct.IsCancellationRequested)
                        break;
                }
            }
        }
        finally
        {
            // 无论正常退出还是故障，都释放 Pipe 的池化段
            await _reader.CompleteAsync().ConfigureAwait(false);
        }
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
