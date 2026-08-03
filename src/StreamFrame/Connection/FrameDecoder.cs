using System.IO.Pipelines;
using System.Threading.Channels;
using StreamFrame.Abstractions;

namespace StreamFrame;

/// <summary>
/// 解码循环：从 PipeReader 消费字节流，按帧定界切帧、codec 解码，产出业务消息到通道。
/// 半包靠 AdvanceTo 保留未消费字节；粘包靠循环切尽所有完整帧。
/// </summary>
internal sealed class FrameDecoder<TMessage>
{
    private readonly PipeReader _reader;
    private readonly IFrameCodec _framing;
    private readonly ICodec<TMessage> _codec;
    private readonly Channel<TMessage> _relay;

    public FrameDecoder(PipeReader reader, IFrameCodec framing, ICodec<TMessage> codec, Channel<TMessage> relay)
    {
        _reader = reader;
        _framing = framing;
        _codec = codec;
        _relay = relay;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        try
        {
            while (true)
            {
                var result = await _reader.ReadAsync(ct).ConfigureAwait(false);
                var buffer = result.Buffer;

                while (_framing.TryDecodeFrame(ref buffer, out var payload))
                {
                    ct.ThrowIfCancellationRequested();
                    var message = _codec.Decode(payload, ct);
                    await _relay.Writer.WriteAsync(message, ct).ConfigureAwait(false);
                }

                _reader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    break;
            }

            _relay.Writer.TryComplete();
        }
        catch (Exception ex)
        {
            _relay.Writer.TryComplete(ex);
            throw;
        }
    }
}
