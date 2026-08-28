using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Threading.Channels;
using StreamFrame;

namespace StreamFrame.Tests;

public class FrameDecoderTests
{
    /// <summary>
    /// 把一个 byte[] 拆成多个小块（模拟 TCP 分片），每次喂一块。
    /// </summary>
    private static IEnumerable<byte[]> Chunk(byte[] bytes, int chunkSize)
    {
        for (var i = 0; i < bytes.Length; i += chunkSize)
        {
            var length = Math.Min(chunkSize, bytes.Length - i);
            var chunk = new byte[length];
            Array.Copy(bytes, i, chunk, 0, length);
            yield return chunk;
        }
    }

    private sealed record DecoderHarness(
        FrameDecoder<string> Decoder,
        Channel<SessionMessage<string>> Relay,
        Pipe Pipe,
        List<FrameErrorEventArgs> Errors,
        List<string> Messages,
        CancellationTokenSource Done) : IDisposable
    {
        public void Dispose() => Done.Cancel();
    }

    private static DecoderHarness CreateDecoder(
        IFramer framing,
        DecodeErrorPolicy policy = DecodeErrorPolicy.Disconnect,
        int? maxIncompleteFrameBytes = null,
        ICodec<string>? codec = null,
        int incompleteFrameTimeoutMs = 0)
    {
        var errors = new List<FrameErrorEventArgs>();
        var relay = Channel.CreateUnbounded<SessionMessage<string>>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = true,
        });
        var pipe = new Pipe();
        var decoder = new FrameDecoder<string>(
            pipe.Reader, framing, codec ?? StringCodec.Instance, relay,
            sessionId: 1,
            maxIncompleteFrameBytes ?? framing.MaxPayloadBytes + 4096,
            incompleteFrameTimeoutMs,
            policy,
            args => { lock (errors) errors.Add(args); });
        return new DecoderHarness(decoder, relay, pipe, errors, new List<string>(), new CancellationTokenSource());
    }

    /// <summary>后台持续把 relay 中的消息收进 Messages（decoder 不再完成 relay，靠取消收尾）。</summary>
    private static void StartDrain(DecoderHarness h)
        => _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var envelope in h.Relay.Reader.ReadAllAsync(h.Done.Token))
                    h.Messages.Add(envelope.Message);
            }
            catch (OperationCanceledException)
            {
            }
        });

    private static async Task WriteAllAsync(DecoderHarness h, byte[] bytes, int chunkSize = 23)
    {
        foreach (var chunk in Chunk(bytes, chunkSize))
            await h.Pipe.Writer.WriteAsync(chunk);
    }

    private static async Task WaitForMessagesAsync(DecoderHarness h, int expected, int timeoutMs = 5000)
    {
        var deadline = TestClock.TickCount64 + timeoutMs;
        while (TestClock.TickCount64 < deadline)
        {
            lock (h.Messages)
            {
                if (h.Messages.Count >= expected)
                    return;
            }
            await Task.Delay(20);
        }
    }

    [Fact]
    public async Task LengthPrefix_GluedFrames_AllDecoded()
    {
        var framing = new LengthPrefixFramer();
        using var h = CreateDecoder(framing);
        StartDrain(h);
        var decodeTask = h.Decoder.RunAsync(CancellationToken.None);

        var p1 = "first-message";
        var p2 = "second-message-with-more-bytes";
        var p3 = "third";

        var all = new TestWrittenBufferWriter();
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p1), all);
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p2), all);
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p3), all);

        await WriteAllAsync(h, all.WrittenSpan.ToArray());
        await WaitForMessagesAsync(h, 3);
        h.Pipe.Writer.Complete();
        await decodeTask;

        lock (h.Messages)
            Assert.Equal(new[] { p1, p2, p3 }, h.Messages);
        Assert.Empty(h.Errors);
    }

    [Fact]
    public async Task LengthPrefix_ChunkedFrames_HandlesHalfAndGluedPackets()
    {
        var framing = new LengthPrefixFramer();
        using var h = CreateDecoder(framing);
        StartDrain(h);
        var decodeTask = h.Decoder.RunAsync(CancellationToken.None);

        var p1 = "A";
        var p2 = "B".PadRight(40, 'C');
        var p3 = "hello world";

        var all = new TestWrittenBufferWriter();
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p1), all);
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p2), all);
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p3), all);

        await WriteAllAsync(h, all.WrittenSpan.ToArray());
        await WaitForMessagesAsync(h, 3);
        h.Pipe.Writer.Complete();
        await decodeTask;

        lock (h.Messages)
            Assert.Equal(new[] { p1, p2, p3 }, h.Messages);
    }

    [Fact]
    public async Task StxEtx_Chunked_AllDecoded()
    {
        var framing = new StxEtxFramer();
        using var h = CreateDecoder(framing);
        StartDrain(h);
        var decodeTask = h.Decoder.RunAsync(CancellationToken.None);

        var p1 = "STX-frame-one";
        var p2 = "STX-frame-two-with-longer-payload";

        var all = new TestWrittenBufferWriter();
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p1), all);
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p2), all);

        await WriteAllAsync(h, all.WrittenSpan.ToArray());
        await WaitForMessagesAsync(h, 2);
        h.Pipe.Writer.Complete();
        await decodeTask;

        lock (h.Messages)
            Assert.Equal(new[] { p1, p2 }, h.Messages);
    }

    [Fact]
    public async Task Decoder_DoesNotCompleteRelay_OnStreamEnd()
    {
        // 通道归连接所有（跨会话复用），decoder 退出后 relay 必须仍可写、仍可读
        var framing = new LengthPrefixFramer();
        using var h = CreateDecoder(framing);
        StartDrain(h);
        var decodeTask = h.Decoder.RunAsync(CancellationToken.None);

        await h.Pipe.Writer.WriteAsync(new byte[] { 0x00, 0x00, 0x00, 0x00 }); // 空负载帧
        await WaitForMessagesAsync(h, 1);
        h.Pipe.Writer.Complete();
        await decodeTask;

        Assert.True(h.Relay.Writer.TryWrite(new SessionMessage<string>(1, "still-alive")));
        await WaitForMessagesAsync(h, 2);
        lock (h.Messages)
            Assert.Contains("still-alive", h.Messages);
    }

    [Fact]
    public async Task InvalidLengthHeader_ReportedAsDiscard()
    {
        var framing = new LengthPrefixFramer();
        using var h = CreateDecoder(framing);
        StartDrain(h);
        var decodeTask = h.Decoder.RunAsync(CancellationToken.None);

        // 超过 MaxPayloadBytes 的长度头 + 尾随垃圾
        await h.Pipe.Writer.WriteAsync(new byte[] { 0x7F, 0xFF, 0xFF, 0xFF, 0x41 });
        h.Pipe.Writer.Complete();
        await decodeTask;

        lock (h.Messages)
            Assert.Empty(h.Messages);
        var discard = Assert.Single(h.Errors);
        Assert.Equal(FrameErrorKind.DiscardedByResync, discard.Kind);
        Assert.Equal(new byte[] { 0x7F, 0xFF, 0xFF, 0xFF }, discard.Bytes.ToArray());
    }

    [Fact]
    public async Task StxEtx_NoiseAndAbortedPartial_ReportedAsDiscard()
    {
        var framing = new StxEtxFramer();
        using var h = CreateDecoder(framing);
        StartDrain(h);
        var decodeTask = h.Decoder.RunAsync(CancellationToken.None);

        // 噪声 N*2 + 被新 STX 中止的半帧(STX A) + 有效帧(STX B ETX)
        var bytes = new byte[] { 0x4E, 0x4E, 0x02, (byte)'A', 0x02, (byte)'B', 0x03 };
        await h.Pipe.Writer.WriteAsync(bytes);
        await WaitForMessagesAsync(h, 1);
        h.Pipe.Writer.Complete();
        await decodeTask;

        lock (h.Messages)
        {
            var message = Assert.Single(h.Messages);
            Assert.Equal("B", message);
        }
        var discard = Assert.Single(h.Errors);
        Assert.Equal(FrameErrorKind.DiscardedByResync, discard.Kind);
        Assert.Equal(new byte[] { 0x4E, 0x4E, 0x02, (byte)'A' }, discard.Bytes.ToArray());
    }

    [Fact]
    public async Task IncompleteFrame_OverLimit_FaultsAndReports()
    {
        var framing = new LengthPrefixFramer();
        using var h = CreateDecoder(framing, maxIncompleteFrameBytes: 16);
        var decodeTask = h.Decoder.RunAsync(CancellationToken.None);

        // 声明 1KB 的帧但只喂 32 字节（> 上限 16），永远凑不齐
        var header = new byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(header, 1024);
        await h.Pipe.Writer.WriteAsync(header.Concat(new byte[32]).ToArray());

        await Assert.ThrowsAsync<SessionFaultException>(() => decodeTask);

        var overflow = Assert.Single(h.Errors);
        Assert.Equal(FrameErrorKind.IncompleteFrameOverflow, overflow.Kind);
        Assert.Equal(36, overflow.Bytes.Length); // 缓冲 36B < 8KB 快照上限，全量携带
    }

    [Fact]
    public async Task DecodeError_DisconnectPolicy_FaultsDecoder()
    {
        var framing = new LengthPrefixFramer();
        using var h = CreateDecoder(framing, policy: DecodeErrorPolicy.Disconnect, codec: new ThrowingDecodeCodec());
        var decodeTask = h.Decoder.RunAsync(CancellationToken.None);

        var payload = Encoding.UTF8.GetBytes("bad");
        var frame = new byte[4 + payload.Length];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(0, 4), payload.Length);
        payload.CopyTo(frame.AsSpan(4));
        await h.Pipe.Writer.WriteAsync(frame);

        await Assert.ThrowsAsync<SessionFaultException>(() => decodeTask);

        var error = Assert.Single(h.Errors);
        Assert.Equal(FrameErrorKind.DecodeFailed, error.Kind);
        Assert.Equal("bad", Encoding.UTF8.GetString(error.Bytes.Span));
        Assert.IsType<InvalidOperationException>(error.Exception);
    }

    [Fact]
    public async Task DecodeError_SkipFramePolicy_ContinuesWithNextFrame()
    {
        var framing = new LengthPrefixFramer();
        using var h = CreateDecoder(framing, policy: DecodeErrorPolicy.SkipFrame, codec: new ThrowingDecodeCodec());
        StartDrain(h);
        var decodeTask = h.Decoder.RunAsync(CancellationToken.None);

        var bad = Encoding.UTF8.GetBytes("bad");
        var good = Encoding.UTF8.GetBytes("good");
        var all = new TestWrittenBufferWriter();
        framing.EncodeFrame(bad, all);   // ThrowingDecodeCodec 只对 "bad" 抛
        framing.EncodeFrame(good, all);
        await WriteAllAsync(h, all.WrittenSpan.ToArray());

        await WaitForMessagesAsync(h, 1);
        h.Pipe.Writer.Complete();
        await decodeTask;

        lock (h.Messages)
        {
            var message = Assert.Single(h.Messages);
            Assert.Equal("good", message);
        }
        var error = Assert.Single(h.Errors);
        Assert.Equal(FrameErrorKind.DecodeFailed, error.Kind);
        Assert.Equal("bad", Encoding.UTF8.GetString(error.Bytes.Span));
    }

    /// <summary>对负载 "bad" 抛异常、其余正常解码的 codec（模拟内容损坏的帧）。</summary>
    private sealed class ThrowingDecodeCodec : ICodec<string>
    {
        public string Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
        {
            var text = Encoding.UTF8.GetString(frame);
            if (text == "bad")
                throw new InvalidOperationException("payload 解析失败（模拟坏数据）");
            return text;
        }

        public void Encode(string message, IBufferWriter<byte> writer, CancellationToken ct = default)
            => writer.Write(Encoding.UTF8.GetBytes(message));
    }
}
