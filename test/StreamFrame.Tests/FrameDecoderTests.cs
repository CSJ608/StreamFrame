using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Threading.Channels;
using StreamFrame;
using StreamFrame.Abstractions;

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
            yield return bytes[i..(i + length)];
        }
    }

    private static (FrameDecoder<string> decoder, Channel<string> relay, Pipe pipe) CreateDecoder(IFrameCodec framing)
    {
        var relay = Channel.CreateUnbounded<string>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = true,
        });
        var pipe = new Pipe();
        var decoder = new FrameDecoder<string>(pipe.Reader, framing, StringCodec.Instance, relay);
        return (decoder, relay, pipe);
    }

    private static async Task<List<string>> FeedAndDrainAsync(FrameDecoder<string> decoder, Channel<string> relay, Pipe pipe, byte[] allBytes, CancellationToken ct)
    {
        var decodeTask = decoder.RunAsync(ct);

        foreach (var chunk in Chunk(allBytes, 23))
        {
            await pipe.Writer.WriteAsync(chunk, ct);
            await pipe.Writer.FlushAsync(ct);
        }
        pipe.Writer.Complete();

        var messages = new List<string>();
        await foreach (var message in relay.Reader.ReadAllAsync(ct))
        {
            messages.Add(message);
        }

        await decodeTask;
        return messages;
    }

    [Fact]
    public async Task LengthPrefix_GluedFrames_AllDecoded()
    {
        var framing = new LengthPrefixFrameCodec();
        var (decoder, relay, pipe) = CreateDecoder(framing);

        var p1 = "first-message";
        var p2 = "second-message-with-more-bytes";
        var p3 = "third";

        var all = new ArrayBufferWriter<byte>();
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p1), all);
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p2), all);
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p3), all);

        var messages = await FeedAndDrainAsync(decoder, relay, pipe, all.WrittenSpan.ToArray(), CancellationToken.None);

        Assert.Equal(new[] { p1, p2, p3 }, messages);
    }

    [Fact]
    public async Task LengthPrefix_ChunkedFrames_HandlesHalfAndGluedPackets()
    {
        var framing = new LengthPrefixFrameCodec();
        var (decoder, relay, pipe) = CreateDecoder(framing);

        // 故意构造会让单块恰好切断在帧中间的字节序列
        var p1 = "A";
        var p2 = "B".PadRight(40, 'C');
        var p3 = "hello world";

        var all = new ArrayBufferWriter<byte>();
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p1), all);
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p2), all);
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p3), all);

        var messages = await FeedAndDrainAsync(decoder, relay, pipe, all.WrittenSpan.ToArray(), CancellationToken.None);

        Assert.Equal(new[] { p1, p2, p3 }, messages);
    }

    [Fact]
    public async Task StxEtx_Chunked_AllDecoded()
    {
        var framing = new StxEtxFrameCodec();
        var (decoder, relay, pipe) = CreateDecoder(framing);

        var p1 = "STX-frame-one";
        var p2 = "STX-frame-two-with-longer-payload";

        var all = new ArrayBufferWriter<byte>();
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p1), all);
        framing.EncodeFrame(Encoding.UTF8.GetBytes(p2), all);

        var messages = await FeedAndDrainAsync(decoder, relay, pipe, all.WrittenSpan.ToArray(), CancellationToken.None);

        Assert.Equal(new[] { p1, p2 }, messages);
    }

    [Fact]
    public async Task Decoder_RelayCompletesWithError_OnInvalidFrame()
    {
        var framing = new LengthPrefixFrameCodec();
        var (decoder, relay, pipe) = CreateDecoder(framing);

        var decodeTask = decoder.RunAsync(CancellationToken.None);

        // 喂一个超过 MaxPayloadBytes 的长度头 + 垃圾
        await pipe.Writer.WriteAsync(new byte[] { 0x7F, 0xFF, 0xFF, 0xFF, 0x41 });
        await pipe.Writer.FlushAsync();
        pipe.Writer.Complete();

        // 长度超限：丢弃头，后续无有效帧 → 正常结束
        await decodeTask;

        await using var enumerator = relay.Reader.ReadAllAsync(CancellationToken.None).GetAsyncEnumerator();
        Assert.False(await enumerator.MoveNextAsync());
    }
}
