using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;
using StreamFrame;
using StreamFrame.Ownership;

namespace StreamFrame.Tests;

public class OwnershipExampleTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DecodedMessages_OwnBytes_AfterSourceIsOverwritten(bool segmented)
    {
        var source = new byte[] { 1, 2, 3, 4 };
        var sequence = segmented ? Segment.Create(source) : new ReadOnlySequence<byte>(source);
        var bytes = new OwnedBytesCodec().Decode(sequence);
        var memory = new OwnedMemoryCodec().Decode(sequence);
        Array.Clear(source, 0, source.Length); // Deterministic invalidation, no pool timing assumption.
        await Task.Yield();
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, bytes);
        Assert.Equal(bytes, memory.ToArray());
        using var writer = new PooledBufferWriter(16);
        new OwnedBytesCodec().Encode(bytes, writer);
        new OwnedMemoryCodec().Encode(memory, writer);
        Assert.Equal(new byte[] { 1, 2, 3, 4, 1, 2, 3, 4 }, writer.WrittenMemory.ToArray());
        ProductionExample.CreateOptions().Validate();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Decoder_AsyncConsumer_AfterPipeStorageIsPoisoned(bool memoryMessages)
    {
        if (memoryMessages)
            await VerifyPipe(new OwnedMemoryCodec(), static value => value.ToArray());
        else
            await VerifyPipe(new OwnedBytesCodec(), static value => value);
    }

    private static async Task VerifyPipe<T>(ICodec<T> codec, Func<T, byte[]> getBytes)
    {
        using var pool = new PoisonPool();
        var pipe = new Pipe(new PipeOptions(pool: pool));
        var relay = Channel.CreateBounded<SessionMessage<T>>(2);
        var metrics = new ConnectionMetrics("ownership-test");
        var decoder = new FrameDecoder<T>(pipe.Reader, new LengthPrefixFramer(), codec,
            relay, metrics, 1, 1024, 0, DecodeErrorPolicy.Disconnect, null);
        await pipe.Writer.WriteAsync(new byte[] { 0, 0, 0, 4, 1, 2, 3, 4 });
        await pipe.Writer.CompleteAsync();
        await decoder.RunAsync(CancellationToken.None); // CompleteAsync returns all borrowed segments.
        Assert.True(pool.Returned > 0);
        await Task.Yield();
        Assert.True(relay.Reader.TryRead(out var received));
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, getBytes(received.Message));
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        private Segment(ReadOnlyMemory<byte> memory) => Memory = memory;
        public static ReadOnlySequence<byte> Create(byte[] bytes)
        {
            var first = new Segment(bytes.AsMemory(0, 2));
            var last = new Segment(bytes.AsMemory(2)) { RunningIndex = 2 };
            first.Next = last;
            return new ReadOnlySequence<byte>(first, 0, last, 2);
        }
    }

    private sealed class PoisonPool : MemoryPool<byte>
    {
        public int Returned { get; private set; }
        public override int MaxBufferSize => int.MaxValue;
        public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
            => new Owner(this, new byte[Math.Max(4096, minBufferSize)]);
        protected override void Dispose(bool disposing) { }

        private sealed class Owner(PoisonPool pool, byte[] bytes) : IMemoryOwner<byte>
        {
            public Memory<byte> Memory => bytes;
            public void Dispose()
            {
                for (var i = 0; i < bytes.Length; i++) bytes[i] = 0xDD;
                pool.Returned++;
            }
        }
    }
}
