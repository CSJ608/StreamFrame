using System.Buffers;
using StreamFrame;

namespace StreamFrame.Ownership;

public sealed class OwnedMemoryCodec : ICodec<ReadOnlyMemory<byte>>
{
    public ReadOnlyMemory<byte> Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return frame.ToArray(); // ReadOnlyMemory alone would not establish ownership.
    }

    public void Encode(ReadOnlyMemory<byte> message, IBufferWriter<byte> writer, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        writer.Write(message.Span);
    }
}
