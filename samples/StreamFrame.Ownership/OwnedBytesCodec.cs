using System.Buffers;
using StreamFrame;

namespace StreamFrame.Ownership;

// 无共享可变状态 / No shared mutable state.
public sealed class OwnedBytesCodec : ICodec<byte[]>
{
    public byte[] Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return frame.ToArray(); // Own the bytes, including for single-segment frames.
    }

    public void Encode(byte[] message, IBufferWriter<byte> writer, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        writer.Write(message);
    }
}
