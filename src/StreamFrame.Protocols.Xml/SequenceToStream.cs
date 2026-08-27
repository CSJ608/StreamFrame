using System.Buffers;

namespace StreamFrame.Protocols.Xml;

/// <summary>
/// 将 <see cref="ReadOnlySequence{T}"/> 暴露为只读 <see cref="Stream"/>，供 <see cref="System.Xml.XmlReader"/> 读取。
/// </summary>
internal sealed class SequenceToStream : Stream
{
    private ReadOnlySequence<byte> _sequence;

    public SequenceToStream(in ReadOnlySequence<byte> sequence)
        => _sequence = sequence;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _sequence.Length;
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var remaining = _sequence;
        if (remaining.IsEmpty)
            return 0;

        var slice = remaining.Slice(0, Math.Min(count, remaining.Length));
        slice.CopyTo(buffer.AsSpan(offset, (int)slice.Length));
        _sequence = remaining.Slice(slice.Length);
        return (int)slice.Length;
    }

    public override void Flush() { }

    public override long Seek(long offset, SeekOrigin origin)
        => throw new NotSupportedException();

    public override void SetLength(long value)
        => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count)
        => throw new NotSupportedException();
}
