using System.Buffers;

namespace StreamFrame.Abstractions;

/// <summary>
/// 将 <see cref="IBufferWriter{T}"/> 暴露为 <see cref="Stream"/>，供 <see cref="System.Xml.XmlWriter"/>、
/// <see cref="System.Xml.Serialization.XmlSerializer"/> 等直接写入 UTF-8 字节。
/// </summary>
public sealed class BufferWriterStream : Stream
{
    private readonly IBufferWriter<byte> _writer;

    public BufferWriterStream(IBufferWriter<byte> writer)
        => _writer = writer ?? throw new ArgumentNullException(nameof(writer));

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Flush() { }

    public override int Read(byte[] buffer, int offset, int count)
        => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin)
        => throw new NotSupportedException();

    public override void SetLength(long value)
        => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count)
        => Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        while (!buffer.IsEmpty)
        {
            var destination = _writer.GetSpan(sizeHint: buffer.Length);
            var written = Math.Min(destination.Length, buffer.Length);
            buffer[..written].CopyTo(destination);
            _writer.Advance(written);
            buffer = buffer[written..];
        }
    }
}
