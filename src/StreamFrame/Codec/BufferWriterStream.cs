using System.Buffers;

namespace StreamFrame;

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
        => WriteCore(buffer.AsSpan(offset, count));

#if !NETSTANDARD2_0
    // Stream 的 Span 虚方法重载自 netstandard2.1 起才有；ns2.0 走上面的 byte[] 重载（同样直写 IBufferWriter）
    public override void Write(ReadOnlySpan<byte> buffer)
        => WriteCore(buffer);
#endif

    private void WriteCore(ReadOnlySpan<byte> buffer)
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
