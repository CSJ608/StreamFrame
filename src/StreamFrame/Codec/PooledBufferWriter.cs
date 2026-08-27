using System.Buffers;

namespace StreamFrame;

/// <summary>
/// 按初始大小租用 ArrayPool 缓冲、可动态扩容的 IWrittenBufferWriter，用于编码中间字节。
/// GetSpan 保证返回至少 sizeHint 长的段（不足时先扩容），写入的数据可立即通过 WrittenSpan 读取。
/// </summary>
internal sealed class PooledBufferWriter : IWrittenBufferWriter, IDisposable
{
    private byte[] _buffer;
    private int _written;

    public PooledBufferWriter(int capacity)
        => _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(capacity, 1));

    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _written);
    public Span<byte> WrittenSpan => _buffer.AsSpan(0, _written);
    public int WrittenCount => _written;

    public void Advance(int count)
    {
        if (count < 0 || _written + count > _buffer.Length)
            throw new ArgumentOutOfRangeException(nameof(count));
        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    private void EnsureCapacity(int sizeHint)
    {
        // 剩余空间必须至少容纳 max(sizeHint, 1) 字节——GetSpan 不允许返回空 span
        // （BuffersExtensions.Write 内部以 GetSpan(0) 多段循环拷贝，依赖非空返回值）。
        var required = _written + Math.Max(sizeHint, 1);
        if (required <= _buffer.Length)
            return;

        var newSize = Math.Max(required, _buffer.Length * 2);
        var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
        Array.Copy(_buffer, newBuffer, _written);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = newBuffer;
    }

    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = Array.Empty<byte>();
    }
}
