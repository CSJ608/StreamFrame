using System.Buffers;

namespace StreamFrame.Tests;

/// <summary>测试用 IWrittenBufferWriter 实现（内部用数组增长）。</summary>
internal sealed class TestWrittenBufferWriter : IWrittenBufferWriter, IDisposable
{
    private byte[] _buffer = new byte[16];
    private int _written;

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
        // GetSpan 不允许返回空 span，必须保证剩余空间 ≥ max(sizeHint, 1)
        var required = _written + Math.Max(sizeHint, 1);
        if (required <= _buffer.Length)
            return;
        Array.Resize(ref _buffer, Math.Max(required, _buffer.Length * 2));
    }

    public void Dispose() { }
}
