using System.Buffers;

namespace StreamFrame;

/// <summary>
/// 字节缓冲写入器：内容写满后可通过 <see cref="WrittenSpan"/> 读取或原地修改。
///
/// 与 <see cref="IBufferWriter{T}"/> 的区别：IBufferWriter 的 GetSpan 可能返回零长度段，
/// 而本接口保证 <see cref="GetSpan(int)"/> 返回的段至少 sizeHint 长，写入的数据可立即读取。
/// 因此适合"先写入、后原地回填"的流式帧编码。
/// </summary>
public interface IWrittenBufferWriter : IBufferWriter<byte>
{
    /// <summary>已写入的字节，可读可原地修改（供回填长度头等使用）。</summary>
    Span<byte> WrittenSpan { get; }

    /// <summary>已写入的字节数。</summary>
    int WrittenCount { get; }
}
