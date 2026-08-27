using System.Buffers;
using System.Text;

namespace StreamFrame.Benchmarks;

/// <summary>基准用的 UTF-8 文本 codec（与 demo 相同：编解码即字符串与字节互转）。</summary>
internal sealed class Utf8TextCodec : ICodec<string>
{
    public static readonly Utf8TextCodec Instance = new();

    public string Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
        => Encoding.UTF8.GetString(frame);

    public void Encode(string message, IBufferWriter<byte> writer, CancellationToken ct = default)
        => writer.Write(Encoding.UTF8.GetBytes(message));
}
