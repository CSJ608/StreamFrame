using System.Buffers;
using System.Text;

namespace StreamFrame.Tests;

/// <summary>
/// 测试用 codec：消息是一个简单的 UTF-8 字符串。
/// </summary>
internal sealed class StringCodec : ICodec<string>
{
    public static readonly StringCodec Instance = new();

    public string Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
        => Encoding.UTF8.GetString(frame);

    public void Encode(string message, IBufferWriter<byte> writer, CancellationToken ct = default)
        => writer.Write(Encoding.UTF8.GetBytes(message));
}
