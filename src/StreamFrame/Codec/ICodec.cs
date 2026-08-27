using System.Buffers;

namespace StreamFrame;

/// <summary>
/// 帧内数据编解码：负责把一帧负载（payload）解析为业务消息、以及把业务消息编码为帧内负载字节。
///
/// 由驱动实现——例如 XML 驱动把负载解析为强类型消息，自定义二进制协议驱动手写字节布局。
/// </summary>
/// <typeparam name="TMessage">业务消息类型，贯穿一条连接。</typeparam>
public interface ICodec<TMessage>
{
    /// <summary>把一帧负载（不含定界字节）解析为一条业务消息。</summary>
    TMessage Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default);

    /// <summary>把一条业务消息编码为帧内负载字节，写入 <paramref name="writer"/>。</summary>
    void Encode(TMessage message, IBufferWriter<byte> writer, CancellationToken ct = default);
}
