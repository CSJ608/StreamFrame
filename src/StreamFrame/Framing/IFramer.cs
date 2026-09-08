using System.Buffers;

namespace StreamFrame;

/// <summary>
/// 数据帧定界策略：负责把业务负载（payload）编成线上帧，以及从字节流中切出完整帧。
///
/// 发送与接收共用同一实现，保证一条连接上帧格式永远一致。
/// </summary>
/// <remarks>
/// 同连接编码与切帧可并行，新旧会话也可能重叠；跨连接共享实例前须确认线程安全。
/// 应无共享可变解析状态或自行同步，框架不提供实例级锁。
/// Encode and decode may overlap, as may old/new sessions. Shared instances must be thread-safe;
/// keep parsing state local or synchronize it yourself. The framework does not lock the instance.
/// </remarks>
public interface IFramer
{
    /// <summary>单帧负载允许的最大字节数，用于防御超长帧撑爆缓冲区。</summary>
    int MaxPayloadBytes { get; }

    /// <summary>
    /// 对完整的帧内负载（不含定界字节）加帧定界并写入 <paramref name="writer"/>。
    /// payload、writer 及其缓冲仅供同步调用期间借用，不得留存供异步使用或释放 writer。
    /// Borrow payload/writer only for this call; do not retain buffers or dispose the writer.
    /// </summary>
    void EncodeFrame(ReadOnlySpan<byte> payload, IBufferWriter<byte> writer);

    /// <summary>
    /// 尝试从 <paramref name="buffer"/> 中切出一帧。
    /// </summary>
    /// <param name="buffer">待解析的字节流；成功时前进到下一帧起点，失败时保留未消费字节。</param>
    /// <param name="payload">切出的帧内负载（不含定界字节）。</param>
    /// <returns>成功切出一帧返回 true；未切出帧返回 false（数据不足或丢弃无效字节）。</returns>
    /// <remarks>
    /// buffer 必须保留原缓冲的未消费后缀。返回 false 时也可通过消费前缀进行重同步；
    /// 连接层在长度减少时继续解析剩余缓冲，未消费任何字节时等待更多输入。
    /// 返回 true 时必须消费完整帧的线上字节（包括空负载帧的定界字节）。
    /// payload 可以是输入切片，供连接层紧接着同步 Decode；实现不得留存 buffer/payload。
    /// Payload may borrow the input for the immediately following synchronous codec call;
    /// the framer must not retain either sequence beyond this call.
    /// </remarks>
    bool TryDecodeFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload);
}
