using System.Buffers;

namespace StreamFrame;

/// <summary>
/// 帧内数据编解码：负责把一帧负载（payload）解析为业务消息、以及把业务消息编码为帧内负载字节。
///
/// 由驱动实现——例如 XML 驱动把负载解析为强类型消息，自定义二进制协议驱动手写字节布局。
/// </summary>
/// <remarks>
/// 并发 / Concurrency: 同一会话的发送 worker 串行编码，但 Encode 与 Decode 可并行。
/// 会话拆除只作有界等待，旧任务可能与新会话重叠（包括 Decode/Decode，阻塞的 Encode 也不能假定已退出）。
/// 实现应无共享可变状态或自行同步；跨连接共享实例的调用方负责确认实现支持并发，框架不提供实例级锁。
/// A send worker serializes its own calls, not the codec instance. Encode/Decode and old/new
/// session calls can overlap. Use stateless implementations or synchronization; callers sharing
/// an instance across connections must ensure it is safe. Avoid blocking synchronous methods.
/// </remarks>
/// <typeparam name="TMessage">业务消息类型，贯穿一条连接。</typeparam>
public interface ICodec<TMessage>
{
    /// <summary>把一帧负载（不含定界字节）解析为一条业务消息。</summary>
    /// <remarks>
    /// frame 仅供同步调用期间借用；返回对象（包括嵌套字段）必须独立拥有数据。
    /// 不得未经拷贝保留 frame、其切片或底层 Memory；管线随后推进/释放，异步消费者可能更晚读取。
    /// ReadOnlyMemory 只限制写入接口，不转移所有权。本接口不转移管线内存所有权。
    /// Borrow frame only during this synchronous call. Copy retained bytes into message-owned storage;
    /// never return pipeline slices, even as ReadOnlyMemory. No pipeline ownership transfer is provided.
    /// </remarks>
    TMessage Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default);

    /// <summary>把一条业务消息编码为帧内负载字节，写入 <paramref name="writer"/>。</summary>
    /// <remarks>
    /// 必须同步完成写入；不得保留 writer 或其缓冲供返回后/异步使用，也不得释放框架拥有的 writer。
    /// 调用方应保持已入队消息及其底层数据不变，普通 SendAsync 完成仅表示入队，编码可能尚未开始。
    /// Complete writes synchronously. Do not retain/dispose the writer or retain its buffers after return.
    /// Queued messages must remain unchanged: SendAsync completion does not mean encoding has finished.
    /// </remarks>
    void Encode(TMessage message, IBufferWriter<byte> writer, CancellationToken ct = default);
}
