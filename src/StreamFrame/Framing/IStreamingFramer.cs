namespace StreamFrame;

/// <summary>
/// 流式帧定界：支持"先占位、后回填"的原地编码，避免把序列化产物整体复制进帧缓冲。
///
/// 使用方式（与 <see cref="StreamConnection{TMessage}"/> 的流式发送配合）：
/// <code>
/// framing.BeginFrame(writer);   // 预留长度位 / 写入 STX
/// codec.Encode(msg, writer);    // 直接写同一缓冲
/// framing.EndFrame(writer);     // 回填长度 / 写入 ETX
/// </code>
/// 相比 <see cref="IFramer.EncodeFrame"/> 的"负载整体进、帧整体出"，本方式
/// 全程单缓冲、零 memcpy。已写入的字节可通过 <see cref="IWrittenBufferWriter.WrittenSpan"/>
/// 原地回填（如 <see cref="LengthPrefixFramer"/> 的 4 字节长度头）。
///
/// 协议语义（帧边界、超长防御）与对应 <see cref="IFramer"/> 完全一致。
/// </summary>
/// <remarks>
/// 沿用 IFramer 的并发与借用约束。BeginFrame/EndFrame 的配对状态应存于本次 writer，
/// 不应使用实例字段记录当前帧；两个调用之间可穿插解码或其它会话/连接的调用。
/// writer 及缓冲不得被保留供方法返回后使用；EndFrame 使用调用方重新传入的 writer。
/// IFramer concurrency rules apply. Keep pair state in the supplied writer, not a current-frame field:
/// other decode/session/connection calls may interleave. Never retain writer/buffers after a method returns.
/// </remarks>
public interface IStreamingFramer : IFramer
{
    /// <summary>帧编码的起始；BeginFrame 后 writer 的任何写入都属于帧内容。</summary>
    void BeginFrame(IWrittenBufferWriter writer);

    /// <summary>帧编码的结束；写入定界符 / 回填长度头。</summary>
    void EndFrame(IWrittenBufferWriter writer);
}
