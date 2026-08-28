namespace StreamFrame;

/// <summary>
/// 会话感知的可选能力：为有严格会话边界的协议（如 HSMS——重连后必须重新握手、
/// 旧会话消息禁止重放、协议计时器从整帧实际写出起算）提供绑定会话的收发。
/// 由 <see cref="StreamConnection{TMessage}"/> 实现；依赖
/// <see cref="IStreamConnection{TMessage}"/> 抽象的上层可通过
/// <c>connection is ISessionAwareStreamConnection&lt;TMessage&gt; sessionAware</c> 探测后使用。
///
/// 现有 <see cref="IStreamConnection{TMessage}.SendAsync"/>（入队即完成、断线后由新会话
/// 续发）与 <see cref="IStreamConnection{TMessage}.GetMessages"/>（跨重连稳定流）语义不变，
/// 两套发送按入队顺序共享同一条 FIFO。
/// </summary>
/// <typeparam name="TMessage">业务消息类型。</typeparam>
public interface ISessionAwareStreamConnection<TMessage> : IStreamConnection<TMessage>
{
    /// <summary>
    /// 当前 TCP 会话的编号：每次成功建立 TCP 会话时分配，单调递增、跨重连不复用
    /// （编号允许有间隔）；无会话（未连接 / 会话拆除 / 停机）时为 0。
    /// 在 <see cref="IStreamConnection{TMessage}.ConnectionChanged"/> 回调与
    /// <see cref="IStreamConnection{TMessage}.WaitForConnectedAsync"/> 完成时读取必定得到
    /// 有效编号（分配先于 Connected 对外发布）；状态离开 Connected 对外可见时已归零。
    ///
    /// 仅作发送绑定的"目标声明"；权威校验在发送 worker 出队时进行，读取时刻与使用
    /// 时刻之间会话仍可能失效——以 <see cref="SendInSessionAsync"/> 的结果为准。
    /// </summary>
    long CurrentSessionId { get; }

    /// <summary>
    /// 发送一条绑定当前会话的消息：任务在<b>整帧全部写入本机 socket</b>（内核接收缓冲，
    /// 不含对端 ACK）后才成功完成。
    ///
    /// 失败语义：会话在整帧写出之前终止（断线重连、对端关闭、Dispose），任务以
    /// <see cref="SessionExpiredException"/> 失败，该消息<b>不会</b>自动转移到新会话重放，
    /// 远端处理结果视为未知（见异常说明）；发送中途 socket 故障，任务以底层 socket 异常
    /// 失败。调用方令牌的提交点：发送 worker 认领本条之前取消会使任务以取消结束且消息
    /// 不再发送；认领之后（帧已开始写出）取消对结果无副作用，写入由会话令牌控制——
    /// 取消单条消息不会撕裂帧、不会杀死连接。
    /// </summary>
    /// <param name="sessionId">目标会话编号（取自 <see cref="CurrentSessionId"/>）。会话已失效时任务以 <see cref="SessionExpiredException"/> 失败。</param>
    /// <param name="message">业务消息。</param>
    /// <param name="ct">调用方取消令牌（仅在提交点之前有效，见备注）。</param>
    Task SendInSessionAsync(long sessionId, TMessage message, CancellationToken ct = default);

    /// <summary>
    /// 以异步流方式消费"消息 + 所属会话编号"：<see cref="SessionMessage{TMessage}.SessionId"/>
    /// 标记每条消息来自哪次 TCP 会话，旧会话解码任务迟到投递的消息带旧编号。
    ///
    /// <b>与 <see cref="IStreamConnection{TMessage}.GetMessages"/> 是同一通道的两个竞争
    /// 消费视图，不是广播</b>：两者同时枚举会互相竞争消费，每条消息只会到达其中一个——
    /// 请二选一使用。生命周期与 GetMessages 相同：跨重连不中断，仅在停机时结束。
    /// </summary>
    IAsyncEnumerable<SessionMessage<TMessage>> GetSessionMessages(CancellationToken ct = default);
}
