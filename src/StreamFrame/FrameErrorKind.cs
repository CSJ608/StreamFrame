namespace StreamFrame;

/// <summary>帧层诊断事件的类别。</summary>
public enum FrameErrorKind
{
    /// <summary>帧结构完整，但帧内负载解码失败（codec 抛出异常）。</summary>
    DecodeFailed,

    /// <summary>字节被帧定界器当作噪声/垃圾丢弃（如非法长度头重同步、STX/ETX 之外的杂散字节）。</summary>
    DiscardedByResync,

    /// <summary>未完成帧的已缓冲字节超过上限，判定流不可恢复。</summary>
    IncompleteFrameOverflow,

    /// <summary>未完成帧超时：帧已开头、缓冲里留着半帧字节，但在 IncompleteFrameTimeoutMs 内未收齐后续字节。</summary>
    IncompleteFrameTimeout,
}
