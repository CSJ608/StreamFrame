namespace StreamFrame;

/// <summary>
/// 重连退避调度：连续失败时按基础间隔倍增（×2，封顶 <see cref="_maxDelayMs"/>），
/// 连接成功后复位；开启退避时叠加 ±20% 抖动，避免大量客户端同时重试的惊群。
/// 未启用退避（上限 ≤ 基础间隔）时恒定返回基础间隔，与历史行为一致。
/// </summary>
internal sealed class RetryDelayScheduler
{
    private const double JitterRatio = 0.2;

    private readonly int _baseDelayMs;
    private readonly int _maxDelayMs;
    private readonly Random _random = new();
    private int _attempt;

    public RetryDelayScheduler(int baseDelayMs, int maxDelayMs)
    {
        _baseDelayMs = baseDelayMs;
        _maxDelayMs = maxDelayMs;
    }

    /// <summary>退避是否启用（上限大于基础间隔才有意义）。</summary>
    public bool Enabled => _maxDelayMs > _baseDelayMs;

    /// <summary>已连续失败的次数（每次 <see cref="NextDelayMs"/> 后递增，成功后复位）。</summary>
    public int Attempt => _attempt;

    /// <summary>
    /// 取下一次重试的等待毫秒数：第 n 次失败后为 base * 2^(n-1) 封顶至 max，
    /// 启用退避时叠加 ±20% 抖动；未启用则恒为 base（无抖动）。
    /// </summary>
    public int NextDelayMs()
    {
        var exponential = Enabled
            ? (long)_baseDelayMs << Math.Min(_attempt, 20) // 位移封顶防溢出
            : _baseDelayMs;
        _attempt++;

        var capped = (int)Math.Min(exponential, Math.Max(_maxDelayMs, _baseDelayMs));
        if (!Enabled)
            return capped;

        var jitter = capped * JitterRatio;
        return (int)Math.Round(capped - jitter + _random.NextDouble() * jitter * 2);
    }

    /// <summary>连接成功后复位退避，下次失败重新从基础间隔开始。</summary>
    public void Reset()
        => _attempt = 0;
}
