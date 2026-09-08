using System.Net.Sockets;
using StreamFrame;

namespace StreamFrame.Tests;

/// <summary>
/// 浸泡/混沌测试（默认不运行）：真实 TCP 回环上的长时间运行与随机故障注入，
/// 验证消息完整性（顺序、无重复、无跨会话错发）与无悬挂。
///
/// 启用方式（本地手动触发，常规 CI 与默认 dotnet test 均不运行）：
/// <code>STREAMFRAME_SOAK_SECONDS=120 dotnet test -f net8.0 --filter FullyQualifiedName~Soak</code>
/// 未设置环境变量时报告 Skip，不把门控跳过计为长测通过。
/// 长期观察：.github/workflows/soak.yml 每夜以 600s 跑本套件（ubuntu + windows），
/// 覆盖用户重连竞速场景（Soak_ReconnectRacing_LongRun，含状态机转移合法性与
/// 会话编号不变式的全程校验）。
/// </summary>
public class SoakTests
{
    private readonly Xunit.Abstractions.ITestOutputHelper _output;

    public SoakTests(Xunit.Abstractions.ITestOutputHelper output) => _output = output;

    /// <summary>
    /// 状态机转移的全程记录与校验器（长期观察的核心断言面）：
    /// 合法边检查、Retry 后必须收敛回 Connected、非 Connected 态会话编号必须为 0、
    /// Connected 态编号严格单调不复用；另统计 Connected→Connected 直连迁移
    /// （双 StartAsync 接受循环竞速的观测指标，合法但值得长期盯）。
    /// </summary>
    private sealed class StateTransitionRecorder
    {
        private readonly List<(ConnectionState State, long SessionId)> _transitions = new(); // 兼作监视锁（net48 需 object 锁）
        private long _lastConnectedId;
        private readonly List<string> _errors = new();

        public int DirectConnectedMigrations;

        public void Record(ConnectionState state, long sessionId)
        {
            lock (_transitions)
            {
                if (_transitions.Count > 0)
                {
                    var previous = _transitions[^1].State;
                    var legal = (previous, state) switch
                    {
                        (ConnectionState.Connecting, ConnectionState.Connecting) => true,   // 连接/接受重试
                        (ConnectionState.Connecting, ConnectionState.Connected) => true,
                        (ConnectionState.Connecting, ConnectionState.Retry) => true,        // 用户在 Connecting 中触发重连
                        (ConnectionState.Connected, ConnectionState.Connected) => true,     // 双 StartAsync 直连迁移（竞速）
                        (ConnectionState.Connected, ConnectionState.Retry) => true,
                        (ConnectionState.Connected, ConnectionState.Disconnected) => true,
                        (ConnectionState.Retry, ConnectionState.Connecting) => true,
                        (ConnectionState.Retry, ConnectionState.Retry) => true,             // 重连期间再次显式重连
                        (ConnectionState.Retry, ConnectionState.Disconnected) => true,
                        (ConnectionState.Connecting, ConnectionState.Disconnected) => true,
                        _ => false,
                    };
                    if (!legal)
                        _errors.Add($"非法状态转移：{previous} → {state}");

                    if (previous == ConnectionState.Connected && state == ConnectionState.Connected)
                        DirectConnectedMigrations++;
                }

                _transitions.Add((state, sessionId));
            }
        }

        /// <summary>终局校验：编号线性化不变式 + 收敛性（终态 Disconnected）。</summary>
        public void Validate()
        {
            lock (_transitions)
            {
                Assert.Empty(_errors); // 事件处理器异常被库隔离，必须在测试任务内断言。
                foreach (var (state, sessionId) in _transitions)
                {
                    if (state == ConnectionState.Connected)
                    {
                        Assert.NotEqual(0L, sessionId);
                        Assert.True(sessionId > _lastConnectedId, $"会话编号必须严格递增：{_lastConnectedId} → {sessionId}");
                        _lastConnectedId = sessionId;
                    }
                    else
                    {
                        Assert.Equal(0L, sessionId); // 离开 Connected 可见时编号必须已归零（P1-1 保证）
                    }
                }

                // 终态必须是 Disconnected（DisposeAsync 之后）
                Assert.Equal(ConnectionState.Disconnected, _transitions[^1].State);
            }
        }
    }

    private static double SoakSeconds()
    {
        var raw = Environment.GetEnvironmentVariable("STREAMFRAME_SOAK_SECONDS");
        return double.TryParse(raw, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var value) && value > 0 ? value : 0;
    }

    [SoakFact]
    public async Task Soak_LongRun_MessageIntegrity()
    {
        var seconds = SoakSeconds();
        var trace = new SoakTrace(_output, verbose: false);
        await using var run = new SoakRun(trace);
        var peer = await run.ConnectAsync();
        var end = TestClock.TickCount64 + (long)(seconds * 1000);
        var sent = 0;
        while (TestClock.TickCount64 < end)
        {
            await run.EnqueueAsync($"m{sent++}");
            await Task.Delay(5);
        }
        await run.BarrierAsync(peer);
        await run.ClosePeerAsync(peer);
        var frames = run.Received.Select(x => x.Wire).Where(x => x.StartsWith("m", StringComparison.Ordinal)).ToArray();
        Assert.Equal(Enumerable.Range(0, sent).Select(i => $"m{i}"), frames);
        trace.Log($"long-stream exact delivery={sent}, reader observed after socket close");
    }

    [SoakFact]
    public Task Soak_Chaos_RandomFaults_NoHangAndIntegrity() => RunFaultsAsync(racing: false);

    [SoakFact]
    public Task Soak_ReconnectRacing_LongRun() => RunFaultsAsync(racing: true);

    private async Task RunFaultsAsync(bool racing)
    {
        var rawSeed = Environment.GetEnvironmentVariable("STREAMFRAME_SOAK_SEED");
        var seed = string.IsNullOrEmpty(rawSeed) ? Environment.TickCount : int.Parse(rawSeed, System.Globalization.CultureInfo.InvariantCulture);
        var random = new Random(seed);
        var trace = new SoakTrace(_output);
        trace.Log($"seed={seed} racing={racing} seconds={SoakSeconds()}");
        var recorder = new StateTransitionRecorder();
        await using var run = new SoakRun(trace);
        run.Server.ConnectionChanged += (_, state) => recorder.Record(state, run.Server.CurrentSessionId);
        SoakPeer? peer = null;
        var plain = 0;
        var bound = 0;
        var actionId = 0;
        var end = TestClock.TickCount64 + (long)(SoakSeconds() * 1000);
        while (TestClock.TickCount64 < end)
        {
            var action = random.Next(100);
            trace.Log($"action={actionId++} draw={action} sid={run.Server.CurrentSessionId} peer={peer?.SessionId}");
            if (peer is null)
                peer = await run.ConnectAsync();
            else if (racing ? action < 70 : action >= 80)
            {
                if (racing && action < 50)
                {
                    var victim = peer;
                    var kill = Task.Run(() => victim.Close());
                    await SoakRun.BoundedAsync(Task.Run(() => run.Server.Reconnect()), "racing reconnect");
                    await SoakRun.BoundedAsync(kill, "peer kill");
                }
                else if (racing || action >= 97)
                    await SoakRun.BoundedAsync(Task.Run(() => run.Server.Reconnect()), "explicit reconnect");
                else if (action >= 95)
                    peer.Client.LingerState = new LingerOption(true, 0);
                await run.ClosePeerAsync(peer);
                await run.WaitAsync(() => run.Server.State != ConnectionState.Connected, "leave Connected");
                peer = null;
            }
            else if (!racing && action < 50)
            {
                var sid = run.Server.CurrentSessionId;
                var count = random.Next(1, 5);
                for (var i = 0; i < count; i++)
                {
                    var wire = $"b{bound++}/{sid}";
                    run.SendBound(sid, wire);
                    trace.Log($"bound-submit id={wire} sid={sid}");
                    await Task.Delay(10);
                }
            }
            else if (racing ? action < 95 : action < 70)
            {
                var count = random.Next(1, 4);
                for (var i = 0; i < count; i++) await run.SendBusinessAsync(plain++);
            }
            else if (!racing)
                await peer.InjectPartialAsync();
            else
                await Task.Delay(random.Next(20, 100));
            await Task.Delay(racing ? random.Next(10, 60) : random.Next(20, 120));
        }

        if (peer is not null) await run.ClosePeerAsync(peer);
        await run.WaitAsync(() => run.Server.State != ConnectionState.Connected, "final disconnect");
        await run.ObserveSendsAsync();

        // A FIFO sentinel proves the surviving queue is drained BEFORE business retries.
        // Every unclaimed attempt must have reached this peer; retries cannot hide a lost queue entry.
        var unclaimed = run.UnclaimedPlain();
        peer = await run.ConnectAsync();
        await run.BarrierAsync(peer);
        run.AssertQueueContinuation(unclaimed);
        run.ReportUnconfirmed();
        for (var retry = 0; retry < 3 && run.Acked.Count < plain; retry++)
        {
            for (var id = 0; id < plain; id++)
                if (!run.Acked.ContainsKey(id)) await run.SendBusinessAsync(id);
            await run.BarrierAsync(peer);
        }
        await run.WaitAsync(() => run.Acked.Count == plain, "all business ACKs");
        await run.ClosePeerAsync(peer);
        await run.StopAsync();
        recorder.Validate();
        Assert.Equal(Enumerable.Range(0, plain), run.Acked.Keys.OrderBy(x => x));
        Assert.Equal(Enumerable.Range(0, plain), run.Delivered.Keys.OrderBy(x => x));
        var observed = run.Received.ToArray();
        // Retried business IDs may repeat, but each unique wire attempt and each bound ID is sent once.
        Assert.Equal(observed.Length, observed.Select(x => x.Wire).Distinct().Count());
        foreach (var session in observed.GroupBy(x => x.SessionId))
        {
            var order = session.Select(x => run.Attempts[x.Wire].Sequence).ToArray();
            Assert.Equal(order.OrderBy(x => x), order);
        }
        foreach (var frame in observed.Where(x => x.Wire.StartsWith("b", StringComparison.Ordinal)))
            Assert.Equal(long.Parse(frame.Wire.Split('/')[1], System.Globalization.CultureInfo.InvariantCulture), frame.SessionId);
        trace.Log($"PASS seed={seed} acked={plain} attempts={run.Attempts.Count} bound={bound} direct-migrations={recorder.DirectConnectedMigrations}");
    }
}
