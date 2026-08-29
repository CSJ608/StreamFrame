using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using StreamFrame;

namespace StreamFrame.Tests;

/// <summary>
/// 浸泡/混沌测试（默认不运行）：真实 TCP 回环上的长时间运行与随机故障注入，
/// 验证消息完整性（顺序、无重复、无跨会话错发）与无悬挂。
///
/// 启用方式（本地手动触发，常规 CI 与默认 dotnet test 均不运行）：
/// <code>STREAMFRAME_SOAK_SECONDS=120 dotnet test -f net8.0 --filter FullyQualifiedName~Soak</code>
/// 未设置环境变量时本套件立即返回（视为通过），不影响提交前的全量验证。
/// 长期观察：.github/workflows/soak.yml 每夜以 600s 跑本套件（ubuntu + windows），
/// 覆盖用户重连竞速场景（Soak_ReconnectRacing_LongRun，含状态机转移合法性与
/// 会话编号不变式的全程校验）。
/// </summary>
public class SoakTests
{
    private readonly Xunit.Abstractions.ITestOutputHelper _output;

    public SoakTests(Xunit.Abstractions.ITestOutputHelper output) => _output = output;

    /// <summary>把库内 Warning（accept 失败等）透传到测试输出——长期观察失败时的第一手诊断。</summary>
    private sealed class SoakDumpLogger : Microsoft.Extensions.Logging.ILogger
    {
        private readonly Xunit.Abstractions.ITestOutputHelper _sink;
        public SoakDumpLogger(Xunit.Abstractions.ITestOutputHelper sink) => _sink = sink;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;
        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var line = $"[LOG {logLevel}] {formatter(state, exception)}{(exception is null ? string.Empty : " (" + exception.GetType().Name + ")")}";
            _sink.WriteLine(line);
        }
    }

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
                        throw new InvalidOperationException($"非法状态转移：{previous} → {state}");

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

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static double SoakSeconds()
    {
        var raw = Environment.GetEnvironmentVariable("STREAMFRAME_SOAK_SECONDS");
        return double.TryParse(raw, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var value) && value > 0 ? value : 0;
    }

    private static async Task ConnectWithRetryAsync(TcpClient client, int port)
    {
        // 混沌场景容忍慢收敛：服务端重连可能处于 2s 级的 accept 重试延迟（TIME_WAIT 竞争等），
        // 连接预算给足 60s，避免把"暂时拒绝"误判为测试失败
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await client.ConnectAsync(IPAddress.Loopback, port);
                return;
            }
            catch (SocketException) when (attempt < 120)
            {
                await Task.Delay(500);
            }
        }
    }

    private static async Task WaitForStateAsync(
        StreamConnection<string> connection,
        Func<ConnectionState, bool> predicate,
        int timeoutMs = 15_000)
    {
        var deadline = TestClock.TickCount64 + timeoutMs;
        while (TestClock.TickCount64 < deadline)
        {
            if (predicate(connection.State))
                return;
            await Task.Delay(20);
        }

        Assert.True(predicate(connection.State), $"等待连接状态超时（{timeoutMs}ms），当前 {connection.State}。");
    }

    [Fact]
    public async Task Soak_LongRun_MessageIntegrity()
    {
        var seconds = SoakSeconds();
        if (seconds <= 0)
            return; // 未启用浸泡模式：跳过（保持默认套件快速）

        var port = GetFreePort();
        await using var server = new StreamConnection<string>(
            new LengthPrefixFramer(), StringCodec.Instance, IPAddress.Loopback, port, isActive: false);
        server.Start(CancellationToken.None);

        using var client = new TcpClient();
        await ConnectWithRetryAsync(client, port);
        await WaitForStateAsync(server, s => s == ConnectionState.Connected);

        // 服务端 → 客户端方向：序列消息流；对端读尽并核对顺序与完整性
        var received = new ConcurrentQueue<string>();
        var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(seconds + 30));
        var readerTask = Task.Run(async () =>
        {
            var stream = client.GetStream();
            var header = new byte[4];
            var body = new byte[4096];
            try
            {
                while (true)
                {
                    var read = 0;
                    while (read < 4)
                    {
                        var n = await stream.ReadAsync(header, read, 4 - read, readCts.Token);
                        if (n == 0)
                            return;
                        read += n;
                    }

                    var length = BinaryPrimitives.ReadInt32BigEndian(header);
                    var payload = new byte[length];
                    read = 0;
                    while (read < length)
                    {
                        var n = await stream.ReadAsync(payload, read, length - read, readCts.Token);
                        if (n == 0)
                            return;
                        read += n;
                    }

                    received.Enqueue(Encoding.UTF8.GetString(payload));
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        var deadline = TestClock.TickCount64 + (int)(seconds * 1000);
        var sent = 0;
        while (TestClock.TickCount64 < deadline)
        {
            await server.SendAsync($"msg-{sent}", CancellationToken.None);
            sent++;
            await Task.Delay(5); // ~200 msg/s
        }

        readCts.Cancel();
        await readerTask;

        // 完整性：顺序、无丢失、无重复
        Assert.True(received.Count >= sent - 5, $"发送 {sent}，对端仅收 {received.Count}（允许尾部少数在途）");
        var index = 0;
        foreach (var frame in received)
        {
            Assert.Equal($"msg-{index}", frame);
            index++;
        }
    }

    [Fact]
    public async Task Soak_Chaos_RandomFaults_NoHangAndIntegrity()
    {
        var seconds = SoakSeconds();
        if (seconds <= 0)
            return; // 未启用浸泡模式：跳过

        var seed = Environment.TickCount;
        var random = new Random(seed);
        var port = GetFreePort();
        await using var server = new StreamConnection<string>(
            new LengthPrefixFramer(), StringCodec.Instance, IPAddress.Loopback, port,
            isActive: false, new StreamConnectionOptions { SendQueueCapacity = 256 });
        server.Start(CancellationToken.None);

        var peerFrames = new ConcurrentQueue<string>();
        var pendingPlain = new ConcurrentQueue<string>();
        var allTasks = new List<Task>();
        TcpClient? client = null;
        NetworkStream? stream = null;
        var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(seconds + 30));
        var readerTask = Task.CompletedTask;

        var deadline = TestClock.TickCount64 + (int)(seconds * 1000);
        var boundSeq = 0;
        var plainSeq = 0;

        try
        {
            while (TestClock.TickCount64 < deadline)
            {
                var action = random.Next(100);
                if (client is null)
                {
                    // （重）连接对端并开始读
                    client = new TcpClient();
                    await ConnectWithRetryAsync(client, port);
                    await WaitForStateAsync(server, s => s == ConnectionState.Connected);
                    stream = client.GetStream();
                    var currentStream = stream;
                    readerTask = Task.Run(async () =>
                    {
                        var header = new byte[4];
                        try
                        {
                            while (true)
                            {
                                var read = 0;
                                while (read < 4)
                                {
                                    var n = await currentStream.ReadAsync(header, read, 4 - read, readCts.Token);
                                    if (n == 0)
                                        return;
                                    read += n;
                                }

                                var length = BinaryPrimitives.ReadInt32BigEndian(header);
                                var payload = new byte[length];
                                read = 0;
                                while (read < length)
                                {
                                    var n = await currentStream.ReadAsync(payload, read, length - read, readCts.Token);
                                    if (n == 0)
                                        return;
                                    read += n;
                                }

                                peerFrames.Enqueue(Encoding.UTF8.GetString(payload));
                            }
                        }
                        catch (Exception ex) when (ex is ObjectDisposedException or IOException or OperationCanceledException)
                        {
                        }
                    });
                }
                else if (action < 50)
                {
                    // 会话绑定突发
                    var id = server.CurrentSessionId;
                    if (id != 0)
                    {
                        for (var i = 0; i < random.Next(1, 5); i++)
                        {
                            var label = $"b{boundSeq++}";
                            allTasks.Add(server.SendInSessionAsync(id, label));
                            await Task.Delay(10);
                        }
                    }
                }
                else if (action < 70)
                {
                    // 普通发送（跨会话续发）：记录标签，终局核对全部送达
                    for (var i = 0; i < random.Next(1, 4); i++)
                    {
                        var label = $"p{plainSeq++}";
                        pendingPlain.Enqueue(label);
                        await server.SendAsync(label);
                    }
                }
                else if (action < 80 && stream is not null)
                {
                    // 半帧注入（未完成帧超时未启用：仅占住解码缓冲）
                    var junk = new byte[] { 0x00, 0x00, 0x0F, 0xA0 };
                    await stream.WriteAsync(junk);
                }
                else if (action < 95)
                {
                    // 杀对端（FIN 路径：正常释放）
                    if (client is not null)
                    {
                        client.Dispose();
                        client = null;
                        stream = null;
                        await WaitForStateAsync(server, s => s != ConnectionState.Connected);
                    }
                }
                else if (action < 97)
                {
                    // 杀对端（RST 路径：Linger(true,0) 硬复位）
                    if (client is not null)
                    {
                        client.LingerState = new LingerOption(true, 0);
                        client.Dispose();
                        client = null;
                        stream = null;
                        await WaitForStateAsync(server, s => s != ConnectionState.Connected);
                    }
                }
                else
                {
                    // 用户显式重连（#47 观察到的楔死触发器，与本会话并发故障竞速）：
                    // 不等当前会话的自动重连，立即发起服务端主动拆除
                    server.Reconnect();
                    if (client is not null)
                    {
                        await WaitForStateAsync(server, s => s != ConnectionState.Connected);
                        client.Dispose();
                        client = null;
                        stream = null;
                    }
                }

                await Task.Delay(random.Next(20, 120));
            }
        }
        finally
        {
            readCts.Cancel();

            // 循环结束时对端可能仍存活：单客户端模式下监听已关闭（AcceptFirstClientOnly），
            // 必须先释放存量对端并等服务端离开 Connected，终局对端才能接入
            client?.Dispose();
            if (client is not null)
            {
                // 非断言式等待（尽力推进；真正的状态断言留给终局阶段）
                var exitDeadline = TestClock.TickCount64 + 15_000;
                while (TestClock.TickCount64 < exitDeadline && server.State == ConnectionState.Connected)
                    await Task.Delay(20);
            }
        }

        // 1) 所有会话绑定发送必须在时限内终结，失败类型合法；成功帧不得重复投递
        var boundSuccesses = new HashSet<string>();
        foreach (var task in allTasks)
        {
            var done = await Task.WhenAny(task, Task.Delay(20_000));
            Assert.True(ReferenceEquals(task, done), "混沌后存在悬挂的会话绑定发送。");
            if (task.Status != TaskStatus.RanToCompletion)
            {
                var ex = task.Exception?.InnerExceptions[0];
                Assert.True(ex is SessionExpiredException or SocketException or OperationCanceledException,
                    $"混沌中失败类型意外：{ex?.GetType().Name}");
            }
        }

        // 2) 重连一个干净对端，等普通消息全部续发送达（FIFO 完整性）
        using (var finalClient = new TcpClient())
        {
            await ConnectWithRetryAsync(finalClient, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
            var finalStream = finalClient.GetStream();
            var finalCts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var finalReader = Task.Run(async () =>
            {
                var header = new byte[4];
                try
                {
                    while (true)
                    {
                        var read = 0;
                        while (read < 4)
                        {
                            var n = await finalStream.ReadAsync(header, read, 4 - read, finalCts.Token);
                            if (n == 0)
                                return;
                            read += n;
                        }

                        var length = BinaryPrimitives.ReadInt32BigEndian(header);
                        var payload = new byte[length];
                        read = 0;
                        while (read < length)
                        {
                            var n = await finalStream.ReadAsync(payload, read, length - read, finalCts.Token);
                            if (n == 0)
                                return;
                            read += n;
                        }

                        peerFrames.Enqueue(Encoding.UTF8.GetString(payload));
                    }
                }
                catch (Exception ex) when (ex is ObjectDisposedException or IOException or OperationCanceledException)
                {
                }
            });

            var finalDeadline = TestClock.TickCount64 + 20_000;
            while (TestClock.TickCount64 < finalDeadline)
            {
                var plainSeen = 0;
                foreach (var f in peerFrames)
                    if (f.StartsWith("p", StringComparison.Ordinal))
                        plainSeen++;
                if (plainSeen >= pendingPlain.Count)
                    break;
                await Task.Delay(100);
            }

            finalCts.Cancel();
            await finalReader;
        }

        // 3) 终局核对：普通消息无丢失无重复（FIFO 序列完整）
        var plainFrames = peerFrames.Where(f => f.StartsWith("p", StringComparison.Ordinal)).ToArray();
        Assert.Equal(pendingPlain.Count, plainFrames.Length);
        Assert.Equal(pendingPlain.ToArray(), plainFrames);

        // 4) 绑定消息帧不重复（同一标签只允许出现一次——错发/重放探测器）
        var boundFrames = peerFrames.Where(f => f.StartsWith("b", StringComparison.Ordinal)).ToArray();
        Assert.Equal(boundFrames.Length, boundFrames.Distinct().Count());
    }

    /// <summary>
    /// 重连竞速的长期观察（默认不运行）：用户显式 Reconnect() 与自动重连（对端死亡）高占比
    /// 并发竞速——双 StartAsync 接受循环、Connected→Connected 直连迁移、会话编号线性化等
    /// #47 排查中判定"不可复现但结构脆弱"的路径，靠长时间高压力 + 全程不变式校验盯着。
    /// 动作分布：~40% 竞速重连（杀对端与 Reconnect 并发）、~20% 纯用户重连、~30% 普通发送
    /// （验证跨会话续发的 FIFO 完整性）、~10% 短等待；对端仅在无存活连接时重接入
    /// （单客户端模式：服务端 Connected 时监听已关闭，并发第二条连接被拒是正确行为）。
    /// </summary>
    [Fact]
    public async Task Soak_ReconnectRacing_LongRun()
    {
        var seconds = SoakSeconds();
        if (seconds <= 0)
            return; // 未启用浸泡模式：跳过

        var output = _output;
        var seed = Environment.TickCount;
        var random = new Random(seed);
        var port = GetFreePort();

        var recorder = new StateTransitionRecorder();
        await using var server = new StreamConnection<string>(
            new LengthPrefixFramer(), StringCodec.Instance, IPAddress.Loopback, port,
            isActive: false, new StreamConnectionOptions { SendQueueCapacity = 256, AcceptRetryDelayMs = 200 },
            logger: new SoakDumpLogger(output));
        server.ConnectionChanged += (_, state) => recorder.Record(state, server.CurrentSessionId);
        server.Start(CancellationToken.None);

        var peerFrames = new ConcurrentQueue<string>();
        var pendingPlain = new ConcurrentQueue<string>();
        var plainSeq = 0;
        TcpClient? client = null;

        var deadline = TestClock.TickCount64 + (int)(seconds * 1000);
        try
        {
            while (TestClock.TickCount64 < deadline)
            {
                var action = random.Next(100);
                if (client is null)
                {
                    // 对端（重）接入（仅在无存活对端时——单客户端模式：服务端 Connected
                    // 时监听已关闭，并发第二条连接被拒是正确行为）
                    client = new TcpClient();
                    await ConnectWithRetryAsync(client, port);
                    var currentStream = client.GetStream();
                    var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(seconds + 60));
                    _ = Task.Run(async () =>
                    {
                        var header = new byte[4];
                        try
                        {
                            while (true)
                            {
                                var read = 0;
                                while (read < 4)
                                {
                                    var n = await currentStream.ReadAsync(header, read, 4 - read, readCts.Token);
                                    if (n == 0)
                                        return;
                                    read += n;
                                }

                                var length = BinaryPrimitives.ReadInt32BigEndian(header);
                                var payload = new byte[length];
                                read = 0;
                                while (read < length)
                                {
                                    var n = await currentStream.ReadAsync(payload, read, length - read, readCts.Token);
                                    if (n == 0)
                                        return;
                                    read += n;
                                }

                                peerFrames.Enqueue(Encoding.UTF8.GetString(payload));
                            }
                        }
                        catch (Exception ex) when (ex is ObjectDisposedException or IOException or OperationCanceledException)
                        {
                        }
                    });
                }
                else if (action < 50)
                {
                    // 竞速重连：杀对端（触发自动重连）与用户显式 Reconnect() 同时打——
                    // 两路 Retry 过渡竞争，可能产生双 StartAsync 接受循环
                    var victim = client;
                    var kill = Task.Run(() => victim.Dispose());
                    server.Reconnect();
                    await kill;
                    await WaitForStateAsync(server, st => st != ConnectionState.Connected, timeoutMs: 15_000);
                    client = null;
                }
                else if (action < 70)
                {
                    // 纯用户重连（对端保持存活：服务端主动关闭路径）
                    server.Reconnect();
                    await WaitForStateAsync(server, st => st != ConnectionState.Connected, timeoutMs: 15_000);
                    client?.Dispose();
                    client = null;
                }
                else if (action < 95)
                {
                    // 普通发送：跨会话续发的 FIFO 完整性探针
                    for (var i = 0; i < random.Next(1, 4); i++)
                    {
                        var label = $"p{plainSeq++}";
                        pendingPlain.Enqueue(label);
                        await server.SendAsync(label);
                    }
                }
                else
                {
                    await Task.Delay(random.Next(20, 100));
                }

                await Task.Delay(random.Next(10, 60));
            }
        }
        finally
        {
            client?.Dispose();
            var exitDeadline = TestClock.TickCount64 + 15_000;
            while (TestClock.TickCount64 < exitDeadline && server.State == ConnectionState.Connected)
                await Task.Delay(20);
        }

        // 终局：干净对端接入，等普通消息全部续发送达
        using (var finalClient = new TcpClient())
        {
            await ConnectWithRetryAsync(finalClient, port);
            await WaitForStateAsync(server, st => st == ConnectionState.Connected, timeoutMs: 30_000);
            var finalStream = finalClient.GetStream();
            var finalCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var finalReader = Task.Run(async () =>
            {
                var header = new byte[4];
                try
                {
                    while (true)
                    {
                        var read = 0;
                        while (read < 4)
                        {
                            var n = await finalStream.ReadAsync(header, read, 4 - read, finalCts.Token);
                            if (n == 0)
                                return;
                            read += n;
                        }

                        var length = BinaryPrimitives.ReadInt32BigEndian(header);
                        var payload = new byte[length];
                        read = 0;
                        while (read < length)
                        {
                            var n = await finalStream.ReadAsync(payload, read, length - read, finalCts.Token);
                            if (n == 0)
                                return;
                            read += n;
                        }

                        peerFrames.Enqueue(Encoding.UTF8.GetString(payload));
                    }
                }
                catch (Exception ex) when (ex is ObjectDisposedException or IOException or OperationCanceledException)
                {
                }
            });

            var finalDeadline = TestClock.TickCount64 + 30_000;
            while (TestClock.TickCount64 < finalDeadline)
            {
                var plainSeen = 0;
                foreach (var f in peerFrames)
                    if (f.StartsWith("p", StringComparison.Ordinal))
                        plainSeen++;
                if (plainSeen >= pendingPlain.Count)
                    break;
                await Task.Delay(100);
            }

            finalCts.Cancel();
            await finalReader;
        }

        await server.DisposeAsync();

        // 全程不变式：合法转移、编号线性化、终态收敛
        recorder.Validate();

        // FIFO 完整性：普通消息无丢失无重复
        var plainFrames = peerFrames.Where(f => f.StartsWith("p", StringComparison.Ordinal)).ToArray();
        Assert.Equal(pendingPlain.Count, plainFrames.Length);
        Assert.Equal(pendingPlain.ToArray(), plainFrames);

        output.WriteLine($"[soak-racing] seed={seed}, 普通消息 {plainSeq} 条全部送达；" +
                         $"Connected→Connected 直连迁移 {recorder.DirectConnectedMigrations} 次（竞速观测指标）");
    }
}
