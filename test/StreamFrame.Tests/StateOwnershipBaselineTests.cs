using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading.Channels;

namespace StreamFrame.Tests;

/// <summary>
/// 状态机与会话所有权行为基线（#68 PR-1）：以确定性或不变量测试固定当前已承诺的语义——
/// 终态后迟到发布的丢弃、用户回调重入（Connected 回调内 Reconnect/Dispose）、
/// 连接成功与停机竞速、并发关闭/重连下的终态不变量。为后续会话所有权重构提供行为底线。
/// </summary>
public class StateOwnershipBaselineTests
{
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(10);

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static StreamConnection<string> Create(int port, bool active, int retryDelayMs = 25)
        => new(
            new LengthPrefixFramer(),
            StringCodec.Instance,
            IPAddress.Loopback,
            port,
            isActive: active,
            new StreamConnectionOptions { ConnectRetryDelayMs = retryDelayMs, AcceptRetryDelayMs = retryDelayMs });

    private static object? Field(StreamConnection<string> connection, string name)
        => typeof(StreamConnection<string>).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(connection);

    /// <summary>白盒直发一次内部状态发布（迟到发布的确定性门控测试用）。</summary>
    private static void PublishState(StreamConnection<string> connection, ConnectionState state)
        => typeof(StreamConnection<string>)
            .GetMethod("CommunicationStateChanging", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(connection, new object?[] { state, null });

    private static async Task<string> ReceiveOne(StreamConnection<string> connection, int budgetMs = 5000)
    {
        using var timeout = new CancellationTokenSource(budgetMs);
        await foreach (var message in connection.GetMessages(timeout.Token))
            return message;
        throw new InvalidOperationException("消息流提前结束");
    }

    private static async Task WaitForAsync(Func<bool> condition, string what, int budgetMs = 8000)
    {
        var deadline = TestClock.TickCount64 + budgetMs;
        while (TestClock.TickCount64 < deadline)
        {
            if (condition())
                return;
            await Task.Delay(20);
        }
        throw new TimeoutException($"等待 {what} 超时");
    }

    /// <summary>终态不变量：停机完成后 State/会话编号/会话资源必须收敛到唯一合法形态。</summary>
    private static void AssertTerminalInvariants(StreamConnection<string> connection, int port)
    {
        Assert.True(connection.IsDisposed);
        Assert.Equal(ConnectionState.Disconnected, connection.State);
        Assert.Equal(0, connection.CurrentSessionId);
        Assert.Null(Field(connection, "_server"));   // 监听器已释放
        Assert.Null(Field(connection, "_socket"));   // 连接 socket 已释放
        Assert.Null(Field(connection, "_session"));  // 无孤儿会话（会话对象集中持有全部会话资源）

        // 监听端口可立即重新占用（未泄漏仍在监听的 socket）
        var rebinding = new TcpListener(IPAddress.Loopback, port);
        rebinding.Start();
        rebinding.Stop();
    }

    [Fact]
    public async Task DisposedState_LatePublications_AreDroppedEntirely()
    {
        var port = GetFreePort();
        await using var server = Create(port, false);
        await using var client = Create(port, true);
        server.Start(CancellationToken.None);
        client.Start(CancellationToken.None);
        await Task.WhenAll(server.WaitForConnectedAsync(), client.WaitForConnectedAsync())
            .WaitAsync(Budget);
        var liveSessionId = server.CurrentSessionId;
        Assert.NotEqual(0, liveSessionId);

        await server.DisposeAsync();

        // 停机后到达的迟到发布（用户 Reconnect / 迟到的会话故障）与竞速中的连接成功：
        // 整体丢弃——终态不被翻转、会话编号不被归零后重分配、不创建会话任务。
        PublishState(server, ConnectionState.Retry);
        Assert.Equal(ConnectionState.Disconnected, server.State);
        Assert.Equal(0, server.CurrentSessionId);

        PublishState(server, ConnectionState.Connected);
        Assert.Equal(ConnectionState.Disconnected, server.State);
        Assert.Equal(0, server.CurrentSessionId);
        Assert.Null(Field(server, "_session"));         // 迟到的 Connected 不得创建会话
        Assert.Null(Field(server, "_whenConnected"));   // 等待器不被迟到的 Connected 唤醒

        AssertTerminalInvariants(server, port);
    }

    [Fact]
    public async Task ConnectedCallback_ReconnectReentry_ConvergesToUsableSession()
    {
        var port = GetFreePort();
        await using var server = Create(port, false);
        await using var firstClient = Create(port, true);

        var states = new ConcurrentQueue<ConnectionState>();
        var connectedSessionIds = new ConcurrentQueue<long>();
        var reentered = 0;
        server.ConnectionChanged += (_, state) =>
        {
            states.Enqueue(state);
            if (state != ConnectionState.Connected)
                return;

            // 线性化契约：Connected 事件可见时编号必已分配（分配先于发布）
            connectedSessionIds.Enqueue(server.CurrentSessionId);

            // 病态重入：仅首个 Connected 回调里同步触发 Reconnect()
            if (Interlocked.CompareExchange(ref reentered, 1, 0) == 0)
                server.Reconnect();
        };

        server.Start(CancellationToken.None);
        firstClient.Start(CancellationToken.None);
        // 服务端就绪不走 WaitForConnectedAsync：回调内重连会消费掉 Connected#1 的等待器，
        // 迟到的等待将挂到 Connected#2（第二个客户端之后才接入）。以客户端就绪 + 回调
        // 执行标志为准。
        await firstClient.WaitForConnectedAsync().WaitAsync(Budget);
        await WaitForAsync(() => Volatile.Read(ref reentered) != 0, "首个 Connected 回调执行重入 Reconnect");
        var firstSessionId = connectedSessionIds.Single();

        // 重连后的新会话由第二个客户端接入
        await using var secondClient = Create(port, true);
        secondClient.Start(CancellationToken.None);
        await secondClient.WaitForConnectedAsync().WaitAsync(Budget);
        await WaitForAsync(() => server.State == ConnectionState.Connected && server.CurrentSessionId != firstSessionId,
            "重入重连后的新会话发布");

        // 收敛后的会话双向可用；编号在 Connected 可见期间恒非零且不被复用
        await secondClient.SendAsync("after-reentry-reconnect");
        Assert.Equal("after-reentry-reconnect", await ReceiveOne(server));
        await server.SendAsync("reply");
        Assert.Equal("reply", await ReceiveOne(secondClient));

        var observedIds = connectedSessionIds.ToArray();
        var observedStates = states.ToArray();
        Assert.All(observedIds, id => Assert.NotEqual(0, id));
        Assert.Equal(2, observedIds.Length);
        Assert.NotEqual(observedIds[0], observedIds[1]);
        Assert.Equal(server.CurrentSessionId, observedIds[1]);
        Assert.Equal(ConnectionState.Connected, server.State);

        // 观测到的状态序列只含合法转移，且终止于 Connected
        Assert.Equal(ConnectionState.Connected, observedStates[^1]);
        for (var i = 1; i < observedStates.Length; i++)
        {
            var legal = observedStates[i - 1] switch
            {
                ConnectionState.Connecting => observedStates[i] is ConnectionState.Connecting or ConnectionState.Connected or ConnectionState.Retry or ConnectionState.Disconnected,
                ConnectionState.Connected => observedStates[i] is ConnectionState.Retry or ConnectionState.Connected or ConnectionState.Disconnected,
                ConnectionState.Retry => observedStates[i] is ConnectionState.Retry or ConnectionState.Connecting or ConnectionState.Disconnected,
                _ => false, // Disconnected 是终态，之后不得再有可观测转移
            };
            Assert.True(legal, $"非法状态转移：{observedStates[i - 1]} → {observedStates[i]}");
        }
    }

    [Fact]
    public async Task ConnectedCallback_DisposeReentry_TerminatesCleanly()
    {
        var port = GetFreePort();
        var server = Create(port, false);
        await using var client = Create(port, true);
        var disposedInCallback = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var disposeCompletedSynchronously = false;

        server.ConnectionChanged += (_, state) =>
        {
            if (state == ConnectionState.Connected)
            {
                // 若停机挂起，回调无法返回：上方 WaitForConnectedAsync（完成点在回调之后）将超时暴露
                disposeCompletedSynchronously = server.DisposeAsync().IsCompletedSuccessfully;
                disposedInCallback.TrySetResult(true);
            }
        };

        server.Start(CancellationToken.None);
        // 服务端等待器先挂起：Connected 发布（回调之后）的完成点在停机取消之后——
        // 回调内 Dispose 的合法结果就是等待器以取消收尾（#65 契约在重入场景同样成立）
        var serverReady = server.WaitForConnectedAsync().WaitAsync(Budget);
        client.Start(CancellationToken.None);
        await client.WaitForConnectedAsync().WaitAsync(Budget);
        await disposedInCallback.Task.WaitAsync(Budget);
        Assert.True(disposeCompletedSynchronously);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => serverReady);

        AssertTerminalInvariants(server, port);
        Assert.Equal(0, server.CurrentSessionId);

        // 停机后的公共契约：消息流自然结束、发送通道关闭
        using var messageBudget = new CancellationTokenSource(Budget);
        await using (var enumerator = server.GetMessages(messageBudget.Token).GetAsyncEnumerator())
            Assert.False(await enumerator.MoveNextAsync()); // 通道已完成：立即 false；若悬挂则由预算令牌超时暴露
        await Assert.ThrowsAnyAsync<ChannelClosedException>(() => server.SendAsync("late"));
        await client.DisposeAsync();
    }

    [Fact]
    public async Task ReconnectWhileAccepting_LateClientStillAdmittedBySupersededLoop()
    {
        var port = GetFreePort();
        await using var server = Create(port, false);

        var states = new ConcurrentQueue<ConnectionState>();
        server.ConnectionChanged += (_, state) => states.Enqueue(state);

        server.Start(CancellationToken.None);
        await WaitForAsync(() => server.State == ConnectionState.Connecting, "接受循环进入监听等待");
        // Start 同步推进到 accept 等待：循环 A 持有 _acceptLock、监听器已发布
        Assert.IsType<Socket>(Field(server, "_server")!);

        // 用户 Reconnect 在 accept 等待期间取代循环 A（Retry → StartCore 递增代次、派发循环 B；
        // B 排队等锁）。Retry/Connecting 的发布在 Reconnect 返回前同步完成——被取代循环
        // 不再发布 Connecting（代次复查前置），状态序列不混入杂音。
        server.Reconnect();
        Assert.Equal(
            new[] { ConnectionState.Connecting, ConnectionState.Retry, ConnectionState.Connecting },
            states.ToArray());

        // 监听器是连接级共享资源：客户端向服务器端口的接入合法——迟到 accept 照常收编
        // （发布 Connected、建立会话），由纪元机制收敛；取代者循环 B 取锁后经代次检查静默退出
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port).WaitAsync(Budget);
        await WaitForAsync(() => server.State == ConnectionState.Connected, "迟到客户端被收编并发布 Connected");
        Assert.NotEqual(0, server.CurrentSessionId);
        Assert.Equal(
            new[] { ConnectionState.Connecting, ConnectionState.Retry, ConnectionState.Connecting, ConnectionState.Connected },
            states.ToArray());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task ConnectSuccess_RacingDispose_TerminalInvariantsHold(int jitterMode)
    {
        // 连接建立与停机的竞速不变量：无论 accept 成功与 Dispose 的相对次序如何，
        // 终态收敛且不泄漏（连接 socket / 监听器 / 孤儿会话任务）。
        // 迭代量刻意保守：这是行为基线测试，不是 soak；过高并发会放大其它测试的
        // 慢收敛敏感（StopSessionCore 同步等待），干扰并行套件。
        const int iterations = 10;
        for (var i = 0; i < iterations; i++)
        {
            var port = GetFreePort();
            await using var server = Create(port, false);
            server.Start(CancellationToken.None);

            using var client = new TcpClient();
            var connect = Task.Run(async () =>
            {
                try
                {
                    if (jitterMode == 1)
                        await Task.Delay(1);
                    await client.ConnectAsync(IPAddress.Loopback, port).WaitAsync(Budget);
                }
                catch (SocketException)
                {
                    // 服务端先停机：连接被拒，属预期分支
                }
                catch (OperationCanceledException)
                {
                }
            });
            var dispose = Task.Run(async () =>
            {
                if (jitterMode == 2)
                    await Task.Delay(1);
                await server.DisposeAsync();
            });

            await Task.WhenAll(connect, dispose).WaitAsync(Budget);
            client.Dispose();
            AssertTerminalInvariants(server, port);
        }
    }

    [Fact]
    public async Task ConcurrentDisposeAndReconnect_TerminalInvariantsHold()
    {
        // 故障重连进行中并发执行用户 Reconnect 与 Dispose：全部参与者收敛后终态不变量成立。
        // 迭代量刻意保守（同上：行为基线而非 soak，避免放大并行套件的慢收敛敏感）。
        const int iterations = 8;
        for (var i = 0; i < iterations; i++)
        {
            var port = GetFreePort();
            var server = Create(port, false);
            await using var client = Create(port, true);
            server.Start(CancellationToken.None);
            client.Start(CancellationToken.None);
            await Task.WhenAll(server.WaitForConnectedAsync(), client.WaitForConnectedAsync())
                .WaitAsync(Budget);

            // 对端拆除 → 服务端进入故障重连（重新监听），与并发 Reconnect/Dispose 竞速
            await client.DisposeAsync();

            var actors = Enumerable.Range(0, 4).Select(n => Task.Run(() =>
            {
                try
                {
                    server.Reconnect();
                }
                catch (ObjectDisposedException)
                {
                    // 已停机：合法竞速结果
                }
            })).Append(Task.Run(() => server.DisposeAsync().AsTask()));

            await Task.WhenAll(actors).WaitAsync(Budget);
            AssertTerminalInvariants(server, port);
        }
    }
}
