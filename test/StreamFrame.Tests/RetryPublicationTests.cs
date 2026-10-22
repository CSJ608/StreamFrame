using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Sockets;
using System.Reflection;

namespace StreamFrame.Tests;

public class RetryPublicationTests
{
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(10);
    private static readonly ConnectionState[] ReconnectedStates =
    {
        ConnectionState.Connecting, ConnectionState.Retry, ConnectionState.Connecting, ConnectionState.Connected,
    };

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static StreamConnection<string> Create(int port, bool active = false)
        => new(new LengthPrefixFramer(), StringCodec.Instance, IPAddress.Loopback, port, active,
            new StreamConnectionOptions { AcceptRetryDelayMs = 25, ConnectRetryDelayMs = 25 });

    private static object? Field(StreamConnection<string> connection, string name)
        => typeof(StreamConnection<string>).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(connection);

    private static async Task WaitForAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(Budget);
        while (!condition())
            await Task.Delay(10, timeout.Token);
    }

    private static async Task<string> Receive(StreamConnection<string> connection)
    {
        using var timeout = new CancellationTokenSource(Budget);
        await foreach (var message in connection.GetMessages(timeout.Token))
            return message;
        throw new InvalidOperationException("消息流提前结束");
    }

    private static async Task AssertUsable(StreamConnection<string> server, StreamConnection<string> client)
    {
        await Task.WhenAll(server.WaitForConnectedAsync(), client.WaitForConnectedAsync()).WaitAsync(Budget);
        await client.SendAsync("late-client-message");
        Assert.Equal("late-client-message", await Receive(server));
        await server.SendInSessionAsync(server.CurrentSessionId, "server-reply").WaitAsync(Budget);
        Assert.Equal("server-reply", await Receive(client));
        Assert.Equal(ConnectionState.Connected, server.State);
        Assert.NotEqual(0, server.CurrentSessionId);
    }

    private sealed class StateLog
    {
        private readonly ConcurrentQueue<(ConnectionState Event, ConnectionState State, long Id)> _events = new();

        public void Record(StreamConnection<string> connection, ConnectionState state)
            => _events.Enqueue((state, connection.State, connection.CurrentSessionId));

        public ConnectionState[] States => _events.Select(item => item.Event).ToArray();

        public void AssertConsistent()
        {
            foreach (var item in _events)
            {
                Assert.Equal(item.Event, item.State);
                if (item.Event == ConnectionState.Connected)
                    Assert.NotEqual(0, item.Id);
                else
                    Assert.Equal(0, item.Id);
            }
        }
    }

    private sealed class RetryCounter : IDisposable
    {
        private readonly MeterListener _listener = new();
        private long _count;

        public RetryCounter(int port)
        {
            var endpoint = $"{IPAddress.Loopback}:{port}";
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                if (instrument.Name != "streamframe.reconnects")
                    return;
                foreach (var tag in tags)
                {
                    if (tag.Key == "endpoint" && Equals(tag.Value, endpoint))
                        Interlocked.Add(ref _count, value);
                }
            });
            _listener.Start();
            foreach (var field in typeof(ConnectionMetrics).GetFields(BindingFlags.Static | BindingFlags.NonPublic))
            {
                if (field.GetValue(null) is Instrument instrument && instrument.Name == "streamframe.reconnects")
                    _listener.EnableMeasurementEvents(instrument);
            }
        }

        public long Count => Interlocked.Read(ref _count);
        public void Dispose() => _listener.Dispose();
    }

    [Fact]
    public async Task ConcurrentReconnect_DuringSecondConnecting_IsEntirelySideEffectFree()
    {
        var port = GetFreePort();
        using var counter = new RetryCounter(port);
        using var resume = new ManualResetEventSlim();
        var entered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = Create(port);
        await using var client = Create(port, true);
        var log = new StateLog();
        var connecting = 0;
        var gateReleased = false;
        server.ConnectionChanged += (_, state) =>
        {
            log.Record(server, state);
            if (state == ConnectionState.Connecting && Interlocked.Increment(ref connecting) == 2)
            {
                entered.TrySetResult(true);
                gateReleased = resume.Wait(Budget);
            }
        };
        server.Start(CancellationToken.None);
        var reconnect = Task.Run(server.Reconnect);
        try
        {
            await entered.Task.WaitAsync(Budget);
            Assert.Equal(1, Field(server, "_retryInProgress"));
            var epoch = Field(server, "_sessionEpoch");
            var loop = Field(server, "_acceptLoopId");
            var sessionCounter = Field(server, "_sessionCounter");
            var listener = Field(server, "_server");
            server.Reconnect();

            // 所有关键断言在测试任务中执行：库会吞掉 ConnectionChanged 中的异常。
            Assert.Equal(ReconnectedStates.Take(3), log.States);
            Assert.Equal(ConnectionState.Connecting, server.State);
            Assert.Equal(0, server.CurrentSessionId);
            Assert.Equal(epoch, Field(server, "_sessionEpoch"));
            Assert.Equal(loop, Field(server, "_acceptLoopId"));
            Assert.Equal(sessionCounter, Field(server, "_sessionCounter"));
            Assert.Same(listener, Field(server, "_server"));
            Assert.Equal(1, counter.Count);
            Assert.Equal(1, Field(server, "_retryInProgress"));

            resume.Set();
            await reconnect.WaitAsync(Budget);
            Assert.True(gateReleased);
            Assert.Equal(0, Field(server, "_retryInProgress"));
            client.Start(CancellationToken.None);
            await AssertUsable(server, client);
            Assert.Equal(ReconnectedStates, log.States);
            log.AssertConsistent();
        }
        finally
        {
            resume.Set();
            await reconnect.WaitAsync(Budget);
        }
    }

    [Fact]
    public async Task ConnectingCallback_ReentrantReconnect_IsEntirelySideEffectFree()
    {
        var port = GetFreePort();
        using var counter = new RetryCounter(port);
        await using var server = Create(port);
        await using var client = Create(port, true);
        var log = new StateLog();
        var connecting = 0;
        var reentered = false;
        ConnectionState? afterReentry = null;
        server.ConnectionChanged += (_, state) =>
        {
            log.Record(server, state);
            if (state == ConnectionState.Connecting && ++connecting == 2)
            {
                server.Reconnect();
                afterReentry = server.State;
                reentered = true;
            }
        };
        server.Start(CancellationToken.None);
        await Task.Run(server.Reconnect).WaitAsync(Budget);
        Assert.True(reentered);
        Assert.Equal(ConnectionState.Connecting, afterReentry);
        Assert.Equal(ReconnectedStates.Take(3), log.States);
        Assert.Equal(1, counter.Count);
        Assert.Equal(0, Field(server, "_retryInProgress"));
        client.Start(CancellationToken.None);
        await AssertUsable(server, client);
        Assert.Equal(ReconnectedStates, log.States);
        log.AssertConsistent();
    }

    [Fact]
    public async Task RetryCallback_ReentrantReconnect_IsRejectedBeforePublication()
    {
        var port = GetFreePort();
        using var counter = new RetryCounter(port);
        await using var server = Create(port);
        await using var client = Create(port, true);
        var log = new StateLog();
        var retries = 0;
        server.ConnectionChanged += (_, state) =>
        {
            log.Record(server, state);
            if (state == ConnectionState.Retry && ++retries == 1)
                server.Reconnect();
        };
        server.Start(CancellationToken.None);
        await Task.Run(server.Reconnect).WaitAsync(Budget);
        Assert.Equal(1, retries);
        Assert.Equal(1, counter.Count);
        Assert.Equal(ReconnectedStates.Take(3), log.States);
        client.Start(CancellationToken.None);
        await AssertUsable(server, client);
        Assert.Equal(ReconnectedStates, log.States);
        log.AssertConsistent();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RetryCallback_StopOrThrow_ReleasesRetryAdmission(bool stop)
    {
        var port = GetFreePort();
        await using var server = Create(port);
        var callbackRan = false;
        server.ConnectionChanged += (_, state) =>
        {
            if (state != ConnectionState.Retry)
                return;
            callbackRan = true;
            if (stop)
                server.DisposeAsync().GetAwaiter().GetResult();
            else
                throw new InvalidOperationException("测试回调异常隔离");
        };
        server.Start(CancellationToken.None);
        await Task.Run(server.Reconnect).WaitAsync(Budget);
        Assert.True(callbackRan);
        Assert.Equal(0, Field(server, "_retryInProgress"));
        if (stop)
        {
            Assert.Equal(ConnectionState.Disconnected, server.State);
            Assert.Equal(0, server.CurrentSessionId);
        }
        else
        {
            await using var client = Create(port, true);
            client.Start(CancellationToken.None);
            await AssertUsable(server, client);
            server.Reconnect();
            Assert.Equal(ConnectionState.Connecting, server.State);
            Assert.Equal(0, Field(server, "_retryInProgress"));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LateAccept_DuringRetryCallback_IsRetainedUntilConnecting(bool stop)
    {
        var port = GetFreePort();
        using var resume = new ManualResetEventSlim();
        var entered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var prematureConnected = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = Create(port);
        await using var client = Create(port, true);
        var log = new StateLog();
        var paused = 0;
        var gateReleased = false;
        server.ConnectionChanged += (_, state) =>
        {
            log.Record(server, state);
            if (state == ConnectionState.Connected && Volatile.Read(ref paused) == 1)
                prematureConnected.TrySetResult(true);
            if (state == ConnectionState.Retry)
            {
                Volatile.Write(ref paused, 1);
                entered.TrySetResult(true);
                gateReleased = resume.Wait(Budget);
                Volatile.Write(ref paused, 0);
            }
        };
        server.Start(CancellationToken.None);
        var ready = server.WaitForConnectedAsync();
        var reconnect = Task.Run(server.Reconnect);
        try
        {
            await entered.Task.WaitAsync(Budget);
            client.Start(CancellationToken.None);
            await client.WaitForConnectedAsync().WaitAsync(Budget);
            // 真实旧 accept 已完成并交出 socket；不是模拟一个没有 socket 的 Connected。
            await WaitForAsync(() => Field(server, "_server") is null);
            // 给错误实现的 Connected 通知机会；正确实现必须一直等到重连发布 Connecting。
            await Task.WhenAny(prematureConnected.Task, Task.Delay(200));
            Assert.False(prematureConnected.Task.IsCompleted);
            Assert.False(ready.IsCompleted);
            Assert.Equal(ConnectionState.Retry, server.State);
            Assert.Equal(0, server.CurrentSessionId);
            Assert.Equal(ReconnectedStates.Take(2), log.States);

            if (stop)
                await server.DisposeAsync();
            resume.Set();
            await reconnect.WaitAsync(Budget);
            Assert.True(gateReleased);
            Assert.Equal(0, Field(server, "_retryInProgress"));
            if (stop)
            {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ready.WaitAsync(Budget));
                Assert.Equal(ConnectionState.Disconnected, server.State);
                Assert.Null(Field(server, "_socket"));
                Assert.Null(Field(server, "_session"));
                Assert.Equal(new[] { ConnectionState.Connecting, ConnectionState.Retry, ConnectionState.Disconnected }, log.States);
            }
            else
            {
                await ready.WaitAsync(Budget);
                await AssertUsable(server, client);
                Assert.Equal(ReconnectedStates, log.States);
                // Retry 回调结束才启动替代循环：不能把已由迟到 accept 满足的监听再打开。
                Assert.Null(Field(server, "_server"));
                using var rejected = new TcpClient();
                await Assert.ThrowsAsync<SocketException>(() => rejected.ConnectAsync(IPAddress.Loopback, port).WaitAsync(Budget));
            }
            log.AssertConsistent();
        }
        finally
        {
            resume.Set();
            await reconnect.WaitAsync(Budget);
        }
    }

    [Fact]
    public async Task BacklogHandshake_IsNotProofOfANewAcceptedSession()
    {
        var port = GetFreePort();
        using var release = new ManualResetEventSlim();
        var held = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = Create(port);
        using var first = new TcpClient();
        using var backlog = new TcpClient();
        server.Start(CancellationToken.None);
        var listenerGate = Field(server, "_listenerGate")!;
        var blocker = Task.Run(() =>
        {
#if NET9_0_OR_GREATER
            lock ((Lock)listenerGate)
#else
            lock (listenerGate)
#endif
            {
                held.TrySetResult(true);
                if (!release.Wait(Budget))
                    throw new TimeoutException("测试未释放监听关闭同步点");
            }
        });
        try
        {
            await held.Task.WaitAsync(Budget);
            await first.ConnectAsync(IPAddress.Loopback, port).WaitAsync(Budget);
            await WaitForAsync(() => Equals(Field(server, "_acceptPending"), 1));
            // 第一个 accept 已完成，但单客户端监听器的关闭还被门控：第二个握手可以成功。
            await backlog.ConnectAsync(IPAddress.Loopback, port).WaitAsync(Budget);
            release.Set();
            await blocker.WaitAsync(Budget);
            await server.WaitForConnectedAsync().WaitAsync(Budget);
            var accepted = Assert.IsType<Socket>(Field(server, "_socket"));
            var remote = Assert.IsType<IPEndPoint>(accepted.RemoteEndPoint);
            var firstEndpoint = Assert.IsType<IPEndPoint>(first.Client.LocalEndPoint);
            var backlogEndpoint = Assert.IsType<IPEndPoint>(backlog.Client.LocalEndPoint);
            Assert.Equal(firstEndpoint.Port, remote.Port);
            Assert.Equal(firstEndpoint.Address.MapToIPv4(), remote.Address.MapToIPv4());
            Assert.NotEqual(backlogEndpoint.Port, remote.Port);
            Assert.Null(Field(server, "_server"));
            // 对 backlog 客户端只检查 Connected 会把第一个客户端的会话错误认作自己的。
            Assert.Equal(ConnectionState.Connected, server.State);
        }
        finally
        {
            release.Set();
            await blocker.WaitAsync(Budget);
        }
    }

    [Fact]
    public async Task ConnectedPublication_ReenteredFromRetryCallback_DoesNotWaitForItself()
    {
        var port = GetFreePort();
        await using var server = Create(port);
        var log = new StateLog();
        ConnectionState? stateAfterPublication = null;
        var returned = false;
        server.ConnectionChanged += (_, state) =>
        {
            log.Record(server, state);
            if (state != ConnectionState.Retry)
                return;
            // 同线程同步发布不能等待外层回调返回，否则 Connecting 永远没有机会发布。
            typeof(StreamConnection<string>).GetMethod("CommunicationStateChanging", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(server, new object?[] { ConnectionState.Connected, null });
            stateAfterPublication = server.State;
            returned = true;
        };
        server.Start(CancellationToken.None);
        await Task.Run(server.Reconnect).WaitAsync(Budget);
        Assert.True(returned);
        Assert.Equal(ConnectionState.Retry, stateAfterPublication);
        Assert.Equal(ReconnectedStates, log.States);
        Assert.Equal(ConnectionState.Connected, server.State);
        Assert.Equal(0, Field(server, "_retryInProgress"));
        log.AssertConsistent();
    }
}
