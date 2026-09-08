using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Microsoft.Extensions.Logging;

namespace StreamFrame.Tests;

public class ListenerRetryTests
{
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(10);

    // 日志在 Bind 失败已退出初始化之后发出；阻塞此处可精确控制释放端口/停机的顺序。
    private sealed class FailureGate : ILogger, IDisposable
    {
        public TaskCompletionSource<bool> Failed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ManualResetEventSlim Continue { get; } = new(false);
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (!formatter(state, exception).StartsWith("Connect to ", StringComparison.Ordinal))
                return;
            Failed.TrySetResult(true);
            if (!Continue.Wait(Budget))
                throw new TimeoutException("测试未释放绑定失败同步点");
        }
        public void Dispose() => Continue.Dispose();
    }

    private static TcpListener Occupy(int port = 0)
    {
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Server.ExclusiveAddressUse = true;
        listener.Start();
        return listener;
    }

    private static StreamConnection<string> Create(int port, bool active, ILogger? logger = null)
        => new(new LengthPrefixFramer(), StringCodec.Instance, IPAddress.Loopback, port, active,
            new StreamConnectionOptions { AcceptRetryDelayMs = 25, ConnectRetryDelayMs = 25 }, logger);

    private static object? RetainedListener(StreamConnection<string> server)
        => typeof(StreamConnection<string>).GetField("_server", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(server);

    private static async Task<string> Receive(StreamConnection<string> connection)
    {
        using var timeout = new CancellationTokenSource(Budget);
        await foreach (var message in connection.GetMessages(timeout.Token))
            return message;
        throw new InvalidOperationException("消息流提前结束");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PublishedListener_StopClosesSocketAndReleasesPort(bool cancel)
    {
        var occupied = Occupy();
        var port = ((IPEndPoint)occupied.LocalEndpoint).Port;
        occupied.Stop();
        using var lifetime = new CancellationTokenSource();
        await using var server = Create(port, false);
        // Start 同步运行到首次未完成的 Accept；此时监听已发布、尚无客户端。
        server.Start(lifetime.Token);
        var listener = Assert.IsType<Socket>(RetainedListener(server));
        Assert.True(listener.IsBound);
        if (cancel)
            lifetime.Cancel();
        else
            await server.DisposeAsync();
        Assert.Null(RetainedListener(server));
        Assert.Throws<ObjectDisposedException>(() => listener.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress));
        var replacement = Occupy(port);
        replacement.Stop();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Initialization_QueuedBehindShutdown_CannotPublish(bool cancel)
    {
        var occupied = Occupy();
        var port = ((IPEndPoint)occupied.LocalEndpoint).Port;
        using var lifetime = new CancellationTokenSource();
        using var failure = new FailureGate();
        await using var server = Create(port, false, failure);
        var start = Task.Run(() => server.Start(lifetime.Token));
        try
        {
            await failure.Failed.Task.WaitAsync(Budget);
            var type = typeof(StreamConnection<string>);
            var listenerGate = type.GetField("_listenerGate", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(server)!;
            var initialize = type.GetMethod("InitServer", BindingFlags.Instance | BindingFlags.NonPublic)!;
            using var attempting = new ManualResetEventSlim();
            using var stopping = new ManualResetEventSlim();
            server.ConnectionChanged += (_, state) =>
            {
                if (state == ConnectionState.Disconnected)
                    stopping.Set();
            };
            Task lateInitialization;
            Task shutdown;
            // 白盒门控准确覆盖：重试已排队，停机标志已发布，随后才允许初始化取锁。
#if NET9_0_OR_GREATER
            lock ((Lock)listenerGate)
#else
            lock (listenerGate)
#endif
            {
                lateInitialization = Task.Run(() =>
                {
                    attempting.Set();
                    var error = Assert.Throws<TargetInvocationException>(() => initialize.Invoke(server, new object[] { CancellationToken.None }));
                    Assert.IsAssignableFrom<OperationCanceledException>(error.InnerException);
                });
                Assert.True(attempting.Wait(Budget));
                shutdown = Task.Run(async () =>
                {
                    if (cancel)
                        lifetime.Cancel();
                    else
                        await server.DisposeAsync();
                });
                Assert.True(stopping.Wait(Budget));
                occupied.Stop();
            }
            await Task.WhenAll(lateInitialization, shutdown).WaitAsync(Budget);
            failure.Continue.Set();
            await start.WaitAsync(Budget);
            Assert.Null(RetainedListener(server));
            var replacement = Occupy(port);
            replacement.Stop();
        }
        finally
        {
            failure.Continue.Set();
            occupied.Stop();
            await start.WaitAsync(Budget);
        }
    }

    [Fact]
    public async Task BindFailure_ReleasedPort_AutomaticallyConnectsAndExchangesMessages()
    {
        var occupied = Occupy();
        var port = ((IPEndPoint)occupied.LocalEndpoint).Port;
        using var gate = new FailureGate();
        await using var server = Create(port, false, gate);
        var start = Task.Run(() => server.Start(CancellationToken.None));
        try
        {
            await gate.Failed.Task.WaitAsync(Budget);
            occupied.Stop();
            gate.Continue.Set();
            await start.WaitAsync(Budget);
            await using var client = Create(port, true);
            client.Start(CancellationToken.None);
            await server.WaitForConnectedAsync().WaitAsync(Budget);
            await client.WaitForConnectedAsync().WaitAsync(Budget);
            await client.SendAsync("after-bind-failure");
            Assert.Equal("after-bind-failure", await Receive(server));
            await server.SendAsync("reply");
            Assert.Equal("reply", await Receive(client));
            Assert.Null(RetainedListener(server)); // 单客户端接受后必须关闭监听器。
            using var second = new TcpClient();
            await Assert.ThrowsAsync<SocketException>(() => second.ConnectAsync(IPAddress.Loopback, port).WaitAsync(Budget));
        }
        finally
        {
            gate.Continue.Set();
            occupied.Stop();
            await start.WaitAsync(Budget);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BindFailure_StopBeforeRetry_DoesNotRetainOrRepublishListener(bool cancel)
    {
        var occupied = Occupy();
        var port = ((IPEndPoint)occupied.LocalEndpoint).Port;
        using var lifetime = new CancellationTokenSource();
        using var gate = new FailureGate();
        await using var server = Create(port, false, gate);
        var start = Task.Run(() => server.Start(lifetime.Token));
        try
        {
            await gate.Failed.Task.WaitAsync(Budget);
            Assert.Null(RetainedListener(server));
            if (cancel)
                lifetime.Cancel();
            else
                await server.DisposeAsync();
            occupied.Stop();
            gate.Continue.Set();
            await start.WaitAsync(Budget);
            Assert.True(server.IsDisposed);
            Assert.Equal(ConnectionState.Disconnected, server.State);
            Assert.Null(RetainedListener(server));
            var replacement = Occupy(port);
            replacement.Stop();
        }
        finally
        {
            gate.Continue.Set();
            occupied.Stop();
            await start.WaitAsync(Budget);
        }
    }
}
