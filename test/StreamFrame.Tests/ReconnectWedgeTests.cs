using System.Net;
using System.Net.Sockets;
using StreamFrame;

namespace StreamFrame.Tests;

/// <summary>
/// #47 的确定性复现/回归：服务端主动发起的重连（用户显式 Reconnect()，尤其是与自动重连
/// 并发竞速时）反复发生后，被动端必须仍能在秒级内接受新连接。楔死形态 = 监听端口重绑失败
/// 或接受循环被堵（疑似 TIME_WAIT 占用 + 未设 SO_REUSEADDR；accept 重试延迟持锁放大故障时长）。
/// </summary>
public class ReconnectWedgeTests
{
    private readonly Xunit.ITestOutputHelper _output;

    public ReconnectWedgeTests(Xunit.ITestOutputHelper output) => _output = output;

    private sealed class DumpLogger : Microsoft.Extensions.Logging.ILogger
    {
        private readonly Xunit.ITestOutputHelper _sink;
        public DumpLogger(Xunit.ITestOutputHelper sink) => _sink = sink;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;
        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => _sink.WriteLine($"[LOG {logLevel}] {formatter(state, exception)}{(exception is null ? string.Empty : " (" + exception.GetType().Name + ")")}");
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static async Task<long> AssertNewSessionAsync(StreamConnection<string> server, TcpClient client, long previousId, int timeoutMs = 30_000)
    {
        var deadline = TestClock.TickCount64 + timeoutMs;
        while (TestClock.TickCount64 < deadline
            && (server.State != ConnectionState.Connected || server.CurrentSessionId <= previousId))
            await Task.Delay(20);
        Assert.Equal(ConnectionState.Connected, server.State);
        var sessionId = server.CurrentSessionId;
        Assert.True(sessionId > previousId, $"连接必须形成新会话：{previousId} → {sessionId}");

        // 握手成功不等于已被 accept（第二客户端可能只进入旧监听器 backlog）。双向帧
        // 证明这个客户端与新编号的会话确实配对，不能把上一客户端的 Connected 当作成功。
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(timeoutMs));
        var stream = client.GetStream();
        await stream.WriteAsync(new byte[] { 0, 0, 0, 1, (byte)'q' }, 0, 5, timeout.Token);
        await using var messages = server.GetSessionMessages(timeout.Token).GetAsyncEnumerator();
        Assert.True(await messages.MoveNextAsync());
        Assert.Equal(sessionId, messages.Current.SessionId);
        Assert.Equal("q", messages.Current.Message);
        await server.SendInSessionAsync(sessionId, "r", timeout.Token).WaitAsync(timeout.Token);
        var reply = new byte[5];
        var read = 0;
        while (read < reply.Length)
        {
            var count = await stream.ReadAsync(reply, read, reply.Length - read, timeout.Token);
            Assert.True(count > 0, "新会话回复未完整送达即关闭");
            read += count;
        }
        Assert.Equal(new byte[] { 0, 0, 0, 1, (byte)'r' }, reply);
        return sessionId;
    }

    private static async Task WaitSessionEndedAsync(StreamConnection<string> server, long sessionId)
    {
        var deadline = TestClock.TickCount64 + 30_000;
        while (TestClock.TickCount64 < deadline && server.CurrentSessionId == sessionId)
            await Task.Delay(20);
        Assert.NotEqual(sessionId, server.CurrentSessionId);
    }

    private static async Task ConnectWithRetryAsync(TcpClient client, int port, int timeoutMs = 10_000)
    {
        // Start() 是异步的：监听器完成 bind 前连接会被拒（CI 的 ubuntu 上线程池调度更慢，
        // 必须容忍启动窗口），重试直到超时
        var deadline = TestClock.TickCount64 + timeoutMs; // 预算宽裕：本断言探测的是永久不可用，并行负载下偶发慢收敛不应误报
        while (true)
        {
            try
            {
                await client.ConnectAsync(IPAddress.Loopback, port);
                return;
            }
            catch (SocketException) when (TestClock.TickCount64 < deadline)
            {
                await Task.Delay(100);
            }
        }
    }

    [Fact]
    public async Task RapidServerInitiatedReconnects_PortStaysAcceptable()
    {
        const int rounds = 8;
        var port = GetFreePort();

        await using var server = new StreamConnection<string>(
            new LengthPrefixFramer(), StringCodec.Instance, IPAddress.Loopback, port,
            isActive: false, new StreamConnectionOptions { AcceptRetryDelayMs = 200 },
            logger: new DumpLogger(_output));
        server.Start(CancellationToken.None);
        long previousSessionId = 0;

        for (var round = 0; round < rounds; round++)
        {
            // 对端接入形成会话（预算与下方一致：探测永久不可用，并行负载下偶发慢收敛不应误报）
            using (var client = new TcpClient())
            {
                await ConnectWithRetryAsync(client, port);
                previousSessionId = await AssertNewSessionAsync(server, client, previousSessionId);
            }
            await WaitSessionEndedAsync(server, previousSessionId);

            // 等自动重连完成、再次形成会话
            using (var client = new TcpClient())
            {
                await ConnectWithRetryAsync(client, port);
                previousSessionId = await AssertNewSessionAsync(server, client, previousSessionId);

                // 两路并发触发（#47 的竞速形态）：杀对端（自动重连）与用户显式 Reconnect() 竞争
                var kill = Task.Run(client.Dispose, cancellationToken: CancellationToken.None);
                server.Reconnect();
                await kill;
            }

            await WaitSessionEndedAsync(server, previousSessionId);
            var rebindDeadline = TestClock.TickCount64 + 20_000;
            using (var probe = new TcpClient())
            {
                await ConnectWithRetryAsync(probe, port, timeoutMs: 20_000);
                var remaining = (int)Math.Max(1, rebindDeadline - TestClock.TickCount64);
                previousSessionId = await AssertNewSessionAsync(server, probe, previousSessionId, remaining);
                Assert.True(TestClock.TickCount64 < rebindDeadline,
                    $"第 {round} 轮：服务端主动重连后 20000ms 内无法接受并收发新连接（监听楔死，#47）。");
            }
            // 探针也是真客户端：等它所属的会话终止，下一轮才能发起另一条单客户端接入。
            await WaitSessionEndedAsync(server, previousSessionId);
            _output.WriteLine($"[{TestClock.TickCount64 % 100000}] 第 {round} 轮重绑及双向帧可用");
        }
    }
}
