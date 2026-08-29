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
    private readonly Xunit.Abstractions.ITestOutputHelper _output;

    public ReconnectWedgeTests(Xunit.Abstractions.ITestOutputHelper output) => _output = output;

    private sealed class DumpLogger : Microsoft.Extensions.Logging.ILogger
    {
        private readonly Xunit.Abstractions.ITestOutputHelper _sink;
        public DumpLogger(Xunit.Abstractions.ITestOutputHelper sink) => _sink = sink;
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

    private static async Task<bool> TryConnectAsync(int port)
    {
        try
        {
            using var probe = new TcpClient();
            await probe.ConnectAsync(IPAddress.Loopback, port);
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
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

    private static async Task WaitConnectedAsync(StreamConnection<string> server, int timeoutMs = 30_000)
    {
        var deadline = TestClock.TickCount64 + timeoutMs; // 预算宽裕：本断言探测的是永久不可用，并行负载下偶发慢收敛不应误报
        while (TestClock.TickCount64 < deadline && server.State != ConnectionState.Connected)
            await Task.Delay(20);
        Assert.Equal(ConnectionState.Connected, server.State);
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

        for (var round = 0; round < rounds; round++)
        {
            // 对端接入形成会话
            using (var client = new TcpClient())
            {
                await ConnectWithRetryAsync(client, port);
                await WaitConnectedAsync(server, timeoutMs: 10_000);
            }

            // 等自动重连完成、再次形成会话
            using (var client = new TcpClient())
            {
                await ConnectWithRetryAsync(client, port);
                await WaitConnectedAsync(server, 30_000);

                // 两路并发触发（#47 的竞速形态）：杀对端（自动重连）与用户显式 Reconnect() 竞争
                var kill = Task.Run(client.Dispose);
                server.Reconnect();
                await kill;
            }

            var rebindDeadline = TestClock.TickCount64 + 20_000; // 修复目标：秒级可用（20s 上限：并行负载下偶发慢收敛，曾以 1/15 概率闪失）
            while (TestClock.TickCount64 < rebindDeadline)
            {
                if (await TryConnectAsync(port))
                    break;
                await Task.Delay(100);
            }

            Assert.True(TestClock.TickCount64 < rebindDeadline,
                $"第 {round} 轮：服务端主动重连后 {10_000}ms 内无法接受新连接（监听楔死，#47）。");
            _output.WriteLine($"[{TestClock.TickCount64 % 100000}] 第 {round} 轮重绑可用 ✓");
        }
    }
}
