using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using StreamFrame;

namespace StreamFrame.Tests;

/// <summary>
/// 连接生命周期测试：Start 令牌取消停机、Disconnected 终态事件、
/// 对端地址语义、IPv4/IPv6 双栈监听。
/// </summary>
public class ConnectionLifetimeTests
{
    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static StreamConnection<string> CreateActive(int port, int retryDelayMs = 200)
        => new(
            new LengthPrefixFramer(),
            StringCodec.Instance,
            IPAddress.Loopback,
            port,
            isActive: true,
            new StreamConnectionOptions { ConnectRetryDelayMs = retryDelayMs });

    private static async Task WaitForStateAsync(
        StreamConnection<string> connection,
        ConnectionState expected,
        int timeoutMs = 5000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (connection.State == expected)
                return;
            await Task.Delay(20);
        }
    }

    [Fact]
    public async Task CancelLifetimeToken_StopsReconnecting_AndGoesDisconnected()
    {
        var port = GetFreePort();
        await using var client = CreateActive(port); // 目标端口永远无服务，持续重试

        using var cts = new CancellationTokenSource();
        client.Start(cts.Token);
        await WaitForStateAsync(client, ConnectionState.Connecting);

        // 取消生命周期令牌 = 停机：不再重试、进入终态
        cts.Cancel();
        await WaitForStateAsync(client, ConnectionState.Disconnected);
        Assert.Equal(ConnectionState.Disconnected, client.State);

        // 消息通道已正常完成：GetMessages 枚举自然结束（不抛异常）
        await using var enumerator = client.GetMessages().GetAsyncEnumerator();
        Assert.False(await enumerator.MoveNextAsync());

        // 停机后发送通道已关闭
        await Assert.ThrowsAnyAsync<ChannelClosedException>(() => client.SendAsync("late"));
    }

    [Fact]
    public async Task DisposeAsync_RaisesDisconnectedEvent()
    {
        var port = GetFreePort();
        var client = CreateActive(port);

        var states = new List<ConnectionState>();
        client.ConnectionChanged += (_, state) => { lock (states) states.Add(state); };

        client.Start(CancellationToken.None);
        await WaitForStateAsync(client, ConnectionState.Connecting);

        await client.DisposeAsync();
        lock (states)
            Assert.Contains(ConnectionState.Disconnected, states);
        Assert.Equal(ConnectionState.Disconnected, client.State);
    }

    [Fact]
    public async Task RemoteIpAddress_PassiveTracksClient_ActiveIsConfigured()
    {
        var port = GetFreePort();
        await using var server = new StreamConnection<string>(
            new LengthPrefixFramer(),
            StringCodec.Instance,
            IPAddress.Loopback,
            port,
            isActive: false);
        await using var client = CreateActive(port);

        Assert.Null(server.RemoteIpAddress); // 被动模式未连接：无对端

        server.Start(CancellationToken.None);
        client.Start(CancellationToken.None);
        await Task.WhenAll(server.WaitForConnectedAsync(), client.WaitForConnectedAsync())
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(IPAddress.Loopback.ToString(), server.RemoteIpAddress); // 已连接客户端
        Assert.Equal(IPAddress.Loopback.ToString(), client.RemoteIpAddress); // 主动模式 = 配置的远端
    }

    [Fact]
    public async Task DualMode_ListenOnAnyAddress_AcceptsIPv4Client()
    {
        var port = GetFreePort();
        await using var server = new StreamConnection<string>(
            new LengthPrefixFramer(),
            StringCodec.Instance,
            IPAddress.Any, // 0.0.0.0 应被归一为双栈监听（::），IPv4 回环客户端可连入
            port,
            isActive: false);
        server.Start(CancellationToken.None);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);
        await server.WaitForConnectedAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(ConnectionState.Connected, server.State);
    }
}
