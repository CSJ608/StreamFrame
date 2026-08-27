using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using StreamFrame;

namespace StreamFrame.Tests;

/// <summary>
/// 单客户端连接行为测试：被动模式 accept 第一个客户端后关闭监听，
/// 确保同一时间只有一个客户端能与服务端建立可通讯连接。
/// </summary>
public class SingleClientConnectionTests
{
    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static StreamConnection<string> CreateServer(int port, bool acceptFirstClientOnly = true)
        => new(
            new LengthPrefixFramer(),
            StringCodec.Instance,
            IPAddress.Loopback,
            port,
            isActive: false,
            new StreamConnectionOptions
            {
                AcceptFirstClientOnly = acceptFirstClientOnly,
                ConnectRetryDelayMs = 200,
            });

    /// <summary>用原始 TCP 客户端尝试连接，返回是否成功建立。</summary>
    private static async Task<bool> TryTcpConnectAsync(int port, int timeoutMs = 1500)
    {
        using var client = new TcpClient();
        using var timeout = new CancellationTokenSource(timeoutMs);
        try
        {
#if NET48
            await client.ConnectAsync(IPAddress.Loopback.ToString(), port); // netfx 无 (IPAddress,int,ct) 重载；回环连接瞬时完成
#else
            await client.ConnectAsync(IPAddress.Loopback, port, timeout.Token);
#endif
            return client.Connected;
        }
        catch (Exception) when (timeout.IsCancellationRequested)
        {
            return false;
        }
        catch (SocketException)
        {
            return false;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    [Fact]
    public async Task SecondClient_IsRejectedAfterFirstConnects()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port);
        server.Start(CancellationToken.None);

        // 等监听就绪
        await Task.Delay(300);

        // 第一个客户端连上，并进入 Connected
        using var firstClient = new TcpClient();
        await firstClient.ConnectAsync(IPAddress.Loopback, port);
        Assert.True(firstClient.Connected, "第一个客户端应能连接成功");

        // 等服务端状态机进入 Connected（监听 socket 已关闭）
        await WaitForServerConnectedAsync(server);
        Assert.Equal(ConnectionState.Connected, server.State);

        // 第二个客户端应被 TCP 层拒绝（监听已关闭，连接立即失败）
        var secondConnected = await TryTcpConnectAsync(port);
        Assert.False(secondConnected, "第二个客户端应被拒绝，无法建立连接");
    }

    [Fact]
    public async Task FirstClient_CanExchangeData()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port);
        server.Start(CancellationToken.None);

        await Task.Delay(300);

        using var firstClient = new TcpClient();
        await firstClient.ConnectAsync(IPAddress.Loopback, port);
        var stream = firstClient.GetStream();

        // 发一个 LengthPrefix 帧
        var payload = System.Text.Encoding.UTF8.GetBytes("hello");
        var frame = new byte[4 + payload.Length];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(0, 4), payload.Length);
        payload.CopyTo(frame.AsSpan(4));
        await stream.WriteAsync(frame);

        // 服务端应解码出该消息
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        string? serverMessage = null;
        await foreach (var msg in server.GetMessages(cts.Token))
        {
            serverMessage = msg;
            break;
        }
        Assert.Equal("hello", serverMessage);

        Assert.Equal(ConnectionState.Connected, server.State);
    }

    [Fact]
    public async Task AcceptFirstClientOnly_Disabled_KeepsListening()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port, acceptFirstClientOnly: false);
        server.Start(CancellationToken.None);

        await Task.Delay(300);

        // 第一个客户端连上
        using var firstClient = new TcpClient();
        await firstClient.ConnectAsync(IPAddress.Loopback, port);
        await WaitForServerConnectedAsync(server);
        Assert.Equal(ConnectionState.Connected, server.State);

        // 开关关闭时：监听保持打开，第二个客户端 TCP 层仍能完成握手
        // （但当前框架不处理它，属于预留行为）
        var secondConnected = await TryTcpConnectAsync(port);
        Assert.True(secondConnected, "开关关闭时第二个客户端应能完成 TCP 握手（监听未关闭）");
    }

    private static async Task WaitForServerConnectedAsync(StreamConnection<string> server)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        while (server.State != ConnectionState.Connected && !cts.IsCancellationRequested)
        {
            await Task.Delay(30, cts.Token);
        }

        if (server.State != ConnectionState.Connected)
            throw new TimeoutException("服务端未在预期时间内进入 Connected 状态");
    }
}
