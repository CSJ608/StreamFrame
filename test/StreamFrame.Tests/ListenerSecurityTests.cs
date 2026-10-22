using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.Logging;

namespace StreamFrame.Tests;

public class ListenerSecurityTests
{
    // 与现有回环生命周期/会话测试相同的秒级预算；不可扩大来等待 TIME_WAIT 消失。
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(5);

    private static bool IsWindows =>
#if NET48
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
#else
        OperatingSystem.IsWindows();
#endif

    private sealed class FailureLog : ILogger
    {
        private string? _lastFailure;
        public string? LastFailure => Volatile.Read(ref _lastFailure);
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            if (message.StartsWith("Connect to ", StringComparison.Ordinal))
                Volatile.Write(ref _lastFailure, message);
        }
    }

    private static StreamConnection<string> CreateServer(IPAddress address, int port = 0, ILogger? logger = null)
        => new(new LengthPrefixFramer(), StringCodec.Instance, address, port, isActive: false,
            new StreamConnectionOptions { AcceptRetryDelayMs = 25 }, logger);

    private static Socket? Listener(StreamConnection<string> server)
        => (Socket?)typeof(StreamConnection<string>)
            .GetField("_server", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(server);

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.IPv6Any, 0);
        listener.Server.DualMode = true;
        listener.Server.ExclusiveAddressUse = true;
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    [Theory]
    [InlineData("127.0.0.1")]
    [InlineData("::1")]
    [InlineData("0.0.0.0")]
    [InlineData("::")]
    public async Task ListenerOptions_WindowsExclusive_OtherPlatformsReuseAddress(string listenAddress)
    {
        await using var server = CreateServer(IPAddress.Parse(listenAddress));
        server.Start(CancellationToken.None);
        // Start 同步推进到首次 accept；用实际端口 0 绑定避免预留端口的竞速。
        var listener = Assert.IsType<Socket>(Listener(server));
        if (IsWindows || listenAddress != "::1")
            Assert.True(listener.DualMode);
        Assert.Equal(IsWindows, listener.ExclusiveAddressUse);
        Assert.Equal(IsWindows ? 0 : 1,
            (int)listener.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress)!);
    }

    [Theory]
    [InlineData("127.0.0.1", "127.0.0.1")]
    [InlineData("::1", "::1")]
    [InlineData("0.0.0.0", "0.0.0.0")]
    [InlineData("0.0.0.0", "127.0.0.1")]
    [InlineData("0.0.0.0", "::1")]
    [InlineData("::", "::")]
    [InlineData("::", "::1")]
    [InlineData("::", "127.0.0.1")]
    public async Task Listening_RejectsCompetingListener(string listenAddress, string competingAddress)
    {
        await using var server = CreateServer(IPAddress.Parse(listenAddress));
        server.Start(CancellationToken.None);
        var listener = Assert.IsType<Socket>(Listener(server));
        var port = ((IPEndPoint)listener.LocalEndPoint!).Port;
        var address = IPAddress.Parse(competingAddress);
        using var competitor = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        if (address.AddressFamily == AddressFamily.InterNetworkV6)
            competitor.DualMode = true;
        competitor.ExclusiveAddressUse = false;
        competitor.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, IsWindows);

        // Windows 即使竞争者主动复用也必须拒绝；Unix 保留复用语义，仅验证非共享监听冲突。
        var error = Assert.Throws<SocketException>(() =>
        {
            competitor.Bind(new IPEndPoint(address, port));
            if (!IsWindows)
                competitor.Listen(1);
        });
        Assert.True(error.SocketErrorCode is SocketError.AddressAlreadyInUse or SocketError.AccessDenied,
            $"重复绑定应因地址被占用而拒绝，实际为 {error.SocketErrorCode}。");
        Assert.Same(listener, Listener(server));
        Assert.Equal(ConnectionState.Connecting, server.State);
    }

    [Theory]
    [InlineData("127.0.0.1", "127.0.0.1", "127.0.0.1", false)]
    [InlineData("127.0.0.1", "127.0.0.1", "127.0.0.1", true)]
    [InlineData("::1", "::1", "::1", false)]
    [InlineData("::1", "::1", "::1", true)]
    [InlineData("0.0.0.0", "127.0.0.1", "::1", false)]
    [InlineData("0.0.0.0", "127.0.0.1", "::1", true)]
    [InlineData("0.0.0.0", "::1", "127.0.0.1", false)]
    [InlineData("0.0.0.0", "::1", "127.0.0.1", true)]
    [InlineData("::", "127.0.0.1", "::1", false)]
    [InlineData("::", "127.0.0.1", "::1", true)]
    [InlineData("::", "::1", "127.0.0.1", false)]
    [InlineData("::", "::1", "127.0.0.1", true)]
    public async Task ServerInitiatedReconnect_RebindsAndExchangesMessages(
        string listenAddress, string firstAddress, string nextAddress, bool keepOldPeerOpen)
    {
        var port = GetFreePort();
        var log = new FailureLog();
        await using var server = CreateServer(IPAddress.Parse(listenAddress), port, log);
        server.Start(CancellationToken.None);
        var firstListener = Assert.IsType<Socket>(Listener(server));
        var firstIp = IPAddress.Parse(firstAddress);
        using var first = new TcpClient(firstIp.AddressFamily);
        await first.ConnectWithoutCancellationAsync(firstIp, port).WaitAsync(Budget, cancellationToken: CancellationToken.None);
        await server.WaitForConnectedAsync(ct: CancellationToken.None).WaitAsync(Budget, cancellationToken: CancellationToken.None);
        await ExchangeAsync(server, first, "before-rebind");
        var firstSession = server.CurrentSessionId;
        Assert.NotEqual(0, firstSession);
        Assert.Null(Listener(server)); // 默认单客户端语义不变：关闭 listener，保留 accepted socket。
        Assert.Throws<ObjectDisposedException>(() => firstListener.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress));

        // 第二个正常客户端仍应在 TCP 层被拒；不可通过保留 listener 绕过独占重绑问题。
        using (var extra = new TcpClient(firstIp.AddressFamily))
            await Assert.ThrowsAsync<SocketException>(() => extra.ConnectWithoutCancellationAsync(firstIp, port).WaitAsync(Budget, cancellationToken: CancellationToken.None));

        server.Reconnect(); // 必须由服务端主动关闭，不能先关客户端规避服务端 FIN_WAIT/TIME_WAIT。
        var eof = first.GetStream().ReadAsync(new byte[1], 0, 1, cancellationToken: CancellationToken.None);
        await eof.WaitAsync(Budget, cancellationToken: CancellationToken.None);
        Assert.Equal(0, await eof);
        if (!keepOldPeerOpen)
        {
            first.Client.Shutdown(SocketShutdown.Send);
            first.Dispose();
        }
        // keepOldPeerOpen 时只接收 FIN，不发送 FIN/Dispose；旧对端保持半开直到新会话可用。
        await WaitForListenerAsync(server, log);
        var rebound = Assert.IsType<Socket>(Listener(server));
        Assert.NotSame(firstListener, rebound);
        Assert.Equal(port, ((IPEndPoint)rebound.LocalEndPoint!).Port);

        var nextIp = IPAddress.Parse(nextAddress);
        using var next = new TcpClient(nextIp.AddressFamily);
        await next.ConnectWithoutCancellationAsync(nextIp, port).WaitAsync(Budget, cancellationToken: CancellationToken.None);
        await server.WaitForConnectedAsync(ct: CancellationToken.None).WaitAsync(Budget, cancellationToken: CancellationToken.None);
        await ExchangeAsync(server, next, "after-rebind");
        Assert.True(server.CurrentSessionId > firstSession);
        Assert.Equal(nextIp.ToString(), server.RemoteIpAddress);
        Assert.Null(Listener(server));
        GC.KeepAlive(first);
    }

    private static async Task WaitForListenerAsync(StreamConnection<string> server, FailureLog log)
    {
        var deadline = TestClock.TickCount64 + (long)Budget.TotalMilliseconds;
        while (TestClock.TickCount64 < deadline)
        {
            if (Listener(server) is not null)
                return;
            await Task.Delay(20);
        }
        Assert.Fail($"服务端主动重连后未在 {Budget.TotalMilliseconds}ms 内重绑。State={server.State}; {log.LastFailure}");
    }

    private static async Task ExchangeAsync(StreamConnection<string> server, TcpClient client, string message)
    {
        var payload = Encoding.UTF8.GetBytes(message);
        var frame = new byte[4 + payload.Length];
        BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(0, 4), payload.Length);
        payload.CopyTo(frame.AsSpan(4));
        var stream = client.GetStream();
        await stream.WriteAsync(frame, 0, frame.Length).WaitAsync(Budget);
        using var timeout = new CancellationTokenSource(Budget);
        await using (var messages = server.GetMessages(timeout.Token).GetAsyncEnumerator())
        {
            Assert.True(await messages.MoveNextAsync());
            Assert.Equal(message, messages.Current);
        }
        await server.SendInSessionAsync(server.CurrentSessionId, message).WaitAsync(Budget);
        var reply = new byte[frame.Length];
        var received = 0;
        while (received < reply.Length)
        {
            var read = stream.ReadAsync(reply, received, reply.Length - received);
            await read.WaitAsync(Budget);
            var count = await read;
            Assert.True(count > 0, "对端在完整帧读完前关闭。");
            received += count;
        }
        Assert.Equal(frame, reply);
    }
}
