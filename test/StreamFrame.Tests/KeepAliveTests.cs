using System.Net;
using System.Net.Sockets;
using System.Reflection;

namespace StreamFrame.Tests;

public class KeepAliveTests
{
#if NET8_0_OR_GREATER
    [Theory]
    [InlineData(1, 1)]
    [InlineData(999, 1)]
    [InlineData(1000, 1)]
    [InlineData(1500, 2)]
    [InlineData(int.MaxValue, 2147484)]
    public void Conversion_RoundsUpWithoutOverflow(int milliseconds, int seconds)
        => Assert.Equal(seconds, StreamConnection<string>.KeepAliveSeconds(milliseconds));
#endif

    [Theory]
    [InlineData(null, null, 30, 5)]
#if NET8_0_OR_GREATER
    [InlineData(1500, 2500, 2, 3)]
    [InlineData(1, 999, 1, 1)]
    [InlineData(1000, 2000, 1, 2)]
#endif
    public async Task Enabled_ReadsExpectedSecondsFromConnectedSocket(
        int? timeMs, int? intervalMs, int expectedTime, int expectedInterval)
    {
        var options = new StreamConnectionOptions { TcpKeepAlive = true };
        if (timeMs.HasValue) options.KeepAliveTimeMs = timeMs.Value;
        if (intervalMs.HasValue) options.KeepAliveIntervalMs = intervalMs.Value;
        await WithConnectedSocket(options, socket =>
        {
            Assert.Equal(1, (int)socket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive)!);
#if NET8_0_OR_GREATER
            var timeOption = SocketOptionName.TcpKeepAliveTime;
            var intervalOption = SocketOptionName.TcpKeepAliveInterval;
#else
            // net48 没有枚举名称；Windows TCP_KEEPIDLE/TCP_KEEPINTVL 原生选项读回秒。
            var timeOption = (SocketOptionName)3;
            var intervalOption = (SocketOptionName)17;
#endif
            Assert.Equal(expectedTime, (int)socket.GetSocketOption(SocketOptionLevel.Tcp, timeOption)!);
            Assert.Equal(expectedInterval, (int)socket.GetSocketOption(SocketOptionLevel.Tcp, intervalOption)!);
        });
    }

    [Fact]
    public async Task Defaults_KeepAliveRemainsDisabled()
        => await WithConnectedSocket(new StreamConnectionOptions(), socket =>
            Assert.Equal(0, (int)socket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive)!));

    private static async Task WithConnectedSocket(StreamConnectionOptions options, Action<Socket> assert)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            await using var connection = new StreamConnection<string>(
                new LengthPrefixFramer(), StringCodec.Instance, IPAddress.Loopback,
                ((IPEndPoint)listener.LocalEndpoint).Port, true, options);
            connection.Start(CancellationToken.None);
            var accepting = listener.AcceptSocketAsync();
            await accepting.WaitAsync(TimeSpan.FromSeconds(5));
            using var peer = await accepting;
            await connection.WaitForConnectedAsync().WaitAsync(TimeSpan.FromSeconds(5));
            var socket = (Socket)typeof(StreamConnection<string>)
                .GetField("_socket", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(connection)!;
            assert(socket);
        }
        finally
        {
            listener.Stop();
        }
    }
}
