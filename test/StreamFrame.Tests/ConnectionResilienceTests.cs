using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using StreamFrame;
using StreamFrame.Abstractions;

namespace StreamFrame.Tests;

/// <summary>
/// 连接级端到端行为测试（真实 TCP 回环）：断线重连后消息送达、解码失败策略、
/// 调试钩子异常隔离、接收空闲超时、发送失败重连。
/// 这些用例对应 1.2.0 之前实测复现的"会话假活"问题。
/// </summary>
public class ConnectionResilienceTests
{
    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static byte[] Frame(string payload)
    {
        var bytes = Encoding.UTF8.GetBytes(payload);
        var frame = new byte[4 + bytes.Length];
        BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(0, 4), bytes.Length);
        bytes.CopyTo(frame.AsSpan(4));
        return frame;
    }

    private static async Task WaitForStateAsync(
        StreamConnection<string> connection,
        Func<ConnectionState, bool> predicate,
        int timeoutMs = 5000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate(connection.State))
                return;
            await Task.Delay(20);
        }
    }

    private static async Task WaitForConnectedAsync(StreamConnection<string> connection, int timeoutMs = 5000)
        => await WaitForStateAsync(connection, s => s == ConnectionState.Connected, timeoutMs);

    private static StreamConnection<string> CreateServer(
        int port,
        StreamConnectionOptions? options = null,
        ICodec<string>? codec = null)
        => new(
            new LengthPrefixFrameCodec(),
            codec ?? StringCodec.Instance,
            IPAddress.Loopback,
            port,
            isActive: false,
            options ?? new StreamConnectionOptions());

    [Fact]
    public async Task DisconnectThenReconnect_MessagesStillDelivered()
    {
        // 1.1.0 的核心缺陷：第一次会话结束（哪怕是对端正常断开）会永久关闭消息通道，
        // 重连后连接看似健康、业务消息却永远不再送达。本用例锁定该回归。
        var port = GetFreePort();
        await using var server = CreateServer(port);
        var received = new List<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var drainTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var message in server.GetMessages(drainCts.Token))
                    lock (received)
                        received.Add(message);
            }
            catch (OperationCanceledException)
            {
            }
        });

        server.Start(CancellationToken.None);

        // 第一个客户端：正常收发
        using (var client1 = new TcpClient())
        {
            await client1.ConnectAsync(IPAddress.Loopback, port);
            await WaitForConnectedAsync(server);
            await client1.GetStream().WriteAsync(Frame("before-disconnect"));
        }

        var deadline = Environment.TickCount64 + 5000;
        while (Environment.TickCount64 < deadline)
        {
            lock (received)
            {
                if (received.Count == 1)
                    break;
            }
            await Task.Delay(20);
        }
        lock (received)
            Assert.Equal(new[] { "before-disconnect" }, received);

        // 对端断开 → 服务端重连流程 → 重新监听
        await WaitForStateAsync(server, s => s != ConnectionState.Connected);
        await WaitForConnectedAsync(server, timeoutMs: 8000);

        // 第二个客户端：重连后的消息必须照常送达
        using (var client2 = new TcpClient())
        {
            await client2.ConnectAsync(IPAddress.Loopback, port);
            await Task.Delay(300);
            await client2.GetStream().WriteAsync(Frame("after-reconnect"));
        }

        deadline = Environment.TickCount64 + 5000;
        while (Environment.TickCount64 < deadline)
        {
            lock (received)
            {
                if (received.Count == 2)
                    break;
            }
            await Task.Delay(20);
        }
        lock (received)
            Assert.Equal(new[] { "before-disconnect", "after-reconnect" }, received);

        drainCts.Cancel();
        await drainTask;
    }

    [Fact]
    public async Task DecodeError_DisconnectPolicy_ReconnectsAndRecovers()
    {
        var port = GetFreePort();
        var codec = new FlakyDecodeCodec(failOnce: true);
        await using var server = CreateServer(port, codec: codec);

        var errors = new List<FrameErrorEventArgs>();
        server.FrameError += (_, e) => { lock (errors) errors.Add(e); };

        var received = new List<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var drainTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var message in server.GetMessages(drainCts.Token))
                    lock (received)
                        received.Add(message);
            }
            catch (OperationCanceledException)
            {
            }
        });

        server.Start(CancellationToken.None);

        using (var client1 = new TcpClient())
        {
            await client1.ConnectAsync(IPAddress.Loopback, port);
            await WaitForConnectedAsync(server);
            await client1.GetStream().WriteAsync(Frame("FLAKY")); // 触发一次解码失败
            await client1.GetStream().WriteAsync(Frame("never-arrives")); // 断线时排队/未投递
        }

        // 解码失败 → 会话断开 → 状态离开 Connected 并重连
        await WaitForStateAsync(server, s => s != ConnectionState.Connected, timeoutMs: 5000);
        await WaitForConnectedAsync(server, timeoutMs: 8000);

        var error = Assert.Single(errors);
        Assert.Equal(FrameErrorKind.DecodeFailed, error.Kind);
        Assert.Equal("FLAKY", Encoding.UTF8.GetString(error.Bytes.Span));

        // 重连后正常消息恢复送达
        using (var client2 = new TcpClient())
        {
            await client2.ConnectAsync(IPAddress.Loopback, port);
            await Task.Delay(300);
            await client2.GetStream().WriteAsync(Frame("recovered"));
        }

        var deadline = Environment.TickCount64 + 5000;
        while (Environment.TickCount64 < deadline)
        {
            lock (received)
            {
                if (received.Count > 0)
                    break;
            }
            await Task.Delay(20);
        }
        lock (received)
            Assert.Equal(new[] { "recovered" }, received);

        drainCts.Cancel();
        await drainTask;
    }

    [Fact]
    public async Task DecodeError_SkipFramePolicy_KeepsConnectionAndDeliversNext()
    {
        var port = GetFreePort();
        var codec = new FlakyDecodeCodec(failOnce: false, alwaysFailOn: "poison");
        var options = new StreamConnectionOptions { DecodeErrorPolicy = DecodeErrorPolicy.SkipFrame };
        await using var server = CreateServer(port, options, codec);

        var errors = new List<FrameErrorEventArgs>();
        server.FrameError += (_, e) => { lock (errors) errors.Add(e); };

        var received = new List<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var drainTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var message in server.GetMessages(drainCts.Token))
                    lock (received)
                        received.Add(message);
            }
            catch (OperationCanceledException)
            {
            }
        });

        server.Start(CancellationToken.None);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);
        await WaitForConnectedAsync(server);

        // 坏帧与好帧粘在同一批字节里
        var bytes = Frame("poison").Concat(Frame("good")).ToArray();
        await client.GetStream().WriteAsync(bytes);

        var deadline = Environment.TickCount64 + 5000;
        while (Environment.TickCount64 < deadline)
        {
            lock (received)
            {
                if (received.Count == 1)
                    break;
            }
            await Task.Delay(20);
        }

        lock (received)
            Assert.Equal(new[] { "good" }, received);
        Assert.Equal(ConnectionState.Connected, server.State); // 不断线

        var error = Assert.Single(errors);
        Assert.Equal(FrameErrorKind.DecodeFailed, error.Kind);
        Assert.Equal("poison", Encoding.UTF8.GetString(error.Bytes.Span));

        drainCts.Cancel();
        await drainTask;
    }

    [Fact]
    public async Task RawBytesHook_Throws_SessionSurvives()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port);

        var rxCount = 0;
        server.RawBytesReceived = _ =>
        {
            Interlocked.Increment(ref rxCount);
            throw new InvalidOperationException("用户调试钩子抛异常");
        };

        var received = new List<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var drainTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var message in server.GetMessages(drainCts.Token))
                    lock (received)
                        received.Add(message);
            }
            catch (OperationCanceledException)
            {
            }
        });

        server.Start(CancellationToken.None);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);
        await WaitForConnectedAsync(server);

        await client.GetStream().WriteAsync(Frame("first"));
        await client.GetStream().WriteAsync(Frame("second"));

        var deadline = Environment.TickCount64 + 5000;
        while (Environment.TickCount64 < deadline)
        {
            lock (received)
            {
                if (received.Count == 2)
                    break;
            }
            await Task.Delay(20);
        }

        lock (received)
            Assert.Equal(new[] { "first", "second" }, received);
        // 回环网络下两帧可能合并为一次接收，钩子触发次数 >= 1 即可
        Assert.True(Volatile.Read(ref rxCount) >= 1, $"钩子应被调用，实际 {rxCount} 次");
        Assert.Equal(ConnectionState.Connected, server.State); // 会话未被钩子异常杀死

        drainCts.Cancel();
        await drainTask;
    }

    [Fact]
    public async Task ReceiveIdleTimeout_TriggersReconnect()
    {
        var port = GetFreePort();
        var options = new StreamConnectionOptions { ReceiveIdleTimeoutMs = 200 };
        await using var server = CreateServer(port, options);
        server.Start(CancellationToken.None);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);
        await WaitForConnectedAsync(server);

        // 不发任何数据，静默超时应判定连接死亡并进入重连
        await WaitForStateAsync(server, s => s != ConnectionState.Connected, timeoutMs: 3000);
        Assert.NotEqual(ConnectionState.Connected, server.State);
    }

    [Fact]
    public async Task SendFailure_TriggersReconnect()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port);
        server.Start(CancellationToken.None);

        // 用 RST 关闭的客户端（LingerState(true,0)）：服务端 socket 发送时才感知死亡
        var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);
        await WaitForConnectedAsync(server);
        client.LingerState = new LingerOption(true, 0);
        client.Close();

        await Task.Delay(300);
        await server.SendAsync("burst-after-death");

        // 发送失败不应被静默吞掉：状态必须离开 Connected 进入重连
        await WaitForStateAsync(server, s => s != ConnectionState.Connected, timeoutMs: 5000);
        Assert.NotEqual(ConnectionState.Connected, server.State);
    }

    [Fact]
    public async Task IncompleteFrameOverLimit_EndsSessionOverTcp()
    {
        var port = GetFreePort();
        var options = new StreamConnectionOptions { MaxIncompleteFrameBufferBytes = 64 };
        await using var server = CreateServer(port, options);

        var errors = new List<FrameErrorEventArgs>();
        server.FrameError += (_, e) => { lock (errors) errors.Add(e); };

        server.Start(CancellationToken.None);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);
        await WaitForConnectedAsync(server);

        // 声明 1KB、只发 128B（> 上限 64）：判定流不可恢复，断线
        var header = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(header, 1024);
        await client.GetStream().WriteAsync(header.Concat(new byte[128]).ToArray());

        await WaitForStateAsync(server, s => s != ConnectionState.Connected, timeoutMs: 3000);
        Assert.NotEqual(ConnectionState.Connected, server.State);

        var overflow = Assert.Single(errors);
        Assert.Equal(FrameErrorKind.IncompleteFrameOverflow, overflow.Kind);
    }

    /// <summary>
    /// 可控的"坏 codec"：遇到指定负载（或首次解码）抛异常，其余正常。
    /// </summary>
    private sealed class FlakyDecodeCodec : ICodec<string>
    {
        private readonly bool _failOnce;
        private readonly string? _alwaysFailOn;
        private int _failed;

        public FlakyDecodeCodec(bool failOnce, string? alwaysFailOn = null)
        {
            _failOnce = failOnce;
            _alwaysFailOn = alwaysFailOn;
        }

        public string Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
        {
            var text = Encoding.UTF8.GetString(frame);
            var shouldFail = _alwaysFailOn == text || (_failOnce && Volatile.Read(ref _failed) == 0);
            if (shouldFail)
            {
                Interlocked.Increment(ref _failed);
                throw new InvalidOperationException($"payload 解析失败: {text}");
            }
            return text;
        }

        public void Encode(string message, IBufferWriter<byte> writer, CancellationToken ct = default)
            => writer.Write(Encoding.UTF8.GetBytes(message));
    }
}
