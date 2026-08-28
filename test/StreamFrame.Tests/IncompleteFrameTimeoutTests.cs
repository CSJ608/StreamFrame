using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using StreamFrame;

namespace StreamFrame.Tests;

/// <summary>
/// 未完成帧超时（IncompleteFrameTimeoutMs）的连接级端到端测试（真实 TCP 回环）。
/// 语义：仅在缓冲里已有半帧字节时计时；缓冲为空的静默连接不触发；新字节到达即重置；
/// 超时判定会话失效并断线重连，FrameError 上报 IncompleteFrameTimeout + 受上限保护的快照。
/// </summary>
public class IncompleteFrameTimeoutTests
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
        var deadline = TestClock.TickCount64 + timeoutMs;
        while (TestClock.TickCount64 < deadline)
        {
            if (predicate(connection.State))
                return;
            await Task.Delay(20);
        }

        Assert.True(predicate(connection.State), $"等待连接状态超时（{timeoutMs}ms），当前 {connection.State}。");
    }

    private static async Task WaitForAsync(Func<bool> condition, int timeoutMs = 5000, string? what = null)
    {
        var deadline = TestClock.TickCount64 + timeoutMs;
        while (TestClock.TickCount64 < deadline)
        {
            if (condition())
                return;
            await Task.Delay(20);
        }

        Assert.True(condition(), $"等待条件超时（{timeoutMs}ms）：{what ?? "未描述"}。");
    }

    private static StreamConnection<string> CreateServer(int port, StreamConnectionOptions options)
        => new(
            new LengthPrefixFramer(),
            StringCodec.Instance,
            IPAddress.Loopback,
            port,
            isActive: false,
            options);

    /// <summary>
    /// 连接服务端（被动模式重连窗口内监听可能尚未就绪，短暂 SocketException 需重试）。
    /// </summary>
    private static async Task ConnectWithRetryAsync(TcpClient client, int port)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await client.ConnectAsync(IPAddress.Loopback, port);
                return;
            }
            catch (SocketException) when (attempt < 10)
            {
                await Task.Delay(200);
            }
        }
    }

    /// <summary>记录 FrameError 事件（拷贝字节，回调返回后可安全断言）。</summary>
    private static List<(FrameErrorKind Kind, byte[] Bytes)> AttachErrorRecorder(StreamConnection<string> server)
    {
        var errors = new List<(FrameErrorKind, byte[])>();
        server.FrameError += (_, e) =>
        {
            lock (errors)
                errors.Add((e.Kind, e.Bytes.ToArray()));
        };
        return errors;
    }

    /// <summary>后台排空消息通道（与 Resilience 测试同款模式），返回已收消息列表。</summary>
    private static List<string> StartMessageDrain(StreamConnection<string> server)
    {
        var received = new List<string>();
        var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        _ = Task.Run(async () =>
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
        return received;
    }

    [Fact]
    public void NegativeTimeout_ThrowsAtConstruction()
    {
        var port = GetFreePort();
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            CreateServer(port, new StreamConnectionOptions { IncompleteFrameTimeoutMs = -1 }));
    }

    [Fact]
    public async Task IdleConnection_NeverReceivedBytes_DoesNotTrigger()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port, new StreamConnectionOptions { IncompleteFrameTimeoutMs = 200 });
        var errors = AttachErrorRecorder(server);
        server.Start(CancellationToken.None);

        using (var client = new TcpClient())
        {
            await client.ConnectAsync(IPAddress.Loopback, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);

            // 连接后一个字节都不发：即使超过未完成帧超时时长，也不能触发（没有进行中的帧）
            await Task.Delay(600);
            Assert.Equal(ConnectionState.Connected, server.State);
        }

        Assert.Empty(errors);
    }

    [Fact]
    public async Task CompleteFrameThenLongIdle_DoesNotTrigger()
    {
        var port = GetFreePort();
        // 余量说明：超时取 500ms 而非 200ms——并行测试负载下，单次 Write 的小帧仍可能被
        // TCP 拆段，解码循环的续读唤醒被拖过 200ms 会让"帧内间隔"合法触发超时（曾偶发）。
        // 帧内续传的重置语义由 TrickleWithinDeadline（1000ms 超时 + 250ms 间隔）覆盖
        await using var server = CreateServer(port, new StreamConnectionOptions { IncompleteFrameTimeoutMs = 500 });
        var errors = AttachErrorRecorder(server);
        var received = StartMessageDrain(server);
        server.Start(CancellationToken.None);

        using (var client = new TcpClient())
        {
            await client.ConnectAsync(IPAddress.Loopback, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);

            // 一帧完整送达后长时间静默：帧已切尽、缓冲为空，不应计时
            await client.GetStream().WriteAsync(Frame("done"));
            await WaitForAsync(() => { lock (received) return received.Count == 1; });
            await Task.Delay(1200);

            Assert.Equal(ConnectionState.Connected, server.State);
        }

        Assert.Empty(errors);
        lock (received)
            Assert.Equal(new[] { "done" }, received);
    }

    [Fact]
    public async Task PartialLengthHeader_TimesOut_EndsSessionAndReconnects()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port, new StreamConnectionOptions { IncompleteFrameTimeoutMs = 200 });
        var errors = AttachErrorRecorder(server);
        var received = StartMessageDrain(server);
        server.Start(CancellationToken.None);

        using (var client1 = new TcpClient())
        {
            await client1.ConnectAsync(IPAddress.Loopback, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);

            // 只发长度头的前 2 字节（完整头是 4 字节大端），半帧永远等不齐
            await client1.GetStream().WriteAsync(new byte[] { 0x00, 0x00 });

            await WaitForAsync(() => { lock (errors) return errors.Count == 1; }, timeoutMs: 3000);
            lock (errors)
            {
                var (kind, bytes) = Assert.Single(errors);
                Assert.Equal(FrameErrorKind.IncompleteFrameTimeout, kind);
                Assert.Equal(new byte[] { 0x00, 0x00 }, bytes);
            }

            // 超时判定会话失效：离开 Connected 进入重连
            await WaitForStateAsync(server, s => s != ConnectionState.Connected);
        }

        // 重连后新会话不受迟到故障影响：新客户端接入驱动回到 Connected，正常收发
        using (var client2 = new TcpClient())
        {
            await ConnectWithRetryAsync(client2, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected, timeoutMs: 8000);
            await Task.Delay(300);
            await client2.GetStream().WriteAsync(Frame("after-timeout"));
        }

        await WaitForAsync(() => { lock (received) return received.Count == 1; }, timeoutMs: 8000);
        lock (received)
            Assert.Equal(new[] { "after-timeout" }, received);
    }

    [Fact]
    public async Task CompleteHeaderPartialBody_TimesOut()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port, new StreamConnectionOptions { IncompleteFrameTimeoutMs = 200 });
        var errors = AttachErrorRecorder(server);
        server.Start(CancellationToken.None);

        using (var client = new TcpClient())
        {
            await client.ConnectAsync(IPAddress.Loopback, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);

            // 长度头完整（声明 10 字节正文）但正文只到 3 字节
            var partial = new byte[] { 0x00, 0x00, 0x00, 0x0A, 0x41, 0x42, 0x43 };
            await client.GetStream().WriteAsync(partial);

            await WaitForAsync(() => { lock (errors) return errors.Count == 1; }, timeoutMs: 3000);
            lock (errors)
            {
                var (kind, bytes) = Assert.Single(errors);
                Assert.Equal(FrameErrorKind.IncompleteFrameTimeout, kind);
                Assert.Equal(partial, bytes);
            }
        }
    }

    [Fact]
    public async Task TrickleWithinDeadline_TimerResets_FrameDelivered()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port, new StreamConnectionOptions { IncompleteFrameTimeoutMs = 1000 });
        var errors = AttachErrorRecorder(server);
        var received = StartMessageDrain(server);
        server.Start(CancellationToken.None);

        using (var client = new TcpClient())
        {
            await client.ConnectAsync(IPAddress.Loopback, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
            var stream = client.GetStream();

            // 每段间隔 250ms（< 1000ms 超时）：每次续传都应重置计时，最终整帧交付
            var header = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(header, 8);
            await stream.WriteAsync(header);
            await Task.Delay(250);
            await stream.WriteAsync(Encoding.UTF8.GetBytes("123"));
            await Task.Delay(250);
            await stream.WriteAsync(Encoding.UTF8.GetBytes("456"));
            await Task.Delay(250);
            await stream.WriteAsync(Encoding.UTF8.GetBytes("78"));

            await WaitForAsync(() => { lock (received) return received.Count == 1; });
            await Task.Delay(400); // 交付后再静默一会儿，确认没有迟到的超时

            Assert.Equal(ConnectionState.Connected, server.State);
        }

        Assert.Empty(errors);
        lock (received)
            Assert.Equal(new[] { "12345678" }, received);
    }

    [Fact]
    public async Task RepeatedTimeoutCycles_RemainIdempotent()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port, new StreamConnectionOptions { IncompleteFrameTimeoutMs = 200 });
        var errors = AttachErrorRecorder(server);
        var received = StartMessageDrain(server);
        server.Start(CancellationToken.None);

        // 连续两轮"半帧超时 → 重连"，验证 epoch 防护下迟到故障不会误杀后续会话。
        // 客户端必须等超时事件落地后再释放：提前 dispose 会让 FIN 抢先触发重连（走对端关闭路径）；
        // 被动模式重连由下一个客户端接入驱动完成。
        for (var cycle = 0; cycle < 2; cycle++)
        {
            using var client = new TcpClient();
            await ConnectWithRetryAsync(client, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected, timeoutMs: 8000);
            await client.GetStream().WriteAsync(new byte[] { 0x00, 0x0F });

            var expected = cycle + 1;
            await WaitForAsync(
                () => { lock (errors) return errors.Count == expected; },
                timeoutMs: 3000, what: $"第 {cycle + 1} 轮未完成帧超时事件");
            await WaitForStateAsync(server, s => s != ConnectionState.Connected);
        }

        lock (errors)
            Assert.Equal(2, errors.Count);

        // 第三轮：正常客户端照常工作
        using (var client = new TcpClient())
        {
            await ConnectWithRetryAsync(client, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected, timeoutMs: 8000);
            await Task.Delay(300);
            await client.GetStream().WriteAsync(Frame("third-session"));
        }
        await WaitForAsync(() => { lock (received) return received.Count == 1; }, timeoutMs: 8000);
        lock (received)
            Assert.Equal(new[] { "third-session" }, received);
    }

    [Fact]
    public async Task SnapshotIsCappedAtLimit()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port, new StreamConnectionOptions { IncompleteFrameTimeoutMs = 200 });
        var errors = AttachErrorRecorder(server);
        server.Start(CancellationToken.None);

        using (var client = new TcpClient())
        {
            await client.ConnectAsync(IPAddress.Loopback, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);

            // 声明 20000 字节正文但只发 9000 字节：快照必须被截到上限（8192），而不是全量拷贝
            var partial = new byte[4 + 9000];
            BinaryPrimitives.WriteInt32BigEndian(partial, 20000);
            await client.GetStream().WriteAsync(partial);

            await WaitForAsync(() => { lock (errors) return errors.Count == 1; }, timeoutMs: 3000);
            lock (errors)
            {
                var (kind, bytes) = Assert.Single(errors);
                Assert.Equal(FrameErrorKind.IncompleteFrameTimeout, kind);
                Assert.Equal(8192, bytes.Length);
            }
        }
    }

    [Fact]
    public async Task DisposeDuringTimeoutReconnect_CompletesCleanly()
    {
        var port = GetFreePort();
        var server = CreateServer(port, new StreamConnectionOptions { IncompleteFrameTimeoutMs = 200 });
        _ = AttachErrorRecorder(server);
        server.Start(CancellationToken.None);

        using (var client = new TcpClient())
        {
            await client.ConnectAsync(IPAddress.Loopback, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
            await client.GetStream().WriteAsync(new byte[] { 0x00, 0x2A });

            // 超时触发、重连流程进行中即 Dispose：必须干净停机、不抛异常
            await WaitForStateAsync(server, s => s != ConnectionState.Connected);
        }

        await server.DisposeAsync();
        Assert.True(server.IsDisposed);
    }
}
