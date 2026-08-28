using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using StreamFrame;

namespace StreamFrame.Tests;

/// <summary>
/// 浸泡/混沌测试（默认不运行）：真实 TCP 回环上的长时间运行与随机故障注入，
/// 验证消息完整性（顺序、无重复、无跨会话错发）与无悬挂。
///
/// 启用方式（本地手动触发，CI 与默认 dotnet test 均不运行）：
/// <code>STREAMFRAME_SOAK_SECONDS=120 dotnet test -f net8.0 --filter FullyQualifiedName~Soak</code>
/// 未设置环境变量时本套件立即返回（视为通过），不影响提交前的全量验证。
/// </summary>
public class SoakTests
{
    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static double SoakSeconds()
    {
        var raw = Environment.GetEnvironmentVariable("STREAMFRAME_SOAK_SECONDS");
        return double.TryParse(raw, out var value) && value > 0 ? value : 0;
    }

    private static async Task ConnectWithRetryAsync(TcpClient client, int port)
    {
        // 混沌场景容忍慢收敛：服务端重连可能处于 2s 级的 accept 重试延迟（TIME_WAIT 竞争等），
        // 连接预算给足 60s，避免把"暂时拒绝"误判为测试失败
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await client.ConnectAsync(IPAddress.Loopback, port);
                return;
            }
            catch (SocketException) when (attempt < 120)
            {
                await Task.Delay(500);
            }
        }
    }

    private static async Task WaitForStateAsync(
        StreamConnection<string> connection,
        Func<ConnectionState, bool> predicate,
        int timeoutMs = 15_000)
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

    [Fact]
    public async Task Soak_LongRun_MessageIntegrity()
    {
        var seconds = SoakSeconds();
        if (seconds <= 0)
            return; // 未启用浸泡模式：跳过（保持默认套件快速）

        var port = GetFreePort();
        await using var server = new StreamConnection<string>(
            new LengthPrefixFramer(), StringCodec.Instance, IPAddress.Loopback, port, isActive: false);
        server.Start(CancellationToken.None);

        using var client = new TcpClient();
        await ConnectWithRetryAsync(client, port);
        await WaitForStateAsync(server, s => s == ConnectionState.Connected);

        // 服务端 → 客户端方向：序列消息流；对端读尽并核对顺序与完整性
        var received = new ConcurrentQueue<string>();
        var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(seconds + 30));
        var readerTask = Task.Run(async () =>
        {
            var stream = client.GetStream();
            var header = new byte[4];
            var body = new byte[4096];
            try
            {
                while (true)
                {
                    var read = 0;
                    while (read < 4)
                    {
                        var n = await stream.ReadAsync(header, read, 4 - read, readCts.Token);
                        if (n == 0)
                            return;
                        read += n;
                    }

                    var length = BinaryPrimitives.ReadInt32BigEndian(header);
                    var payload = new byte[length];
                    read = 0;
                    while (read < length)
                    {
                        var n = await stream.ReadAsync(payload, read, length - read, readCts.Token);
                        if (n == 0)
                            return;
                        read += n;
                    }

                    received.Enqueue(Encoding.UTF8.GetString(payload));
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        var deadline = TestClock.TickCount64 + (int)(seconds * 1000);
        var sent = 0;
        while (TestClock.TickCount64 < deadline)
        {
            await server.SendAsync($"msg-{sent}", CancellationToken.None);
            sent++;
            await Task.Delay(5); // ~200 msg/s
        }

        readCts.Cancel();
        await readerTask;

        // 完整性：顺序、无丢失、无重复
        Assert.True(received.Count >= sent - 5, $"发送 {sent}，对端仅收 {received.Count}（允许尾部少数在途）");
        var index = 0;
        foreach (var frame in received)
        {
            Assert.Equal($"msg-{index}", frame);
            index++;
        }
    }

    [Fact]
    public async Task Soak_Chaos_RandomFaults_NoHangAndIntegrity()
    {
        var seconds = SoakSeconds();
        if (seconds <= 0)
            return; // 未启用浸泡模式：跳过

        var seed = Environment.TickCount;
        var random = new Random(seed);
        var port = GetFreePort();
        await using var server = new StreamConnection<string>(
            new LengthPrefixFramer(), StringCodec.Instance, IPAddress.Loopback, port,
            isActive: false, new StreamConnectionOptions { SendQueueCapacity = 256 });
        server.Start(CancellationToken.None);

        var peerFrames = new ConcurrentQueue<string>();
        var pendingPlain = new ConcurrentQueue<string>();
        var allTasks = new List<Task>();
        TcpClient? client = null;
        NetworkStream? stream = null;
        var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(seconds + 30));
        var readerTask = Task.CompletedTask;

        var deadline = TestClock.TickCount64 + (int)(seconds * 1000);
        var boundSeq = 0;
        var plainSeq = 0;

        try
        {
            while (TestClock.TickCount64 < deadline)
            {
                var action = random.Next(100);
                if (client is null)
                {
                    // （重）连接对端并开始读
                    client = new TcpClient();
                    await ConnectWithRetryAsync(client, port);
                    await WaitForStateAsync(server, s => s == ConnectionState.Connected);
                    stream = client.GetStream();
                    var currentStream = stream;
                    readerTask = Task.Run(async () =>
                    {
                        var header = new byte[4];
                        try
                        {
                            while (true)
                            {
                                var read = 0;
                                while (read < 4)
                                {
                                    var n = await currentStream.ReadAsync(header, read, 4 - read, readCts.Token);
                                    if (n == 0)
                                        return;
                                    read += n;
                                }

                                var length = BinaryPrimitives.ReadInt32BigEndian(header);
                                var payload = new byte[length];
                                read = 0;
                                while (read < length)
                                {
                                    var n = await currentStream.ReadAsync(payload, read, length - read, readCts.Token);
                                    if (n == 0)
                                        return;
                                    read += n;
                                }

                                peerFrames.Enqueue(Encoding.UTF8.GetString(payload));
                            }
                        }
                        catch (Exception ex) when (ex is ObjectDisposedException or IOException or OperationCanceledException)
                        {
                        }
                    });
                }
                else if (action < 50)
                {
                    // 会话绑定突发
                    var id = server.CurrentSessionId;
                    if (id != 0)
                    {
                        for (var i = 0; i < random.Next(1, 5); i++)
                        {
                            var label = $"b{boundSeq++}";
                            allTasks.Add(server.SendInSessionAsync(id, label));
                            await Task.Delay(10);
                        }
                    }
                }
                else if (action < 70)
                {
                    // 普通发送（跨会话续发）：记录标签，终局核对全部送达
                    for (var i = 0; i < random.Next(1, 4); i++)
                    {
                        var label = $"p{plainSeq++}";
                        pendingPlain.Enqueue(label);
                        await server.SendAsync(label);
                    }
                }
                else if (action < 80 && stream is not null)
                {
                    // 半帧注入（未完成帧超时未启用：仅占住解码缓冲）
                    var junk = new byte[] { 0x00, 0x00, 0x0F, 0xA0 };
                    await stream.WriteAsync(junk);
                }
                else if (action < 95)
                {
                    // 杀对端（FIN 路径：正常释放）
                    if (client is not null)
                    {
                        client.Dispose();
                        client = null;
                        stream = null;
                        await WaitForStateAsync(server, s => s != ConnectionState.Connected);
                    }
                }
                else
                {
                    // 杀对端（RST 路径：Linger(true,0) 硬复位）
                    // 注：不混入用户显式 Reconnect()——用户重连与自动重连并发竞速会触发
                    // 双 StartAsync 接受循环的存量脆弱点（监听状态机楔死，v2.3.0 评审
                    // "附带发现"同源），属独立的状态过渡原子化设计任务，不在本套件覆盖
                    if (client is not null)
                    {
                        client.LingerState = new LingerOption(true, 0);
                        client.Dispose();
                        client = null;
                        stream = null;
                        await WaitForStateAsync(server, s => s != ConnectionState.Connected);
                    }
                }

                await Task.Delay(random.Next(20, 120));
            }
        }
        finally
        {
            readCts.Cancel();

            // 循环结束时对端可能仍存活：单客户端模式下监听已关闭（AcceptFirstClientOnly），
            // 必须先释放存量对端并等服务端离开 Connected，终局对端才能接入
            client?.Dispose();
            if (client is not null)
            {
                // 非断言式等待（尽力推进；真正的状态断言留给终局阶段）
                var exitDeadline = TestClock.TickCount64 + 15_000;
                while (TestClock.TickCount64 < exitDeadline && server.State == ConnectionState.Connected)
                    await Task.Delay(20);
            }
        }

        // 1) 所有会话绑定发送必须在时限内终结，失败类型合法；成功帧不得重复投递
        var boundSuccesses = new HashSet<string>();
        foreach (var task in allTasks)
        {
            var done = await Task.WhenAny(task, Task.Delay(20_000));
            Assert.True(ReferenceEquals(task, done), "混沌后存在悬挂的会话绑定发送。");
            if (task.Status != TaskStatus.RanToCompletion)
            {
                var ex = task.Exception?.InnerExceptions[0];
                Assert.True(ex is SessionExpiredException or SocketException or OperationCanceledException,
                    $"混沌中失败类型意外：{ex?.GetType().Name}");
            }
        }

        // 2) 重连一个干净对端，等普通消息全部续发送达（FIFO 完整性）
        using (var finalClient = new TcpClient())
        {
            await ConnectWithRetryAsync(finalClient, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
            var finalStream = finalClient.GetStream();
            var finalCts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var finalReader = Task.Run(async () =>
            {
                var header = new byte[4];
                try
                {
                    while (true)
                    {
                        var read = 0;
                        while (read < 4)
                        {
                            var n = await finalStream.ReadAsync(header, read, 4 - read, finalCts.Token);
                            if (n == 0)
                                return;
                            read += n;
                        }

                        var length = BinaryPrimitives.ReadInt32BigEndian(header);
                        var payload = new byte[length];
                        read = 0;
                        while (read < length)
                        {
                            var n = await finalStream.ReadAsync(payload, read, length - read, finalCts.Token);
                            if (n == 0)
                                return;
                            read += n;
                        }

                        peerFrames.Enqueue(Encoding.UTF8.GetString(payload));
                    }
                }
                catch (Exception ex) when (ex is ObjectDisposedException or IOException or OperationCanceledException)
                {
                }
            });

            var finalDeadline = TestClock.TickCount64 + 20_000;
            while (TestClock.TickCount64 < finalDeadline)
            {
                var plainSeen = 0;
                foreach (var f in peerFrames)
                    if (f.StartsWith("p", StringComparison.Ordinal))
                        plainSeen++;
                if (plainSeen >= pendingPlain.Count)
                    break;
                await Task.Delay(100);
            }

            finalCts.Cancel();
            await finalReader;
        }

        // 3) 终局核对：普通消息无丢失无重复（FIFO 序列完整）
        var plainFrames = peerFrames.Where(f => f.StartsWith("p", StringComparison.Ordinal)).ToArray();
        Assert.Equal(pendingPlain.Count, plainFrames.Length);
        Assert.Equal(pendingPlain.ToArray(), plainFrames);

        // 4) 绑定消息帧不重复（同一标签只允许出现一次——错发/重放探测器）
        var boundFrames = peerFrames.Where(f => f.StartsWith("b", StringComparison.Ordinal)).ToArray();
        Assert.Equal(boundFrames.Length, boundFrames.Distinct().Count());
    }
}
