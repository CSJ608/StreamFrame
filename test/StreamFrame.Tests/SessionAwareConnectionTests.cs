using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using StreamFrame;

namespace StreamFrame.Tests;

/// <summary>
/// 会话感知收发（ISessionAwareStreamConnection）的连接级端到端测试（真实 TCP 回环）：
/// 会话编号的线性化与生命周期、绑定会话发送的完成/失败语义（整帧写完才完成、不跨会话重放、
/// 拆除立即失败、调用方取消的提交点）、接收视图带会话编号、与普通 SendAsync 的行为对照。
///
/// 时序技巧：Windows 回环对背压几乎免疫（对端不读、RCVBUF 调小也能全量灌入），
/// "写阻塞"用 <see cref="DelayedCodec"/> 在 worker 的编码阶段制造确定性停顿。
/// </summary>
public class SessionAwareConnectionTests
{
    /// <summary>编码停顿的测试 codec：让发送 worker 在写出前可预测地停住。</summary>
    private sealed class DelayedCodec : ICodec<string>
    {
        private readonly int _encodeDelayMs;

        public DelayedCodec(int encodeDelayMs) => _encodeDelayMs = encodeDelayMs;

        public string Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
            => StringCodec.Instance.Decode(frame, ct);

        public void Encode(string message, IBufferWriter<byte> writer, CancellationToken ct = default)
        {
            Thread.Sleep(_encodeDelayMs);
            StringCodec.Instance.Encode(message, writer, ct);
        }
    }

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

    private static StreamConnection<string> CreateServer(
        int port, StreamConnectionOptions? options = null, ICodec<string>? codec = null)
        => new(
            new LengthPrefixFramer(),
            codec ?? StringCodec.Instance,
            IPAddress.Loopback,
            port,
            isActive: false,
            options ?? new StreamConnectionOptions());

    /// <summary>从流中读一帧并返回其负载（长度前缀 + 正文）。</summary>
    private static async Task<string> ReadFrameAsync(NetworkStream stream, int timeoutMs = 10_000)
    {
        var header = await ReadExactlyAsync(stream, 4, timeoutMs);
        var length = BinaryPrimitives.ReadInt32BigEndian(header);
        Assert.True(length is > 0 and < 1024 * 1024, $"帧长度异常：{length}。");
        var body = await ReadExactlyAsync(stream, length, timeoutMs);
        return Encoding.UTF8.GetString(body);
    }

    /// <summary>精确读 n 字节（超时即失败）。net48 无 Task.WaitAsync，统一用 WhenAny 模式。</summary>
    private static async Task<byte[]> ReadExactlyAsync(NetworkStream stream, int n, int timeoutMs = 10_000)
    {
        var buffer = new byte[n];
        var read = 0;
        while (read < n)
        {
            var readTask = stream.ReadAsync(buffer, read, n - read);
            var winner = await Task.WhenAny(readTask, Task.Delay(timeoutMs));
            Assert.True(ReferenceEquals(readTask, winner), $"读字节超时（{timeoutMs}ms）。");
            var count = await readTask;
            Assert.True(count > 0, "对端提前关闭。");
            read += count;
        }

        return buffer;
    }

    /// <summary>断言窗口期内没有任何字节到达。</summary>
    private static async Task AssertNoBytesAsync(NetworkStream stream, int windowMs = 400)
    {
        var buffer = new byte[256];
        var readTask = stream.ReadAsync(buffer, 0, buffer.Length);
        var winner = await Task.WhenAny(readTask, Task.Delay(windowMs));
        if (ReferenceEquals(winner, readTask))
            Assert.Fail($"不应有字节到达，实际收到 {await readTask} 字节。");
    }

    /// <summary>等待任务在时限内成功完成，超时直接失败（net48 无 Task.WaitAsync）。</summary>
    private static async Task AwaitDoneAsync(Task task, int timeoutMs = 8000, string? what = null)
    {
        var winner = await Task.WhenAny(task, Task.Delay(timeoutMs));
        Assert.True(ReferenceEquals(task, winner), $"任务超时（{timeoutMs}ms）：{what ?? "未描述"}。");
        await task;
    }

    /// <summary>等待任务在时限内以异常结束并返回该异常，超时/成功完成直接失败。</summary>
    private static async Task<Exception> AwaitFailureAsync(Task task, int timeoutMs = 8000, string? what = null)
    {
        var winner = await Task.WhenAny(task, Task.Delay(timeoutMs));
        Assert.True(ReferenceEquals(task, winner), $"任务超时（{timeoutMs}ms）：{what ?? "未描述"}。");
        return await Assert.ThrowsAnyAsync<Exception>(() => task);
    }

    [Fact]
    public void Implements_SessionAware_Interface()
        => Assert.IsAssignableFrom<ISessionAwareStreamConnection<string>>(
            CreateServer(GetFreePort()));

    [Fact]
    public async Task SessionId_ValidAtLinearizationPoints_AndNeverReused()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port);
        Assert.Equal(0, server.CurrentSessionId);

        var inCallback = new List<(ConnectionState State, long Id)>();
        server.ConnectionChanged += (_, s) =>
        {
            lock (inCallback)
                inCallback.Add((s, server.CurrentSessionId));
        };
        server.Start(CancellationToken.None);

        long id1;
        using (var client1 = new TcpClient())
        {
            await ConnectWithRetryAsync(client1, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);

            id1 = server.CurrentSessionId;
            Assert.NotEqual(0, id1);
            lock (inCallback)
            {
                // Connected 回调可见时编号必须已分配（线性化点）
                var connected = inCallback.First(x => x.State == ConnectionState.Connected);
                Assert.Equal(id1, connected.Id);
            }

            client1.Dispose(); // 触发重连
        }

        await WaitForStateAsync(server, s => s != ConnectionState.Connected);
        Assert.Equal(0, server.CurrentSessionId); // 离开 Connected 可见时已归零

        long id2;
        using (var client2 = new TcpClient())
        {
            await ConnectWithRetryAsync(client2, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected, timeoutMs: 8000);
            id2 = server.CurrentSessionId;
            Assert.True(id2 > id1, $"会话编号必须单调递增：{id1} → {id2}");
        }

        await server.DisposeAsync();
        Assert.Equal(0, server.CurrentSessionId); // 停机后归零
    }

    [Fact]
    public async Task SendInSession_CompletesOnlyAfterFullFrameWritten()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port, codec: new DelayedCodec(500));
        server.Start(CancellationToken.None);

        using var client = new TcpClient();
        await ConnectWithRetryAsync(client, port);
        await WaitForStateAsync(server, s => s == ConnectionState.Connected);
        var stream = client.GetStream();

        var sendTask = server.SendInSessionAsync(server.CurrentSessionId, "delayed");
        await Task.Delay(200); // worker 仍停在编码阶段：字节未写，任务不可能完成
        Assert.False(sendTask.IsCompleted, "整帧尚未写出，任务不应完成（不能入队即完成）。");

        var message = await ReadFrameAsync(stream); // 对端收完整帧
        await AwaitDoneAsync(sendTask, 5000, "整帧写完");

        Assert.Equal("delayed", message);
        Assert.Equal(TaskStatus.RanToCompletion, sendTask.Status);
    }

    [Fact]
    public async Task SessionTeardown_PendingBoundSends_FailPromptly()
    {
        var port = GetFreePort();
        await using var server = CreateServer(
            port, new StreamConnectionOptions { SendQueueCapacity = 1 }, new DelayedCodec(400));
        server.Start(CancellationToken.None);

        using var client = new TcpClient();
        await ConnectWithRetryAsync(client, port);
        await WaitForStateAsync(server, s => s == ConnectionState.Connected);
        var id = server.CurrentSessionId;

        var writingTask = server.SendInSessionAsync(id, "writing"); // worker 认领后停在编码阶段
        var tasks = new List<Task>
        {
            server.SendInSessionAsync(id, "queued"),   // 占满队列（容量 1）
            server.SendInSessionAsync(id, "waiting"),  // 队列满，WriteAsync 等待空位
        };
        await Task.Delay(100); // 让第 1 条进入编码

        server.Reconnect(); // 会话拆除：无需等重连完成，挂起发送必须立即失败

        // 排队中的条目只经拆除清扫终结：失败类型必须精确为会话失效
        foreach (var task in tasks)
        {
            var ex = await AwaitFailureAsync(task, 3000, "挂起的会话绑定发送及时失败");
            Assert.IsType<SessionExpiredException>(ex);
        }

        // 正在写出的条目是拆除竞争的合法双结局：整帧恰好写完 → 成功；否则会话失效失败。
        // 两者都符合契约，但必须在时限内终结（不悬挂）
        var writingDone = await Task.WhenAny(writingTask, Task.Delay(3000));
        Assert.True(ReferenceEquals(writingDone, writingTask), "写出中条目应在时限内终结。");
        if (writingTask.Status != TaskStatus.RanToCompletion)
        {
            var ex = await Assert.ThrowsAnyAsync<Exception>(() => writingTask);
            Assert.True(ex is SessionExpiredException or SocketException or OperationCanceledException,
                $"失败类型应为会话失效/socket 故障，实际 {ex.GetType().Name}。");
        }

        // 队满等待中的条目被 fault 后，其迟到的入队仍会成功（新 worker 腾出空位）——
        // worker 认领时的编号校验（跨会话残留防线）必须跳过它、绝不发送。
        // 连入新对端驱动排空，并断言三条旧消息一个字节都没有发到新会话
        using (var client2 = new TcpClient())
        {
            await ConnectWithRetryAsync(client2, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected, timeoutMs: 8000);
            await Task.Delay(500); // 留出排空窗口
            await AssertNoBytesAsync(client2.GetStream());
        }
    }

    [Fact]
    public async Task SessionSwitch_BoundSendFails_NoReplay_PlainSendStillReplays()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port, codec: new DelayedCodec(400));
        server.Start(CancellationToken.None);

        using (var client1 = new TcpClient())
        {
            await ConnectWithRetryAsync(client1, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
            var id1 = server.CurrentSessionId;

            var writingTask = server.SendInSessionAsync(id1, "writing");
            var bound = new List<Task> { server.SendInSessionAsync(id1, "bound-drops") };
            _ = server.SendAsync("plain-replays"); // 普通发送：跨会话续发（既有语义）

            await Task.Delay(100);
            server.Reconnect(); // 触发会话切换

            foreach (var task in bound)
            {
                var ex = await AwaitFailureAsync(task, 3000, "会话切换后旧绑定发送失败");
                Assert.IsType<SessionExpiredException>(ex); // 排队条目只经拆除清扫终结
            }

            // 正在写出的条目：拆除竞争的合法双结局（整帧恰好写完→成功；否则失效失败），不悬挂即可
            var writingDone = await Task.WhenAny(writingTask, Task.Delay(3000));
            Assert.True(ReferenceEquals(writingDone, writingTask), "写出中条目应在时限内终结。");
            if (writingTask.Status != TaskStatus.RanToCompletion)
            {
                var ex = await Assert.ThrowsAnyAsync<Exception>(() => writingTask);
                Assert.True(ex is SessionExpiredException or SocketException or OperationCanceledException,
                    $"失败类型意外：{ex.GetType().Name}。");
            }
        }

        // 新会话：绑定旧编号的发送直接失败；普通消息由新 worker 续发；绑定消息绝不重放
        using (var client2 = new TcpClient())
        {
            await ConnectWithRetryAsync(client2, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected, timeoutMs: 8000);
            var id2 = server.CurrentSessionId;
            Assert.NotEqual(0, id2);

            await Assert.ThrowsAsync<SessionExpiredException>(
                () => server.SendInSessionAsync(id2 + 100, "stale"));

            await server.SendInSessionAsync(id2, "new-session-ok");

            var stream = client2.GetStream();
            // 队列顺序：bound-drops（跳过）→ plain-replays → new-session-ok
            Assert.Equal("plain-replays", await ReadFrameAsync(stream));
            Assert.Equal("new-session-ok", await ReadFrameAsync(stream));
            await AssertNoBytesAsync(stream); // 旧会话的绑定消息没有转移到新会话
        }
    }

    [Fact]
    public async Task Dispose_PendingBoundSends_AllFail_Promptly()
    {
        var port = GetFreePort();
        var server = CreateServer(port, codec: new DelayedCodec(400));
        server.Start(CancellationToken.None);

        using (var client = new TcpClient())
        {
            await ConnectWithRetryAsync(client, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
            var id = server.CurrentSessionId;

            var writingTask = server.SendInSessionAsync(id, "writing");
            var tasks = new List<Task>();
            for (var i = 1; i < 4; i++)
                tasks.Add(server.SendInSessionAsync(id, $"pending-{i}"));
            await Task.Delay(100);

            await server.DisposeAsync(); // 停机：挂起的会话绑定发送必须全部异常收尾

            foreach (var task in tasks)
            {
                var ex = await AwaitFailureAsync(task, 3000, "Dispose 后挂起发送异常收尾");
                Assert.IsType<SessionExpiredException>(ex); // 排队条目只经停机清扫终结（清扫先于通道完成）
            }

            // 正在写出的条目：拆除竞争的合法双结局（写完→成功；否则失效失败），不悬挂即可
            var writingDone = await Task.WhenAny(writingTask, Task.Delay(3000));
            Assert.True(ReferenceEquals(writingDone, writingTask), "写出中条目应在时限内终结。");
            if (writingTask.Status != TaskStatus.RanToCompletion)
            {
                var ex = await Assert.ThrowsAnyAsync<Exception>(() => writingTask);
                Assert.True(ex is SessionExpiredException or SocketException or OperationCanceledException,
                    $"失败类型应为会话失效/socket 故障，实际 {ex.GetType().Name}。");
            }
        }

        Assert.True(server.IsDisposed);
    }

    [Fact]
    public async Task CallerCancel_BeforeCommit_CancelsAndNotSent()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port, codec: new DelayedCodec(500));
        server.Start(CancellationToken.None);

        using var client = new TcpClient();
        await ConnectWithRetryAsync(client, port);
        await WaitForStateAsync(server, s => s == ConnectionState.Connected);
        var stream = client.GetStream();

        var firstTask = server.SendInSessionAsync(server.CurrentSessionId, "first");
        await Task.Delay(100); // worker 认领第 1 条并停在编码

        using var cts = new CancellationTokenSource();
        var cancelTask = server.SendInSessionAsync(server.CurrentSessionId, "cancel-me", cts.Token);
        cts.Cancel(); // 提交点之前：任务及时以取消结束，不等待第 1 条写完

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => AwaitDoneAsync(cancelTask, 2000, "提交点前取消"));

        // 对端读走第 1 帧后，第 2 条（已取消）绝不出现
        Assert.Equal("first", await ReadFrameAsync(stream));
        await AwaitDoneAsync(firstTask, 5000, "第 1 条整帧写完");
        await AssertNoBytesAsync(stream);
    }

    [Fact]
    public async Task StaleGhostReconnect_DoesNotPolluteLiveSession()
    {
        // 评审 P1-1 回归：垂死旧会话的迟到故障（epoch 已过期）必须被整体丢弃——
        // 不得把活会话的 State 污染成 Retry、不得把 CurrentSessionId 归零。
        // 这种延迟到达只能经反射调度模拟（公共 API 无法构造）
        var port = GetFreePort();
        await using var server = CreateServer(port);
        server.Start(CancellationToken.None);

        long id1;
        using (var client1 = new TcpClient())
        {
            await ConnectWithRetryAsync(client1, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
            id1 = server.CurrentSessionId;
        }

        await WaitForStateAsync(server, s => s != ConnectionState.Connected);
        using (var client2 = new TcpClient())
        {
            await ConnectWithRetryAsync(client2, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected, timeoutMs: 8000);
            var id2 = server.CurrentSessionId;
            Assert.True(id2 > id1);

            var schedule = typeof(StreamConnection<string>).GetMethod(
                "ScheduleReconnect", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.NotNull(schedule);
            schedule!.Invoke(server, new object[] { 1 }); // 旧会话（epoch=1）的幽灵故障，早已过期

            await Task.Delay(300);
            Assert.Equal(ConnectionState.Connected, server.State); // 不被谎报为 Retry
            Assert.Equal(id2, server.CurrentSessionId);            // 活会话编号不被归零

            var task = server.SendInSessionAsync(id2, "alive");
            Assert.Equal("alive", await ReadFrameAsync(client2.GetStream()));
            await AwaitDoneAsync(task, 3000, "活会话发送");
        }
    }

    [Fact]
    public async Task GhostFault_DuringConnectedPublication_DoesNotKillNewbornSession()
    {
        // 评审"附带发现"的行为锁定：Connected 发布与 StartSession 之间到达的旧纪元幽灵故障
        // 必须被丢弃。入口的过期检查 + 纪元在发布点提前递增共同保证这一点——
        // 在发布线程上（StartSession 尚未执行）注入幽灵是最紧的时序构造
        var port = GetFreePort();
        await using var server = CreateServer(port);

        var injectGhost = false;
        server.ConnectionChanged += (_, s) =>
        {
            if (s != ConnectionState.Connected || !injectGhost)
                return;
            injectGhost = false;
            typeof(StreamConnection<string>).GetMethod(
                "ScheduleReconnect", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(server, new object[] { 1 }); // 旧会话（epoch=1）的幽灵
        };
        server.Start(CancellationToken.None);

        using (var client1 = new TcpClient())
        {
            await ConnectWithRetryAsync(client1, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
        }

        await WaitForStateAsync(server, s => s != ConnectionState.Connected);
        injectGhost = true; // 第二次 Connected 发布的回调里注入幽灵
        using (var client2 = new TcpClient())
        {
            await ConnectWithRetryAsync(client2, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected, timeoutMs: 8000);
            var id2 = server.CurrentSessionId;
            Assert.NotEqual(0, id2);

            await Task.Delay(400); // 幽灵若未被丢弃，此刻已把会话 2 卷进 Retry
            Assert.Equal(ConnectionState.Connected, server.State);
            Assert.Equal(id2, server.CurrentSessionId);

            var task = server.SendInSessionAsync(id2, "alive");
            Assert.Equal("alive", await ReadFrameAsync(client2.GetStream()));
            await AwaitDoneAsync(task, 3000, "新生会话发送");
        }
    }

    [Fact]
    public async Task CallerCancel_AfterCommit_HasNoEffect()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port);
        server.Start(CancellationToken.None);

        using var client = new TcpClient();
        await ConnectWithRetryAsync(client, port);
        await WaitForStateAsync(server, s => s == ConnectionState.Connected);

        using var cts = new CancellationTokenSource();
        var task = server.SendInSessionAsync(server.CurrentSessionId, "committed", cts.Token);
        await AwaitDoneAsync(task, 5000, "提交后发送完成"); // 已成功完成（已过提交点）

        cts.Cancel(); // 迟到的取消不得翻转结果
        await Task.Delay(200);
        Assert.Equal(TaskStatus.RanToCompletion, task.Status);
    }

    [Fact]
    public async Task GetSessionMessages_CarrySessionId_AcrossReconnects()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port);
        var received = new List<SessionMessage<string>>();
        var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var message in server.GetSessionMessages(drainCts.Token))
                    lock (received)
                        received.Add(message);
            }
            catch (OperationCanceledException)
            {
            }
        });
        server.Start(CancellationToken.None);

        long id1;
        using (var client1 = new TcpClient())
        {
            await ConnectWithRetryAsync(client1, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
            id1 = server.CurrentSessionId;

            var stream = client1.GetStream();
            await stream.WriteAsync(Frame("s1a"));
            await stream.WriteAsync(Frame("s1b"));
            await WaitForAsync(() => { lock (received) return received.Count == 2; }, what: "会话 1 消息送达");
        }

        using (var client2 = new TcpClient())
        {
            await ConnectWithRetryAsync(client2, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected, timeoutMs: 8000);
            var id2 = server.CurrentSessionId;
            Assert.True(id2 > id1);

            await client2.GetStream().WriteAsync(Frame("s2a"));
            await WaitForAsync(() => { lock (received) return received.Count == 3; }, what: "会话 2 消息送达");
        }

        lock (received)
        {
            Assert.Equal(new[] { ("s1a", id1), ("s1b", id1), ("s2a", received[2].SessionId) },
                received.Select(m => (m.Message, m.SessionId)).ToArray());
            Assert.True(received[2].SessionId > id1, "新会话消息必须带新编号。");
        }
    }

    [Fact]
    public async Task ConsumerViews_Compete_NotBroadcast()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port);
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var fromPlain = new List<string>();
        var fromSession = new List<SessionMessage<string>>();

        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var m in server.GetMessages(cts.Token))
                    lock (fromPlain)
                        fromPlain.Add(m);
            }
            catch (OperationCanceledException)
            {
            }
        });
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var m in server.GetSessionMessages(cts.Token))
                    lock (fromSession)
                        fromSession.Add(m);
            }
            catch (OperationCanceledException)
            {
            }
        });
        server.Start(CancellationToken.None);

        using (var client = new TcpClient())
        {
            await ConnectWithRetryAsync(client, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
            await client.GetStream().WriteAsync(Frame("only-one"));
        }

        await WaitForAsync(() =>
        {
            lock (fromPlain)
            lock (fromSession)
                return fromPlain.Count + fromSession.Count == 1;
        }, what: "消息恰好被两个视图之一消费");

        lock (fromPlain)
        lock (fromSession)
            Assert.Equal(1, fromPlain.Count + fromSession.Count);
    }

    [Fact]
    public async Task GetMessages_And_GetSessionMessages_SameSequence_AcrossRuns()
    {
        // 单消费者视图的等价性在独立运行中比较（同一连接上同时枚举会竞争消费）
        var plain = await CollectViaGetMessagesAsync();
        var sessionView = await CollectViaGetSessionMessagesAsync();

        Assert.Equal(plain, sessionView.Messages);
        Assert.All(sessionView.Ids, id => Assert.True(id > 0));
        Assert.Single(sessionView.Ids.Distinct()); // 单次运行内全部消息来自同一会话
    }

    private async Task<List<string>> CollectViaGetMessagesAsync()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port);
        var received = new List<string>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var m in server.GetMessages(cts.Token))
                    lock (received)
                        received.Add(m);
            }
            catch (OperationCanceledException)
            {
            }
        });
        await SendThreeFramesAsync(server, port, () => { lock (received) return received.Count == 3; });
        return received;
    }

    private async Task<(List<string> Messages, List<long> Ids)> CollectViaGetSessionMessagesAsync()
    {
        var port = GetFreePort();
        await using var server = CreateServer(port);
        var received = new List<SessionMessage<string>>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var m in server.GetSessionMessages(cts.Token))
                    lock (received)
                        received.Add(m);
            }
            catch (OperationCanceledException)
            {
            }
        });
        await SendThreeFramesAsync(server, port, () => { lock (received) return received.Count == 3; });

        lock (received)
            return (received.Select(m => m.Message).ToList(), received.Select(m => m.SessionId).ToList());
    }

    private async Task SendThreeFramesAsync(
        StreamConnection<string> server, int port, Func<bool> receivedAll)
    {
        server.Start(CancellationToken.None);
        using var client = new TcpClient();
        await ConnectWithRetryAsync(client, port);
        await WaitForStateAsync(server, s => s == ConnectionState.Connected);
        var stream = client.GetStream();
        await stream.WriteAsync(Frame("m1"));
        await stream.WriteAsync(Frame("m2"));
        await stream.WriteAsync(Frame("m3"));
        await WaitForAsync(receivedAll, what: "三个帧送达");
    }

    [Fact]
    public async Task TeardownVersusRegister_Stress_AllTasksEnd_NoCrossSessionSend()
    {
        const int Rounds = 15;
        const int SendsPerRound = 4;

        var port = GetFreePort();
        await using var server = CreateServer(port, codec: new DelayedCodec(30));
        server.Start(CancellationToken.None);

        for (var round = 0; round < Rounds; round++)
        {
            var peerBytes = new MemoryStream();
            var successes = 0;
            using var client = new TcpClient();
            await ConnectWithRetryAsync(client, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
            var stream = client.GetStream();

            // 后台持续读（模拟对端消费）
            var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var readTask = Task.Run(async () =>
            {
                var buffer = new byte[16384];
                try
                {
                    while (true)
                    {
#if NET48
                        var n = await stream.ReadAsync(buffer, 0, buffer.Length, readCts.Token);
#else
                        var n = await stream.ReadAsync(buffer, readCts.Token);
#endif
                        if (n == 0)
                            break;
                        lock (peerBytes)
                            peerBytes.Write(buffer, 0, n);
                    }
                }
                catch (OperationCanceledException)
                {
                }
            });

            var sends = Enumerable.Range(0, SendsPerRound)
                .Select(i => server.SendInSessionAsync(server.CurrentSessionId, $"r{round}-{i}"))
                .ToArray();

            // 与拆除并发：不等发送结果，直接切换会话（按轮次交替两种拆除路径）
            if (round % 2 == 0)
                server.Reconnect();
            else
                client.Dispose(); // 对端关闭路径

            foreach (var send in sends)
            {
                var done = await Task.WhenAny(send, Task.Delay(8000));
                Assert.True(ReferenceEquals(send, done), "发送任务在时限内未结束（悬挂）。");
                if (send.Status == TaskStatus.RanToCompletion)
                    successes++;
                else
                    Assert.True(send.Exception?.InnerExceptions[0] is SessionExpiredException
                        or SocketException or OperationCanceledException,
                        $"失败类型意外：{send.Exception?.InnerExceptions[0].GetType().Name}");
            }

            // 停读前留出排空窗口：成功的帧可能还在对端接收缓冲里未被读出
            await Task.Delay(300);
            readCts.Cancel();
            _ = await Task.WhenAny(readTask, Task.Delay(2000)); // 尽力收尾，超时不再等待

            // 成功核对（仅 Reconnect 轮次：对端全程在读，成功帧必然可达且完整）：
            // 客户端主动 Dispose 轮次不对账——本地关闭时未读数据随 RST 丢弃属 TCP 语义，
            // "写入本机 socket"之外的投递结果不在本 API 的公共保证内。
            // 按帧内容对账：每个完整帧的负载必须带本轮标签——检出任何跨会话/跨轮次的错发
            if (round % 2 == 0)
            {
                lock (peerBytes)
                {
                    var bytes = peerBytes.ToArray();
                    var offset = 0;
                    var frames = 0;
                    while (offset + 4 <= bytes.Length)
                    {
                        var length = BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(offset, 4));
                        Assert.True(length is > 0 and < 1024, $"帧长度异常：{length}。");
                        if (offset + 4 + length > bytes.Length)
                            break; // 拆除时刻正在写的半帧（其任务必然已失败）
                        var payload = Encoding.UTF8.GetString(bytes, offset + 4, length);
                        Assert.StartsWith($"r{round}-", payload, StringComparison.Ordinal); // 错发探测器（P1-2 场景）
                        offset += 4 + length;
                        frames++;
                    }

                    Assert.True(successes <= frames,
                        $"成功 {successes} 次发送但对端仅收到 {frames} 个完整帧——存在未完整写出的\"成功\"。");
                }
            }

            // 下一轮前的清理：等待离开 Connected（对端关闭路径由 FIN 触发）
            if (round % 2 == 1)
                await WaitForStateAsync(server, s => s != ConnectionState.Connected, timeoutMs: 8000);
        }

        // 压测后连接仍健康：新会话正常收发
        using (var client = new TcpClient())
        {
            await ConnectWithRetryAsync(client, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected, timeoutMs: 8000);
            var task = server.SendInSessionAsync(server.CurrentSessionId, "healthy");
            Assert.Equal("healthy", await ReadFrameAsync(client.GetStream()));
            await AwaitDoneAsync(task, 5000, "压测后发送");
        }
    }
}
