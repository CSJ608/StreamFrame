using System.Buffers;
using System.Net;
using System.Text;
using System.Xml.Linq;
using StreamFrame;
using StreamFrame.Protocols.Xml;

// 本 demo 演示 StreamFrame 的四种典型用法：
//   1) XML 消息 + 4 字节长度前缀帧（对应 SamSung 风格）
//   2) 纯文本消息 + STX/ETX 包裹帧
//   3) 断线自动重连
//   4) 心跳保活 + 接收空闲超时（半开连接探测）
// 所有场景共用同一套连接核心，仅 framing / codec / 选项不同。

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\n按 Ctrl+C 退出…");
};

await RunXmlLengthPrefixScenarioAsync(cts.Token);
await RunStxEtxTextScenarioAsync(cts.Token);
await RunReconnectScenarioAsync(cts.Token);
await RunHeartbeatScenarioAsync(cts.Token);

Console.WriteLine("演示结束。");

// ---------------------------------------------------------------------------
// 场景 1：XML + 4 字节长度前缀
// ---------------------------------------------------------------------------
static async Task RunXmlLengthPrefixScenarioAsync(CancellationToken ct)
{
    const int port = 5100;
    Console.WriteLine("=== 场景 1：XML 消息 + LengthPrefix 帧（4 字节大端长度头） ===");

    var server = new StreamConnection<XDocument>(
        new LengthPrefixFramer(),
        new XmlDocumentCodec(),
        IPAddress.Loopback,
        port,
        isActive: false,
        new StreamConnectionOptions { ConnectRetryDelayMs = 1000 });

    var client = new StreamConnection<XDocument>(
        new LengthPrefixFramer(),
        new XmlDocumentCodec(),
        IPAddress.Loopback,
        port,
        isActive: true,
        new StreamConnectionOptions { ConnectRetryDelayMs = 1000 });

    WireLogging(server, "Srv");
    WireLogging(client, "Cli");

    var serverReceived = 0;
    _ = Task.Run(async () =>
    {
        await foreach (var doc in server.GetMessages(ct))
        {
            Interlocked.Increment(ref serverReceived);
            var id = doc.Root?.Element("Id")?.Value;
            Console.WriteLine($"[Srv] 收到 XML: Id={id}");

            // 回一条应答
            var reply = XDocument.Parse($"<Reply><Echo>{id}</Echo></Reply>");
            await server.SendAsync(reply, ct);
        }
    }, ct);

    client.ConnectionChanged += (_, state) =>
    {
        if (state == ConnectionState.Connected)
            Console.WriteLine("[Cli] 已连接，开始发送…");
    };

    client.Start(ct);
    server.Start(ct);

    // 等双方连接就绪（不再是 Task.Delay 盲等）
    await Task.WhenAll(client.WaitForConnectedAsync(ct), server.WaitForConnectedAsync(ct));

    for (var i = 1; i <= 3; i++)
    {
        var msg = XDocument.Parse($"<Message><Id>{i}</Id><Name>Device-{i}</Name></Message>");
        Console.WriteLine($"[Cli] 发送 XML #{i}");
        await client.SendAsync(msg, ct);
        await Task.Delay(300, ct);
    }

    await Task.Delay(500, ct);

    // 验证服务端确实收到了 3 条
    Assert(serverReceived == 3, $"服务端应收到 3 条消息，实际 {serverReceived}");

    Console.WriteLine("场景 1 通过 ✓");
    await server.DisposeAsync();
    await client.DisposeAsync();
    await Task.Delay(200, ct);
}

// ---------------------------------------------------------------------------
// 场景 2：STX/ETX 包裹的纯文本
// ---------------------------------------------------------------------------
static async Task RunStxEtxTextScenarioAsync(CancellationToken ct)
{
    const int port = 5200;
    Console.WriteLine("\n=== 场景 2：纯文本 + STX/ETX 包裹帧 ===");

    var server = new StreamConnection<string>(
        new StxEtxFramer(),
        new Utf8TextCodec(),
        IPAddress.Loopback,
        port,
        isActive: false,
        new StreamConnectionOptions { ConnectRetryDelayMs = 1000 });

    var client = new StreamConnection<string>(
        new StxEtxFramer(),
        new Utf8TextCodec(),
        IPAddress.Loopback,
        port,
        isActive: true,
        new StreamConnectionOptions { ConnectRetryDelayMs = 1000 });

    WireLogging(server, "Srv2");
    WireLogging(client, "Cli2");

    var serverReceived = 0;
    _ = Task.Run(async () =>
    {
        await foreach (var text in server.GetMessages(ct))
        {
            Interlocked.Increment(ref serverReceived);
            Console.WriteLine($"[Srv2] 收到文本: \"{text}\"");
            await server.SendAsync($"ACK:{text}", ct);
        }
    }, ct);

    client.Start(ct);
    server.Start(ct);

    await Task.WhenAll(client.WaitForConnectedAsync(ct), server.WaitForConnectedAsync(ct));

    var payloads = new[] { "hello", "world-01", "multiline\nline2" };
    for (var i = 0; i < payloads.Length; i++)
    {
        Console.WriteLine($"[Cli2] 发送文本 \"{payloads[i]}\"");
        await client.SendAsync(payloads[i], ct);
        await Task.Delay(250, ct);
    }

    await Task.Delay(400, ct);

    Assert(serverReceived == payloads.Length, $"服务端应收到 {payloads.Length} 条，实际 {serverReceived}");

    Console.WriteLine("场景 2 通过 ✓");
    await server.DisposeAsync();
    await client.DisposeAsync();
}

// ---------------------------------------------------------------------------
// 场景 3：断线自动重连
// ---------------------------------------------------------------------------
static async Task RunReconnectScenarioAsync(CancellationToken ct)
{
    const int port = 5300;
    Console.WriteLine("\n=== 场景 3：主动端断线自动重连 ===");

    // 主动端：连接一个不存在的端口 → 反复重连（ConnectRetryDelayMs=800ms）
    var client = new StreamConnection<string>(
        new LengthPrefixFramer(),
        new Utf8TextCodec(),
        IPAddress.Loopback,
        port,
        isActive: true,
        new StreamConnectionOptions { ConnectRetryDelayMs = 800 });

    var seen = new List<string>();
    client.ConnectionChanged += (_, state) =>
    {
        lock (seen)
        {
            seen.Add(state.ToString());
        }
    };

    client.Start(ct);

    // 等待主动端出现至少 2 次 Connecting（重试），最多 5 秒
    var deadline = DateTime.UtcNow.AddSeconds(5);
    while (!ct.IsCancellationRequested && DateTime.UtcNow < deadline)
    {
        lock (seen)
        {
            if (seen.Count(s => s == nameof(ConnectionState.Connecting)) >= 2)
                break;
        }

        await Task.Delay(100, ct);
    }

    int ConnectingCount()
    {
        lock (seen)
        {
            return seen.Count(s => s == nameof(ConnectionState.Connecting));
        }
    }

    Assert(ConnectingCount() >= 2, $"主动端应反复重试（至少 2 次 Connecting），实际 {ConnectingCount()} 次");

    // 此刻启动服务端，主动端应在下一次重试时连上（最多等 4 秒）
    var server = new StreamConnection<string>(
        new LengthPrefixFramer(),
        new Utf8TextCodec(),
        IPAddress.Loopback,
        port,
        isActive: false,
        new StreamConnectionOptions { ConnectRetryDelayMs = 500, AcceptRetryDelayMs = 500 });
    server.Start(ct);

    var serverGot = 0;
    _ = Task.Run(async () =>
    {
        await foreach (var text in server.GetMessages(ct))
        {
            Interlocked.Increment(ref serverGot);
            Console.WriteLine($"[Srv3] 收到重连后消息: \"{text}\"");
        }
    }, ct);

    WireFrameErrors(server, "Srv3");
    WireFrameErrors(client, "Cli3");

    try
    {
        await client.WaitForConnectedAsync(ct).WaitAsync(TimeSpan.FromSeconds(4), ct);
    }
    catch (TimeoutException)
    {
    }
    Assert(client.State == ConnectionState.Connected, "主动端应能连上后启动的服务端");

    await client.SendAsync("after-reconnect", ct);
    await Task.Delay(400, ct);

    Assert(serverGot == 1, $"服务端应收到重连后的 1 条消息，实际 {serverGot}");

    // ---- 第二段：已建立的会话断线 → 重连 → 消息仍送达 ----
    // （1.1.0 的假活缺陷正发生在这里：第一次断线会永久关闭服务端消息通道）
    Console.WriteLine("\n主动端强制重连，模拟链路中断…");
    client.Reconnect();

    // 注意不能用 WaitForConnectedAsync 等"重连"：发起强制重连的瞬间旧会话仍是 Connected，
    // 该调用会走"已连接立即完成"的快速路径。这里轮询双方状态直到都回到 Connected。
    var reconnectDeadline = DateTime.UtcNow.AddSeconds(15);
    while (!(client.State == ConnectionState.Connected && server.State == ConnectionState.Connected)
           && DateTime.UtcNow < reconnectDeadline && !ct.IsCancellationRequested)
    {
        await Task.Delay(50, ct);
    }
    Assert(client.State == ConnectionState.Connected && server.State == ConnectionState.Connected,
        $"重连后双方应回到 Connected（client={client.State}, server={server.State}）");

    await client.SendAsync("survives-reconnect", ct);
    await Task.Delay(500, ct);

    Assert(serverGot == 2, $"断线重连后服务端应累计收到 2 条消息，实际 {serverGot}");

    Console.WriteLine("场景 3 通过 ✓");
    await server.DisposeAsync();
    await client.DisposeAsync();
}

// ---------------------------------------------------------------------------
// 场景 4：心跳保活 + 接收空闲超时
// ---------------------------------------------------------------------------
// 心跳范式（协议无关，框架不内置心跳——消息形态由驱动决定）：
//   1) 一侧周期性发送心跳报文（周期取空闲超时的 1/3 左右）
//   2) 双方开启 ReceiveIdleTimeoutMs（= 3× 心跳周期，容忍丢 1-2 次）
//   3) 对端回 PONG（或任何业务流量），双向都有字节即可重置空闲计时
//   4) 对端“猝死”（断电/拔线，无 FIN/RST）时，静默超时判定连接死亡并自动重连
static async Task RunHeartbeatScenarioAsync(CancellationToken ct)
{
    const int port = 5400;
    Console.WriteLine("\n=== 场景 4：心跳保活 + 接收空闲超时 ===");

    var heartbeatInterval = TimeSpan.FromMilliseconds(500);
    var options = new StreamConnectionOptions
    {
        ReceiveIdleTimeoutMs = 1500, // = 3× 心跳周期：容忍偶尔丢 1-2 次心跳
        ConnectRetryDelayMs = 500,
        AcceptRetryDelayMs = 500,
    };

    var server = new StreamConnection<string>(
        new LengthPrefixFramer(), new Utf8TextCodec(),
        IPAddress.Loopback, port, isActive: false, options);
    var client = new StreamConnection<string>(
        new LengthPrefixFramer(), new Utf8TextCodec(),
        IPAddress.Loopback, port, isActive: true, options);

    // 服务端：收到任何消息回 PONG（维持双向流量，双方空闲计时都被重置）
    _ = Task.Run(async () =>
    {
        await foreach (var message in server.GetMessages(ct))
            await server.SendAsync($"PONG:{message}", ct);
    }, ct);

    // 客户端：周期心跳
    var heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
    var heartbeatTask = Task.Run(async () =>
    {
        try
        {
            while (!heartbeatCts.IsCancellationRequested)
            {
                await client.SendAsync("PING", heartbeatCts.Token);
                Console.WriteLine("[Cli4] PING");
                await Task.Delay(heartbeatInterval, heartbeatCts.Token);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }, ct);

    client.Start(ct);
    server.Start(ct);
    await Task.WhenAll(client.WaitForConnectedAsync(ct), server.WaitForConnectedAsync(ct));

    // 心跳正常运行 2.2 秒（≈ 4 个周期）：双方保持 Connected，且零状态事件（连接稳定）
    var stableStates = new List<ConnectionState>();
    client.ConnectionChanged += (_, s) => { lock (stableStates) stableStates.Add(s); };
    await Task.Delay(2200, ct);
    Assert(client.State == ConnectionState.Connected && server.State == ConnectionState.Connected,
        $"心跳期间双方应保持 Connected（client={client.State}, server={server.State}）");
    lock (stableStates)
        Assert(stableStates.Count == 0, $"心跳期间不应有任何状态事件，实际 {stableStates.Count} 次");
    Console.WriteLine("心跳保活正常 ✓（连接稳定，零状态事件）");

    // 模拟对端“猝死”：停止心跳且完全静默（连接仍在，只是没有字节）
    heartbeatCts.Cancel();
    await heartbeatTask;
    Console.WriteLine("已静默，等待空闲超时…");

    // 静默超过 ReceiveIdleTimeoutMs 后，接收侧判定会话死亡并进入重连：
    // 双方进程都活着时重连会立刻成功，状态表现为“翻动”（Retry/Connecting/Connected 循环）
    // 而非停在断开——观察 ConnectionChanged 事件即可证明空闲检测生效。
    // （真实半开场景——对端断电/不可达——重连会持续失败，状态停留在 Connecting/Retry）
    var flapStates = new List<ConnectionState>();
    client.ConnectionChanged += (_, s) => { lock (flapStates) flapStates.Add(s); };
    var deadline = DateTime.UtcNow.AddSeconds(6);
    while (DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
    {
        lock (flapStates)
        {
            if (flapStates.Count >= 2)
                break;
        }
        await Task.Delay(100, ct);
    }
    lock (flapStates)
        Assert(flapStates.Count >= 2,
            $"静默后应触发空闲判定与重连（至少 2 次状态事件），实际 {flapStates.Count} 次：[{string.Join(", ", flapStates)}]");
    Console.WriteLine($"空闲超时生效 ✓（触发的状态序列：[{string.Join(", ", flapStates)}]，进入重连循环）");

    Console.WriteLine("场景 4 通过 ✓");
    await server.DisposeAsync();
    await client.DisposeAsync();
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------
static void WireLogging<T>(IStreamConnection<T> conn, string tag)
{
    conn.ConnectionChanged += (_, state) => Console.WriteLine($"[{tag}] 状态 -> {state}");
    conn.RawBytesReceived = bytes => Console.WriteLine($"[{tag}] RX HEX: {Convert.ToHexString(bytes.Span)}");
    conn.RawBytesSent = bytes => Console.WriteLine($"[{tag}] TX HEX: {Convert.ToHexString(bytes.Span)}");
}

/// <summary>演示帧层诊断事件：坏帧/被丢弃的噪声字节/未完成帧超限都带字节与原因。</summary>
static void WireFrameErrors<T>(IStreamConnection<T> conn, string tag)
    => conn.FrameError += (_, e) => Console.WriteLine(
        $"[{tag}] 帧诊断 {e.Kind}: {Convert.ToHexString(e.Bytes.Span)}" +
        (e.Exception is null ? string.Empty : $" ({e.Exception.Message})"));

static void Assert(bool condition, string message)
{
    if (!condition)
    {
        Console.WriteLine($"断言失败：{message}");
        Environment.Exit(1);
    }
}

/// <summary>简单的 UTF-8 文本 codec（演示自定义 codec 有多么容易）。</summary>
internal sealed class Utf8TextCodec : ICodec<string>
{
    public string Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
        => Encoding.UTF8.GetString(frame);

    public void Encode(string message, IBufferWriter<byte> writer, CancellationToken ct = default)
        => writer.Write(Encoding.UTF8.GetBytes(message));
}
