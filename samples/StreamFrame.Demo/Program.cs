using System.Buffers;
using System.Net;
using System.Text;
using System.Xml.Linq;
using StreamFrame;
using StreamFrame.Abstractions;
using StreamFrame.Protocols.Xml;

// 本 demo 演示 StreamFrame 的三种帧/编解码组合：
//   1) XML 消息 + 4 字节长度前缀帧（对应 SamSung 风格）
//   2) 纯文本消息 + STX/ETX 包裹帧
// 两种场景共用同一套连接核心，仅 framing 与 codec 不同。

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

Console.WriteLine("演示结束。");

// ---------------------------------------------------------------------------
// 场景 1：XML + 4 字节长度前缀
// ---------------------------------------------------------------------------
static async Task RunXmlLengthPrefixScenarioAsync(CancellationToken ct)
{
    const int port = 5100;
    Console.WriteLine("=== 场景 1：XML 消息 + LengthPrefix 帧（4 字节大端长度头） ===");

    var server = new StreamConnection<XDocument>(
        new LengthPrefixFrameCodec(),
        new XmlDocumentCodec(),
        IPAddress.Loopback,
        port,
        isActive: false,
        new StreamConnectionOptions { ConnectRetryDelayMs = 1000 });

    var client = new StreamConnection<XDocument>(
        new LengthPrefixFrameCodec(),
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
        new StxEtxFrameCodec(),
        new Utf8TextCodec(),
        IPAddress.Loopback,
        port,
        isActive: false,
        new StreamConnectionOptions { ConnectRetryDelayMs = 1000 });

    var client = new StreamConnection<string>(
        new StxEtxFrameCodec(),
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
        new LengthPrefixFrameCodec(),
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
        new LengthPrefixFrameCodec(),
        new Utf8TextCodec(),
        IPAddress.Loopback,
        port,
        isActive: false,
        new StreamConnectionOptions { ConnectRetryDelayMs = 500 });
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

    try
    {
        await Task.WhenAll(
            client.WaitForConnectedAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct),
            server.WaitForConnectedAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct));
    }
    catch (TimeoutException)
    {
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
