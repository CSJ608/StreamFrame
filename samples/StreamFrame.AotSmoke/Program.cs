using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using StreamFrame;

// 发布验证与原生运行验证是两道独立检查；见 README.md 和 ci.yml。
var fault = args.Length == 0 ? "none" : args.Length == 1 ? args[0] : "invalid";
if (fault is not ("none" or "--fault=connect" or "--fault=missing" or "--fault=consumer" or "--fault=content"))
{
    Console.Error.WriteLine("Usage: StreamFrame.AotSmoke [--fault=connect|missing|consumer|content]");
    return 2;
}

var phase = "connection";
try
{
    // 公共 API 尚不暴露 port=0 的实际监听地址。先向 OS 申请端口；释放到绑定之间
    // 存在竞争，因此仅在连接阶段最多重选三次，消息阶段绝不重试。
    for (var attempt = 1; attempt <= 3; attempt++)
    {
        using var reservation = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        reservation.ExclusiveAddressUse = true;
        reservation.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)reservation.LocalEndPoint!).Port;
        await using var server = CreateConnection(port, isActive: false);
        await using var client = CreateConnection(port, isActive: true);
        using var connectDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        Console.WriteLine($"Connecting attempt={attempt}, port={port}, fault={fault}");
        if (fault != "--fault=connect")
        {
            reservation.Dispose();
            server.Start(default);
        }
        // connect 故障保留已绑定但未监听的 socket，避免误连别的服务。
        client.Start(default);
        try
        {
            await Task.WhenAll(client.WaitForConnectedAsync(connectDeadline.Token),
                server.WaitForConnectedAsync(connectDeadline.Token));
        }
        catch (OperationCanceledException) when (connectDeadline.IsCancellationRequested)
        {
            Console.Error.WriteLine($"Connection deadline exceeded (attempt={attempt}, port={port}).");
            if (fault == "--fault=connect" || attempt == 3)
                throw new SmokeFailure("connection", "Both peers must connect within the deadline.");
            continue;
        }

        phase = "messages";
        using var messageDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var consumer = ConsumeAsync(server, fault, messageDeadline.Token);
        var sender = SendAsync(client, fault, messageDeadline.Token);
        var work = Task.WhenAll(sender, consumer);
        try
        {
            await work.WaitAsync(messageDeadline.Token);
        }
        finally
        {
            // 无论成功、超时还是消费抛错，都先停机，再观察所有已启动的应用任务。
            // 只忽略收尾取消；消费/发送的业务异常仍传播至进程非零退出。
            messageDeadline.Cancel();
            await client.DisposeAsync();
            await server.DisposeAsync();
            try { await work; }
            catch (OperationCanceledException) when (messageDeadline.IsCancellationRequested) { }
        }

        Console.WriteLine("AOT smoke OK: received=3, session-bound=2, ordinary=1, content verified");
        return 0;
    }
    throw new SmokeFailure("connection", "Connection attempts exhausted.");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"AOT smoke FAILED [{(ex is SmokeFailure failure ? failure.Category : phase)}]: {ex}");
    return 1;
}

static StreamConnection<JsonElement> CreateConnection(int port, bool isActive)
    => new(new LengthPrefixFramer(), SystemTextJsonCodec.Instance, IPAddress.Loopback, port, isActive,
        new StreamConnectionOptions { ConnectRetryDelayMs = 200, IncompleteFrameTimeoutMs = 5_000 });

static async Task SendAsync(StreamConnection<JsonElement> client, string fault, CancellationToken ct)
{
    var sessionId = client.CurrentSessionId;
    if (sessionId <= 0)
        throw new SmokeFailure("session", "Connected client must have a valid session ID.");
    using var first = JsonDocument.Parse("""{"type":"aot","n":1}""");
    using var second = JsonDocument.Parse("""{"type":"aot","n":2}""");
    using var third = JsonDocument.Parse(fault == "--fault=content"
        ? """{"type":"wrong","n":3}""" : """{"type":"aot","n":3}""");
    await client.SendInSessionAsync(sessionId, first.RootElement.Clone(), ct);
    await client.SendInSessionAsync(sessionId, second.RootElement.Clone(), ct);
    if (fault != "--fault=missing")
        await client.SendAsync(third.RootElement.Clone(), ct);
}

static async Task ConsumeAsync(StreamConnection<JsonElement> server, string fault, CancellationToken ct)
{
    var received = 0;
    await foreach (var message in server.GetMessages(ct))
    {
        if (fault == "--fault=consumer")
            throw new SmokeFailure("consumer", "Injected background consumer failure.");
        var expected = ++received;
        if (message.ValueKind != JsonValueKind.Object ||
            !message.TryGetProperty("type", out var type) || type.ValueKind != JsonValueKind.String ||
            type.GetString() != "aot" || !message.TryGetProperty("n", out var number) ||
            number.ValueKind != JsonValueKind.Number || !number.TryGetInt32(out var n) || n != expected)
            throw new SmokeFailure("content", $"Unexpected message #{expected}: {message.GetRawText()}");
        Console.WriteLine($"Received and verified message {received}/3");
        if (received == 3)
            return;
    }
    throw new SmokeFailure("messages", $"Message stream ended early: received={received}, expected=3.");
}

internal sealed class SmokeFailure(string category, string message) : Exception(message)
{
    public string Category { get; } = category;
}

/// <summary>System.Text.Json 的 span 直写 codec（AOT 安全：无反射序列化）。</summary>
internal sealed class SystemTextJsonCodec : ICodec<JsonElement>
{
    public static readonly SystemTextJsonCodec Instance = new();

    public JsonElement Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
    {
        using var document = JsonDocument.Parse(frame.ToArray());
        return document.RootElement.Clone();
    }

    public void Encode(JsonElement message, IBufferWriter<byte> writer, CancellationToken ct = default)
    {
        var raw = message.GetRawText();
        var span = writer.GetSpan(System.Text.Encoding.UTF8.GetMaxByteCount(raw.Length));
        var written = System.Text.Encoding.UTF8.GetBytes(raw, span);
        writer.Advance(written);
    }
}
