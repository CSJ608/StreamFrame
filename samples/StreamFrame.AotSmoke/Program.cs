using System.Buffers;
using System.Net;
using System.Text.Json;
using StreamFrame;

// AOT/裁剪兼容性冒烟：引用 StreamFrame 的最小可运行路径（连接生命周期 + 定界 + codec +
// 会话感知 + 指标记录调用），确保库不依赖被裁剪掉的能力。CI 以 PublishAot 发布本工程，
// 裁剪/AOT 警告会因 TreatWarningsAsErrors 直接红掉。
var port = 19_002;

var server = new StreamConnection<JsonElement>(
    new LengthPrefixFramer(), SystemTextJsonCodec.Instance,
    IPAddress.Loopback, port, isActive: false,
    options: new StreamConnectionOptions { IncompleteFrameTimeoutMs = 5_000 });
var client = new StreamConnection<JsonElement>(
    new LengthPrefixFramer(), SystemTextJsonCodec.Instance,
    IPAddress.Loopback, port, isActive: true,
    options: new StreamConnectionOptions { ConnectRetryDelayMs = 200 });

var received = 0;
_ = Task.Run(async () =>
{
    await foreach (var message in server.GetMessages())
    {
        if (Interlocked.Increment(ref received) == 3)
            break;
    }
});

server.Start(default);
client.Start(default);
await client.WaitForConnectedAsync();

if (client is ISessionAwareStreamConnection<JsonElement> sessionAware)
{
    var sessionId = sessionAware.CurrentSessionId;
    await sessionAware.SendInSessionAsync(sessionId, JsonDocument.Parse("""{"type":"aot","n":1}""").RootElement);
    await sessionAware.SendInSessionAsync(sessionId, JsonDocument.Parse("""{"type":"aot","n":2}""").RootElement);
}

await client.SendAsync(JsonDocument.Parse("""{"type":"aot","n":3}""").RootElement);
await Task.Delay(3_000);

await server.DisposeAsync();
await client.DisposeAsync();
Console.WriteLine(received == 3 ? "AOT smoke OK" : $"unexpected received={received}");

/// <summary>System.Text.Json 的 span 直写 codec（AOT 安全：无反射序列化）。</summary>
internal sealed class SystemTextJsonCodec : ICodec<JsonElement>
{
    public static readonly SystemTextJsonCodec Instance = new();

    public JsonElement Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
        => JsonDocument.Parse(frame.ToArray()).RootElement;

    public void Encode(JsonElement message, IBufferWriter<byte> writer, CancellationToken ct = default)
    {
        // span 直写（大报文指南推荐写法）：编码器直写 IBufferWriter，无中间数组
        var raw = message.GetRawText();
        var span = writer.GetSpan(System.Text.Encoding.UTF8.GetMaxByteCount(raw.Length));
        var written = System.Text.Encoding.UTF8.GetBytes(raw, span);
        writer.Advance(written);
    }
}
