# StreamFrame

[![NuGet](https://img.shields.io/nuget/v/StreamFrame.svg)](https://www.nuget.org/packages/StreamFrame)
[![NuGet Downloads](https://img.shields.io/nuget/dt/StreamFrame)](https://www.nuget.org/stats/packages/StreamFrame?groupby=Version)

English | [简体中文](README.md)

**A general-purpose socket communication library** that pluggably separates *frame delimiting* from *payload encoding/decoding*. Built for scenarios where peers exchange data over sockets (device communication, logistics/WMS integration, etc.): the common parts (connection management, reconnection, I/O, TCP packet fragmentation/reassembly, message dispatching) live in the core, while the parts that vary (framing, codec) are pluggable drivers.

## Why StreamFrame

| | Hand-rolled | StreamFrame |
|---|---|---|
| Onboarding a new device/protocol | Rewrite connection, delimiting, encoding | Write one `ICodec<T>` driver |
| Frame boundaries (length-prefix / STX-ETX / custom) | Every project rolls its own | Two built-in + pluggable |
| Fragmented/partial TCP packets | Manual buffer stitching | Handled by Pipelines |
| Reconnection / state machine | Hand-rolled | Built-in auto-reconnect |
| Send performance | Multiple copies | Optional single-buffer streaming encode |

## Installation

```
dotnet add package StreamFrame
```

XML message driver (optional):

```
dotnet add package StreamFrame.Protocols.Xml
```

## Quick start

One connection = one **framer** + one **codec** + address/port/mode. Framing and codec are both **fixed per connection**: a connection carries exactly one message type in exactly one frame format.

```csharp
using System.Net;
using System.Xml.Linq;
using StreamFrame;
using StreamFrame.Protocols.Xml;

// Server (passive listener)
var server = new StreamConnection<XDocument>(
    new LengthPrefixFramer(),              // 4-byte big-endian length header; or StxEtxFramer
    new XmlDocumentCodec(),                // swap in your own ICodec<T> for a custom protocol
    IPAddress.Any, 5100, isActive: false);
server.Start(ct);

await foreach (var doc in server.GetMessages(ct))
{
    var id = doc.Root?.Element("Id")?.Value;
    await server.SendAsync(XDocument.Parse($"<Reply><Echo>{id}</Echo></Reply>"), ct);
}

// Client (active connector)
var client = new StreamConnection<XDocument>(
    new LengthPrefixFramer(),
    new XmlDocumentCodec(),
    IPAddress.Parse("127.0.0.1"), 5100, isActive: true);
client.Start(ct);
await client.SendAsync(XDocument.Parse("<Message><Id>1</Id></Message>"), ct);
```

## Concepts

### Framing (`IFramer`) — slicing frames out of a byte stream

| Implementation | Delimiting | Suitable for |
|---|---|---|
| `LengthPrefixFramer` | 4-byte **big-endian** length header + payload | General purpose, binary-safe |
| `StxEtxFramer` | Wrapped in STX `0x02` … ETX `0x03` | XML / plain-text payloads known to be marker-safe |

Both built-in implementations support **streaming single-buffer encoding** (optional, removes the payload memcpy on send). Implement `IFramer` to define a custom frame format.

### Codec (`ICodec<T>`) — parsing/writing the payload inside a frame

```csharp
public interface ICodec<TMessage>
{
    TMessage Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default);
    void Encode(TMessage message, IBufferWriter<byte> writer, CancellationToken ct = default);
}
```

**Onboarding a new device = writing one driver**: implement `ICodec<TMessage>`, define your message type, pick a framer — everything else is reused. See `StreamFrame.Protocols.Xml` for the official example.

### Connection (`IStreamConnection<T>`) — the transport

- **Client/server dual mode**: `isActive: true` connects out, `false` listens; IPv4/IPv6 dual-stack (listening on `IPAddress.Any` is normalized to dual-stack; IPv4 client addresses are displayed as plain IPv4)
- **Auto-reconnect**: `Connecting → Connected → Retry` state machine; `GetMessages` is a stable stream across reconnections — received messages are not lost and enumeration does not break when the connection drops and recovers
- **Start & stop**: the `ct` passed to `Start(ct)` is the connection's **lifetime token** — cancelling it stops connection/reconnection and tears the link down (state enters the terminal `Disconnected`, `GetMessages` completes naturally; create a new connection afterwards). `DisposeAsync` does the same. After shutdown, `SendAsync` throws `ChannelClosedException`
- **Waiting for readiness**: `await conn.WaitForConnectedAsync(ct)` — completes immediately when connected; otherwise waits for the next successful connection (or cancellation/dispose). No state polling, no `Task.Delay` guessing
- **Robustness**: payload decode failures, incomplete-frame overflows, send failures, and receive idle timeouts all invalidate the session and rebuild it automatically (no more "connection looks alive while messages silently vanish")
- **Liveness detection (optional)**: TCP KeepAlive and receive idle timeout to catch half-open connections (peer power loss / unplugged cable)
- **Events**: `ConnectionChanged` state changes, `FrameError` frame-level diagnostics, `RawBytesReceived/Sent` raw bytes (HEX debugging)
- **Backpressure**: bounded send queue — `SendAsync` waits when full; the receive side buffers without limit by default, or set `ReceiveQueueCapacity` — decoding pauses when the consumer lags, propagating TCP backpressure to the peer and preventing unbounded memory growth

## Diagnostics & debugging

### FrameError — frame-level diagnostics

When the peer sends bad data, the `FrameError` event hands you the **offending bytes** and the reason directly — no more eyeballing HEX dumps:

```csharp
client.FrameError += (_, e) =>
{
    // e.Kind: DecodeFailed (frame intact, payload parsing failed)
    //         DiscardedByResync (bytes discarded as noise by the framer)
    //         IncompleteFrameOverflow (incomplete-frame buffer over its limit)
    // e.Bytes: copied; safe to retain long-term
    // e.Exception: the original exception for DecodeFailed
    Console.WriteLine($"[{e.Kind}] {Convert.ToHexString(e.Bytes.Span)} {e.Exception?.Message}");
};
```

The policy for payload decode failures is set by `StreamConnectionOptions.DecodeErrorPolicy`:

| Policy | Behavior |
|---|---|
| `Disconnect` (default) | Reconnect — after content corruption the stream state is usually untrustworthy |
| `SkipFrame` | Drop the bad frame and continue; suits noisy lines |

### RawBytesReceived / RawBytesSent — raw byte taps

Full output from the socket layer (including noise bytes later discarded by the framer); the send side fires per chunk actually written (bytes already on the wire remain visible after a partial-send failure). **Memory contract**: callback arguments are slices of internal buffers, valid only during the synchronous callback — copy them if you need to retain; handler exceptions are isolated and never affect the session.

### Incomplete-frame protection

A peer that announces a huge frame but never completes it (or a stream with STX but no closing ETX) would hold the buffer forever. `MaxIncompleteFrameBufferBytes` (default = frame limit + 4 KB) puts a hard cap on half-frames: exceeding it disconnects and reports via `FrameError`.

### Logging (optional)

Pass an `ILogger` when constructing the connection to route internal events (connect retries, session faults, user-callback exceptions) to your logs — no more silence in production:

```csharp
var conn = new StreamConnection<XDocument>(..., logger: loggerFactory.CreateLogger("StreamFrame"));
```

Without one, no logging happens (zero-dependency usage).

### Liveness tips

For production, enable `TcpKeepAlive = true`; for protocols with periodic traffic, also consider `ReceiveIdleTimeoutMs` (e.g., 3× the heartbeat period) as a second safety net against half-open connections.

## Performance

Measured with BenchmarkDotNet (details and how to reproduce in [bench/README.md](bench/README.md)):

- **Streaming encode**: halves per-frame heap allocation (single vs. double buffer); 25–35% faster for small payloads, on par for large ones;
- **Frame decoding**: `LengthPrefixFramer` ≈10 ns/frame vs `StxEtxFramer` ≈1.1 µs/frame (byte-by-byte scanning) — prefer length-prefix for high-throughput scenarios.

## Dependencies

- [System.IO.Pipelines](https://www.nuget.org/packages/System.IO.Pipelines)
- [System.Threading.Channels](https://www.nuget.org/packages/System.Threading.Channels)
- [Microsoft.Extensions.Logging.Abstractions](https://www.nuget.org/packages/Microsoft.Extensions.Logging.Abstractions) (logging abstractions only — no logging framework is pulled in)

## Tests & samples

```bash
dotnet build StreamFrame.slnx
dotnet test
dotnet run --project samples/StreamFrame.Demo          # 3-scenario end-to-end demo
dotnet run -c Release --project bench/StreamFrame.Benchmarks   # benchmarks (~5-10 min)
```

## Project layout

```
src/StreamFrame/                  # core library (no business dependencies)
src/StreamFrame.Protocols.Xml/    # XML message driver (sample codec)
test/StreamFrame.Tests/           # xUnit tests
samples/StreamFrame.Demo/         # console end-to-end demo
bench/StreamFrame.Benchmarks/     # BenchmarkDotNet benchmarks
```

## License

MIT
