<p align="center">
  <img src="https://raw.githubusercontent.com/CSJ608/StreamFrame/main/docs/logo/icon-512.png" width="120" alt="StreamFrame logo" />
</p>

# StreamFrame

[![CI](https://github.com/CSJ608/StreamFrame/actions/workflows/ci.yml/badge.svg)](https://github.com/CSJ608/StreamFrame/actions/workflows/ci.yml)
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
- **Auto-reconnect**: `Connecting → Connected → Retry` state machine; optional exponential backoff (`MaxRetryDelayMs`, doubling with cap + ±20% jitter, auto-reset on success); `GetMessages` is a stable stream across reconnections — received messages are not lost and enumeration does not break when the connection drops and recovers
- **Start & stop**: the `ct` passed to `Start(ct)` is the connection's **lifetime token** — cancelling it stops connection/reconnection and tears the link down (state enters the terminal `Disconnected`, `GetMessages` completes naturally; create a new connection afterwards). `DisposeAsync` does the same. After shutdown, `SendAsync` throws `ChannelClosedException`
- **Waiting for readiness**: `await conn.WaitForConnectedAsync(ct)` — completes immediately when connected; otherwise waits for the next successful connection (or cancellation/dispose). No state polling, no `Task.Delay` guessing
- **Robustness**: payload decode failures, incomplete-frame overflows/timeouts, send failures, and receive idle timeouts all invalidate the session and rebuild it automatically (no more "connection looks alive while messages silently vanish")
- **Liveness detection (optional)**: TCP KeepAlive and receive idle timeout to catch half-open connections (peer power loss / unplugged cable)
- **Events**: `ConnectionChanged` state changes, `FrameError` frame-level diagnostics, `RawBytesReceived/Sent` raw bytes (HEX debugging)
- **Built-in metrics**: `System.Diagnostics.Metrics` (meter `StreamFrame`) — frame/byte counters, reconnects, session duration, send-queue watermark; see the "Built-in metrics" section
- **Backpressure**: bounded send queue — `SendAsync` waits when full; the receive side buffers without limit by default, or set `ReceiveQueueCapacity` — decoding pauses when the consumer lags, propagating TCP backpressure to the peer and preventing unbounded memory growth
- **Session-aware messaging (optional, advanced)**: `CurrentSessionId` / `SendInSessionAsync` (completes only after the whole frame is written to the socket; fails on session loss and never replays across sessions) / `GetSessionMessages` (messages carry their session id) — for protocols with strict session boundaries such as HSMS; see the dedicated section below

## Session-aware messaging (advanced)

For typical business traffic that tolerates replay, `SendAsync` (completes on enqueue; queued messages continue on the next session) + `GetMessages` (a stable stream across reconnects) is enough. Some protocols have strict session boundaries — re-handshake after reconnect, no replay of old-session messages, protocol timers starting when the frame is actually written (HSMS Select/T3/T6/T8). For those there is an optional capability interface `ISessionAwareStreamConnection<TMessage>` (implemented by `StreamConnection<TMessage>`; upper layers depending on the interface abstraction detect it with `is`):

```csharp
if (connection is ISessionAwareStreamConnection<MyMessage> sessionAware)
{
    long id = sessionAware.CurrentSessionId;   // allocated per established TCP session, monotonic, never reused; 0 when no session

    // Completes only after the whole frame is handed to the local socket;
    // session ends before that → SessionExpiredException, the message is never replayed
    await sessionAware.SendInSessionAsync(id, message, ct);

    // Receive view: every message carries the session it arrived on
    // (late deliveries from an old session's decoder still carry the old id)
    await foreach (var m in sessionAware.GetSessionMessages(ct))
        Handle(m.SessionId, m.Message);
}
```

Semantics at a glance:

- **Id linearization**: reading `CurrentSessionId` from a `ConnectionChanged` callback or after `WaitForConnectedAsync` always yields a valid id (allocation happens before Connected becomes visible); it is already zero when a non-Connected state becomes visible.
- **"Written" means**: the whole frame has been handed to the local socket (kernel buffer), peer ACK not included — the best signal available at the application layer. When the task fails, **treat the remote outcome as unknown** (the peer may have received part or all of the bytes); reconcile via your protocol's transaction correlation, idempotency or recovery flow.
- **Cancellation commit point**: cancelling before the send worker claims the entry cancels the task and the message is never sent; cancelling after the claim (frame write in progress) has no effect — cancelling one message neither tears frames nor kills the connection.
- **Relation to `SendAsync`**: both share one FIFO (serialized in enqueue order); the plain `SendAsync` cross-session replay behavior is unchanged. Session-bound sends never transfer to a new session on any failure path.
- **The two receive APIs are not a broadcast**: `GetMessages` and `GetSessionMessages` are competing consumer views of the same channel — enumerating both splits messages between them; pick one.

## Diagnostics & debugging

### FrameError — frame-level diagnostics

When the peer sends bad data, the `FrameError` event hands you the **offending bytes** and the reason directly — no more eyeballing HEX dumps:

```csharp
client.FrameError += (_, e) =>
{
    // e.Kind: DecodeFailed (frame intact, payload parsing failed)
    //         DiscardedByResync (bytes discarded as noise by the framer)
    //         IncompleteFrameOverflow (incomplete-frame buffer over its limit)
    //         IncompleteFrameTimeout (incomplete frame timed out waiting for the rest)
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

A peer that announces a huge frame but never completes it (or a stream with STX but no closing ETX) would hold the buffer forever. Two complementary guards:

- `MaxIncompleteFrameBufferBytes` (default = frame limit + 4 KB) — a cap in **bytes**: exceeding it disconnects, blocking memory-flooding peers;
- `IncompleteFrameTimeoutMs` (default 0 = off) — a cap in **time**: if a started frame receives no further bytes for this long, the session is torn down and `FrameError` reports `IncompleteFrameTimeout` with a buffer snapshot capped at 8 KB.

The incomplete-frame timeout only counts while a frame is actually in progress: an idle connection with an empty buffer never trips it (each received byte resets it; it clears once a whole frame is cut). It complements `ReceiveIdleTimeoutMs` (see below) — the latter also times total silence, which suits protocols with periodic traffic; for protocols that may stay idle for a long time but must not tolerate a stalled half-frame (e.g. HSMS T8), use the incomplete-frame timeout. Note its scope is "waiting for further bytes from the network": when `ReceiveQueueCapacity` is set and the consumer stalls completely, the decode loop blocks on the message-channel write and the timeout does not tick during that period (the memory guard `MaxIncompleteFrameBufferBytes` still applies).

### Built-in metrics

Connections ship [System.Diagnostics.Metrics](https://learn.microsoft.com/dotnet/core/diagnostics/metrics-instrumentation) instruments (meter name `StreamFrame`, tag `endpoint`) with zero external dependencies — subscribe with a `MeterListener` or OpenTelemetry in production; unsubscribed, each record costs nanoseconds:

| Instrument | Kind | Meaning |
|---|---|---|
| `streamframe.frames_sent` / `frames_received` | Counter | Business frames sent/received |
| `streamframe.bytes_sent` / `bytes_received` | Counter | Bytes sent/received (framing bytes/noise included) |
| `streamframe.reconnects` | Counter | Reconnect count |
| `streamframe.session_duration` | Histogram | Lifetime of one TCP session (seconds) |
| `streamframe.send_queue_length` | Histogram | Send-queue watermark (sampled per enqueue) |

```csharp
// Minimal subscription (example): OTel's MeterProvider.AddMeter("StreamFrame") hooks the full ecosystem
using var listener = new MeterListener { InstrumentPublished = (i, l) => l.EnableMeasurementEvents(i) };
listener.SetMeasurementEventCallback<long>((instrument, value, _, _) => { /* report */ });
listener.Start();
```

> The netstandard2.0 target gets the same API via the `System.Diagnostics.DiagnosticSource` package (works on .NET Framework).

### Logging (optional)

Pass an `ILogger` when constructing the connection to route internal events (connect retries, session faults, user-callback exceptions) to your logs — no more silence in production:

```csharp
var conn = new StreamConnection<XDocument>(..., logger: loggerFactory.CreateLogger("StreamFrame"));
```

Without one, no logging happens (zero-dependency usage).

### Liveness & heartbeat pattern

For production, enable `TcpKeepAlive = true`; an application-level heartbeat combined with `ReceiveIdleTimeoutMs` (3× the heartbeat period, tolerating 1-2 lost beats) is an even stronger combo. The framework does not ship a heartbeat (the message shape belongs to your protocol) — the pattern:

```csharp
var options = new StreamConnectionOptions { ReceiveIdleTimeoutMs = 1500 };

// One side beats periodically; the other echoes PONG on any message (bidirectional
// traffic resets both idle timers). Full runnable sample: demo scenario 4.
_ = Task.Run(async () =>
{
    while (!cts.IsCancellationRequested)
    {
        await client.SendAsync("PING", cts.Token);
        await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token); // ~1/3 of the timeout
    }
});
```

When the peer dies silently (power loss / unplugged cable, no FIN/RST), the idle timeout kills the session and auto-reconnect kicks in; if the peer is unreachable, reconnection keeps failing and the state stays in `Connecting`/`Retry`.

Choosing between the two receive timeouts: `ReceiveIdleTimeoutMs` demands **periodic traffic** — use it when the protocol has heartbeats/periodic reports (it also covers half-open connections); when the protocol may stay silent for long stretches (traffic only expected while a frame is in progress), use `IncompleteFrameTimeoutMs` instead — silence is not a fault, only a stalled half-frame is.

## Performance

Measured with BenchmarkDotNet (details and how to reproduce in [bench/README.md](bench/README.md)):

- **Streaming encode**: halves per-frame heap allocation (single vs. double buffer); 25–35% faster for small payloads, on par for large ones;
- **Frame decoding**: `LengthPrefixFramer` ≈8.3 ns/frame, `StxEtxFramer` ≈50 ns/frame (≈22× faster after SearchValues vectorization on net8+) — both handle tens of millions of frames per second;
- **End-to-end** (real TCP loopback, 2026-08-29 two-round ranges, built-in metrics on): one-way throughput ≈130–135k msgs/s at 1KB and ≈210–240k at 64B (LengthPrefix); round-trip latency ≈66–74 µs; the XML codec costs 2–16 µs per message (400B–4KB) — serialization dominates, not framing. **Framework tax depends on message size**: for small messages the bounded-queue pipeline is even faster than the naive serialized-NetworkStream-write baseline (13–52%); at 64KB the cost is ≈3–4× (encoding-buffer allocations dominate — recorded as an optimization direction);
- **Cost of the new features, measured**: built-in metrics cost 0.5–1.0 ns per record with zero allocation when nobody subscribes (fine to leave on); `SendInSessionAsync` is roughly 2× the time of `SendAsync` with +≈470 B/msg allocated (the price of session semantics — use `SendAsync` when you don't need them); the receive views and the disabled incomplete-frame timeout show no measurable difference. Absolute values vary by machine; full data and noise disclosure in [bench/README.md](bench/README.md).

### Large-message guide (≥64KB)

For 64KB-class messages the dominant costs are the codec style and the message type, not the framework (attribution data in [bench/README.md](bench/README.md)). Three recommendations:

1. **Use the span-based write overload in your codec** — `Encoding.GetBytes(ReadOnlySpan<char>, IBufferWriter<byte>)` produces no intermediate array (the naive style adds a full-size allocation + copy per message):

   ```csharp
   // Recommended: zero intermediate arrays
   public void Encode(string message, IBufferWriter<byte> writer, CancellationToken ct)
       => Encoding.UTF8.GetBytes(message.AsSpan(), writer);
   // Not recommended: writer.Write(Encoding.UTF8.GetBytes(message));  // full-size byte[] per message
   ```

2. **Pick byte[] / ReadOnlyMemory<byte> as the message type**: string messages inherently allocate ≈2× the payload size per message for UTF-16 materialization; with byte payloads and a pass-through codec the framework sits within ≈20–30% of raw TCP;
3. Keep **streaming encode** on (default, `UseStreamingEncode`) — send buffers now start at the previous frame's size (adaptive, capped at 1MB).

## Supported frameworks

| Package target | Runtime |
|---|---|
| `net10.0` (recommended, LTS until 2028-11) | .NET 10 |
| `net8.0` (LTS until 2026-11) | .NET 8 |
| `netstandard2.0` | .NET Framework 4.6.2+, Unity, Mono, ... |

The `netstandard2.0` asset is validated by the **full net48 test suite** (real TCP loopback); TCP KeepAlive parameters are set via `SIO_KEEPALIVE_VALS` on .NET Framework. CI runs all tests on Ubuntu (net8/net10) and Windows (net48).

## Dependencies

- [System.IO.Pipelines](https://www.nuget.org/packages/System.IO.Pipelines)
- [System.Threading.Channels](https://www.nuget.org/packages/System.Threading.Channels)
- [Microsoft.Extensions.Logging.Abstractions](https://www.nuget.org/packages/Microsoft.Extensions.Logging.Abstractions) (logging abstractions only — no logging framework is pulled in)

## Tests & samples

```bash
dotnet build StreamFrame.slnx
dotnet test
dotnet run --project samples/StreamFrame.Demo          # 5-scenario end-to-end demo
dotnet run -c Release --project bench/StreamFrame.Benchmarks   # benchmarks (~5-10 min)
dotnet test -f net8.0 --collect:"XPlat Code Coverage"  # coverage (CI also collects & summarizes)
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
