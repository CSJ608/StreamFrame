# StreamFrame benchmarks

[中文](README.md)

## Reproduce

Run from the repository root with the .NET 10 SDK:

```powershell
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --verify-network
# Formal sampling: run sequentially. 36 cases per class, 72 total.
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --filter '*OneWayThroughputBenchmarks*' --warmupCount 3 --iterationCount 10 --launchCount 1 --iterationTime 250 --outliers DontRemove --exporters json --artifacts bench/results/local-oneway
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --filter '*RoundTripLatencyBenchmarks*' --warmupCount 3 --iterationCount 10 --launchCount 1 --iterationTime 250 --outliers DontRemove --exporters json --artifacts bench/results/local-rtt
# Independent microbenchmarks; do not subtract from network results.
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --filter '*CodecBenchmarks*'
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --filter '*FramingBenchmarks*'
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --filter '*MetricsOverheadBenchmarks*'
```

Use `--list flat` to inspect actual class names. Without a filter all benchmarks run; duration depends on the machine, matrix and BDN adaptive pilot, so there is no fixed 5–15 minute estimate. `--verify-network` checks correctness, not performance: two consecutive batches in each of 72 cases validate complete contents and completion across invocations.

## Comparison contract

| Item | Work shared by both transports |
|---|---|
| Scenario | `OneWayThroughputBenchmarks` never echoes; `RoundTripLatencyBenchmarks` waits for each complete echo |
| Parameters | LengthPrefix/StxEtx × 64/1024/65536 bytes × Bytes/StringSpan/StringAlloc × DirectTcp/StreamFrame |
| Payload | Reused ASCII `x` prepared in Setup; no delimiter bytes in StxEtx payload; wire overhead is 4/2 bytes |
| Send | 256 messages per batch, calling and awaiting each send; same Codec and streaming Framer inside measurement, no pre-encoded shortcut |
| Receive | Actual framing, owning byte[]/string materialization, and full byte/character content validation |
| One-way completion | Server decodes and validates all 256 messages; client SendAsync enqueue alone is insufficient |
| RTT completion | Server decodes/validates and re-encodes echo; client decodes/validates before sending the next message; 256 RTTs |
| Lifecycle | Connect and prebuild payload in GlobalSetup; 30-second batch deadline, cancel/observe failed work without reusing failed batches; release in GlobalCleanup |
| Socket | Same-machine IPv4 loopback, receive 64KiB, NoDelay=false (Nagle on), KeepAlive=false, OS-default send buffer; readback was 64KiB on both sides |

**Remaining differences**: DirectTcp omits queues, Pipe, reconnect and general stream assembly. It knows the fixed wire length, uses a reusable array with per-frame `ReadExactlyAsync`, then calls the actual Framer/Codec. It uses the same pooled writer type per send but starts with the known frame size. StreamFrame uses bounded queues, background workers, batched Pipe reception and adaptive pooled send buffers. Matching application work and final completion does not make scheduling, system calls or buffering identical. The difference describes these two implementations in this scenario, not an isolated framework tax or a general advantage over raw TCP. Do not subtract RTT and one-way measurements.

## Codec and allocation

- `Bytes`: `writer.Write(byte[])` still copies; decode uses `ToArray()` to allocate/copy an owning message.
- `StringSpan`: UTF-8 span encoding, UTF-16 string materialization on decode. Character data is about twice the wire payload for this ASCII experiment, not a universal text ratio.
- `StringAlloc`: same string materialization plus a temporary UTF-8 encoding array copied into the writer. These are real workload differences, not all framework cost.
- `CodecBenchmarks` measures XML independently: XDocument construction/serialization differs from byte messages. Subtracting XML microbenchmarks from network timings does not isolate framework cost.
- `LargeMessageStringBenchmarks` and `LargeMessageByteArrayBenchmarks` retain the older no-echo codec exploration (10000-message batches, different coverage); session experiments such as `SessionAwareSendBenchmarks` answer separate questions. Do not combine them with this 256-message comparison for attribution; inspect actual names with `--list flat`.

BDN `Mean` and `Allocated` are divided by `OperationsPerInvoke=256`: per message for one-way, per complete RTT for echo (including both endpoints). On .NET 10, MemoryDiagnoser uses the [process-wide cumulative managed allocation counter](https://github.com/dotnet/BenchmarkDotNet/blob/v0.15.8/src/BenchmarkDotNet/Engines/GcStats.cs), including asynchronous threads, both connections and the harness. This is not thread-local allocation, live heap size, native Socket memory or copy counts. Pooling can reduce allocation without eliminating copies; Allocated alone cannot establish zero framework allocation.

## Results and limitations

See the [issue-70 raw results](results/issue-70/README.md) for machine, source SHA, complete BDN logs (individual iterations, GC, errors and noise warnings), JSON/CSV/Markdown reports and commands. Dry only establishes execution, not performance. Formal sampling retains outliers. One launch cannot establish stable differences across sessions; overlapping uncertainty or drift must not become precise percentage claims. A development laptop loopback run does not represent real networks, other CPUs, OSes or runtimes.

The former claims of 13–63% faster small messages, 3–4× large-message cost, only 20–30% byte-message overhead and zero framework allocation are withdrawn: they subtracted echoing throughput from a byte-count-only TCP control. Historical numbers are not necessarily wrong, but workloads differ and auditable raw records were not checked into this repository. Earlier microbenchmark percentages and session-cost summaries are also no longer current promises; rerun their individual classes as needed.

Unit correction: the historical 2.6ns/message × 100000 messages/s = **260µs/s = 0.26ms/s**, about 0.026% of one CPU core's second, not 0.026µs/s. This corrects arithmetic; metrics overhead was not remeasured here.

“Streaming zero-copy” means removing one whole-payload copy from a separate payload buffer into a frame buffer. Codec writes, pool growth, Socket/kernel transfer and received-message materialization can still copy/allocate. `FramingBenchmarks` measures the frame path only, not end-to-end zero-copy or a universal percentage improvement.
