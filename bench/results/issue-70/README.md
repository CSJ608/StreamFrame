# Issue #70 — matched network workloads / 同工作量网络基准

[中文方法说明](../../README.md) · [English methodology](../../README.en.md)

## Provenance / 来源

- Measurement source / 测量源码：[e6960285642e32e505f179506c426311fef9dd15](https://github.com/CSJ608/StreamFrame/commit/e6960285642e32e505f179506c426311fef9dd15), based on main `297450f`. Later commits only archive results/documentation; no production library changes.
- 2026-09-08 Asia/Shanghai: one-way 18:06:53 (3m28s total); RTT 18:10:24 (2m26s total), sequential. 72/72 cases, each 10 retained measurements. Correctness check: all 72 cases, two consecutive batches of 256; BDN Dry: 36 one-way cases, **not performance evidence**.
- Intel Core i5-12500H (12 physical/16 logical cores), 32GB RAM (OS-visible 33341944KiB), Windows 10 19045.6466 x64, SDK 10.0.400, .NET 10.0.11, BDN 0.15.8. Both endpoints in one process on IPv4 loopback. Socket readback: receive/send=65536, NoDelay=false, KeepAlive=false.
- 1 launch, 3 warmups, 10 measurements, target iteration time 250ms; adaptive pilot determines invocation count; 256 operations/invocation, outliers **not removed**. Development laptop, no concurrent agent builds/tests during sampling; other desktop/OS load, temperature and frequency were not controlled.
- `Allocated` is process-wide managed allocation per message (one-way) or complete RTT (both endpoints), including harness/async work; it excludes native memory and is not live memory or copy counts.

## Evidence / 原始记录

- [One-way complete table](oneway/results/StreamFrame.Benchmarks.OneWayThroughputBenchmarks-report-github.md), [raw JSON with individual values](oneway/results/StreamFrame.Benchmarks.OneWayThroughputBenchmarks-report-full-compressed.json), [BDN log](oneway/StreamFrame.Benchmarks.OneWayThroughputBenchmarks-20260908-180653.log).
- [RTT complete table](rtt/results/StreamFrame.Benchmarks.RoundTripLatencyBenchmarks-report-github.md), [raw JSON with individual values](rtt/results/StreamFrame.Benchmarks.RoundTripLatencyBenchmarks-report-full-compressed.json), [BDN log](rtt/StreamFrame.Benchmarks.RoundTripLatencyBenchmarks-20260908-181024.log).
- CSV and HTML exports are alongside each table. Console logs include the invocation parameters. [Environment](environment.txt), [validation](result-validation.txt), [correctness log](verification.log), [first failed check](verification-failed-1.log), [build](build.log), [test](test.log).
- [run.ps1](run.ps1) reproduces the sequential formal commands after a Release build; default output is a fresh `bench/results/local-parity` directory. The archived execution used `bench/results/issue-70` as the output root. See method guides for the exact CLI flags and separate `--verify-network`/`--job Dry` commands. Do not overwrite archived evidence.

## What this run supports / 本轮支持的结论

单向小消息噪声明显：LengthPrefix/Bytes/64B 的 DirectTcp 均值18.57µs、BDN Error ±15.927µs，StreamFrame 23.48µs、±19.548µs，不能据均值排名。单向有最短迭代71.947ms提示，RTT有98.815ms提示（低于BDN建议100ms）；完整警告及离群点保留。250ms是目标而非保证，pilot与实际阶段漂移可改变迭代时长。

Small one-way payloads are noisy: LengthPrefix/Bytes/64B measured 18.57µs with BDN Error ±15.927µs for DirectTcp, and 23.48µs ±19.548µs for StreamFrame. Ranking these means is unsupported. BDN warned about minimum iterations of 71.947ms (one-way) and 98.815ms (RTT), below its suggested 100ms. Full warnings and outliers are retained; 250ms was a target, and drift after pilot changes actual iteration duration.

64KiB单向字节消息在本次均值中StreamFrame更低（LengthPrefix 37.01µs vs 62.63µs；StxEtx 50.70µs vs 68.69µs），字符串与RTT方向/幅度不同。此为单次实现对照观察，不是跨时段稳定优势，不外推“快于裸TCP”，不计算稳定百分比。接收批处理、预知帧长、缓冲与调度仍有差异；不能将差值称为纯框架税。

For 64KiB one-way byte messages, this run's StreamFrame means were lower (LengthPrefix 37.01µs vs 62.63µs; StxEtx 50.70µs vs 68.69µs); string and RTT comparisons differ in direction/magnitude. This is one implementation comparison, not a stable cross-session advantage or evidence of being generally faster than raw TCP. No stable percentage is claimed. Receive batching, known frame length, buffers and scheduling still differ, so subtraction does not isolate framework tax.

分配量与工作内容相符：下表是64KiB单向LengthPrefix，每消息托管字节，包含测试驱动。StxEtx完整表方向一致。字符串span比字节数组多约64KiB，分配式编码再多约64KiB；RTT约执行双份物化/编码。这里支持解释消息表示与中间数组的分配来源，不能从单次计数证明框架零分配，耗时也不保证span总是更快。

Allocation agrees with the actual work. The following 64KiB one-way LengthPrefix rows include the harness; the complete StxEtx table has the same allocation ordering. StringSpan adds about 64KiB over Bytes, and StringAlloc adds about another 64KiB. RTT performs roughly twice the materialization/encoding work. This supports the representation/intermediate-array explanation, not zero framework allocation or a guarantee that span is always faster in elapsed time.

| Codec | DirectTcp allocated B/message | StreamFrame allocated B/message |
|---|---:|---:|
| Bytes | 65,645 | 66,163 |
| StringSpan | 131,351 | 132,402 |
| StringAlloc | 196,750 | 198,212 |

没有证据确立跨launch稳定的时间差值。未重复采样追求精确百分比；本轮验收交付的是一致工作量、可追溯结果及诚实边界，不是普适性能保证。XML/帧/指标/会话历史微基准未在本轮重跑，也不与这些结果相减。

No cross-launch stable timing difference is established. Sampling was not repeated to chase a precise percentage. This deliverable establishes matched application work, traceable observations and their limits, not universal performance guarantees. XML/framing/metrics/session microbenchmarks were not rerun and are not subtracted from these results.

Generated evidence hashes: [SHA256SUMS.txt](SHA256SUMS.txt). Raw exports retain BDN whitespace; .gitattributes disables newline conversion in this archive.
