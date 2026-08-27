# StreamFrame 基准测试

[BenchmarkDotNet](https://benchmarkdotnet.org/) 基准，为 README 的性能说法提供可复现数据。

## 运行

```bash
dotnet run -c Release --project bench/StreamFrame.Benchmarks
```

（约 5–10 分钟；`--filter` 可只跑部分用例。）

## 测什么

- **编码两路径对比**：`Streaming`（`BeginFrame → 写负载 → EndFrame`，单缓冲，发送侧默认路径）vs `Plain`（负载先进缓冲 A，`EncodeFrame` 再整体拷贝进帧缓冲 B，两段缓冲含一次 memcpy）。负载为 64B / 1KB / 64KB 的 XML 风格文本。
- **切帧吞吐**：从 100 × 1KB 粘包缓冲中循环 `TryDecodeFrame`。

## 结果

| 方法 | 负载 | 耗时 | 分配 |
|---|---|---:|---:|
| LengthPrefix_Plain | 64B | 36.3 ns | 64 B |
| LengthPrefix_Streaming | 64B | **27.3 ns（-25%）** | **32 B（-50%）** |
| StxEtx_Plain | 64B | 36.7 ns | 32 B |
| StxEtx_Streaming | 64B | **24.4 ns（-34%）** | 32 B |
| LengthPrefix_Plain | 1KB | 57.6 ns | 64 B |
| LengthPrefix_Streaming | 1KB | 67.3 ns（+17%）¹ | **32 B（-50%）** |
| StxEtx_Plain | 1KB | 77.0 ns | 32 B |
| StxEtx_Streaming | 1KB | 82.6 ns（+7%）¹ | 32 B |
| LengthPrefix_Plain | 64KB | 4.11 µs | 64 B |
| LengthPrefix_Streaming | 64KB | 4.11 µs（持平） | **32 B（-50%）** |
| StxEtx_Plain | 64KB | 6.38 µs | 32 B |
| StxEtx_Streaming | 64KB | 6.04 µs（-5%） | 32 B |
| LengthPrefix 切帧 100×1KB | — | **1.0 µs**（≈10 ns/帧） | 0 |
| StxEtx 切帧 100×1KB | — | **≈110 µs**（≈1.1 µs/帧） | 0 |

¹ 恰好压在 `EncodeBufferInitialSize`（默认 1024）的缓冲增长边界上：流式路径触发一次扩容租借，
抵消了省下的那次拷贝。负载远大于初始缓冲后两条路径的拷贝量趋同（几何扩容摊销）。

测试环境：Windows（本地开发机），.NET 8，BenchmarkDotNet 0.15.8，SimpleJob(warmup 3 / iteration 15)。
数值仅供量级参考，请以自己环境的复现为准。

## 读数要点

- **流式编码的实测收益主要是"每帧堆分配减半"（一个缓冲对象 vs 两个）**，小负载（≤ 百字节）耗时也快 25–35%；
  大负载下省掉的那次 memcpy 被缓冲增长摊销，耗时基本持平。"零拷贝"指的是帧路径不再整体搬运负载，不是字面意义的零成本。
- **切帧吞吐相差约两个数量级**：`LengthPrefixFramer` 读长度头直接跳转（≈10 ns/帧）；
  `StxEtxFramer` 逐字节扫描定位边界（≈1.1 µs/帧）。高吞吐/大帧场景优先长度前缀，STX/ETX 留给既有协议兼容。
