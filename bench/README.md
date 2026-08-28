# StreamFrame 基准测试

[BenchmarkDotNet](https://benchmarkdotnet.org/) 基准，为 README 的性能说法提供可复现数据。

## 运行

```bash
dotnet run -c Release --project bench/StreamFrame.Benchmarks                       # 全部（约 15 分钟）
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --filter "*EndToEnd*"   # 只跑端到端
```

> 该版本的 BenchmarkDotNet 不支持重复 `--filter`（一次只能一个模式），多个类请分次运行。
> 基准项目已纳入解决方案（IDE 可见）；`dotnet test` 依 `IsTestProject` 机制自动跳过。

## 测什么

- **帧定界微基准**（`FramingBenchmarks`）：`Streaming`（`BeginFrame → 写负载 → EndFrame`，单缓冲，发送侧默认路径）vs `Plain`（负载先进缓冲 A，`EncodeFrame` 再整体拷贝进帧缓冲 B）× 64B/1KB/64KB；以及切帧吞吐（100 × 1KB 粘包）。
- **Codec 基准**（`CodecBenchmarks`）：官方 XML 驱动 `XmlDocumentCodec` 对典型设备报文（~400B 单值上报 / ~4KB 批量明细）的编解码开销。
- **端到端基准**（`EndToEndBenchmarks`）：真实 TCP 回环上的完整连接（收发队列 + Pipe + 定界 + codec + socket）——单向吞吐（连发 1 万条 1KB 消息，双 framer）与往返延迟（逐条乒乓 2000 次）。

## 结果（2026-08-28 重跑：StxEtx 向量化 + 内置指标已接入；Codec 表为历史数据）

| 方法 | 负载 | 耗时 | 分配 |
|---|---|---:|---:|
| LengthPrefix_Plain | 64B | 28.1 ns | 64 B |
| LengthPrefix_Streaming | 64B | **21.4 ns（-24%）** | **32 B（-50%）** |
| StxEtx_Plain | 64B | 28.6 ns | 32 B |
| StxEtx_Streaming | 64B | **19.1 ns（-33%）** | 32 B |
| LengthPrefix_Plain | 1KB | 45.5 ns | 64 B |
| LengthPrefix_Streaming | 1KB | 56.3 ns（+24%）¹ | **32 B（-50%）** |
| StxEtx_Plain | 1KB | 60.9 ns | 32 B |
| StxEtx_Streaming | 1KB | 68.3 ns（+12%）¹ | 32 B |
| LengthPrefix_Plain | 64KB | 3.44 µs | 64 B |
| LengthPrefix_Streaming | 64KB | 3.47 µs（持平） | **32 B（-50%）** |
| StxEtx_Plain | 64KB | 4.51 µs | 32 B |
| StxEtx_Streaming | 64KB | 4.64 µs（+3%） | 32 B |
| LengthPrefix 切帧 100×1KB | — | **0.82 µs**（≈8.3 ns/帧） | 0 |
| StxEtx 切帧 100×1KB | — | **≈5.0 µs**（≈50 ns/帧，向量化后提速约 22 倍） | 0 |

¹ 恰好压在 `EncodeBufferInitialSize`（默认 1024）的缓冲增长边界上：流式路径触发一次扩容租借，
抵消了省下的那次拷贝。负载远大于初始缓冲后两条路径的拷贝量趋同（几何扩容摊销）。

### Codec（XmlDocumentCodec）

| 操作 | 报文 | 耗时 | 分配 |
|---|---|---:|---:|
| Decode | ~400B | 3.0 µs | 16.7 KB |
| Decode | ~4KB | 15.6 µs | 29.0 KB |
| Encode | ~400B | 1.9 µs | 7.8 KB |
| Encode | ~4KB | 10.9 µs | 10.4 KB |

### 端到端（真实 TCP 回环，1KB 文本消息）

| 指标 | LengthPrefix | StxEtx |
|---|---:|---:|
| 单向吞吐（每消息折算） | 6.21 µs ≈ **16.1 万条/秒** | 6.05 µs ≈ 16.5 万条/秒 |
| 往返延迟（乒乓串行） | 50.8 µs | 52.4 µs |
| 每消息堆分配（吞吐模式） | 6.5 KB | 6.9 KB |

测试环境：Windows（本地开发机），.NET 8，BenchmarkDotNet 0.15.8，SimpleJob(warmup 3 / iteration 15；e2e 为 2/10)。
数值仅供量级参考，请以自己环境的复现为准。

## 读数要点

- **流式编码的实测收益主要是"每帧堆分配减半"（一个缓冲对象 vs 两个）**，小负载（≤ 百字节）耗时也快 25–35%；
  大负载下省掉的那次 memcpy 被缓冲增长摊销，耗时基本持平。"零拷贝"指的是帧路径不再整体搬运负载，不是字面意义的零成本。
- **切帧吞吐相差约两个数量级**：`LengthPrefixFramer` 读长度头直接跳转（≈10 ns/帧）；
  `StxEtxFramer` 在 net8+ 用 `SearchValues` 向量化定位边界（≈50 ns/帧；netstandard2.0 目标回退逐字节实现）。长度前缀在超大帧上仍占优，STX/ETX 适合既有协议兼容。
