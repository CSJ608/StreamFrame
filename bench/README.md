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

## 结果

### 测量环境（2026-08-29，宏基准为两轮区间）

- CPU：12th Gen Intel Core i5-12500H（笔记本）；内存 32GB；Windows 10 19045；.NET 10.0.11
- BenchmarkDotNet SimpleJob：微基准 warmup 3 / iteration 15，宏基准 warmup 2 / iteration 10；真实 TCP 回环
- **噪声披露**：测量在长时间高负载的开发机上完成，宏基准（回环端到端）绝对值两轮漂移可达 ±30%，下表以区间标注、仅供量级参考；**关键差值（会话发送成本、接收视图、超时开销）经第三轮复核稳定**。微基准与差值结论可信度高。请以自己环境的复现为准。

### 内置指标开销（微基准，无监听者的生产默认态）

| 调用 | 耗时 | 分配 |
|---|---:|---:|
| 单次计数/直方图记录 | 0.5–1.0 ns | 0 |
| 接收路径每消息合计（字节块 + 帧） | ≈1.6 ns | 0 |
| 发送路径每消息合计（入队采样 + 字节块 + 帧） | ≈2.6 ns | 0 |

结论：不订阅 Meter 时指标开销为**亚纳秒到个位纳秒级、零分配**——按 10 万条/秒的流量折算，发送路径合计 ≈0.026 µs/秒，完全可忽略。

### 帧定界（微基准，2026-08-28 数据仍有效）

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

### 端到端单向吞吐（真实 TCP 回环，1 万条/轮，两轮区间）

| Framer | 64B | 1KB | 64KB |
|---|---:|---:|---:|
| LengthPrefix | 4.1–4.7 µs/条 ≈ 21–24 万条/秒 | 4.1–7.7 µs/条 ≈ 13–24 万条/秒 | 134–188 µs/条 ≈ 0.53–0.75 万条/秒 |
| StxEtx | 2.2–6.9 µs/条 ≈ 15–46 万条/秒 | 3.8–8.7 µs/条 ≈ 12–26 万条/秒 | 127–157 µs/条 ≈ 0.64–0.79 万条/秒 |
| 每消息堆分配（LengthPrefix） | 0.8 KB | 6.7 KB | 394–409 KB |

### 裸 TCP 地板（同口径回环：预生成帧字节逐条 NetworkStream.Write，两轮区间）

| 负载 | 地板 | StreamFrame（LengthPrefix） | 框架税（绝对 / 百分比并列） |
|---|---:|---:|---|
| 64B | 10.1–13.0 µs/条 | 4.1–4.7 µs/条 | **-6~-8 µs/条（快 48–63%）**² |
| 1KB | 9.4–16.0 µs/条 | 7.4–7.7 µs/条 | **-2~-8 µs/条（快 13–52%）**² |
| 64KB | 45–50 µs/条 | 134–188 µs/条³ | **+89~138 µs/条（慢 198~276%）**³ |

² 小消息下框架反而**快于**裸 NetworkStream 逐条 Write 的写法：StreamFrame 的有界队列把
"基准线程的逐条入队"与"worker 的 socket 写出"解耦成流水线，而裸写法把每次内核写串行在
发送线程上。框架的真实代价体现在 64KB 大消息：每条约 +0.1 ms 与 ~400 KB 分配（编码缓冲
几何扩容 + 池租借未命中）。³ 该行使用"分配式 GetBytes 的字符串 codec + 服务端回显"，归因表明其中大头是 codec 与消息类型成本（见"大报文归因"）：byte[] 负载 + 透传 codec 时框架距地板仅 59–73 µs（≈20–45%）。

### 大报文归因（64KB，计数消费无回显，2026-08-29，含自适应发送缓冲）

| 变体 | 每消息耗时 | 每消息分配 | 归因 |
|---|---:|---:|---|
| byte[] 消息 + 透传 codec | 59–73 µs | 64 KB | **框架纯成本基线**：分配仅为解码侧 ToArray（codec 固有），框架自身零分配；耗时较裸 TCP 地板（45–50 µs）高 ≈20–30% |
| string 消息 + span 直写 codec | 70–104 µs（误差大，见噪声披露） | 129 KB | +64 KB = **消息类型固有税**（UTF-8 → UTF-16 string 物化， unavoidable for TMessage=string） |
| string 消息 + 分配中间数组的 GetBytes（旧写法） | 73–92 µs | 193 KB | 再 +65 KB = **codec 写法税**（GetBytes 中间数组，可用 span 重载消除） |

结论：64KB 场景的"框架税"大部分是 codec 写法与消息类型的成本——byte[] 负载 + 透传/低拷贝
codec 时框架距裸 TCP 仅 ≈20–30%。使用建议见 README"大报文指南"。

### 会话感知与新特性开销（三轮，1KB LengthPrefix，同上口径）

| 对比 | 基线 | 变体 | 结论 |
|---|---:|---:|---|
| 发送方式 | SendAsync 13.2–17.2 µs/条 | SendInSessionAsync 24.7–31.6 µs/条 | **成本 ≈ +10~18 µs/条（中位 +11）**，分配 +≈470 B/条（信封 + 完成源 + 注册表；两轮区间内 SendInSession 误差棒较大，三轮方向与量级一致） |
| 接收视图 | GetMessages 13.7–15.8 µs/条 | GetSessionMessages 12.7–15.8 µs/条 | **无可测差异**（同一通道，信封解包平凡）；分配同为 ≈3.4 KB/条 |
| 未完成帧超时 | 关闭 13.5–20.9 µs/条 | 开启未触发 13.3–18.5 µs/条 | **无可测差异**（默认关闭为真零开销；开启后仅在半帧等待时才有计时令牌开销） |

### 往返延迟（乒乓串行，两轮区间）

| Framer | RTT |
|---|---:|
| LengthPrefix | 71.8–73.1 µs |
| StxEtx | 65.6–73.8 µs |

### Codec（XmlDocumentCodec，历史数据）

| 操作 | 报文 | 耗时 | 分配 |
|---|---|---:|---:|
| Decode | ~400B | 3.0 µs | 16.7 KB |
| Decode | ~4KB | 15.6 µs | 29.0 KB |
| Encode | ~400B | 1.9 µs | 7.8 KB |
| Encode | ~4KB | 10.9 µs | 10.4 KB |

## 读数要点

- **框架税是分尺寸的**：小消息（≤1KB）下有界队列的流水线让 StreamFrame 反而快于"裸 NetworkStream 逐条 Write"的对照写法；大消息（64KB）真实代价显现（≈3–4×、每条 ~400 KB 分配）——高吞吐大报文是后续优化方向（编码缓冲按负载预扩容 / 池化命中率）。
- **会话感知发送的诚实成本**：`SendInSessionAsync` 约为普通发送的 2 倍耗时、+≈470 B/条分配（信封、完成源、注册表、状态字 CAS）。对协议计时器语义（T3/T6 从整帧写出起算）而言通常值得，但高频小消息场景若不需要会话语义，用 `SendAsync`。
- **内置指标可以常开**：无监听时 0.5–1.0 ns/次、零分配，接收/发送路径每消息合计 ≈1.6/2.6 ns。
- **新特性默认关闭即零开销**：未完成帧超时关闭时无可测差异；接收视图切换也无成本。
- **流式编码的实测收益主要是"每帧堆分配减半"**（一个缓冲对象 vs 两个），小负载耗时也快 24–33%；大负载下省掉的那次 memcpy 被缓冲增长摊销。"零拷贝"指帧路径不再整体搬运负载，不是字面意义的零成本。
- **切帧吞吐**：`LengthPrefixFramer` 读长度头直接跳转（≈8.3 ns/帧）；`StxEtxFramer` 在 net8+ 用 `SearchValues` 向量化定位边界（≈50 ns/帧；netstandard2.0 目标回退逐字节实现）。
- **回环数据的边界**：以上宏基准全部来自同机回环，仅代表同机进程通信的量级；真实网络的延迟/带宽会改变各分量的占比。复现命令见文首。
