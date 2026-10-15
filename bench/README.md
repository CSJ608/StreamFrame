# StreamFrame 基准测试

[English](README.en.md)

## 复现

从仓库根目录运行（.NET 10 SDK）：

```powershell
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --verify-network
# 正式采样：逐个运行，不并行。每类36组，共72组。
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --filter '*OneWayThroughputBenchmarks*' --warmupCount 3 --iterationCount 10 --launchCount 1 --iterationTime 250 --outliers DontRemove --exporters json --artifacts bench/results/local-oneway
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --filter '*RoundTripLatencyBenchmarks*' --warmupCount 3 --iterationCount 10 --launchCount 1 --iterationTime 250 --outliers DontRemove --exporters json --artifacts bench/results/local-rtt
# 独立微基准（不可直接与网络结果相减）
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --filter '*CodecBenchmarks*'
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --filter '*FramingBenchmarks*'
dotnet run -c Release --project bench/StreamFrame.Benchmarks -- --filter '*MetricsOverheadBenchmarks*'
```

先用 `--list flat` 核对类名。无筛选会运行全部基准，耗时取决于机器、矩阵与 BDN 自适应 pilot，不再承诺“5–15分钟”。`--verify-network` 是正确性检查，不是性能采样：72组每组连续两批，检查跨调用完成点与完整内容。

## 对照契约

| 项目 | 两种传输共同执行的工作 |
|---|---|
| 场景 | `OneWayThroughputBenchmarks` 不回显；`RoundTripLatencyBenchmarks` 每条等待完整回显 |
| 参数 | LengthPrefix/StxEtx × 64/1024/65536 字节 × Bytes/StringSpan/StringAlloc × DirectTcp/StreamFrame |
| 负载 | Setup预建、重复使用的ASCII `x`；StxEtx负载不含定界符；线上帧额外4/2字节 |
| 发送 | 每批256条，逐条调用并await发送；都在测量内运行同一个Codec及流式Framer，无预编码帧捷径 |
| 接收 | 都实际定界、生成拥有数据的byte[]或string，并逐字节/字符校验完整内容 |
| 单向完成 | 服务端完成256条解码和校验；客户端SendAsync仅入队不算完成 |
| 往返完成 | 服务端解码校验后重新编码回显；客户端解码校验后才发送下一条；256次RTT |
| 生命周期 | 连接/预建负载在GlobalSetup；每批30秒期限，失败取消并观察工作，不复用失败批次；GlobalCleanup释放 |
| Socket | 同机IPv4回环，接收64KiB，NoDelay=false（Nagle开启），KeepAlive=false，发送缓冲用同机OS默认；本次读回两侧均64KiB |

**仍有差异**：DirectTcp省略队列、Pipe、重连与通用流重组，已知固定帧长，用可复用数组`ReadExactlyAsync`逐帧读取后调用Framer/Codec；每帧使用同一个池化writer类型，但初始容量直接取已知帧长。StreamFrame使用有界队列、后台worker、Pipe批量接收与自适应池化发送缓冲。相同应用工作与最终完成点不代表相同调度、系统调用或缓冲工作。两者差值是这两个实现的场景差值，不能精确隔离“框架自身成本”，也不能推断普遍快于裸TCP。RTT与单向不能交叉相减。

## Codec与分配

- `Bytes`：编码`writer.Write(byte[])`仍复制数据；解码`ToArray()`分配并复制拥有独立数据的消息。
- `StringSpan`：编码UTF-8 span直写；解码生成UTF-16字符串。本实验ASCII负载的字符数据约占线上负载2倍，非所有文本的通用比例。
- `StringAlloc`：同样物化字符串，编码额外生成UTF-8中间数组并复制到writer。差别包含真实工作，不能统称框架税。
- `CodecBenchmarks`独立测XML：构建/序列化XDocument与字节消息不同，不从网络结果减去XML微基准来推导框架成本。
- `LargeMessageStringBenchmarks`、`LargeMessageByteArrayBenchmarks`保留旧的无回显codec探索（批量10000、覆盖不一致）；`SessionAwareSendBenchmarks`等会话实验为独立问题。不要与本次256条对照拼表归因，实际类名用`--list flat`查看。

BDN `Mean`和`Allocated`已按`OperationsPerInvoke=256`折算：单向每条，往返每次完整RTT（含两端工作）。本次.NET 10上的MemoryDiagnoser使用[进程累计托管分配计数](https://github.com/dotnet/BenchmarkDotNet/blob/v0.15.8/src/BenchmarkDotNet/Engines/GcStats.cs)，包括异步线程、双方连接与测试驱动；不是单线程分配、存活堆、原生Socket内存或网络复制次数。池化减少托管分配不代表没有复制，也不能由一列Allocated推出框架零分配。

## 结果与限制

本轮机器、源码SHA、完整BDN日志（含逐迭代值、GC、异常、噪声提示）、JSON/CSV/Markdown汇总与执行命令见 [issue-70原始结果](results/issue-70/README.md)。Dry仅证明能跑，不用于结论。正式采样保留离群点；单次launch不能证明跨时段稳定差值，误差区间重叠或漂移不能包装成精确百分比。开发笔记本单机回环不代表真实网络、其他CPU、OS或运行时。

撤回旧文档中快13–63%、大报文3–4倍、字节消息仅多20–30%及“框架自身零分配”等结论：旧端到端吞吐带服务端回显，而裸TCP只计字节；另一个大报文Codec实验确实不回显，但仍多了消息物化等工作，不能与仅读字节的对照跨实验相减推出纯框架税。历史测量不一定数值错误，但工作不匹配且没有随仓库保存可审计原始记录，不能作为本次结论证据。旧的微基准百分比与会话成本摘要也不再作为当前承诺；可用各自类重新测量。

单位勘误：历史发送指标2.6ns/消息 × 100000消息/s = **260µs/s = 0.26ms/s**，约占一个CPU核每秒时间的0.026%，不是0.026µs/s。此处仅纠正算术，不声称本轮重测了指标开销。

“流式零拷贝”仅指省去从独立负载缓冲整体复制到帧缓冲的那一次复制；Codec写入、池扩容、Socket/内核传输和接收消息物化仍可能复制/分配。`FramingBenchmarks`只测帧路径，不能外推端到端零拷贝或统一百分比收益。
