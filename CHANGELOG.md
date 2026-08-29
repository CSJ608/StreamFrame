# Changelog

本项目版本变更记录，格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 新增
- **NativeAOT/裁剪兼容性门禁**：`samples/StreamFrame.AotSmoke` 冒烟工程（引用库的最小可运行路径，含会话感知发送）+ ci.yml 新增 `aot` job（ubuntu 上 PublishAot 发布，IL2026/IL3050 等警告经 TreatWarningsAsErrors 直接红掉；非必需检查，不影响分支保护）。本地 managed 侧已验证零裁剪/AOT 警告。
- README 双语新增 System.Text.Json 最简 codec 示例（span 直写、AOT 安全）。

### 变更
- 接收 Pipe 段尺寸对齐 `SocketReceiveBufferSize`（大报文整帧常驻单段，Kestrel 同款做法）；本机基准两轮 48.75µs / 93µs，效果在噪声内无法确立，保留改动（语义合理、98×3 测试无回归），待每夜 soak 与后续基准复检。
- CHANGELOG 修复：v2.5.0 归档时脚本锚点未匹配导致 2.5.0 内容错挂在 2.4.0 段落（Release 正文回退为自动生成）——已按两个 tag 的实际内容拆分还原（监听加固归 2.4.0，代次门控/大报文/观察基建归 2.5.0）。

### 修复
- 暂无

## [2.5.0] - 2026-08-29

### 新增
- **重连竞速长期观察基建**：`StateTransitionRecorder`（状态机合法转移、非 Connected 态编号归零、Connected 态编号严格递增、终态收敛的全程校验 + Connected→Connected 直连迁移计数）、`Soak_ReconnectRacing_LongRun` 竞速专用混沌（60% 动作并发竞速 + FIFO 完整性探针）、每夜 600s 双平台 soak CI（`.github/workflows/soak.yml`，北京时间 03:00，失败自动通知）。

### 变更
- **大报文优化与归因**：发送编码缓冲自适应初始尺寸（按连接记忆上一帧高水位、封顶 1MB——稳态尺寸相近的协议不再从 1KB 起爬几何增长梯子）。新增 64KB 三 codec 归因基准（byte[] 透传 / string+span / string+分配式），结论：框架纯成本距裸 TCP 仅 ≈20–30%，其余为 codec 写法税（span 重载可消除中间数组）与 string 消息固有税（UTF-16 物化 ≈2× 报文）。README 双语新增"大报文指南"，bench/README 附归因表。

### 修复
- **接受循环代次门控（#47 阶段二，竞速泄漏监听器 bug）**：用户重连与自动重连竞速产生多个并发 StartAsync 接受循环时，被取代的"僵尸"循环完成 accept 后可能看到 `_server` 已被新循环替换而跳过单客户端的监听器关闭——**泄漏一个仍在监听但永无人 accept 的 socket**，客户端 SYN 进入无人消费的 backlog 后 `ConnectAsync` 永久挂起（远比"拒绝"严重）。新增 `_acceptLoopId` 代次：每次 StartCore 递增，被取代的循环在取得 `_acceptLock` 后静默退出，监听器的创建/关闭/accept 只归属当代循环。由新增的重连竞速混沌（`Soak_ReconnectRacing_LongRun`，60% 动作占比并发竞速）复现并锁定。

## [2.4.0] - 2026-08-29

### 新增
- **内置指标（Meter `StreamFrame`，`System.Diagnostics.Metrics`，零外部依赖）**：帧/字节收发计数（`streamframe.frames_sent/received`、`bytes_sent/received`）、重连次数（`reconnects`）、会话时长（`session_duration` 直方图）、发送队列水位（`send_queue_length` 直方图，入队采样）；单一 `endpoint` 标签，订阅方式与仪器清单见 README"内置指标"。netstandard2.0 目标经 `System.Diagnostics.DiagnosticSource` 包获得同款 API（netfx 可用）。
- **浸泡/混沌测试套件（默认不运行）**：`STREAMFRAME_SOAK_SECONDS=<秒> dotnet test --filter FullyQualifiedName~Soak` 手动启用——长时间消息流完整性（顺序/无丢失/无重复）与随机故障混沌（对端 FIN/RST 死亡 + 半帧注入 + 绑定/普通发送突发，终局核对无悬挂、普通消息跨会话续发送达、绑定消息无重复投递）。已在本机通过 20s/60s 两档验证。

### 变更
- **`StxEtxFramer` 切帧向量化**：net8+ 用 `SearchValues` 一次跳到最近的 STX/ETX 候选（不含候选的整段字节直接跳过），实测 ≈1.1 µs/帧 → ≈50 ns/帧（约 22 倍）；netstandard2.0 目标保留逐字节实现，语义逐字节等价（两套独立实现，现有 StxEtx/FrameDecoder 测试全绿）。README/bench 性能数据以 2026-08-28 重跑刷新（含内置指标的真实路径）。
- 会话纪元（epoch）改为在 Connected 发布点分配（与会话编号同点），彻底闭合"发布后、任务未创建"窗口内旧纪元幽灵故障卷入新生会话的瞬态误杀（v2.3.0 评审附带发现）；发布窗口注入幽灵的回归测试锁定该行为。
- demo 新增场景 5：会话感知收发（编号生命周期、整帧写完才完成、旧会话发送失效不重放、新会话照常收发）。
- **基准矩阵扩容 + 性能文档重写**：新增内置指标开销微基准（0.5–1.0 ns/次、零分配）、裸 TCP 地板基准（框架税：小消息反而快于裸写 13–52%，64KB 约 3–4×）、会话感知/接收视图/未完成帧超时的三轮对比（SendInSessionAsync ≈2× SendAsync、+≈470 B/条；其余无可测差异）；端到端吞吐参数化到 64B/1KB/64KB 两轮区间。bench/README 全量刷新（环境、噪声披露、框架税绝对+百分比并列），README 双语性能摘要同步。

### 修复
- **被动端监听韧性加固（#47，防御性）**：排查确认原始"用户 `Reconnect()` 竞速楔死监听"**无法复现**（顺序/并发两组确定性复现 + 混沌套件恢复该动作后多轮全绿，此前观察到的楔死系当时测试自身缺陷），但修复排查中识别的两个真实弱点——① 接受循环的重试延迟改为锁外等待（旧实现整个重试循环持有 `_acceptLock`，绑定/接受持续失败期间会堵死其它接受循环）；② 监听 socket 设置 `SO_REUSEADDR`（服务端主动关闭后立即重绑不受 2MSL/TIME_WAIT 限制；Windows 实测宽松，Linux 上无此选项会遇 `EADDRINUSE`，一并设置保持跨平台一致）。新增确定性回归测试（8 轮服务端主动重连，端口须秒级可重新接受），混沌套件恢复用户重连动作。

## [2.3.1] - 2026-08-28

### 修复
- **迟到的过期故障重连污染活会话**（v2.3.0 评审 P1-1）：垂死旧会话的迟到故障（epoch 已被替换，如 net48 线程池慢导致第二个 `ScheduleReconnect` 延迟到达）此前会在 epoch 校验之前发布 `Retry` 并把 `CurrentSessionId` 归零——完全存活的会话被谎报为重连中、`WaitForConnectedAsync` 悬挂、活会话编号被误判失效。现改为过期故障在任何对外发布之前整体丢弃（gate 内权威复查保留）。回归测试以反射调度过期故障验证。
- **残留的旧会话绑定条目可能在新会话 socket 上错发并报告成功**（v2.3.0 评审 P1-2，"绝不跨会话重放"保证的漏洞）：`Connected→Connected` 直连拆除（双 `StartAsync` 竞速/Connected 回调内 `Reconnect()` 重入）路径下，旧会话排队条目既不被清扫也无编号校验，会被新 worker 发到新 socket 并以成功收尾。现发送 worker 认领时校验条目 `SessionId`，不属当前会话的绑定条目一律以 `SessionExpiredException` 失败、绝不发送。压测改为按帧内容对账（跨会话错发探测器）。
- 发送失败类型收敛（评审 P2-5/P2-6）：停机时清扫先于发送通道完成（队满等待中的条目统一以 `SessionExpiredException` 收尾而非偶发 `ChannelClosedException`）；worker 写出失败时 internal 的 `SessionFaultException` 与 `ObjectDisposedException` 统一映射为 `SessionExpiredException`，不再向调用方泄漏非文档类型。
- `Reconnect()` 在 `Start()` 之前调用由 NRE 改为明确的 `InvalidOperationException`（存量问题顺手修复）。
- 未完成帧超时关闭（默认）时不再每读循环拷贝半帧快照（评审 P2-1，2.2.x 路径的小开销）。
- `SendInSessionAsync` 注册竞态窗口的失败以条目携带的同一异常收尾，不再留下未观察的任务异常（评审 P2-4）。

## [2.3.0] - 2026-08-28

### 新增
- **会话感知收发（`ISessionAwareStreamConnection<TMessage>` 可选能力接口，[#39](https://github.com/CSJ608/StreamFrame/issues/39)）**：为有严格会话边界的协议（HSMS 的 Select/T3/T6、禁止重放）提供——`CurrentSessionId`（每次 TCP 会话建立时分配、单调递增不复用，无会话时为 0；分配/归零先于对应状态对外发布，回调与 `WaitForConnectedAsync` 完成时读取必得一致值）、`SendInSessionAsync`（**整帧写入本机 socket 后才完成**；会话在写出前终止以 `SessionExpiredException` 失败且绝不转移到新会话重放；会话拆除时立即 fault 挂起条目，不空等重连；调用方取消的提交点 = worker 认领，认领后取消不撕裂帧）、`GetSessionMessages`（消息带所属会话编号的接收视图，与 `GetMessages` 为二选一的竞争消费视图）。发送队列内部信封化，`SendAsync`/`GetMessages` 现有语义一字不改。
- **未完成帧超时（`IncompleteFrameTimeoutMs`，默认 0 = 关闭）**（[#38](https://github.com/CSJ608/StreamFrame/issues/38)）：帧已开头、缓冲里留着半帧字节，但连续这么久未收到后续字节时判定会话失效并断线重连——与 `ReceiveIdleTimeoutMs`（完全静默也计时，要求周期流量）互补，适合允许长时间空闲、但半帧卡死必须判死的长度前缀协议（如 HSMS T8）。计时只在半帧进行中生效：缓冲为空不计时、收到新字节即重置、整帧切尽后归零；超时经 `FrameError` 上报新类别 `IncompleteFrameTimeout` 并携带受 8KB 上限保护的缓冲快照。纯增量、默认行为不变。

### 变更
- LOGO 资产对齐 social preview 定稿：图标符号改为与横幅裸符号完全同构——青色波改为**自左下低位上穿环洞**（旧版为下行穿过且环洞被底色矩形遮盖、波不显于洞内），环壁加粗至约 21.5% 边长、洞圆角收小，背景由纯色 `#4338CA` 改为横幅同款对角渐变 `#1E1B4B → #3730A3`；`docs/logo/` 全套按 social-preview.png 像素实测几何重绘，PNG 由 SVG 母版经 resvg 4096 栅格化降采样产出（单一矢量源），README 双语版经 raw 地址引用自动生效，NuGet 包图标随本版本发布生效（social preview 横幅本身不变）。

## [2.2.1] - 2026-08-28

### 变更
- 更换项目 LOGO：旧版为三元素拼贴（文档框+方块+波浪，小尺寸发糊）；新版为单一符号"帧环贯穿字节流"（白色粗环 + 青色右倾波自环心穿过两侧），16px 仍可辨认。`docs/logo/` 全套资产（SVG 母版 + 128/512 PNG）同步更新；README 双语版经 raw 地址引用自动生效，NuGet 包图标随下个版本发布生效；另增 1280×640 social preview 横幅（`docs/logo/social-preview.png`，GitHub Settings → Social preview 手动上传用）。
- 依赖升级（Dependabot）：`System.IO.Pipelines` 8.0.0→10.0.11（进包）、`System.Threading.Channels` 8.0.0→10.0.11（netstandard2.0 资产）；测试工具 `xunit.runner.visualstudio` 2.8.2→4.0.0（不进包）。至此外部依赖与 .NET 10 LTS 世代对齐。

### 新增
- CI 覆盖率报告：Ubuntu 矩阵（net8/net10）测试时收集 XPlat Code Coverage，写入 job Summary（总体 + 按程序集明细），原始 cobertura XML 以 artifact 保留 14 天；当前基线约行覆盖 85% / 分支 81%。本地可用 `dotnet test -f net8.0 --collect:"XPlat Code Coverage"` 复现。
- 心跳保活示例（demo 场景 4）：周期心跳 + 接收空闲超时的完整范式——有心跳时连接稳定（零状态事件），停止心跳后空闲判定触发重连；README 活性探测小节同步补充代码片段。

### 修复
- 接收空闲超时在部分平台（Windows）会把取消中的接收折算成 0 字节完成，被误判为"对端正常关闭"（FIN）——现统一按会话故障处理（结果同为重连，但诊断语义正确；最小复现确认后修复）。
- demo 场景 3 强制重连后用 `WaitForConnectedAsync` 等待双方就绪存在竞态（旧会话瞬时仍为 Connected 会走"立即完成"快速路径）——改为轮询双方状态，连续 10 次运行无抖动。

## [2.2.0] - 2026-08-27

### 变更
- 依赖升级（Dependabot）：`Microsoft.Extensions.Logging.Abstractions` 8.0.1→10.0.11、`Microsoft.Bcl.AsyncInterfaces` 8.0.0→10.0.11（传递依赖对齐，均含 net8.0/net10.0/net462/netstandard2.0 资产，net48 全量测试实测通过）；`PolySharp` 1.16.0、`Meziantou.Analyzer` 3.0.189；测试工具链 `coverlet.collector` 10.0.1、`Microsoft.NET.Test.Sdk` 18.9.0、`xunit` 2.9.3、`xunit.runner.visualstudio` 2.8.2（均不进包）。

### 新增
- 仓库自动化与社区基建：Dependabot（NuGet + Actions 每周检查，minor/patch 合并提 PR）、CodeQL 安全扫描（push/PR + 每周）、issue 表单模板（Bug 报告/功能建议）、CONTRIBUTING.md、GitHub Discussions。
- **重连指数退避（可选）**：`MaxRetryDelayMs`（默认 0 = 不启用，行为不变）——连续失败按基础间隔 ×2 倍增封顶、±20% 抖动、连接成功自动复位；重试日志携带尝试次数。对端长时间宕机时不再以固定间隔永久敲击。
- 连接侧 socket 按目标地址族创建（IPv4 字面量用纯 IPv4 socket，监听侧保持双栈）。
- **XML 文档随 NuGet 包发布**（`GenerateDocumentationFile`）：此前全量中文 XML 注释从未进包，用户在 IDE 中没有 IntelliSense；现以 `<inheritdoc />` 补齐全部接口实现成员的文档引用，并由 `TreatWarningsAsErrors` 强制今后不出现未文档化的公共 API。
- 基准测试扩容：`CodecBenchmarks`（XmlDocumentCodec 典型报文编解码开销）与 `EndToEndBenchmarks`（真实 TCP 回环的单向吞吐（双 framer）与往返延迟）；基准项目纳入解决方案（IDE 可见，随解决方案构建；`dotnet test` 依 IsTestProject 机制自动跳过）。

### 修复
- 暂无

## [2.1.0] - 2026-08-27

### 新增
- **多目标框架：`netstandard2.0;net8.0;net10.0`**——打开 .NET Framework 4.6.2+/Unity/Mono 受众（设备通讯/WMS 场景存量巨大）。ns2.0 资产经 net48 全量测试（真实 TCP 回环）验证；CI 矩阵 Ubuntu（net8/net10）+ Windows（net48）；发布流水线改为"测试矩阵全过 → 再发布"。兼容要点：PolySharp 语言 polyfill、Socket TaskExtensions 回退 + 可取消等待、KeepAlive 经 SIO_KEEPALIVE_VALS、StxEtx 定界器改手工扫描（SequenceReader 无 ns2.0 包资产）。net9+ 目标采用 `System.Threading.Lock`。
- 帧定界基准测试 `bench/StreamFrame.Benchmarks`（BenchmarkDotNet）：LengthPrefix/StxEtx 的流式 vs 纯函数编码对比与切帧吞吐，为"流式零拷贝"说法提供可复现数据。
- 项目 LOGO（`docs/logo/`，SVG 母版 + PNG），README 双语版接入展示、两个 NuGet 包启用包图标（随本版本发布生效）。
- README 新增 CI 构建状态徽章与"支持框架"矩阵；main 分支保护（必须走 PR 且 CI 通过、禁 force push/删除、强制线性历史）。
- 英文版 README（`README.en.md`，与中文版互链），便于国际用户检索与阅读。

### 变更
- CI Actions 升级到 Node 24 运行时：`actions/checkout@v7`、`actions/setup-dotnet@v6`、`softprops/action-gh-release@v3`（消除 Node 20 弃用告警）。
- `GetMessages`/发送 worker 改用 `WaitToReadAsync+TryRead` 等价循环（ns2.0 的 Channels 包无 `ReadAllAsync`；各目标行为一致）。
- 帧定界基准测试 `bench/StreamFrame.Benchmarks`（BenchmarkDotNet）：LengthPrefix/StxEtx 的流式 vs 纯函数编码对比与切帧吞吐，为"流式零拷贝"说法提供可复现数据。
- 项目 LOGO（`docs/logo/`，SVG 母版 + PNG），README 双语版接入展示、两个 NuGet 包启用包图标（随下个版本发布生效）。
- README 新增 CI 构建状态徽章；main 分支保护（必须走 PR 且 CI 通过、禁 force push/删除、强制线性历史）。
- 英文版 README（`README.en.md`，与中文版互链），便于国际用户检索与阅读。

### 修复
- 暂无

## [2.0.0] - 2026-08-27

### 破坏性变更
- **命名统一，消灭两个"codec"的混淆**：帧定界侧改名——`IFrameCodec`→`IFramer`、`IStreamingFrameCodec`→`IStreamingFramer`、`LengthPrefixFrameCodec`→`LengthPrefixFramer`、`StxEtxFrameCodec`→`StxEtxFramer`；"codec"一词从此只指帧内编解码 `ICodec<TMessage>`。
- **命名空间收敛**：删除 `StreamFrame.Abstractions`，所有公共类型统一进根命名空间 `StreamFrame`——用户只需一个 `using StreamFrame;`（此前具体实现类住在 "Abstractions" 命名空间里，名不副实）。
- **`Start(ct)` 的 `ct` 升级为生命周期令牌**：取消它会停止连接/重连并拆线（`Disconnected` 终态、`GetMessages` 自然结束、之后需新建连接），与 `DisposeAsync` 共用同一条幂等停机路径。1.x 中取消已连接的连接毫无作用且未如实文档化。
- **`ConnectionState` 新增 `Disconnected` 终态**：停机时广播；1.x 取消/停机后状态停留在 `Connecting`。
- **`DeviceIpAddress`→`RemoteIpAddress`（`string?`）**：语义修正——主动模式为配置的远端地址，被动模式为已连接客户端地址（未连接为 `null`，不再返回魔法值 `"NA"`）；双栈下 IPv4 客户端地址归一显示为 IPv4。
- **`IStreamConnection<TMessage>` 新增 `WaitForConnectedAsync`**（接口实现者需适配；此前 1.2.0 的 `FrameError` 已属破坏，一并归入 2.0）。
- **socket 改为 IPv6 双栈**（同一 socket 同时支持 IPv4/IPv6；监听 `IPAddress.Any` 自动按双栈处理）。1.x 仅 IPv4。

### 修复
- 主动模式连接失败重试不再泄漏 socket：`ConnectAsync` 失败/取消时立即释放本次尝试创建的 socket（此前每次失败重试泄漏一个，等终结器回收）。
- CHANGELOG 底部版本对比链接补齐 `[1.2.0]` 并修正 `[Unreleased]` 指向（v1.2.0 发版时漏更新）。

### 变更
- `Start` 重入防护：重复调用抛 `InvalidOperationException`（此前并发/重复 Start 会各自建立 socket 并互相覆盖，泄漏连接）；重建连接请用 `Reconnect()`。
- 发布管线合并为单条流水线 `release.yml`：tag 触发"版本号校验 → 构建 → 测试 → GitHub Release → 推 nuget.org"，测试未通过不会发包（此前发布与推送是两条独立 workflow，测试失败时包仍会被推出）。`workflow_dispatch` 手动触发仅做构建+测试演练，不发布；需重发版本时在对应 tag 的运行记录上 Re-run（推送带 `--skip-duplicate`，幂等）。
- tag 与两个 csproj 的 `<Version>` 一致性在流水线中显式校验，不一致直接失败（此前会静默发出错误版本号的包）。

### 新增
- 代码质量棘轮：引入 Meziantou.Analyzer（SDK 自带 NetAnalyzers 之上的补充规则）与 `.editorconfig`，现存告警清零并把库项目的 `TreatWarningsAsErrors` 打开（告警即构建失败）；`FrameErrorKind`/`DecodeErrorPolicy` 拆分为独立文件（一类型一文件）。
- `WaitForConnectedAsync`：等待连接进入 Connected（已连接立即完成；取消或 Dispose 以取消结束），替代轮询状态或 `Task.Delay` 盲等；demo 全部改用该 API。
- 可选 `ILogger` 构造参数：连接重试、会话故障、用户回调异常等内部事件输出到日志（此前 `Debug.WriteLine` 在 Release 构建中完全不可见，生产排障只能靠猜）。
- `ReceiveQueueCapacity` 选项：接收消息通道可设上限（默认 0 不限制）；消费慢时解码暂停读取、TCP 背压自然传导到对端，封堵慢消费者导致的内存无限增长。
- 选项构造校验：非法取值（负延迟、0 队列容量等）在构造时抛清晰的 `ArgumentOutOfRangeException`（此前在运行中的重连/收发路径深处抛难定位的异常）。
- LICENSE 文件（MIT 此前只在 README 与包元数据中声明，仓库内无正文）。
- push / PR 持续集成 `ci.yml`（构建 + 测试），回归不再等到发版才暴露。

## [1.2.0] - 2026-08-27

### 修复
- **会话假活（严重）**：消息通道不再被解码循环在会话停止时关闭——此前任何一次断线重连（或一条解码失败的报文）都会永久关闭通道，之后连接看似健康（Connected、字节仍在收发），业务消息却永远不再送达。`GetMessages` 现在是跨重连的稳定消息流，仅在 `DisposeAsync` 后正常结束。
- **已收消息不再丢失**：对端"发完数据立即断开"时，解码循环退出前会投递所有已缓冲的完整帧（此前会话停止的取消可能抢在投递之前）。
- **会话拆除不再泄漏旧 socket**：每次重连都会关闭上一个连接的 socket（此前等终结器收场）。
- **迟到的过期故障不再误杀新会话**：会话故障的重连按会话编号（epoch）判定是否仍有效。
- **发送失败不再静默**：发送 worker 遇到 socket 故障会上抛并触发重连（此前只吞异常退出，队列随后塞满、`SendAsync` 永久阻塞）。
- **用户回调异常全部隔离**：`ConnectionChanged` / `RawBytesReceived` / `RawBytesSent` / `FrameError` 的处理器抛异常不再中断状态机或拆会话（此前 `RawBytesReceived` 抛异常会引发重连风暴）。

### 变更
- 帧内容解码失败（codec 抛异常）默认断线重连（此前解码循环静默死亡、连接假活）；可用 `DecodeErrorPolicy.SkipFrame` 改为丢弃坏帧继续。
- `GetMessages` 不再因会话中断/解码失败向枚举方抛异常（错误改经 `FrameError` 事件上报）。
- `RawBytesSent` 改为按 socket 实际写出的分片回调（此前整帧成功才回调，部分发送不可见）。
- `IStreamConnection<TMessage>` 新增 `FrameError` 事件（自实现该接口的第三方驱动需适配）。

### 新增
- `FrameError` 帧层诊断事件：解码失败、被定界器丢弃的噪声字节、未完成帧超限，均携带已拷贝字节（可安全留存）与原因。
- `IFrameDiscardReporting` 可选接口：定界器精确上报重同步丢弃的字节；`LengthPrefixFrameCodec` / `StxEtxFrameCodec` 已实现。
- `MaxIncompleteFrameBufferBytes` 选项：未完成帧缓冲硬上限（默认 = 帧上限 + 4KB），封堵"声明超长帧永不补齐 / 只有 STX 没有 ETX"的无界内存占用。
- 活性探测选项：`TcpKeepAlive` + `KeepAliveTimeMs` / `KeepAliveIntervalMs`、`ReceiveIdleTimeoutMs`（默认全部关闭，行为与 1.1.0 一致）。
- 连接端到端测试 7 项 + 解码循环测试 4 项（重连送达、解码失败双策略、未完成帧超限、丢弃上报、钩子异常隔离、空闲超时、发送失败重连）。

## [1.1.0] - 2026-08-04

### 变更
- 被动模式新增 `AcceptFirstClientOnly` 选项（默认 `true`）：accept 到第一个客户端后关闭监听 socket，后续连接在 TCP 层被立即拒绝，保证单客户端语义与参考实现一致。

### 新增
- 单客户端连接行为测试：第二个客户端被拒绝、第一个客户端可正常收发数据、开关关闭时保持监听。

## [1.0.1] - 2026-08-04

### 变更
- README 补全项目介绍并打包进 NuGet（`PackageReadmeFile`），nuget.org 包页展示完整说明。

## [1.0.0] - 2026-08-03

首个正式发布。

### 新增
- 核心库 `StreamFrame`：通用 socket 通讯框架，帧边界判定（`IFrameCodec`）与帧内编解码（`ICodec<TMessage>`）插件化。
  - `LengthPrefixFrameCodec`：4 字节大端长度头定界。
  - `StxEtxFrameCodec`：STX/ETX 成对包裹定界。
  - `StreamConnection<TMessage>`：客户端/服务端双模式、自动重连、粘包/半包处理、有界发送队列背压。
  - 可选流式单缓冲编码（`IStreamingFrameCodec`），消除发送侧 memcpy。
  - `RawBytesReceived` / `RawBytesSent` 原始字节调试事件。
- 示例驱动 `StreamFrame.Protocols.Xml`：`XmlDocumentCodec`（含 DTD 拒绝的 XXE 防御）。
- 发布自动化：`release.yml`（tag 自动建 GitHub Release）、`publish-nuget.yml`（OIDC Trusted Publishing 推送 nuget.org）。
- 单元测试 25 个 + 三场景端到端 demo。

[Unreleased]: https://github.com/CSJ608/StreamFrame/compare/v2.5.0...HEAD
[2.5.0]: https://github.com/CSJ608/StreamFrame/compare/v2.4.0...v2.5.0
[2.4.0]: https://github.com/CSJ608/StreamFrame/compare/v2.3.1...v2.4.0
[2.3.1]: https://github.com/CSJ608/StreamFrame/compare/v2.3.0...v2.3.1
[2.3.0]: https://github.com/CSJ608/StreamFrame/compare/v2.2.1...v2.3.0
[2.2.1]: https://github.com/CSJ608/StreamFrame/compare/v2.2.0...v2.2.1
[2.2.0]: https://github.com/CSJ608/StreamFrame/compare/v2.1.0...v2.2.0
[2.1.0]: https://github.com/CSJ608/StreamFrame/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/CSJ608/StreamFrame/compare/v1.2.0...v2.0.0
[1.2.0]: https://github.com/CSJ608/StreamFrame/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/CSJ608/StreamFrame/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/CSJ608/StreamFrame/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/CSJ608/StreamFrame/releases/tag/v1.0.0
