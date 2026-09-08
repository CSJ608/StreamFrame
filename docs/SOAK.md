# Soak 验收与重放

`.github/workflows/soak.yml` 每夜北京时间 03:00 执行三个长测，每项 600 秒，覆盖 Ubuntu net8/net10 和 Windows net8/net10/net48。未设置有效正数 `STREAMFRAME_SOAK_SECONDS` 时报告 Skip。

本地先以单目标短时诊断，例如 PowerShell：

```powershell
$env:STREAMFRAME_SOAK_SECONDS = '10'
$env:STREAMFRAME_SOAK_SEED = '67002'
dotnet test test/StreamFrame.Tests/StreamFrame.Tests.csproj -c Release -f net48 --filter FullyQualifiedName~Soak --logger 'console;verbosity=detailed'
```

seed 固定随机动作序列；TCP、调度及按时间截止的迭代数不保证逐字节确定重放。日志同时记录动作序号、随机值、消息/尝试 ID、SessionId、入队任务结果、编码认领、实际本机写出字节、对端采集、ACK 和停止阶段。手动 workflow 输入 seed 可重用失败 seed；未指定则生成并输出 seed。

## 三个独立断言面

- 无故障长流：发送结束后使用同一 FIFO 队列的哨兵，等待对端 ACK，精确核对全部消息及顺序，不允许尾部少收。
- 故障下库保证：普通 `SendAsync` 仅保证入队。编码入口观测已认领；原始字节回调观测本机写出，不代表对端确认。终局在重试前排空队列，检查未认领条目送达。确定性 `SendQueueContinuationTests` 独立门控取消发生在首帧写完与下次出队之间，禁止旧 worker 消耗待续发条目。绑定消息仍检查合法任务结局、无重复、接收会话等于绑定会话；每个接收会话内所有尝试保持发送顺序。
- 业务交付：`p业务ID/尝试号`，对端按业务 ID 去重应用，每次完整接收后发 ACK；只重试未 ACK 的业务 ID，并精确校验所有业务 ID 的应用与 ACK 集合。半帧注入会暂停该连接的 ACK，终局在干净会话重试。`DeliveryProtocolTests` 强制 ACK 丢失，验证入队/应用不会伪造 ACK，重试不会重复应用。

未确认尝试按观测分类：未认领、已认领但写出不完整/无法归属、本机整帧已写出但远端未知、对端已采集但 ACK 缺失。`RawBytesSent` 公共事件没有不可变会话编号，日志中的 sid 是当时快照；拆除恰好切断分片时保留归属不确定性，不把快照当作新的库保证。所有历史读任务在故障关闭时被观察，终局不提前取消采集。

## 有界停止与验收

.NET Framework 的测试端 `NetworkStream.ReadAsync` 在本例中不因令牌取消而结束。排空后关闭所属 socket，再有界观察读任务；连接、发送、排空、ACK、读任务和服务端释放均有命名阶段期限。失败保留原始非零退出和 TRX，不能靠增加 job 上限消除悬挂。

短测不算完整验收。Issue #67 要求相同实现的完整 600 秒五目标矩阵连续至少 3 次通过，记录运行链接、提交、seed 和每项实际时长；优先复用夜间运行，串行计数，不重复 dispatch 同一验收。实现变更或完整运行失败重置连续通过计数。验收未齐时 Issue 保持开放，即使实现 PR 已合并。
