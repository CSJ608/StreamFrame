# NativeAOT 冒烟验证

`ci.yml` 的 `aot` job 保持非必需检查，不改变分支保护或普通 `build-test` 检查名。

- **可发布**：`dotnet publish` 成功，证明当前引用路径通过 NativeAOT 编译及裁剪分析；工程保留 `PublishAot` 和 `TreatWarningsAsErrors`，包括 IL2026/IL3050 等警告门禁。
- **运行冒烟通过**：在 Linux x64 上直接执行生成的原生程序，退出 0 并报告 `AOT smoke OK`。它验证真实 TCP 回环连接、两条会话绑定发送、一条普通发送，以及接收顺序和 JSON 内容。发布成功本身不代表运行通过。

在具有 .NET 8/10 SDK 和 NativeAOT 工具链的 Linux x64 环境执行：

```bash
dotnet publish samples/StreamFrame.AotSmoke -c Release -r linux-x64 -o artifacts/aot
timeout 40s ./artifacts/aot/StreamFrame.AotSmoke
```

连接每次最多等待 5 秒，消息收发整体最多等待 10 秒。使用 OS 分配的空闲端口；由于当前公共 API 未暴露被动端 `port=0` 的实际地址，探测释放与框架绑定存在竞争，连接超时可重选端口，最多三次。双方都连接成功才发送，消息或消费失败绝不重试。结束时取消任务、释放双方连接，并观察发送/消费任务；没有用固定睡眠判断成功。JSON 解码结果拥有独立数据，临时 `JsonDocument` 及时释放。

CI 先执行无故障的正常运行（任何非零直接使 job 失败），再执行以下受控故障。每次必须退出 **1** 且日志包含对应 `AOT smoke FAILED [类别]:` 才算反证通过；退出 0、启动错误、崩溃和外层超时 124 都不能当成有效故障。业务异常原文保留在日志中。无效命令行退出 2。

| 参数 | 注入方式 | 错误类别 |
|---|---|---|
| `--fault=connect` | 保留已绑定但未监听的端口，不启动服务端 | `connection` |
| `--fault=missing` | 不发送第三条消息，实际等待接收期限 | `messages` |
| `--fault=consumer` | 消费任务收到首条后抛错 | `consumer` |
| `--fault=content` | 第三条消息携带错误内容 | `content` |

例如单独执行 `timeout 40s ./artifacts/aot/StreamFrame.AotSmoke --fault=consumer` 应返回 1；直接将此命令作为 CI 运行步骤会使步骤失败。CI 中只有专门的反证步骤检查“期望非零”，正常路径没有容错或 `continue-on-error`。

本项覆盖现有 TCP + LengthPrefixFramer + JSON 路径，并不证明所有 codec/传输均兼容 AOT。[#55](https://github.com/CSJ608/StreamFrame/issues/55) 将来扩展 TLS/串口时，需要分别扩展发布与原生运行验证；本项不提前实现这些传输。
