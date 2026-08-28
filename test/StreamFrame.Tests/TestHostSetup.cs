using System.Runtime.CompilerServices;

namespace StreamFrame.Tests;

/// <summary>
/// 测试宿主初始化：CI 跑机（2 核）上并行测试里的 Thread.Sleep（编码停顿的测试 codec）
/// 会阻塞线程池线程，net48 的线程注入速率慢（约 1 个/500ms），曾把其他测试的秒级等待
/// 拖到超时。抬高最小线程数根治这类环境性抖动（仅影响测试宿主进程）。
/// </summary>
internal static class TestHostSetup
{
    [ModuleInitializer]
    internal static void ConfigureThreadPool()
    {
        var workers = Math.Max(8, Environment.ProcessorCount * 4);
        ThreadPool.SetMinThreads(workers, workers);
    }
}
