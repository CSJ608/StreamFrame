using System.Buffers;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace StreamFrame.Tests;

/// <summary>net48 缺失 API 的测试侧 polyfill（net8/net10 走运行时内置）。</summary>
internal static class TestClock
{
    public static long TickCount64 =>
#if NET48
        Environment.TickCount; // netfx 仅有 int 版（24.9 天回绕，测试时长内无碍）
#else
        Environment.TickCount64;
#endif
}

#if NET48
internal static class TestTaskWaitAsyncCompat
{
    public static async Task WaitAsync(this Task task, TimeSpan timeout)
    {
        var completed = await Task.WhenAny(task, Task.Delay(timeout)).ConfigureAwait(false);
        if (completed != task)
            throw new TimeoutException($"等待超过 {timeout}。");
        await task.ConfigureAwait(false);
    }

    public static async Task WaitAsync(this Task task, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var delay = Task.Delay(timeout, cancellationToken);
        var completed = await Task.WhenAny(task, delay).ConfigureAwait(false);
        if (completed != task)
        {
            delay.Ignore();
            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException(cancellationToken);
            throw new TimeoutException($"等待超过 {timeout}。");
        }
        await task.ConfigureAwait(false);
    }
}

internal static class TestTaskExtensions
{
    public static void Ignore(this Task _)
    {
    }
}
#endif
