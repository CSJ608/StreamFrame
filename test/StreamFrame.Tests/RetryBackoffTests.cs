using System.Net;
using System.Net.Sockets;

namespace StreamFrame.Tests;

/// <summary>
/// 重连指数退避测试：调度器数学（倍增/封顶/抖动边界/复位）+ 真实 TCP 场景冒烟。
/// </summary>
public class RetryBackoffTests
{
    // ----- 调度器数学 -----

    [Fact]
    public void Disabled_ReturnsConstantBaseDelay()
    {
        var scheduler = new RetryDelayScheduler(baseDelayMs: 100, maxDelayMs: 0);
        Assert.False(scheduler.Enabled);

        for (var i = 0; i < 10; i++)
            Assert.Equal(100, scheduler.NextDelayMs()); // 无倍增、无抖动——与历史行为一致
    }

    [Fact]
    public void Enabled_GrowsExponentiallyThenCaps()
    {
        var scheduler = new RetryDelayScheduler(baseDelayMs: 100, maxDelayMs: 500);
        Assert.True(scheduler.Enabled);

        // 第 n 次失败后：base*2^(n-1) 封顶 500，±20% 抖动
        Assert.InRange(scheduler.NextDelayMs(), 80, 120);    // 100
        Assert.InRange(scheduler.NextDelayMs(), 160, 240);    // 200
        Assert.InRange(scheduler.NextDelayMs(), 320, 480);    // 400
        Assert.InRange(scheduler.NextDelayMs(), 400, 600);    // 800 → 封顶 500
        Assert.InRange(scheduler.NextDelayMs(), 400, 600);    // 保持 500
        Assert.Equal(5, scheduler.Attempt);
    }

    [Fact]
    public void Reset_RestartsFromBaseDelay()
    {
        var scheduler = new RetryDelayScheduler(baseDelayMs: 100, maxDelayMs: 500);
        for (var i = 0; i < 4; i++)
            scheduler.NextDelayMs();
        Assert.Equal(4, scheduler.Attempt);

        scheduler.Reset(); // 模拟连接成功
        Assert.Equal(0, scheduler.Attempt);
        Assert.InRange(scheduler.NextDelayMs(), 80, 120); // 重新从基础间隔开始
    }

    // ----- 端到端回归：开启退避不破坏连接/重连/消息送达（间隔数值依赖环境的连接失败耗时，不做时序断言） -----

    [Fact]
    public async Task Backoff_Enabled_StillConnectsAndDelivers()
    {
        var port = GetFreePort();

        // 主动端先空转重试几轮（退避生效中），随后服务端上线
        await using var client = new StreamConnection<string>(
            new LengthPrefixFramer(),
            StringCodec.Instance,
            IPAddress.Loopback,
            port,
            isActive: true,
            new StreamConnectionOptions { ConnectRetryDelayMs = 100, MaxRetryDelayMs = 400 });
        client.Start(CancellationToken.None);
        await Task.Delay(1500);

        await using var server = new StreamConnection<string>(
            new LengthPrefixFramer(),
            StringCodec.Instance,
            IPAddress.Loopback,
            port,
            isActive: false);
        server.Start(CancellationToken.None);

        await client.WaitForConnectedAsync().WaitAsync(TimeSpan.FromSeconds(15));

        var received = 0;
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var drainTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in server.GetMessages(drainCts.Token))
                    Interlocked.Increment(ref received);
            }
            catch (OperationCanceledException)
            {
            }
        });
        await server.WaitForConnectedAsync().WaitAsync(TimeSpan.FromSeconds(15));

        await client.SendAsync("after-backoff");
        var deadline = TestClock.TickCount64 + 5000;
        while (TestClock.TickCount64 < deadline && Volatile.Read(ref received) < 1)
            await Task.Delay(20);

        Assert.Equal(1, Volatile.Read(ref received));
        drainCts.Cancel();
        await drainTask;
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
