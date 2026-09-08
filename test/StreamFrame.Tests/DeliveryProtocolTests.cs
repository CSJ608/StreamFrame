using StreamFrame;

namespace StreamFrame.Tests;

public class DeliveryProtocolTests
{
    private readonly Xunit.Abstractions.ITestOutputHelper _output;
    public DeliveryProtocolTests(Xunit.Abstractions.ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task LostAck_RetryUsesSameBusinessId_ReceiverAppliesOnce()
    {
        await using var run = new SoakRun(new SoakTrace(_output));
        var first = await run.ConnectAsync();
        await first.InjectPartialAsync(); // Peer receives, but deliberately withholds ACK on this broken session.
        await run.SendBusinessAsync(42);
        await run.WaitAsync(() => run.Delivered.ContainsKey(42), "first application");
        await run.ClosePeerAsync(first);
        Assert.Empty(run.Acked); // Enqueue and peer application do not fabricate acknowledgement.
        await run.WaitAsync(() => run.Server.State != ConnectionState.Connected, "old session ended");
        var second = await run.ConnectAsync();
        await run.SendBusinessAsync(42);
        await run.BarrierAsync(second);
        Assert.Single(run.Delivered);
        Assert.Single(run.Acked);
        Assert.Equal(new[] { "p42/0", "p42/1" }, run.Received.Where(x => x.Wire.StartsWith("p", StringComparison.Ordinal)).Select(x => x.Wire));
        await run.ClosePeerAsync(second); // Includes net48 pending ReadAsync shutdown and task observation.
    }
}
