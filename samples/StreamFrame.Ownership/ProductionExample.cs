using System.Net;
using StreamFrame;

namespace StreamFrame.Ownership;

public static class ProductionExample
{
    // Starting values for <= 64 KiB payloads; tune to the device protocol and workload.
    public static StreamConnectionOptions CreateOptions() => new()
    {
        SendQueueCapacity = 128,
        ReceiveQueueCapacity = 128,
        MaxIncompleteFrameBufferBytes = 64 * 1024 + 4,
        IncompleteFrameTimeoutMs = 5_000,
        ReceiveIdleTimeoutMs = 0, // Idle is legal; use e.g. 15_000 only with a 5s heartbeat.
        TcpKeepAlive = true,
        KeepAliveTimeMs = 30_000,
        KeepAliveIntervalMs = 1_500, // Modern .NET: rounded up to 2 seconds.
        AcceptFirstClientOnly = true,
    };

    public static async Task RunAsync(IPAddress address, int port, CancellationToken stoppingToken)
    {
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        await using var connection = new StreamConnection<ReadOnlyMemory<byte>>(
            new LengthPrefixFramer(64 * 1024), new OwnedMemoryCodec(),
            address, port, isActive: true, options: CreateOptions());
        connection.Start(lifetime.Token);
        try
        {
            using (var ready = CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token))
            {
                ready.CancelAfter(TimeSpan.FromSeconds(10));
                await connection.WaitForConnectedAsync(ready.Token);
            }

            // Owned immutable data: do not change/recycle this array after enqueue.
            await connection.SendAsync(new byte[] { 1, 2, 3 }, lifetime.Token);
            // SendAsync means enqueue only. Protocol ACK/retry/deduplication belongs to the application.
            await foreach (var message in connection.GetMessages(lifetime.Token))
            {
                await Task.Delay(20, lifetime.Token); // Simulated asynchronous business processing.
                Console.WriteLine(BitConverter.ToString(message.ToArray())); // Still valid after await.
            }
        }
        catch (OperationCanceledException) when (lifetime.IsCancellationRequested)
        {
            // Requested shutdown. A readiness timeout otherwise propagates to the caller.
        }
        finally
        {
            lifetime.Cancel(); // Stop reconnects; await using also disposes on timeout/failure.
        }
    }
}
