using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using StreamFrame;

namespace StreamFrame.Tests;

public class SendQueueContinuationTests
{
    [Fact]
    public async Task CancelledWorker_AfterSuccessfulWrite_LeavesUnclaimedPlainEntryForNextSession()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        await using var server = new StreamConnection<string>(new LengthPrefixFramer(), StringCodec.Instance,
            IPAddress.Loopback, port, isActive: false);
        var written = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var first = 0;
        var gateTimedOut = 0;
        server.RawBytesSent += _ =>
        {
            if (Interlocked.Increment(ref first) != 1) return;
            written.TrySetResult(true);
            if (!release.Wait(TimeSpan.FromSeconds(10))) Interlocked.Exchange(ref gateTimedOut, 1);
        };
        server.Start(CancellationToken.None);
        using var peer1 = new TcpClient();
        await SoakRun.BoundedAsync(peer1.ConnectAsync(IPAddress.Loopback, port), "first regression peer connect");
        await UntilAsync(() => server.CurrentSessionId != 0);
        var oldSession = server.CurrentSessionId;
        var workerField = typeof(StreamConnection<string>).GetField("_sendWorkerTask",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        // Connected is published before StartSession finishes; wait for the worker to be installed.
        await UntilAsync(() => workerField.GetValue(server) is Task);
        var oldWorker = (Task)workerField.GetValue(server)!;
        try
        {
            await server.SendAsync("already-written");
            Assert.Same(written.Task, await Task.WhenAny(written.Task, Task.Delay(5000)));
            await server.SendAsync("must-continue");
            // Callback gates the old worker after the complete write, before its next dequeue.
            await SoakRun.BoundedAsync(Task.Run(() => server.Reconnect()), "regression reconnect");
            Assert.NotEqual(oldSession, server.CurrentSessionId);
        }
        finally { release.Set(); }
        await SoakRun.BoundedAsync(oldWorker, "old worker exit", 5000);
        Assert.Equal(0, Volatile.Read(ref gateTimedOut));
        peer1.Dispose();
        using var peer2 = new TcpClient();
        await SoakRun.BoundedAsync(peer2.ConnectAsync(IPAddress.Loopback, port), "second regression peer connect");
        await UntilAsync(() => server.CurrentSessionId > oldSession);
        await server.SendAsync("sentinel");
        Assert.Equal("must-continue", await ReadAsync(peer2));
        Assert.Equal("sentinel", await ReadAsync(peer2));
    }

    private static async Task UntilAsync(Func<bool> condition)
    {
        var end = TestClock.TickCount64 + 5000;
        while (!condition() && TestClock.TickCount64 < end) await Task.Delay(10);
        Assert.True(condition());
    }

    private static async Task<string> ReadAsync(TcpClient client)
    {
        async Task<byte[]> ReadBytes(int count)
        {
            var bytes = new byte[count];
            var offset = 0;
            while (offset < count)
            {
                var task = client.GetStream().ReadAsync(bytes, offset, count - offset);
                if (await Task.WhenAny(task, Task.Delay(5000)) != task)
                {
                    client.Dispose();
                    try { await SoakRun.BoundedAsync(task, "failed read cleanup"); }
                    catch (Exception ex) when (ex is IOException or ObjectDisposedException) { }
                    Assert.Fail("frame read timed out");
                }
                var read = await task;
                Assert.True(read > 0);
                offset += read;
            }
            return bytes;
        }
        var header = await ReadBytes(4);
        var length = BinaryPrimitives.ReadInt32BigEndian(header);
        Assert.InRange(length, 1, 1024);
        return Encoding.UTF8.GetString(await ReadBytes(length));
    }
}
