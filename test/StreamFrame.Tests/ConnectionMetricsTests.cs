using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using StreamFrame;

namespace StreamFrame.Tests;

/// <summary>
/// 内置指标（Meter "StreamFrame"）的可观测性验证：用 MeterListener 直接对既有 instrument
/// 启用测量（InstrumentPublished 只对 Start 之后新发布的仪器触发，进程内仪器可能早已创建），
/// 驱动一次完整的连接生命周期，断言各仪器都有真实记录。
/// </summary>
public class ConnectionMetricsTests
{
    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static async Task ConnectWithRetryAsync(TcpClient client, int port)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await client.ConnectAsync(IPAddress.Loopback, port);
                return;
            }
            catch (SocketException) when (attempt < 10)
            {
                await Task.Delay(200);
            }
        }
    }

    private static async Task WaitForStateAsync(
        StreamConnection<string> connection,
        Func<ConnectionState, bool> predicate,
        int timeoutMs = 8000)
    {
        var deadline = TestClock.TickCount64 + timeoutMs;
        while (TestClock.TickCount64 < deadline)
        {
            if (predicate(connection.State))
                return;
            await Task.Delay(20);
        }

        Assert.True(predicate(connection.State), $"等待连接状态超时，当前 {connection.State}。");
    }

    [Fact]
    public async Task ConnectionLifecycle_ProducesAllInstruments()
    {
        var recorded = new ConcurrentDictionary<string, double>();
        using var listener = new MeterListener();
        listener.SetMeasurementEventCallback<long>(
            (instrument, value, _, _) => recorded.AddOrUpdate(instrument.Name, value, (_, sum) => sum + value));
        listener.SetMeasurementEventCallback<double>(
            (instrument, value, _, _) => recorded.AddOrUpdate(instrument.Name, value, (_, sum) => sum + value));
        listener.Start();

        // 进程内静态仪器可能早于本监听器创建：逐个直接启用
        foreach (var field in typeof(ConnectionMetrics).GetFields(
                     System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static))
        {
            if (field.GetValue(null) is Instrument instrument)
                listener.EnableMeasurementEvents(instrument);
        }

        var port = GetFreePort();
        await using var server = new StreamConnection<string>(
            new LengthPrefixFramer(), StringCodec.Instance, IPAddress.Loopback, port, isActive: false);
        var received = new List<string>();
        var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var m in server.GetMessages(drainCts.Token))
                    lock (received)
                        received.Add(m);
            }
            catch (OperationCanceledException)
            {
            }
        });
        server.Start(CancellationToken.None);

        long id1;
        using (var client1 = new TcpClient())
        {
            await ConnectWithRetryAsync(client1, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
            id1 = server.CurrentSessionId;

            var payload = new string('M', 1024);
            var sendTask = server.SendInSessionAsync(id1, payload); // 出站：回环缓冲即可写完，对端无需先读
            await sendTask.WaitAsync(TimeSpan.FromSeconds(10));

            // 入站恰好一帧（长度 3 + "in!"）：驱动 frames_received / bytes_received
            await client1.GetStream().WriteAsync(new byte[] { 0x00, 0x00, 0x00, 0x03, (byte)'i', (byte)'n', 0x21 });
            var deadline = TestClock.TickCount64 + 10_000;
            while (TestClock.TickCount64 < deadline)
            {
                lock (received)
                {
                    if (received.Count == 1)
                        break;
                }
                await Task.Delay(20);
            }
            lock (received)
                Assert.Single(received);

            client1.Dispose(); // 触发重连
        }

        await WaitForStateAsync(server, s => s != ConnectionState.Connected);
        using (var client2 = new TcpClient())
        {
            await ConnectWithRetryAsync(client2, port);
            await WaitForStateAsync(server, s => s == ConnectionState.Connected);
        }

        await server.DisposeAsync();

        // MeterListener 的测量回调经内部队列异步派发：对全部预期键做带截止时间的轮询，
        // 而不是固定延迟（负载下排空时间不定）
        string[] keys =
        {
            "streamframe.frames_sent", "streamframe.frames_received", "streamframe.bytes_sent",
            "streamframe.bytes_received", "streamframe.reconnects", "streamframe.session_duration",
            "streamframe.send_queue_length",
        };
        var drainDeadline = TestClock.TickCount64 + 10_000;
        while (TestClock.TickCount64 < drainDeadline && !keys.All(k => recorded.ContainsKey(k)))
            await Task.Delay(50);

        Assert.True(recorded.TryGetValue("streamframe.frames_sent", out var framesSent) && framesSent >= 1,
            $"frames_sent 应有记录，实际 {framesSent}");
        Assert.True(recorded.TryGetValue("streamframe.frames_received", out var framesReceived) && framesReceived >= 1,
            $"frames_received 应有记录，实际 {framesReceived}");
        Assert.True(recorded.TryGetValue("streamframe.bytes_sent", out var bytesSent) && bytesSent >= 1028,
            $"bytes_sent 应含整帧字节，实际 {bytesSent}");
        Assert.True(recorded.TryGetValue("streamframe.bytes_received", out var bytesReceived) && bytesReceived >= 7,
            $"bytes_received 应有记录，实际 {bytesReceived}");
        Assert.True(recorded.TryGetValue("streamframe.reconnects", out var reconnects) && reconnects >= 1,
            $"reconnects 应有记录，实际 {reconnects}");
        Assert.True(recorded.TryGetValue("streamframe.session_duration", out var duration) && duration > 0,
            $"session_duration 应有记录，实际 {duration}");
        Assert.True(recorded.TryGetValue("streamframe.send_queue_length", out var queue) && queue >= 0,
            "send_queue_length 应有采样");
    }
}
