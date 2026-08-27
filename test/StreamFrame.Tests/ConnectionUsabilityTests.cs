using System.Net;
using System.Net.Sockets;
using System.Text;
using StreamFrame;
using StreamFrame.Abstractions;

namespace StreamFrame.Tests;

/// <summary>
/// 易用性相关的连接行为测试：等待连接就绪、Start 重入防护、选项构造校验、
/// 接收队列容量（慢消费者背压）。
/// </summary>
public class ConnectionUsabilityTests
{
    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static StreamConnection<string> CreateConnection(
        int port,
        bool isActive,
        StreamConnectionOptions? options = null)
        => new(
            new LengthPrefixFrameCodec(),
            StringCodec.Instance,
            IPAddress.Loopback,
            port,
            isActive,
            options ?? new StreamConnectionOptions { ConnectRetryDelayMs = 200 });

    [Fact]
    public async Task WaitForConnectedAsync_CompletesWhenConnectionEstablished()
    {
        var port = GetFreePort();
        await using var client = CreateConnection(port, isActive: true);
        await using var server = CreateConnection(port, isActive: false);

        client.Start(CancellationToken.None);
        // 服务端稍后才启动：等待者必须跨过若干次连接失败重试
        await Task.Delay(400);
        server.Start(CancellationToken.None);

        await client.WaitForConnectedAsync().WaitAsync(TimeSpan.FromSeconds(5));
        await server.WaitForConnectedAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(ConnectionState.Connected, client.State);
    }

    [Fact]
    public async Task WaitForConnectedAsync_AlreadyConnected_CompletesImmediately()
    {
        var port = GetFreePort();
        await using var client = CreateConnection(port, isActive: true);
        await using var server = CreateConnection(port, isActive: false);

        client.Start(CancellationToken.None);
        server.Start(CancellationToken.None);
        await client.WaitForConnectedAsync().WaitAsync(TimeSpan.FromSeconds(5));

        var again = client.WaitForConnectedAsync();
        Assert.Equal(TaskStatus.RanToCompletion, again.Status);
    }

    [Fact]
    public async Task WaitForConnectedAsync_PendingWaiter_CompletesOnConnect()
    {
        var port = GetFreePort();
        await using var client = CreateConnection(port, isActive: true);
        await using var server = CreateConnection(port, isActive: false);

        client.Start(CancellationToken.None);
        var waitTask = client.WaitForConnectedAsync();
        Assert.False(waitTask.IsCompleted); // 服务端未启动，必然仍在等待

        server.Start(CancellationToken.None);
        await waitTask.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task WaitForConnectedAsync_Dispose_CancelsPendingWaiter()
    {
        var port = GetFreePort();
        await using var client = CreateConnection(port, isActive: true);

        client.Start(CancellationToken.None); // 服务端永远不启动
        var waitTask = client.WaitForConnectedAsync();
        Assert.False(waitTask.IsCompleted);

        await client.DisposeAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waitTask);
    }

    [Fact]
    public async Task Start_SecondCall_ThrowsInvalidOperation()
    {
        var port = GetFreePort();
        await using var client = CreateConnection(port, isActive: true);

        client.Start(CancellationToken.None);
        Assert.Throws<InvalidOperationException>(() => client.Start(CancellationToken.None));
    }

    [Fact]
    public void Ctor_InvalidOptions_ThrowsArgumentOutOfRange()
    {
        var port = GetFreePort();

        void CreateWith(Action<StreamConnectionOptions> mutate)
        {
            var options = new StreamConnectionOptions();
            mutate(options);
            Assert.ThrowsAny<ArgumentException>(() =>
                CreateConnection(port, isActive: true, options));
        }

        CreateWith(o => o.ConnectRetryDelayMs = -1);
        CreateWith(o => o.AcceptRetryDelayMs = -1);
        CreateWith(o => o.SocketReceiveBufferSize = 0);
        CreateWith(o => o.SendQueueCapacity = 0);
        CreateWith(o => o.EncodeBufferInitialSize = 0);
        CreateWith(o => o.ReceiveQueueCapacity = -1);
        CreateWith(o => o.ReceiveIdleTimeoutMs = -1);
        CreateWith(o =>
        {
            o.TcpKeepAlive = true;
            o.KeepAliveTimeMs = 0;
        });
        CreateWith(o =>
        {
            o.TcpKeepAlive = true;
            o.KeepAliveIntervalMs = -1;
        });
    }

    [Fact]
    public async Task BoundedReceiveQueue_SlowConsumer_DeliversAllInOrder()
    {
        var port = GetFreePort();
        var options = new StreamConnectionOptions
        {
            ConnectRetryDelayMs = 200,
            ReceiveQueueCapacity = 2, // 容量远小于消息数：解码循环必须反复等待消费者
        };
        await using var server = CreateConnection(port, isActive: false, options);

        var received = new List<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var drainTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var message in server.GetMessages(drainCts.Token))
                {
                    lock (received)
                        received.Add(message);
                    await Task.Delay(30); // 模拟慢消费者
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        server.Start(CancellationToken.None);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);
        await server.WaitForConnectedAsync().WaitAsync(TimeSpan.FromSeconds(5));

        // 一次性灌入 5 帧（容量 2）：解码必须暂停等待消费、随后按序全部送达
        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        foreach (var i in Enumerable.Range(1, 5))
            new LengthPrefixFrameCodec().EncodeFrame(Encoding.UTF8.GetBytes($"msg-{i}"), buffer);
        await client.GetStream().WriteAsync(buffer.WrittenSpan.ToArray());

        var deadline = Environment.TickCount64 + 5000;
        while (Environment.TickCount64 < deadline)
        {
            lock (received)
            {
                if (received.Count == 5)
                    break;
            }
            await Task.Delay(20);
        }

        lock (received)
            Assert.Equal(Enumerable.Range(1, 5).Select(i => $"msg-{i}"), received);

        drainCts.Cancel();
        await drainTask;
    }
}
