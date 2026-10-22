using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Channels;
using System.Xml;
using StreamFrame.Ownership;
using StreamFrame.Protocols.Xml;

namespace StreamFrame.Tests;

public class SecurityConfigurationTests
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(8);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Xml_ExactByteLimit_AcceptsSingleAndSegmentedInput(bool segmented)
    {
        var bytes = Encoding.UTF8.GetBytes("<r>安全</r>");
        var sequence = segmented ? Segment.Split(bytes) : new ReadOnlySequence<byte>(bytes);
        Assert.Equal(!segmented, sequence.IsSingleSegment);
        var codec = new XmlDocumentCodec { MaxDocumentBytes = bytes.Length };
        Assert.Equal("安全", codec.Decode(sequence).Root?.Value);
        Assert.Throws<InvalidOperationException>(() =>
            new XmlDocumentCodec { MaxDocumentBytes = bytes.Length - 1 }.Decode(sequence));
    }

    [Theory]
    [InlineData("<!DOCTYPE r [<!ENTITY entity 'expanded'>]><r>&entity;</r>")]
    [InlineData("<!DOCTYPE r [<!ENTITY entity SYSTEM 'file:///streamframe-not-read'>]><r>&entity;</r>")]
    public void Xml_EntityDtd_StillRejectedAtByteLimit(string xml)
    {
        var bytes = Encoding.UTF8.GetBytes(xml);
        var sequence = Segment.Split(bytes);
        var codec = new XmlDocumentCodec { MaxDocumentBytes = bytes.Length };
        Assert.Throws<XmlException>(() => codec.Decode(sequence));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FullReceiveQueue_TeardownIsBounded_AndResumedMessagesKeepOwnership(bool reconnect)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            var codec = new SignalingOwnedCodec();
            await using var connection = CreateConnection(listener, codec);
            connection.Start(CancellationToken.None);
            using var peer = await WithinAsync(listener.AcceptTcpClientAsync());
            await connection.WaitForConnectedAsync().WaitAsync(Deadline);
            var oldSession = connection.CurrentSessionId;
            await peer.GetStream().WriteAsync(Frames("old-a", "old-b"));
            // 未启动消费；第二次 Decode 证明第一条已入容量为 1 的队列。
            await codec.SecondDecoded.Task.WaitAsync(Deadline);

            if (reconnect)
                await Task.Run(connection.Reconnect).WaitAsync(Deadline);
            else
                await Task.Run(async () => await connection.DisposeAsync()).WaitAsync(Deadline);

            using var nextPeer = reconnect ? await WithinAsync(listener.AcceptTcpClientAsync()) : null;
            var newSession = 0L;
            if (nextPeer is not null)
            {
                await connection.WaitForConnectedAsync().WaitAsync(Deadline);
                newSession = connection.CurrentSessionId;
                Assert.True(newSession > oldSession);
                await nextPeer.GetStream().WriteAsync(Frames("new-a"));
            }
            else
            {
                Assert.True(connection.IsDisposed);
            }

            var received = new List<SessionMessage<ReadOnlyMemory<byte>>>();
            await using var messages = connection.GetSessionMessages().GetAsyncEnumerator();
            for (var i = 0; i < (reconnect ? 3 : 1); i++)
            {
                Assert.True(await WithinAsync(messages.MoveNextAsync().AsTask()));
                received.Add(messages.Current);
            }
            await Task.Run(async () => await connection.DisposeAsync()).WaitAsync(Deadline);
            Assert.False(await WithinAsync(messages.MoveNextAsync().AsTask()));
            await Task.Yield(); // 异步业务侧仍持有消息；解码源已被覆写，连接也已释放。
            var expected = reconnect ? new[] { "old-a", "old-b", "new-a" } : new[] { "old-a" };
            Assert.Equal(expected, received.Select(m => Encoding.UTF8.GetString(m.Message.ToArray())));
            // 跨会话队列会保留旧消息，但不得把旧消息重新标成新会话。
            Assert.Equal(reconnect ? new[] { oldSession, oldSession, newSession } : new[] { oldSession },
                received.Select(m => m.SessionId));
        }
        finally { listener.Stop(); }
    }

    [Fact]
    public async Task PeriodicReceiveTraffic_ResetsIdleTimeout_UntilTrafficStops()
    {
        const int IdleMs = 1500;
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            await using var connection = CreateConnection(listener, new OwnedMemoryCodec(), IdleMs);
            var retry = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            connection.ConnectionChanged += (_, state) =>
            {
                if (state == ConnectionState.Retry) retry.TrySetResult(true);
            };
            connection.Start(CancellationToken.None);
            using var peer = await WithinAsync(listener.AcceptTcpClientAsync());
            await connection.WaitForConnectedAsync().WaitAsync(Deadline);
            var session = connection.CurrentSessionId;
            await using var messages = connection.GetSessionMessages().GetAsyncEnumerator();
            var ticks = Channel.CreateUnbounded<bool>();
            using var timer = new Timer(_ => ticks.Writer.TryWrite(true), null, 0, 100);
            var elapsed = Stopwatch.StartNew();
            // 定时器只控制心跳节拍；实际到达和状态转换均由信号驱动，不猜测调度耗时。
            do
            {
                await WithinAsync(ticks.Reader.ReadAsync().AsTask());
                await peer.GetStream().WriteAsync(Frames("heartbeat"));
                Assert.True(await WithinAsync(messages.MoveNextAsync().AsTask()));
                Assert.Equal(session, messages.Current.SessionId);
                Assert.Equal("heartbeat", Encoding.UTF8.GetString(messages.Current.Message.ToArray()));
                Assert.False(retry.Task.IsCompleted);
            } while (elapsed.ElapsedMilliseconds < IdleMs * 2);
            timer.Change(Timeout.Infinite, Timeout.Infinite);
            Assert.Equal(ConnectionState.Connected, connection.State);
            // 对端仍打开：停止流量后必须真的触发 idle 重连，而非把超时禁用了。
            await retry.Task.WaitAsync(Deadline);
        }
        finally { listener.Stop(); }
    }

    private static StreamConnection<ReadOnlyMemory<byte>> CreateConnection(
        TcpListener listener, ICodec<ReadOnlyMemory<byte>> codec, int idleMs = 0)
    {
        var options = ProductionExample.CreateOptions();
        options.ReceiveQueueCapacity = 1;
        options.ReceiveIdleTimeoutMs = idleMs;
        options.ConnectRetryDelayMs = 100;
        return new StreamConnection<ReadOnlyMemory<byte>>(new LengthPrefixFramer(64 * 1024), codec,
            IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port, isActive: true, options);
    }

    private static byte[] Frames(params string[] payloads)
    {
        var writer = new TestWrittenBufferWriter();
        foreach (var payload in payloads)
            new LengthPrefixFramer().EncodeFrame(Encoding.UTF8.GetBytes(payload), writer);
        return writer.WrittenSpan.ToArray();
    }

    private static async Task<T> WithinAsync<T>(Task<T> task)
    {
        await task.WaitAsync(Deadline); // net48 复用已有的非泛型兼容扩展。
        return await task;
    }

    private sealed class SignalingOwnedCodec : ICodec<ReadOnlyMemory<byte>>
    {
        private readonly OwnedMemoryCodec _inner = new();
        private int _decoded;
        public TaskCompletionSource<bool> SecondDecoded { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ReadOnlyMemory<byte> Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
        {
            var message = _inner.Decode(frame, ct);
            // 确定性覆写借用的 payload，避免依赖共享池恰好复用才能发现别名错误。
            foreach (var part in frame)
            {
                Assert.True(MemoryMarshal.TryGetArray(part, out var source));
                Array.Clear(source.Array!, source.Offset, source.Count);
            }
            if (Interlocked.Increment(ref _decoded) == 2) SecondDecoded.TrySetResult(true);
            return message;
        }
        public void Encode(ReadOnlyMemory<byte> message, IBufferWriter<byte> writer, CancellationToken ct = default)
            => _inner.Encode(message, writer, ct);
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        private Segment(ReadOnlyMemory<byte> memory) => Memory = memory;
        public static ReadOnlySequence<byte> Split(byte[] bytes)
        {
            // 在“安”的 UTF-8 字符内部切段，覆盖跨段读取而非仅拼接 ASCII。
            var first = new Segment(bytes.AsMemory(0, 4));
            var last = new Segment(bytes.AsMemory(4)) { RunningIndex = 4 };
            first.Next = last;
            return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
        }
    }
}
