using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace StreamFrame.Benchmarks;

/// <summary>Matched application work; transport scheduling and buffering still differ.</summary>
[MemoryDiagnoser]
public abstract class NetworkBenchmarkBase
{
    public const int Messages = 256;
    [Params("LengthPrefix", "StxEtx")]
    public string Framer { get; set; } = "LengthPrefix";
    [Params(64, 1024, 65536)]
    public int PayloadBytes { get; set; } = 64;
    [Params("Bytes", "StringSpan", "StringAlloc")]
    public string CodecMode { get; set; } = "Bytes";
    [Params("DirectTcp", "StreamFrame")]
    public string Transport { get; set; } = "DirectTcp";
    protected abstract bool Echo { get; }
    private BenchmarkEndpoint _server = null!;
    private BenchmarkEndpoint _client = null!;
    private object _payload = null!;
    private CancellationTokenSource _lifetime = null!;
    private Task _serverWork = Task.CompletedTask;

    [GlobalSetup]
    public async Task Setup()
    {
        _lifetime = new CancellationTokenSource();
        var text = new string('x', PayloadBytes);
        _payload = CodecMode == "Bytes" ? Encoding.UTF8.GetBytes(text) : text;
        IStreamingFramer framer = Framer == "LengthPrefix" ? new LengthPrefixFramer() : new StxEtxFramer();
        var codec = new BenchmarkCodec(CodecMode);
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        if (Transport == "DirectTcp")
        {
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var sender = new TcpClient();
            await sender.ConnectAsync(IPAddress.Loopback, port, deadline.Token);
            var receiver = await listener.AcceptTcpClientAsync(deadline.Token);
            listener.Stop();
            _client = new DirectTcpEndpoint(sender, framer, codec, PayloadBytes);
            _server = new DirectTcpEndpoint(receiver, framer, codec, PayloadBytes);
        }
        else
        {
            // StreamConnection owns its listener; release the reserved ephemeral port.
            listener.Stop();
            var options = new StreamConnectionOptions { SocketReceiveBufferSize = 65536, TcpKeepAlive = false };
            var server = new StreamConnection<object>(framer, codec, IPAddress.Loopback, port, false, options);
            var client = new StreamConnection<object>(framer, codec, IPAddress.Loopback, port, true, options);
            _server = new FrameworkEndpoint(server, _lifetime.Token);
            _client = new FrameworkEndpoint(client, _lifetime.Token);
            server.Start(_lifetime.Token);
            client.Start(_lifetime.Token);
            await Task.WhenAll(server.WaitForConnectedAsync(), client.WaitForConnectedAsync()).WaitAsync(TimeSpan.FromSeconds(15));
            // Read-only instrumentation outside measurement; no production API/configuration changes.
            foreach (var connection in new[] { server, client })
            {
                var socket = (Socket)typeof(StreamConnection<object>).GetField("_socket", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(connection)!;
                Console.WriteLine($"StreamFrame socket: receive={socket.ReceiveBufferSize}, send={socket.SendBufferSize}, NoDelay={socket.NoDelay}, KeepAlive={socket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive)}");
                if (socket.ReceiveBufferSize != 65536 || socket.NoDelay)
                    throw new InvalidOperationException("Socket settings differ from the direct TCP control.");
            }
        }
    }

    [Benchmark(OperationsPerInvoke = Messages)]
    public async Task Transfer()
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        var ct = deadline.Token;
        _serverWork = Consume(ct);
        try
        {
            for (var i = 0; i < Messages; i++)
            {
                await _client.Send(_payload, ct);
                if (Echo) Validate(await _client.Receive(ct));
            }
            await _serverWork.WaitAsync(ct);
        }
        catch
        {
            deadline.Cancel();
            _lifetime.Cancel();
            try { await _serverWork; } catch { }
            throw;
        }
    }

    private async Task Consume(CancellationToken ct)
    {
        for (var i = 0; i < Messages; i++)
        {
            var message = await _server.Receive(ct);
            Validate(message);
            if (Echo) await _server.Send(message, ct);
        }
    }

    private void Validate(object message)
    {
        var valid = _payload is byte[] expected
            ? message is byte[] actual && actual.AsSpan().SequenceEqual(expected)
            : message is string value && string.Equals(value, (string)_payload, StringComparison.Ordinal);
        if (!valid) throw new InvalidDataException("Received payload differs from the complete expected payload.");
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _lifetime.Cancel();
        if (_client is not null) await _client.DisposeAsync();
        if (_server is not null) await _server.DisposeAsync();
        try { await _serverWork; } catch (OperationCanceledException) { }
        _lifetime.Dispose();
    }
}

/// <summary>No echo; completes after decoding and validating all 256 received messages.</summary>
public class OneWayThroughputBenchmarks : NetworkBenchmarkBase
{
    protected override bool Echo => false;
}

/// <summary>256 sequential round trips; each waits for a decoded, validated echo.</summary>
public class RoundTripLatencyBenchmarks : NetworkBenchmarkBase
{
    protected override bool Echo => true;
}

internal sealed class BenchmarkCodec(string mode) : ICodec<object>
{
    public object Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
        => mode == "Bytes" ? frame.ToArray() : Encoding.UTF8.GetString(frame);

    public void Encode(object message, IBufferWriter<byte> writer, CancellationToken ct = default)
    {
        if (mode == "Bytes") writer.Write((byte[])message);
        else if (mode == "StringSpan") Encoding.UTF8.GetBytes(((string)message).AsSpan(), writer);
        else writer.Write(Encoding.UTF8.GetBytes((string)message));
    }
}

internal abstract class BenchmarkEndpoint : IAsyncDisposable
{
    public abstract ValueTask Send(object message, CancellationToken ct);
    public abstract ValueTask<object> Receive(CancellationToken ct);
    public abstract ValueTask DisposeAsync();
}

internal sealed class FrameworkEndpoint(StreamConnection<object> connection, CancellationToken lifetime) : BenchmarkEndpoint
{
    private readonly IAsyncEnumerator<object> _messages = connection.GetMessages(lifetime).GetAsyncEnumerator(lifetime);
    private Task<bool>? _pendingReceive;
    public override async ValueTask Send(object message, CancellationToken ct) => await connection.SendAsync(message, ct);
    public override async ValueTask<object> Receive(CancellationToken ct)
    {
        _pendingReceive = _messages.MoveNextAsync().AsTask();
        if (!await _pendingReceive.WaitAsync(ct)) throw new EndOfStreamException();
        return _messages.Current;
    }
    public override async ValueTask DisposeAsync()
    {
        await connection.DisposeAsync();
        if (_pendingReceive is not null)
        {
            try { await _pendingReceive; } catch (OperationCanceledException) { }
        }
        await _messages.DisposeAsync();
    }
}
