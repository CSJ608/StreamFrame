using System.Buffers;
using System.Net.Sockets;

namespace StreamFrame.Benchmarks;

/// <summary>
/// Same codec and streaming framer. Reads a known fixed wire length into a reusable buffer,
/// then parses/materializes it. Omits queues, Pipe, reconnect and arbitrary-size assembly.
/// </summary>
internal sealed class DirectTcpEndpoint : BenchmarkEndpoint
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    private readonly IStreamingFramer _framer;
    private readonly BenchmarkCodec _codec;
    private readonly byte[] _receiveBuffer;

    public DirectTcpEndpoint(TcpClient client, IStreamingFramer framer, BenchmarkCodec codec, int payloadBytes)
    {
        _client = client;
        // NetworkStream constructor requires a blocking socket; async I/O uses nonblocking mode afterwards.
        _stream = client.GetStream();
        client.Client.Blocking = false;
        client.NoDelay = false;
        client.ReceiveBufferSize = 65536;
        client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, false);
        _framer = framer;
        _codec = codec;
        _receiveBuffer = new byte[payloadBytes + (framer is LengthPrefixFramer ? 4 : 2)];
        Console.WriteLine($"Direct TCP socket: receive={client.ReceiveBufferSize}, send={client.SendBufferSize}, NoDelay={client.NoDelay}, KeepAlive=false");
    }

    public override async ValueTask Send(object message, CancellationToken ct)
    {
        using var writer = new PooledBufferWriter(_receiveBuffer.Length);
        _framer.BeginFrame(writer);
        _codec.Encode(message, writer, ct);
        _framer.EndFrame(writer);
        await _stream.WriteAsync(writer.WrittenMemory, ct);
    }

    public override async ValueTask<object> Receive(CancellationToken ct)
    {
        await _stream.ReadExactlyAsync(_receiveBuffer.AsMemory(), ct);
        var buffer = new ReadOnlySequence<byte>(_receiveBuffer);
        if (!_framer.TryDecodeFrame(ref buffer, out var payload) || !buffer.IsEmpty)
            throw new InvalidDataException("Expected exactly one complete frame.");
        return _codec.Decode(payload, ct);
    }

    public override ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
