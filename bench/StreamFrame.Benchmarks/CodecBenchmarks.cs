using System.Buffers;
using System.Text;
using System.Xml.Linq;
using BenchmarkDotNet.Attributes;
using StreamFrame.Protocols.Xml;

namespace StreamFrame.Benchmarks;

/// <summary>
/// XML codec（官方示例驱动 XmlDocumentCodec）的编解码开销基准。
/// 回答"接 XML 设备时序列化值不值得担心"——用典型设备报文尺寸度量。
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 15)]
public class CodecBenchmarks
{
    private readonly XmlDocumentCodec _codec = new();
    private XDocument _small = null!;
    private XDocument _large = null!;
    private ReadOnlySequence<byte> _smallBytes = default;
    private ReadOnlySequence<byte> _largeBytes = default;

    [GlobalSetup]
    public void Setup()
    {
        _small = XDocument.Parse(MakeMessage(items: 3));   // ~400B 报文（心跳/单值上报）
        _large = XDocument.Parse(MakeMessage(items: 48));  // ~4KB 报文（批量数据/明细）
        _smallBytes = new ReadOnlySequence<byte>(Encode(_small));
        _largeBytes = new ReadOnlySequence<byte>(Encode(_large));
    }

    [Benchmark] public XDocument Decode_Small400B() => _codec.Decode(_smallBytes);
    [Benchmark] public XDocument Decode_Large4KB() => _codec.Decode(_largeBytes);

    [Benchmark] public int Encode_Small400B() => EncodeToBuffer(_small).WrittenCount;
    [Benchmark] public int Encode_Large4KB() => EncodeToBuffer(_large).WrittenCount;

    // ----- 辅助 -----

    private ArrayBufferWriter<byte> EncodeToBuffer(XDocument doc)
    {
        var writer = new ArrayBufferWriter<byte>();
        _codec.Encode(doc, writer);
        return writer;
    }

    private static byte[] Encode(XDocument doc)
    {
        var writer = new ArrayBufferWriter<byte>();
        new XmlDocumentCodec().Encode(doc, writer);
        return writer.WrittenSpan.ToArray();
    }

    private static string MakeMessage(int items)
    {
        var sb = new StringBuilder(128);
        sb.Append("<Message><Id>42</Id><Device>PLC-A</Device><Items>");
        for (var i = 0; i < items; i++)
            sb.Append("<Item><Index>").Append(i).Append("</Index><Value>0123456789ABCDEF</Value></Item>");
        sb.Append("</Items></Message>");
        return sb.ToString();
    }
}
