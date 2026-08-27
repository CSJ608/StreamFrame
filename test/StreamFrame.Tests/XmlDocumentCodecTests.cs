using System.Buffers;
using System.Text;
using System.Xml.Linq;
using StreamFrame.Protocols.Xml;

namespace StreamFrame.Tests;

public class XmlDocumentCodecTests
{
    [Fact]
    public void EncodeThenDecode_RoundTripsDocument()
    {
        var codec = new XmlDocumentCodec();
        var doc = XDocument.Parse("<Message><Id>42</Id><Name>DeviceA</Name></Message>");

        var writer = new ArrayBufferWriter<byte>();
        codec.Encode(doc, writer);

        // 编码结果应包含 XML 内容
        var xmlText = Encoding.UTF8.GetString(writer.WrittenSpan);
        Assert.Contains("<Message>", xmlText);
        Assert.Contains("DeviceA", xmlText);

        // 解码还原
        var decoded = codec.Decode(new ReadOnlySequence<byte>(writer.WrittenSpan.ToArray()));
        Assert.Equal("42", decoded.Root?.Element("Id")?.Value);
        Assert.Equal("DeviceA", decoded.Root?.Element("Name")?.Value);
    }

    [Fact]
    public void Decode_RejectsDtd()
    {
        var codec = new XmlDocumentCodec();
        var malicious = Encoding.UTF8.GetBytes(
            "<!DOCTYPE Message [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]><Message>&xxe;</Message>");

        Assert.ThrowsAny<Exception>(() => codec.Decode(new ReadOnlySequence<byte>(malicious)));
    }

    [Fact]
    public void Decode_EmptyFrame_Throws()
    {
        var codec = new XmlDocumentCodec();
        Assert.ThrowsAny<Exception>(() => codec.Decode(new ReadOnlySequence<byte>(Array.Empty<byte>())));
    }
}
