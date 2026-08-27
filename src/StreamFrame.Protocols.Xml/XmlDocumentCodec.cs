using System.Buffers;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace StreamFrame.Protocols.Xml;

/// <summary>
/// 把 XML 报文解析为 <see cref="XDocument"/> 的通用 codec。
///
/// 驱动骨架示例：用户可按此结构自定义二进制 codec。
/// </summary>
public sealed class XmlDocumentCodec : ICodec<XDocument>
{
    /// <summary>帧内负载的上限，防御异常输入。</summary>
    public int MaxDocumentBytes { get; init; } = 16 * 1024 * 1024;

    public XDocument Decode(in ReadOnlySequence<byte> frame, CancellationToken ct = default)
    {
        if (frame.Length > MaxDocumentBytes)
            throw new InvalidOperationException($"XML frame of {frame.Length} bytes exceeds MaxDocumentBytes={MaxDocumentBytes}.");

        ct.ThrowIfCancellationRequested();

        using var stream = new SequenceToStream(frame);
        using var reader = XmlReader.Create(stream, new XmlReaderSettings
        {
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null,
        });
        return XDocument.Load(reader, LoadOptions.None);
    }

    public void Encode(XDocument message, IBufferWriter<byte> writer, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        using var xmlWriter = XmlWriter.Create(new BufferWriterStream(writer), new XmlWriterSettings
        {
            Encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            OmitXmlDeclaration = false,
        });
        message.WriteTo(xmlWriter);
        xmlWriter.Flush();
    }
}
