#if NET48
using System.Buffers;
using System.IO;
using System.Text;
using System.Threading.Tasks;

// 块式命名空间：一个文件多个命名空间（文件级命名空间仅限单个）
namespace System.Text
{
    internal static class EncodingSequenceCompat
    {
        public static string GetString(this Encoding encoding, in ReadOnlySequence<byte> bytes)
            => bytes.IsSingleSegment
                ? encoding.GetString(bytes.First.Span.ToArray())
                : encoding.GetString(bytes.ToArray());

        public static string GetString(this Encoding encoding, ReadOnlySpan<byte> bytes)
            => encoding.GetString(bytes.ToArray());
    }
}

namespace System.IO
{
    internal static class TestStreamCompat
    {
        // netfx 的 Stream 没有单参（ReadOnlyMemory）的 WriteAsync 重载
        public static Task WriteAsync(this Stream stream, byte[] buffer)
            => stream.WriteAsync(buffer, 0, buffer.Length);
    }
}
#endif

#if NET48
namespace System.Threading.Channels
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    internal static class TestChannelCompat
    {
        // netfx 的 Channels 包没有 ReadAllAsync（netcore 为实例方法，优先于本扩展）
        public static async IAsyncEnumerable<T> ReadAllAsync<T>(
            this ChannelReader<T> reader, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out var item))
                    yield return item;
            }
        }
    }
}
#endif
