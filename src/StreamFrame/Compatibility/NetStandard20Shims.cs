#if NETSTANDARD2_0
using System;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace System.Threading.Tasks
{
    internal static class TaskWaitAsyncCompat
    {
        /// <summary>Task.WaitAsync(CancellationToken) 的 netstandard2.0 等价物（net5+ 内置）。</summary>
        public static async Task WaitAsync(this Task task, CancellationToken cancellationToken)
        {
            var cancelled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = cancellationToken.Register(
                static state => ((TaskCompletionSource<bool>)state!).TrySetResult(true),
                cancelled,
                useSynchronizationContext: false);

            var completed = await Task.WhenAny(task, cancelled.Task).ConfigureAwait(false);
            if (completed != task)
                throw new OperationCanceledException(cancellationToken);

            await task.ConfigureAwait(false);
        }

        /// <summary>
        /// 旧 SocketTaskExtensions（System.Net.Sockets 4.3.0）没有带 CancellationToken 的重载：
        /// 用 3 参重载发起操作，等待阶段可被取消（netfx 本就无法真正中止已提交的 socket I/O，
        /// 取消后操作结果被丢弃，socket 随后关闭）。
        /// </summary>
        public static async Task<int> WithCancellation(this Task<int> task, CancellationToken cancellationToken)
        {
            var cancelled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = cancellationToken.Register(
                static state => ((TaskCompletionSource<bool>)state!).TrySetResult(true),
                cancelled,
                useSynchronizationContext: false);

            var completed = await Task.WhenAny(task, cancelled.Task).ConfigureAwait(false);
            if (completed != task)
                throw new OperationCanceledException(cancellationToken);

            return await task.ConfigureAwait(false);
        }
    }
}

namespace System.Net.Sockets
{
    internal static class SocketMemoryCompat
    {
        public static Task<int> ReceiveAsync(
            this Socket socket, Memory<byte> buffer, SocketFlags socketFlags, CancellationToken cancellationToken)
            => MemoryMarshal.TryGetArray((ReadOnlyMemory<byte>)buffer, out var segment)
                ? SocketTaskExtensions.ReceiveAsync(socket, segment, socketFlags).WithCancellation(cancellationToken)
                : Task.FromException<int>(new ArgumentException(
                    "netstandard2.0 目标要求接收缓冲为数组支撑的 Memory。", nameof(buffer)));

        public static Task<int> SendAsync(
            this Socket socket, ReadOnlyMemory<byte> buffer, SocketFlags socketFlags, CancellationToken cancellationToken)
            => MemoryMarshal.TryGetArray(buffer, out var segment)
                ? SocketTaskExtensions.SendAsync(socket, segment, socketFlags).WithCancellation(cancellationToken)
                : Task.FromException<int>(new ArgumentException(
                    "netstandard2.0 目标要求发送缓冲为数组支撑的 Memory。", nameof(buffer)));
    }
}
#endif
