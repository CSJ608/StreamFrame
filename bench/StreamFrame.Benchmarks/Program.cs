using BenchmarkDotNet.Running;

namespace StreamFrame.Benchmarks;

public static class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Contains("--verify-network"))
        {
            var cases = 0;
            foreach (var echo in new[] { false, true })
            foreach (var framer in new[] { "LengthPrefix", "StxEtx" })
            foreach (var size in new[] { 64, 1024, 65536 })
            foreach (var codec in new[] { "Bytes", "StringSpan", "StringAlloc" })
            foreach (var transport in new[] { "DirectTcp", "StreamFrame" })
            {
                NetworkBenchmarkBase benchmark = echo ? new RoundTripLatencyBenchmarks() : new OneWayThroughputBenchmarks();
                benchmark.Framer = framer;
                benchmark.PayloadBytes = size;
                benchmark.CodecMode = codec;
                benchmark.Transport = transport;
                try
                {
                    await benchmark.Setup();
                    await benchmark.Transfer();
                    await benchmark.Transfer(); // Detect stale completion / bytes across invocation boundaries.
                    Console.WriteLine($"PASS echo={echo} {framer} {size} {codec} {transport}: 2 x 256 validated messages");
                    cases++;
                }
                finally { await benchmark.Cleanup(); }
            }
            Console.WriteLine($"Network verification passed: {cases}/72 cases (not a timing measurement).");
            return;
        }
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
