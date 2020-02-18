using BenchmarkDotNet.Running;

namespace TinyActors.Benchmarks
{
    public static class Program
    {
        public static void Main() => BenchmarkRunner.Run<CounterBenchmarks>();
    }
}
