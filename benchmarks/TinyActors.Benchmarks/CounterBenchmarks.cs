using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;

namespace TinyActors.Benchmarks
{
    // Per-key counter incremented from several concurrent clients
    [MemoryDiagnoser]
    [SimpleJob(RunStrategy.Throughput, launchCount: 1, warmupCount: 3, targetCount: 10)]
    public class CounterBenchmarks
    {
        private const int Operations = 20_000;
        private const int Clients = 4;

        private sealed class CounterActor : IActorBehavior<int, long>
        {
            private long _value;

            public ValueTask<long> ReceiveAsync(int message)
            {
                _value += message;
                return new ValueTask<long>(_value);
            }
        }

        private sealed class LockedCounter
        {
            public long Value;
        }

        private ActorSystem<int, int, long> _system = null!;
        private LockedCounter[] _locked = null!;
        private ConcurrentDictionary<int, long> _dictionary = null!;

        [Params(1, 100)]
        public int Keys { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            _system = new ActorSystem<int, int, long>(_ => new CounterActor());
            _locked = Enumerable.Range(0, Keys).Select(_ => new LockedCounter()).ToArray();
            _dictionary = new ConcurrentDictionary<int, long>();
        }

        [GlobalCleanup]
        public void Cleanup() => _system.DisposeAsync().AsTask().GetAwaiter().GetResult();

        [Benchmark]
        public Task Actors() => RunClients(async client =>
        {
            for (var i = 0; i < Operations / Clients; i++)
            {
                await _system.AskAsync((client + i) % Keys, 1).ConfigureAwait(false);
            }
        });

        [Benchmark(Baseline = true)]
        public Task Lock() => RunClients(client =>
        {
            for (var i = 0; i < Operations / Clients; i++)
            {
                var counter = _locked[(client + i) % Keys];
                lock (counter)
                {
                    counter.Value++;
                }
            }

            return Task.CompletedTask;
        });

        [Benchmark]
        public Task ConcurrentDictionary() => RunClients(client =>
        {
            for (var i = 0; i < Operations / Clients; i++)
            {
                _dictionary.AddOrUpdate((client + i) % Keys, 1, (_, value) => value + 1);
            }

            return Task.CompletedTask;
        });

        private static Task RunClients(Func<int, Task> client) =>
            Task.WhenAll(Enumerable.Range(0, Clients).Select(c => Task.Run(() => client(c))));
    }
}
