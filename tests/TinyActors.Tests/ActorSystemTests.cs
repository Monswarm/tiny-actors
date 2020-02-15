using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace TinyActors.Tests
{
    public class ActorSystemTests
    {
        private static readonly DateTime Start = new DateTime(2020, 2, 15, 10, 0, 0, DateTimeKind.Utc);

        private sealed class Counter : IActorBehavior<string, int>
        {
            private int _count;

            public ValueTask<int> ReceiveAsync(string message)
            {
                if (message == "boom")
                {
                    throw new InvalidOperationException("boom");
                }

                _count++;
                return new ValueTask<int>(_count);
            }
        }

        private sealed class OverlapProbe : IActorBehavior<int, int>
        {
            private readonly Func<int> _enter;
            private readonly Action _leave;

            public OverlapProbe(Func<int> enter, Action leave)
            {
                _enter = enter;
                _leave = leave;
            }

            public async ValueTask<int> ReceiveAsync(int message)
            {
                var running = _enter();
                await Task.Yield();
                _leave();
                return running;
            }
        }

        private sealed class Recorder : IActorBehavior<int, int>
        {
            private readonly List<int> _seen;

            public Recorder(List<int> seen)
            {
                _seen = seen;
            }

            public ValueTask<int> ReceiveAsync(int message)
            {
                _seen.Add(message);
                return new ValueTask<int>(message);
            }
        }

        [Fact]
        public async Task Messages_are_processed_in_order()
        {
            var seen = new List<int>();
            await using var system = new ActorSystem<string, int, int>(_ => new Recorder(seen));

            var asks = Enumerable.Range(0, 200).Select(i => system.AskAsync("a", i).AsTask()).ToList();
            await Task.WhenAll(asks);

            Assert.Equal(Enumerable.Range(0, 200), seen);
        }

        [Fact]
        public async Task An_actor_never_handles_two_messages_at_once()
        {
            var running = 0;
            var maxRunning = 0;
            int Enter()
            {
                var now = Interlocked.Increment(ref running);
                int max;
                while (now > (max = Volatile.Read(ref maxRunning)))
                {
                    Interlocked.CompareExchange(ref maxRunning, now, max);
                }

                return now;
            }

            await using var system = new ActorSystem<string, int, int>(
                _ => new OverlapProbe(Enter, () => Interlocked.Decrement(ref running)));

            var asks = Enumerable.Range(0, 100)
                .Select(i => Task.Run(() => system.AskAsync("a", i).AsTask()))
                .ToList();
            await Task.WhenAll(asks);

            Assert.Equal(1, maxRunning);
        }

        [Fact]
        public async Task Concurrent_requests_activate_a_single_actor_per_key()
        {
            var created = 0;
            await using var system = new ActorSystem<string, string, int>(_ =>
            {
                Interlocked.Increment(ref created);
                return new Counter();
            });

            var asks = Enumerable.Range(0, 50)
                .Select(_ => Task.Run(() => system.AskAsync("a", "x").AsTask()))
                .ToList();
            var results = await Task.WhenAll(asks);

            Assert.Equal(1, created);
            Assert.Equal(Enumerable.Range(1, 50), results.OrderBy(r => r));
        }

        [Fact]
        public async Task A_failing_actor_is_restarted_and_the_caller_gets_the_error()
        {
            var created = 0;
            await using var system = new ActorSystem<string, string, int>(_ =>
            {
                Interlocked.Increment(ref created);
                return new Counter();
            });

            Assert.Equal(1, await system.AskAsync("a", "x"));
            Assert.Equal(2, await system.AskAsync("a", "x"));

            await Assert.ThrowsAsync<InvalidOperationException>(async () => await system.AskAsync("a", "boom"));

            // fresh state after the restart
            Assert.Equal(1, await system.AskAsync("a", "x"));
            Assert.Equal(2, created);
        }

        [Fact]
        public async Task Idle_actors_are_passivated_and_reactivated_on_demand()
        {
            var created = 0;
            var clock = new FakeClock(Start);
            await using var system = new ActorSystem<string, string, int>(
                _ =>
                {
                    Interlocked.Increment(ref created);
                    return new Counter();
                },
                new ActorSystemOptions { IdleTimeout = TimeSpan.FromMinutes(5) },
                clock);

            Assert.Equal(1, await system.AskAsync("a", "x"));
            Assert.Equal(2, await system.AskAsync("a", "x"));

            clock.Advance(TimeSpan.FromMinutes(4));
            Assert.Equal(0, await system.SweepAsync(clock.UtcNow));
            Assert.Equal(1, system.ActiveCount);

            clock.Advance(TimeSpan.FromMinutes(2));
            Assert.Equal(1, await system.SweepAsync(clock.UtcNow));
            Assert.Equal(0, system.ActiveCount);

            // reactivated with fresh state
            Assert.Equal(1, await system.AskAsync("a", "x"));
            Assert.Equal(2, created);
        }
    }
}
