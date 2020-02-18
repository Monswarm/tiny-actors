# tiny-actors

Pet project. I wanted to build my own lightweight actor solution for .NET: single node, no dependencies, Orleans-like but much smaller.

## What's in it

- One mailbox per actor on top of `System.Threading.Channels`.
- Request/response with `ValueTask`.
- Activation on demand by key and passivation on inactivity.
- Basic supervision: if an actor fails, the caller gets the exception and the actor is restarted with fresh state.

## Usage

```csharp
class Counter : IActorBehavior<string, int>
{
    private int _count;

    public ValueTask<int> ReceiveAsync(string message)
    {
        _count++;
        return new ValueTask<int>(_count);
    }
}

await using var system = new ActorSystem<string, string, int>(key => new Counter());

var n = await system.AskAsync("player-1", "hit");
```

To passivate idle actors, start the sweeper in the background:

```csharp
_ = system.RunSweeperAsync(TimeSpan.FromSeconds(30), cts.Token);
```

## Tests

```
dotnet test
```

Requires the .NET Core 3.1 SDK.

## Benchmarks

A per-key counter incremented from 4 concurrent clients (20,000 operations per iteration), comparing actors against a `lock` per counter and a `ConcurrentDictionary` using `AddOrUpdate`.

```
dotnet run -c Release -p benchmarks/TinyActors.Benchmarks
```

BenchmarkDotNet 0.12.0, .NET Core 3.1, Linux (Docker), 8 cores / 16 threads, 10 iterations after 3 warmup iterations:

|               Method | Keys |        Mean |     Error |    StdDev | Ratio |    Gen 0 |  Allocated |
|--------------------- |----- |------------:|----------:|----------:|------:|---------:|-----------:|
|               Actors |    1 | 11,188.5 us | 454.17 us | 270.27 us | 15.28 | 140.6250 | 6875.58 KB |
|                 Lock |    1 |    732.1 us |  20.06 us |  13.27 us |  1.00 |        - |    1.45 KB |
| ConcurrentDictionary |    1 |  2,259.6 us |  26.05 us |  17.23 us |  3.09 |        - |    1.45 KB |
|               Actors |  100 | 11,513.3 us | 688.85 us | 455.63 us | 59.37 | 125.0000 | 6617.47 KB |
|                 Lock |  100 |    194.0 us |   5.43 us |   3.59 us |  1.00 |        - |    1.45 KB |
| ConcurrentDictionary |  100 |    321.2 us |   7.23 us |   4.78 us |  1.66 |        - |    1.45 KB |

An actor costs quite a bit more than a `lock` for an operation this cheap (a `TaskCompletionSource` and a mailbox entry per request, about 340 bytes per operation). The advantage is not speed but not sharing state and not blocking threads.
