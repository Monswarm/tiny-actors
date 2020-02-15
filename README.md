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
