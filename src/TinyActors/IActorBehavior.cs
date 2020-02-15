using System.Threading.Tasks;

namespace TinyActors
{
    // An actor handles messages one at a time. If it implements IDisposable /
    // IAsyncDisposable it is disposed when passivated or restarted.
    public interface IActorBehavior<TMessage, TResult>
    {
        ValueTask<TResult> ReceiveAsync(TMessage message);
    }
}
