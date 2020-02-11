using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace TinyActors
{
    // Message queue of an actor. Many producers, a single consumer.
    public sealed class Mailbox<T>
    {
        private readonly Channel<T> _channel;

        // capacity <= 0: unbounded
        public Mailbox(int capacity = 0)
        {
            _channel = capacity > 0
                ? Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
                {
                    SingleReader = true,
                    FullMode = BoundedChannelFullMode.Wait
                })
                : Channel.CreateUnbounded<T>(new UnboundedChannelOptions
                {
                    SingleReader = true
                });
        }

        public bool TryPost(T message) => _channel.Writer.TryWrite(message);

        public ValueTask PostAsync(T message, CancellationToken cancellationToken = default) =>
            _channel.Writer.WriteAsync(message, cancellationToken);

        public IAsyncEnumerable<T> ReadAllAsync(CancellationToken cancellationToken = default) =>
            _channel.Reader.ReadAllAsync(cancellationToken);

        // Accepts no more messages; what is already queued can still be read
        public void Complete() => _channel.Writer.TryComplete();
    }
}
