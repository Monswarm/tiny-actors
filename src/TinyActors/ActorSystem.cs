using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TinyActors
{
    // One actor per key, single node. Activated on its first message and
    // passivated after IdleTimeout without activity.
    public sealed class ActorSystem<TKey, TMessage, TResult> : IAsyncDisposable
        where TKey : notnull
    {
        private readonly ConcurrentDictionary<TKey, ActorCell> _cells = new ConcurrentDictionary<TKey, ActorCell>();
        private readonly Func<TKey, IActorBehavior<TMessage, TResult>> _factory;
        private readonly ActorSystemOptions _options;
        private readonly IClock _clock;
        private volatile bool _disposed;

        public ActorSystem(
            Func<TKey, IActorBehavior<TMessage, TResult>> factory,
            ActorSystemOptions? options = null,
            IClock? clock = null)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _options = options ?? new ActorSystemOptions();
            _clock = clock ?? new SystemClock();
        }

        public int ActiveCount => _cells.Count;

        public async ValueTask<TResult> AskAsync(TKey key, TMessage message)
        {
            var reply = new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            var envelope = new Envelope(message, reply);

            while (true)
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(ActorSystem<TKey, TMessage, TResult>));
                }

                var cell = _cells.GetOrAdd(key, k => new ActorCell(this, k));
                if (cell.TryPost(envelope))
                {
                    break;
                }

                // The actor is being passivated: wait for it to finish before activating another one
                await cell.Completion.ConfigureAwait(false);
            }

            return await reply.Task.ConfigureAwait(false);
        }

        // Passivates idle actors. Returns how many were passivated.
        public async Task<int> SweepAsync(DateTime now)
        {
            var stopping = new List<Task>();
            foreach (var cell in _cells.Values)
            {
                if (cell.IsIdle(now))
                {
                    cell.Complete();
                    stopping.Add(cell.Completion);
                }
            }

            await Task.WhenAll(stopping).ConfigureAwait(false);
            return stopping.Count;
        }

        public async Task RunSweeperAsync(TimeSpan interval, CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
                    await SweepAsync(_clock.UtcNow).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        public async ValueTask DisposeAsync()
        {
            _disposed = true;

            var stopping = new List<Task>();
            foreach (var cell in _cells.Values)
            {
                cell.Complete();
                stopping.Add(cell.Completion);
            }

            await Task.WhenAll(stopping).ConfigureAwait(false);
        }

        private readonly struct Envelope
        {
            public Envelope(TMessage message, TaskCompletionSource<TResult> reply)
            {
                Message = message;
                Reply = reply;
            }

            public TMessage Message { get; }
            public TaskCompletionSource<TResult> Reply { get; }
        }

        private sealed class ActorCell
        {
            private readonly ActorSystem<TKey, TMessage, TResult> _system;
            private readonly TKey _key;
            private readonly Mailbox<Envelope> _mailbox = new Mailbox<Envelope>();
            private readonly TaskCompletionSource<bool> _completion =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            private int _started;
            private int _pending;
            private long _lastActivity;

            public ActorCell(ActorSystem<TKey, TMessage, TResult> system, TKey key)
            {
                _system = system;
                _key = key;
                Touch();
            }

            public Task Completion => _completion.Task;

            public bool IsIdle(DateTime now)
            {
                if (Volatile.Read(ref _pending) != 0)
                {
                    return false;
                }

                var last = new DateTime(Volatile.Read(ref _lastActivity), DateTimeKind.Utc);
                return now - last >= _system._options.IdleTimeout;
            }

            public bool TryPost(Envelope envelope)
            {
                EnsureStarted();

                Interlocked.Increment(ref _pending);
                if (_mailbox.TryPost(envelope))
                {
                    Touch();
                    return true;
                }

                Interlocked.Decrement(ref _pending);
                return false;
            }

            // Messages already queued are still processed before stopping
            public void Complete()
            {
                EnsureStarted();
                _mailbox.Complete();
            }

            private void Touch() =>
                Volatile.Write(ref _lastActivity, _system._clock.UtcNow.Ticks);

            // GetOrAdd can create cells that lose the race: only the one in use is started
            private void EnsureStarted()
            {
                if (Interlocked.Exchange(ref _started, 1) == 0)
                {
                    Task.Run(RunAsync);
                }
            }

            private async Task RunAsync()
            {
                IActorBehavior<TMessage, TResult>? behavior = null;
                Exception? fatal = null;

                try
                {
                    behavior = _system._factory(_key);

                    await foreach (var envelope in _mailbox.ReadAllAsync().ConfigureAwait(false))
                    {
                        try
                        {
                            var result = await behavior.ReceiveAsync(envelope.Message).ConfigureAwait(false);
                            envelope.Reply.TrySetResult(result);
                        }
                        catch (Exception ex)
                        {
                            // Supervision: the caller gets the error and the actor starts over
                            envelope.Reply.TrySetException(ex);
                            var failed = behavior;
                            behavior = null;
                            await DisposeBehaviorAsync(failed).ConfigureAwait(false);
                            behavior = _system._factory(_key);
                        }
                        finally
                        {
                            Interlocked.Decrement(ref _pending);
                            Touch();
                        }
                    }
                }
                catch (Exception ex)
                {
                    // The actor could not be recreated
                    fatal = ex;
                }

                _mailbox.Complete();
                if (fatal != null)
                {
                    await foreach (var envelope in _mailbox.ReadAllAsync().ConfigureAwait(false))
                    {
                        envelope.Reply.TrySetException(fatal);
                    }
                }

                await DisposeBehaviorAsync(behavior).ConfigureAwait(false);

                ((ICollection<KeyValuePair<TKey, ActorCell>>)_system._cells)
                    .Remove(new KeyValuePair<TKey, ActorCell>(_key, this));
                _completion.TrySetResult(true);
            }

            private static async ValueTask DisposeBehaviorAsync(IActorBehavior<TMessage, TResult>? behavior)
            {
                switch (behavior)
                {
                    case IAsyncDisposable asyncDisposable:
                        await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                        break;
                    case IDisposable disposable:
                        disposable.Dispose();
                        break;
                }
            }
        }
    }
}
