using System;

namespace TinyActors
{
    public sealed class ActorSystemOptions
    {
        // Inactivity time after which an actor is passivated
        public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(5);
    }
}
