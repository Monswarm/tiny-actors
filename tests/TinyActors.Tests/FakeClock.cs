using System;

namespace TinyActors.Tests
{
    public sealed class FakeClock : IClock
    {
        public FakeClock(DateTime now)
        {
            UtcNow = now;
        }

        public DateTime UtcNow { get; private set; }

        public void Advance(TimeSpan time) => UtcNow += time;
    }
}
