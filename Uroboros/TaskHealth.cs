#nullable enable
using System;
using System.Collections.Concurrent;

public sealed class TaskHealthTracker
{
    public sealed record Health(
        DateTimeOffset? LastOkAtUtc,
        DateTimeOffset? LastFailAtUtc,
        string? LastError,
        long? LastDurationMs
    );

    private readonly ConcurrentDictionary<string, Health> _map = new(StringComparer.OrdinalIgnoreCase);

    public Health Get(string name)
        => _map.TryGetValue(name, out var h) ? h : new Health(null, null, null, null);

    public void MarkOk(string name, DateTimeOffset atUtc, long durationMs)
        => _map[name] = new Health(atUtc, _map.TryGetValue(name, out var h) ? h.LastFailAtUtc : null, null, durationMs);

    public void MarkFail(string name, DateTimeOffset atUtc, string err, long? durationMs)
    {
        _map.TryGetValue(name, out var h);
        _map[name] = new Health(h?.LastOkAtUtc, atUtc, err, durationMs ?? h?.LastDurationMs);
    }
}

public sealed class NextRunTracker
{
    private readonly ConcurrentDictionary<string, DateTimeOffset> _next = new(StringComparer.OrdinalIgnoreCase);

    public void SetNext(string name, DateTimeOffset nextUtc) => _next[name] = nextUtc;
    public DateTimeOffset? GetNext(string name) => _next.TryGetValue(name, out var v) ? v : null;
}
