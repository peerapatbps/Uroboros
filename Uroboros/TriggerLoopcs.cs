using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Uroboros;

public static class TriggerLoop
{
    public sealed record Job(string Name, Func<IEngineTask> Factory, DateTimeOffset NextRunUtc);

    public static Task RunAsync(
        EngineContext ctx,
        Scheduler sched,
        StageGate gate,
        TaskConfigService cfgSvc,
        NextRunTracker nextRun,
        IReadOnlyList<(string Name, Func<IEngineTask> Factory, int DefaultIntervalMs)> catalog,
        CancellationToken ct)
    {
        if (catalog is null) throw new ArgumentNullException(nameof(catalog));
        if (catalog.Count == 0) return Task.CompletedTask;

        // build default map once (case-insensitive)
        var defaultMsByName = catalog
            .GroupBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.First().DefaultIntervalMs, StringComparer.OrdinalIgnoreCase);

        // hard floor to avoid 0/negative and crazy spam
        const int MinIntervalMs = 250;

        return Task.Run(async () =>
        {
            var tick = TimeSpan.FromMilliseconds(250);

            // ✅ keep last seen forcerun stamp per task (avoid re-trigger forever)
            var lastForceSeen = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

            // snapshot once at seed
            var snap0 = cfgSvc.Snapshot();
            var now0 = DateTimeOffset.UtcNow;

            var jobs = new List<Job>(catalog.Count);

            foreach (var it in catalog)
            {
                var defMs = ClampMs(defaultMsByName[it.Name], MinIntervalMs);
                var ms = ClampMs(snap0.GetIntervalMsOrDefault(it.Name, defMs), MinIntervalMs);

                var next = now0 + TimeSpan.FromMilliseconds(ms);
                jobs.Add(new Job(it.Name, it.Factory, next));
                nextRun.SetNext(it.Name, next);

                // seed lastForceSeen from current config (so old values don't trigger)
                if (snap0.Map.TryGetValue(it.Name, out var rc0))
                    lastForceSeen[it.Name] = rc0.UpdatedAtUnixMs;
                else
                    lastForceSeen[it.Name] = 0;
            }

            while (!ct.IsCancellationRequested)
            {
                var now = DateTimeOffset.UtcNow;
                var snap = cfgSvc.Snapshot();

                for (int i = 0; i < jobs.Count; i++)
                {
                    var j = jobs[i];

                    // ✅ Apply ForceRun hint (updated_at_unixms) BEFORE due check
                    // Treat only near-future timestamps (force-run pattern now+3s) to avoid "save config" noise.
                    if (snap.Map.TryGetValue(j.Name, out var rcfg) && rcfg.Enabled)
                    {
                        var stamp = rcfg.UpdatedAtUnixMs;
                        lastForceSeen.TryGetValue(j.Name, out var seen);

                        if (stamp > seen)
                        {
                            DateTimeOffset dueUtc;
                            try { dueUtc = DateTimeOffset.FromUnixTimeMilliseconds(stamp); }
                            catch { dueUtc = default; }

                            if (dueUtc != default)
                            {
                                // window: (now - 1s) .. (now + 30s)
                                if (dueUtc >= now.AddSeconds(-1) && dueUtc <= now.AddSeconds(30))
                                {
                                    // pull next earlier if needed
                                    if (dueUtc < j.NextRunUtc)
                                    {
                                        j = j with { NextRunUtc = dueUtc };
                                        jobs[i] = j;
                                        nextRun.SetNext(j.Name, dueUtc);
                                    }

                                    // mark consumed (so it won't keep re-trigger)
                                    lastForceSeen[j.Name] = stamp;
                                }
                                else
                                {
                                    // not in force window -> ignore, but still advance "seen" to prevent repeated checks
                                    lastForceSeen[j.Name] = stamp;
                                }
                            }
                            else
                            {
                                lastForceSeen[j.Name] = stamp;
                            }
                        }
                    }

                    // not due yet
                    if (now < j.NextRunUtc)
                        continue;

                    var defMs = ClampMs(defaultMsByName[j.Name], MinIntervalMs);
                    var everyMs = ClampMs(snap.GetIntervalMsOrDefault(j.Name, defMs), MinIntervalMs);
                    var every = TimeSpan.FromMilliseconds(everyMs);

                    // CASE 1) PAUSED: do NOT enqueue, but move phase forward to prevent burst on resume
                    if (gate.IsPaused)
                    {
                        var nextPaused = ComputeNextKeepPhase(j.NextRunUtc, now, every);
                        jobs[i] = j with { NextRunUtc = nextPaused };
                        nextRun.SetNext(j.Name, nextPaused);
                        continue;
                    }

                    // CASE 2) NOT PAUSED: due => maybe enqueue if enabled
                    if (snap.IsEnabled(j.Name))
                    {
                        sched.TryEnqueue(j.Factory());
                        // ถ้าคุณอยากให้ CLI เห็นชัด ๆ ว่ามีการ enqueue จริง ให้ปลดคอมเมนต์บรรทัดนี้
                        // try { ctx.Log.Info($"[TRIG] enqueued: {j.Name}"); } catch { }
                    }

                    // Keep phase advance regardless of enabled (so disable while waiting stops immediately)
                    var next = ComputeNextKeepPhase(j.NextRunUtc, now, every);
                    jobs[i] = j with { NextRunUtc = next };
                    nextRun.SetNext(j.Name, next);
                }

                await Task.Delay(tick, ct).ConfigureAwait(false);
            }
        }, ct);

        static int ClampMs(int ms, int minMs)
        {
            if (ms < minMs) return minMs;
            if (ms > 24 * 60 * 60 * 1000) return 24 * 60 * 60 * 1000; // safety: 24h
            return ms;
        }

        static DateTimeOffset ComputeNextKeepPhase(DateTimeOffset prevNext, DateTimeOffset now, TimeSpan every)
        {
            // base: prevNext + every
            var next = prevNext + every;

            if (next > now)
                return next;

            // catch-up jump (no backlog enqueue, just jump phase)
            var deltaMs = (now - prevNext).TotalMilliseconds;
            var everyMs = every.TotalMilliseconds;

            if (everyMs <= 0.0) // absolute safety (should never happen due to clamp)
                return now + TimeSpan.FromMilliseconds(250);

            var k = (long)Math.Floor(deltaMs / everyMs) + 1;  // at least 1 step forward
            return prevNext + TimeSpan.FromMilliseconds(everyMs * k);
        }
    }
}