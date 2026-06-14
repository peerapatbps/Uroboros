using System.Diagnostics;

namespace Uroboros.SelfTest;

// ── Result types (serialised as camelCase by JsonSerializerDefaults.Web) ─────

public sealed record SelfTestCase(
    string  Name,
    bool    Passed,
    string? Error,
    long    DurationMs);

public sealed record SelfTestReport(
    bool              Ok,
    int               Total,
    int               Passed,
    int               Failed,
    long              DurationMs,
    DateTimeOffset    ServerTsUtc,
    List<SelfTestCase> Cases);

// ── Runner ────────────────────────────────────────────────────────────────────

public static class SelfTestRunner
{
    /// <summary>
    /// Run all self-test suites synchronously on the calling thread.
    /// Safe to call from any async handler via Task.Run if needed.
    /// </summary>
    public static SelfTestReport RunAll()
    {
        var sw    = Stopwatch.StartNew();
        var cases = new List<SelfTestCase>();

        cases.AddRange(ST_HtmlText.Run());
        cases.AddRange(ST_TaskHealth.Run());
        cases.AddRange(ST_StageGate.Run());
        cases.AddRange(ST_SqliteProvider.Run());

        sw.Stop();

        int passed = cases.Count(c => c.Passed);
        int failed = cases.Count - passed;

        return new SelfTestReport(
            Ok:          failed == 0,
            Total:       cases.Count,
            Passed:      passed,
            Failed:      failed,
            DurationMs:  sw.ElapsedMilliseconds,
            ServerTsUtc: DateTimeOffset.UtcNow,
            Cases:       cases);
    }

    // ── Assertion helpers shared by all ST_ files ─────────────────────────────

    internal static SelfTestCase Run(string name, Action body)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            body();
            return new SelfTestCase(name, true, null, sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            return new SelfTestCase(name, false, ex.Message, sw.ElapsedMilliseconds);
        }
    }

    internal static void Assert(bool condition, string? message = null)
    {
        if (!condition)
            throw new Exception(message ?? "Assertion failed");
    }

    internal static void AssertEqual<T>(T expected, T actual, string? context = null)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
            throw new Exception(
                $"{(context is null ? "" : context + ": ")}Expected [{expected}] but got [{actual}]");
    }

    internal static void AssertNotNull(object? obj, string? context = null)
    {
        if (obj is null)
            throw new Exception($"{(context is null ? "" : context + ": ")}Expected non-null value");
    }

    internal static void AssertNull(object? obj, string? context = null)
    {
        if (obj is not null)
            throw new Exception(
                $"{(context is null ? "" : context + ": ")}Expected null but got [{obj}]");
    }

    internal static void AssertContains(string haystack, string needle, string? context = null)
    {
        if (!haystack.Contains(needle, StringComparison.Ordinal))
            throw new Exception(
                $"{(context is null ? "" : context + ": ")}Expected [{haystack}] to contain [{needle}]");
    }

    internal static void AssertNotContains(string haystack, string needle, string? context = null)
    {
        if (haystack.Contains(needle, StringComparison.Ordinal))
            throw new Exception(
                $"{(context is null ? "" : context + ": ")}Expected [{haystack}] NOT to contain [{needle}]");
    }
}
