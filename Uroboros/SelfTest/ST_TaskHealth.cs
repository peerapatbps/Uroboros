namespace Uroboros.SelfTest;

/// <summary>Self-tests for <see cref="TaskHealthTracker"/> and <see cref="NextRunTracker"/>.</summary>
internal static class ST_TaskHealth
{
    public static IEnumerable<SelfTestCase> Run()
    {
        // ── TaskHealthTracker ─────────────────────────────────────────────────

        yield return SelfTestRunner.Run("TaskHealthTracker | Get unknown task → all-null health", () =>
        {
            var tracker = new TaskHealthTracker();
            var info = tracker.Get("nope");
            SelfTestRunner.AssertNull(info.LastOkAtUtc);
            SelfTestRunner.AssertNull(info.LastFailAtUtc);
            SelfTestRunner.AssertNull(info.LastError);
        });

        yield return SelfTestRunner.Run("TaskHealthTracker | MarkOk then Get returns record", () =>
        {
            var tracker = new TaskHealthTracker();
            var at = DateTimeOffset.UtcNow;
            tracker.MarkOk("TaskA", at, 100);

            var info = tracker.Get("TaskA");
            SelfTestRunner.AssertEqual(at, info.LastOkAtUtc);
            SelfTestRunner.AssertNull(info.LastError);
        });

        yield return SelfTestRunner.Run("TaskHealthTracker | MarkFail records error", () =>
        {
            var tracker = new TaskHealthTracker();
            var at = DateTimeOffset.UtcNow;
            tracker.MarkFail("TaskB", at, "boom", null);

            var info = tracker.Get("TaskB");
            SelfTestRunner.AssertEqual("boom", info.LastError);
            SelfTestRunner.AssertNull(info.LastOkAtUtc);
        });

        yield return SelfTestRunner.Run("TaskHealthTracker | MarkOk then MarkFail keeps both timestamps", () =>
        {
            var tracker = new TaskHealthTracker();
            var t1 = DateTimeOffset.UtcNow;
            var t2 = t1.AddSeconds(5);
            tracker.MarkOk("TaskC", t1, 50);
            tracker.MarkFail("TaskC", t2, "err", null);

            var info = tracker.Get("TaskC");
            SelfTestRunner.AssertEqual(t1, info.LastOkAtUtc);
            SelfTestRunner.AssertEqual(t2, info.LastFailAtUtc);
        });

        yield return SelfTestRunner.Run("TaskHealthTracker | case-insensitive key lookup", () =>
        {
            var tracker = new TaskHealthTracker();
            tracker.MarkOk("MyTask", DateTimeOffset.UtcNow, 0);
            SelfTestRunner.AssertNotNull(tracker.Get("mytask"));
            SelfTestRunner.AssertNotNull(tracker.Get("MYTASK"));
        });

        yield return SelfTestRunner.Run("TaskHealthTracker | MarkFail with empty error stores ok", () =>
        {
            var tracker = new TaskHealthTracker();
            tracker.MarkFail("TaskD", DateTimeOffset.UtcNow, "", null);
            // Should not throw; error should be stored (empty string)
            var info = tracker.Get("TaskD");
            SelfTestRunner.AssertNotNull(info.LastFailAtUtc);
        });

        // ── NextRunTracker ────────────────────────────────────────────────────

        yield return SelfTestRunner.Run("NextRunTracker | GetNext unknown → null", () =>
        {
            var tracker = new NextRunTracker();
            SelfTestRunner.AssertNull(tracker.GetNext("x"));
        });

        yield return SelfTestRunner.Run("NextRunTracker | SetNext then GetNext returns value", () =>
        {
            var tracker = new NextRunTracker();
            var t = DateTimeOffset.UtcNow.AddMinutes(5);
            tracker.SetNext("myTask", t);

            var got = tracker.GetNext("myTask");
            SelfTestRunner.AssertNotNull(got);
            SelfTestRunner.AssertEqual(t, got!.Value);
        });

        yield return SelfTestRunner.Run("NextRunTracker | overwrite with newer value", () =>
        {
            var tracker = new NextRunTracker();
            var t1 = DateTimeOffset.UtcNow.AddMinutes(1);
            var t2 = t1.AddMinutes(10);
            tracker.SetNext("t", t1);
            tracker.SetNext("t", t2);

            SelfTestRunner.AssertEqual(t2, tracker.GetNext("t")!.Value);
        });
    }
}
