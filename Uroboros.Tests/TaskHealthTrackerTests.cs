using Xunit;

public sealed class TaskHealthTrackerTests
{
    [Fact]
    public void Get_Unknown_Name_Returns_All_Nulls()
    {
        var tracker = new TaskHealthTracker();
        var h = tracker.Get("nope");
        Assert.Null(h.LastOkAtUtc);
        Assert.Null(h.LastFailAtUtc);
        Assert.Null(h.LastError);
        Assert.Null(h.LastDurationMs);
    }

    [Fact]
    public void MarkOk_Sets_LastOkAtUtc_And_Duration()
    {
        var tracker = new TaskHealthTracker();
        var now = DateTimeOffset.UtcNow;
        tracker.MarkOk("T", now, 500);
        var h = tracker.Get("T");
        Assert.Equal(now, h.LastOkAtUtc);
        Assert.Equal(500L, h.LastDurationMs);
        Assert.Null(h.LastError);
    }

    [Fact]
    public void MarkFail_Sets_LastFailAtUtc_And_Error()
    {
        var tracker = new TaskHealthTracker();
        var now = DateTimeOffset.UtcNow;
        tracker.MarkFail("T", now, "boom", 100);
        var h = tracker.Get("T");
        Assert.Equal(now, h.LastFailAtUtc);
        Assert.Equal("boom", h.LastError);
        Assert.Equal(100L, h.LastDurationMs);
        Assert.Null(h.LastOkAtUtc);
    }

    [Fact]
    public void MarkOk_After_MarkFail_Preserves_LastFailAtUtc()
    {
        var tracker = new TaskHealthTracker();
        var failTime = DateTimeOffset.UtcNow.AddMinutes(-5);
        tracker.MarkFail("T", failTime, "err", null);
        tracker.MarkOk("T", DateTimeOffset.UtcNow, 200);
        var h = tracker.Get("T");
        Assert.Equal(failTime, h.LastFailAtUtc);
        Assert.NotNull(h.LastOkAtUtc);
    }

    [Fact]
    public void MarkFail_After_MarkOk_Preserves_LastOkAtUtc()
    {
        var tracker = new TaskHealthTracker();
        var okTime = DateTimeOffset.UtcNow.AddMinutes(-5);
        tracker.MarkOk("T", okTime, 100);
        tracker.MarkFail("T", DateTimeOffset.UtcNow, "err", null);
        var h = tracker.Get("T");
        Assert.Equal(okTime, h.LastOkAtUtc);
        Assert.NotNull(h.LastFailAtUtc);
    }

    [Fact]
    public void Get_Is_CaseInsensitive()
    {
        var tracker = new TaskHealthTracker();
        tracker.MarkOk("myTask", DateTimeOffset.UtcNow, 1);
        Assert.NotNull(tracker.Get("MYTASK").LastOkAtUtc);
        Assert.NotNull(tracker.Get("mytask").LastOkAtUtc);
    }
}

public sealed class NextRunTrackerTests
{
    [Fact]
    public void GetNext_Unknown_Returns_Null()
    {
        var t = new NextRunTracker();
        Assert.Null(t.GetNext("x"));
    }

    [Fact]
    public void SetNext_Then_GetNext_Returns_Value()
    {
        var t = new NextRunTracker();
        var dt = DateTimeOffset.UtcNow.AddMinutes(10);
        t.SetNext("task1", dt);
        Assert.Equal(dt, t.GetNext("task1"));
    }

    [Fact]
    public void SetNext_Overwrites_Previous()
    {
        var t = new NextRunTracker();
        var dt1 = DateTimeOffset.UtcNow;
        var dt2 = dt1.AddHours(1);
        t.SetNext("task1", dt1);
        t.SetNext("task1", dt2);
        Assert.Equal(dt2, t.GetNext("task1"));
    }
}
