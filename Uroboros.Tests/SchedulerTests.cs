using Xunit;

public sealed class SchedulerTests
{
    private static EngineContext MakeCtx() => new EngineContext
    {
        Log = new NullLogger(),
        Db = new NullDbService(),
        Clock = new SystemClock()
    };

    // ── TryEnqueue ────────────────────────────────────────────────

    [Fact]
    public void TryEnqueue_WhenGatePaused_ReturnsFalse()
    {
        var gate = new StageGate(startPaused: true);
        var sched = new Scheduler(MakeCtx(), 1, gate);
        var task = new StubTask("T");
        Assert.False(sched.TryEnqueue(task));
    }

    [Fact]
    public void TryEnqueue_WhenGateNotPaused_ReturnsTrue()
    {
        var gate = new StageGate();
        var sched = new Scheduler(MakeCtx(), 1, gate);
        var task = new StubTask("T");
        Assert.True(sched.TryEnqueue(task));
    }

    [Fact]
    public void TryEnqueueManual_WhenGatePaused_ReturnsTrue()
    {
        var gate = new StageGate(startPaused: true);
        var sched = new Scheduler(MakeCtx(), 1, gate);
        var task = new StubTask("T");
        // manual enqueue bypasses pause gate
        Assert.True(sched.TryEnqueueManual(task));
    }

    [Fact]
    public void TryEnqueue_WhenTaskDisabled_ReturnsFalse()
    {
        var gate = new StageGate();
        var sched = new Scheduler(MakeCtx(), 1, gate, isTaskEnabled: _ => false);
        var task = new StubTask("T");
        Assert.False(sched.TryEnqueue(task));
    }

    [Fact]
    public void TryEnqueue_WhenTaskEnabled_ReturnsTrue()
    {
        var gate = new StageGate();
        var sched = new Scheduler(MakeCtx(), 1, gate, isTaskEnabled: _ => true);
        var task = new StubTask("T");
        Assert.True(sched.TryEnqueue(task));
    }

    // ── Cancel ────────────────────────────────────────────────────

    [Fact]
    public void Cancel_UnknownId_ReturnsFalse()
    {
        var gate = new StageGate();
        var sched = new Scheduler(MakeCtx(), 1, gate);
        Assert.False(sched.Cancel(Guid.NewGuid()));
    }

    [Fact]
    public void CancelAll_WhenNothingRunning_ReturnsFalse()
    {
        var gate = new StageGate();
        var sched = new Scheduler(MakeCtx(), 1, gate);
        Assert.False(sched.CancelAll());
    }

    // ── SnapshotRunning ───────────────────────────────────────────

    [Fact]
    public void SnapshotRunning_Initially_Empty()
    {
        var gate = new StageGate();
        var sched = new Scheduler(MakeCtx(), 1, gate);
        Assert.Empty(sched.SnapshotRunning());
    }

    // ── DropIfRunning policy ──────────────────────────────────────

    [Fact]
    public void TryEnqueue_DropIfRunning_WhenNotRunning_Enqueues()
    {
        var gate = new StageGate();
        var sched = new Scheduler(MakeCtx(), 1, gate);
        var task = new StubTask("T", RunPolicy.DropIfRunning);
        Assert.True(sched.TryEnqueue(task));
    }

    // ── SkipIfRunning policy ──────────────────────────────────────

    [Fact]
    public void TryEnqueue_SkipIfRunning_WhenNotRunning_Enqueues()
    {
        var gate = new StageGate();
        var sched = new Scheduler(MakeCtx(), 1, gate);
        var task = new StubTask("T", RunPolicy.SkipIfRunning);
        Assert.True(sched.TryEnqueue(task));
    }

    // ── Helpers ───────────────────────────────────────────────────

    private sealed class StubTask : IEngineTask
    {
        public TaskSpec Spec { get; }
        public StubTask(string name, RunPolicy policy = RunPolicy.Queue)
            => Spec = new TaskSpec(Name: name, Group: "TEST", Policy: policy);
        public Task ExecuteAsync(EngineContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class NullLogger : ILogger
    {
        public void Info(string msg) { }
        public void Warn(string msg) { }
        public void Error(Exception ex, string msg) { }
    }
}
