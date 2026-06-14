using Xunit;

public sealed class TaskRegistryTests
{
    private static IEngineTask MakeStub(string name) =>
        new StubEngineTask(new TaskSpec(Name: name, Group: "TEST"));

    [Fact]
    public void TryCreate_Unregistered_ReturnsFalse()
    {
        var reg = new TaskRegistry();
        var found = reg.TryCreate("unknown", out var task);
        Assert.False(found);
    }

    [Fact]
    public void Register_Then_TryCreate_ReturnsTrue_And_NonNull()
    {
        var reg = new TaskRegistry();
        reg.Register("DEMO", () => MakeStub("DEMO"));
        var found = reg.TryCreate("DEMO", out var task);
        Assert.True(found);
        Assert.NotNull(task);
    }

    [Fact]
    public void TryCreate_Is_CaseInsensitive()
    {
        var reg = new TaskRegistry();
        reg.Register("demo", () => MakeStub("demo"));
        Assert.True(reg.TryCreate("DEMO", out _));
        Assert.True(reg.TryCreate("Demo", out _));
    }

    [Fact]
    public void ListNames_EmptyRegistry_ReturnsEmpty()
    {
        var reg = new TaskRegistry();
        Assert.Empty(reg.ListNames());
    }

    [Fact]
    public void ListNames_ReturnsSortedAlphabetically()
    {
        var reg = new TaskRegistry();
        reg.Register("C", () => MakeStub("C"));
        reg.Register("A", () => MakeStub("A"));
        reg.Register("B", () => MakeStub("B"));
        var names = reg.ListNames();
        Assert.Equal(new[] { "A", "B", "C" }, names);
    }

    [Fact]
    public void Register_SameName_Overwrites_Factory()
    {
        var reg = new TaskRegistry();
        reg.Register("X", () => MakeStub("first"));
        reg.Register("X", () => MakeStub("second"));
        reg.TryCreate("X", out var task);
        Assert.NotNull(task);
        Assert.Equal("second", task!.Spec.Name);
    }

    [Fact]
    public void Register_Each_Call_Creates_New_Instance()
    {
        var reg = new TaskRegistry();
        reg.Register("T", () => MakeStub("T"));
        reg.TryCreate("T", out var t1);
        reg.TryCreate("T", out var t2);
        Assert.NotSame(t1, t2);
    }

    private sealed class StubEngineTask : IEngineTask
    {
        public TaskSpec Spec { get; }
        public StubEngineTask(TaskSpec spec) => Spec = spec;
        public Task ExecuteAsync(EngineContext ctx, CancellationToken ct) => Task.CompletedTask;
    }
}
