using Xunit;

public sealed class StageGateTests
{
    [Fact]
    public void Default_IsPaused_False()
    {
        var g = new StageGate();
        Assert.False(g.IsPaused);
    }

    [Fact]
    public void StartPaused_True_IsPaused_True()
    {
        var g = new StageGate(startPaused: true);
        Assert.True(g.IsPaused);
    }

    [Fact]
    public void Pause_Sets_IsPaused_True()
    {
        var g = new StageGate();
        g.Pause();
        Assert.True(g.IsPaused);
    }

    [Fact]
    public void Pause_Then_Resume_Clears_IsPaused()
    {
        var g = new StageGate();
        g.Pause();
        g.Resume();
        Assert.False(g.IsPaused);
    }

    [Fact]
    public void Resume_When_NotPaused_Stays_False()
    {
        var g = new StageGate();
        g.Resume();
        Assert.False(g.IsPaused);
    }

    [Fact]
    public void Pause_Is_Idempotent()
    {
        var g = new StageGate();
        g.Pause();
        g.Pause();
        Assert.True(g.IsPaused);
    }
}
